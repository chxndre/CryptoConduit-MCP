// Extracted from market-scout src/price_feed/chainlink_rtds.rs
// Changes: native_tls → rustls, extended to all 6 assets.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::core::infrastructure::recovery::ReconnectBackoff;

const RTDS_URL: &str = "wss://ws-live-data.polymarket.com";
const PING_INTERVAL_SECS: u64 = 10;

/// All 6 supported Chainlink price feed symbols.
pub const ALL_CHAINLINK_SYMBOLS: &[&str] = &[
    "btc/usd", "eth/usd", "sol/usd", "xrp/usd", "doge/usd", "bnb/usd",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainlinkPriceUpdate {
    pub symbol: String,
    pub price: f64,
    pub timestamp_ms: u64,
    #[serde(skip, default = "Instant::now")]
    pub received_at: Instant,
}

impl ChainlinkPriceUpdate {
    /// Map Chainlink symbol (e.g. "btc/usd") to Binance symbol (e.g. "BTCUSDT").
    pub fn to_binance_symbol(&self) -> Option<String> {
        chainlink_to_binance(&self.symbol)
    }
}

/// Map a Chainlink symbol like "btc/usd" to a Binance pair like "BTCUSDT".
pub fn chainlink_to_binance(chainlink_sym: &str) -> Option<String> {
    let asset = chainlink_sym.split('/').next()?;
    Some(format!("{}USDT", asset.to_uppercase()))
}

pub struct ChainlinkRtds {
    symbols: Vec<String>,
    price_tx: broadcast::Sender<ChainlinkPriceUpdate>,
}

impl ChainlinkRtds {
    pub fn new(symbols: Vec<String>) -> (Self, broadcast::Receiver<ChainlinkPriceUpdate>) {
        let (tx, rx) = broadcast::channel(1000);

        info!(
            symbols = ?symbols,
            "Creating Chainlink RTDS client"
        );

        (Self { symbols, price_tx: tx }, rx)
    }

    /// Create a client for all 6 supported assets.
    pub fn new_all() -> (Self, broadcast::Receiver<ChainlinkPriceUpdate>) {
        let symbols = ALL_CHAINLINK_SYMBOLS
            .iter()
            .map(|s| s.to_string())
            .collect();
        Self::new(symbols)
    }

    /// Subscribe to price updates (additional receivers).
    pub fn subscribe(&self) -> broadcast::Receiver<ChainlinkPriceUpdate> {
        self.price_tx.subscribe()
    }

    /// Run the WebSocket connection loop (infinite reconnect with exponential backoff).
    pub async fn connect(&self) -> Result<()> {
        let mut backoff = ReconnectBackoff::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
        );
        loop {
            match self.connect_once().await {
                Ok(_) => {
                    warn!("Chainlink RTDS connection closed, reconnecting...");
                    backoff.reset();
                }
                Err(e) => {
                    let delay = backoff.current();
                    error!(error = %e, delay_secs = delay.as_secs(), "Chainlink RTDS error, reconnecting...");
                    backoff.wait().await;
                }
            }
        }
    }

    async fn connect_once(&self) -> Result<()> {
        info!(url = RTDS_URL, "Connecting to Chainlink RTDS");

        // Use rustls via the built-in connector from tokio-tungstenite's
        // rustls-tls-webpki-roots feature. Passing None for the connector
        // lets tokio-tungstenite pick the default TLS backend (rustls when
        // the feature is enabled).
        let (ws_stream, response) = connect_async_tls_with_config(
            RTDS_URL,
            None,
            false,
            None, // uses default rustls connector from feature flag
        )
        .await
        .context("Failed to connect to Chainlink RTDS")?;

        info!(
            status = response.status().as_u16(),
            "Connected to Chainlink RTDS"
        );

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to price feeds — one filter per symbol
        let filters: Vec<serde_json::Value> = self
            .symbols
            .iter()
            .map(|sym| {
                serde_json::json!({
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "filters": serde_json::json!({"symbol": sym}).to_string()
                })
            })
            .collect();

        let subscribe_msg = serde_json::json!({
            "action": "subscribe",
            "subscriptions": filters
        });

        write
            .send(Message::Text(subscribe_msg.to_string()))
            .await
            .context("Failed to send subscribe message")?;

        info!(symbols = ?self.symbols, "Subscribed to Chainlink price feeds");

        let mut ping_interval =
            tokio::time::interval(tokio::time::Duration::from_secs(PING_INTERVAL_SECS));
        ping_interval.tick().await; // consume first immediate tick

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_message(&text);
                        }
                        Some(Ok(Message::Ping(data))) => {
                            debug!("Received RTDS ping");
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            info!(?frame, "RTDS close frame");
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            error!(error = %e, "RTDS WebSocket error");
                            return Err(e.into());
                        }
                        None => break,
                    }
                }
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!(error = %e, "Failed to send RTDS ping");
                        return Err(e.into());
                    }
                    debug!("Sent RTDS keepalive ping");
                }
            }
        }

        Ok(())
    }

    fn handle_message(&self, text: &str) {
        let msg: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                debug!(error = %e, "Failed to parse RTDS message");
                return;
            }
        };

        // Skip non-update messages (subscription confirmations, etc.)
        let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if msg_type != "update" {
            debug!(msg_type = msg_type, "Non-update RTDS message");
            return;
        }

        let payload = match msg.get("payload") {
            Some(p) => p,
            None => return,
        };

        let symbol = match payload.get("symbol").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => return,
        };

        // Parse price — can be a number or a string
        let price = match payload.get("value") {
            Some(v) => match v.as_f64() {
                Some(p) => p,
                None => match v.as_str().and_then(|s| s.parse::<f64>().ok()) {
                    Some(p) => p,
                    None => return,
                },
            },
            None => return,
        };

        let timestamp_ms = payload
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            });

        let update = ChainlinkPriceUpdate {
            symbol,
            price,
            timestamp_ms,
            received_at: Instant::now(),
        };

        let _ = self.price_tx.send(update);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chainlink_to_binance() {
        assert_eq!(chainlink_to_binance("btc/usd"), Some("BTCUSDT".into()));
        assert_eq!(chainlink_to_binance("eth/usd"), Some("ETHUSDT".into()));
        assert_eq!(chainlink_to_binance("doge/usd"), Some("DOGEUSDT".into()));
        assert_eq!(chainlink_to_binance("bnb/usd"), Some("BNBUSDT".into()));
    }

    #[test]
    fn test_all_symbols_count() {
        assert_eq!(ALL_CHAINLINK_SYMBOLS.len(), 6);
    }

    #[test]
    fn test_price_update_to_binance() {
        let update = ChainlinkPriceUpdate {
            symbol: "sol/usd".into(),
            price: 135.42,
            timestamp_ms: 1710000000000,
            received_at: Instant::now(),
        };
        assert_eq!(update.to_binance_symbol(), Some("SOLUSDT".into()));
    }
}
