// Extracted from market-scout src/polymarket/ws.rs
// Changes: native_tls → rustls, added whale filtering (min_size_usd).

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Message};
use tracing::{debug, error, info};

use crate::core::infrastructure::recovery::ReconnectBackoff;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// A trade event received from the Polymarket WebSocket.
#[derive(Debug, Clone)]
pub struct PolymarketTradeEvent {
    pub asset_id: String,
    pub price: f64,
    pub size: f64,
    /// USD value of the trade (size * price).
    pub size_usd: f64,
    pub side: String,
    pub timestamp_ms: u64,
}

/// WebSocket client for streaming Polymarket trade events.
///
/// Subscribes to the `market` channel for a set of token IDs (received via
/// `watch` channel). Reconnects automatically when token IDs change or the
/// connection drops.
///
/// Supports whale filtering: only trades >= `min_size_usd` are broadcast.
pub struct PolymarketTradesWs {
    price_tx: broadcast::Sender<PolymarketTradeEvent>,
    token_ids_rx: watch::Receiver<Vec<String>>,
    min_size_usd: f64,
}

impl PolymarketTradesWs {
    /// Create a new trades WebSocket client.
    /// `min_size_usd` filters out small trades (0.0 = broadcast everything).
    pub fn new(
        token_ids_rx: watch::Receiver<Vec<String>>,
        min_size_usd: f64,
    ) -> (Self, broadcast::Receiver<PolymarketTradeEvent>) {
        let (tx, rx) = broadcast::channel(10000);
        (
            Self {
                price_tx: tx,
                token_ids_rx,
                min_size_usd,
            },
            rx,
        )
    }

    /// Subscribe to trade events (additional receivers).
    pub fn subscribe(&self) -> broadcast::Receiver<PolymarketTradeEvent> {
        self.price_tx.subscribe()
    }

    /// Run the WebSocket connection loop (infinite reconnect with exponential backoff).
    pub async fn connect(&mut self) -> Result<()> {
        let mut backoff = ReconnectBackoff::new(
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(60),
        );
        loop {
            match self.connect_once().await {
                Ok(_) => {
                    info!("Polymarket trades WS closed, reconnecting...");
                    backoff.reset();
                }
                Err(e) => {
                    let delay = backoff.current();
                    error!(error = %e, delay_secs = delay.as_secs(), "Polymarket trades WS error, reconnecting...");
                    backoff.wait().await;
                }
            }
        }
    }

    async fn connect_once(&mut self) -> Result<()> {
        let token_ids = self.token_ids_rx.borrow().clone();
        if token_ids.is_empty() {
            debug!("No token IDs for trades WS, waiting for update...");
            self.token_ids_rx.changed().await.ok();
            return Ok(());
        }

        info!(tokens = token_ids.len(), "Connecting to Polymarket trades WS");

        // Use default rustls connector from tokio-tungstenite's
        // rustls-tls-webpki-roots feature (passing None).
        let (ws_stream, response) = connect_async_tls_with_config(
            WS_URL,
            None,
            false,
            None, // uses default rustls connector from feature flag
        )
        .await
        .context("Failed to connect to Polymarket trades WS")?;

        info!(
            status = response.status().as_u16(),
            "Connected to Polymarket trades WS"
        );

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to token IDs
        let sub = serde_json::json!({
            "type": "market",
            "assets_ids": token_ids,
        });
        write
            .send(Message::Text(sub.to_string()))
            .await
            .context("Failed to send subscribe message")?;

        info!(
            tokens = token_ids.len(),
            "Subscribed to Polymarket trade events"
        );

        let price_tx = &self.price_tx;
        let min_size_usd = self.min_size_usd;
        let token_ids_rx = &mut self.token_ids_rx;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            Self::handle_message(price_tx, &text, min_size_usd);
                        }
                        Some(Ok(Message::Ping(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            info!(?frame, "Polymarket trades WS close frame");
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            error!(error = %e, "Polymarket trades WS read error");
                            return Err(e.into());
                        }
                        None => break,
                    }
                }
                _ = token_ids_rx.changed() => {
                    let new_count = token_ids_rx.borrow().len();
                    info!(tokens = new_count, "Token IDs updated, reconnecting trades WS");
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_message(
        tx: &broadcast::Sender<PolymarketTradeEvent>,
        text: &str,
        min_size_usd: f64,
    ) {
        // Messages may be a single JSON object or an array
        let values: Vec<serde_json::Value> = if text.starts_with('[') {
            match serde_json::from_str(text) {
                Ok(v) => v,
                Err(e) => {
                    debug!(error = %e, "Failed to parse trades WS array");
                    return;
                }
            }
        } else {
            match serde_json::from_str(text) {
                Ok(v) => vec![v],
                Err(e) => {
                    debug!(error = %e, "Failed to parse trades WS message");
                    return;
                }
            }
        };

        for v in &values {
            // Only process trade-related events
            if let Some(et) = v.get("event_type").and_then(|e| e.as_str())
                && et != "last_trade_price" && et != "trade"
            {
                continue;
            }

            let asset_id = match v.get("asset_id").and_then(|a| a.as_str()) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let price = match parse_f64(v, "price") {
                Some(p) => p,
                None => continue,
            };

            let size = parse_f64(v, "size").unwrap_or(0.0);
            let size_usd = size * price;

            // Whale filter: skip trades below the minimum USD threshold
            if size_usd < min_size_usd {
                continue;
            }

            let side = v
                .get("side")
                .and_then(|s| s.as_str())
                .unwrap_or("UNKNOWN")
                .to_string();

            let timestamp_ms = parse_timestamp_ms(v);

            let _ = tx.send(PolymarketTradeEvent {
                asset_id,
                price,
                size,
                size_usd,
                side,
                timestamp_ms,
            });
        }
    }
}

/// Parse a field as f64 from either a number or string value.
fn parse_f64(v: &serde_json::Value, key: &str) -> Option<f64> {
    let val = v.get(key)?;
    if let Some(f) = val.as_f64() {
        return Some(f);
    }
    val.as_str().and_then(|s| s.parse().ok())
}

/// Extract a millisecond timestamp from the message, falling back to wall clock.
fn parse_timestamp_ms(v: &serde_json::Value) -> u64 {
    for key in &["timestamp", "timestamp_ms", "time"] {
        if let Some(val) = v.get(key) {
            let n = if let Some(n) = val.as_u64() {
                n
            } else if let Some(s) = val.as_str() {
                match s.parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => continue,
                }
            } else {
                continue;
            };
            // Convert seconds to millis if needed
            return if n < 1_000_000_000_000 { n * 1000 } else { n };
        }
    }
    // Fallback: current time
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_f64_number() {
        let v: serde_json::Value = serde_json::json!({"price": 0.65});
        assert!((parse_f64(&v, "price").unwrap() - 0.65).abs() < 1e-9);
    }

    #[test]
    fn test_parse_f64_string() {
        let v: serde_json::Value = serde_json::json!({"price": "0.42"});
        assert!((parse_f64(&v, "price").unwrap() - 0.42).abs() < 1e-9);
    }

    #[test]
    fn test_parse_f64_missing() {
        let v: serde_json::Value = serde_json::json!({"other": 1});
        assert!(parse_f64(&v, "price").is_none());
    }

    #[test]
    fn test_parse_timestamp_seconds_to_millis() {
        let v: serde_json::Value = serde_json::json!({"timestamp": 1710000000});
        assert_eq!(parse_timestamp_ms(&v), 1710000000000);
    }

    #[test]
    fn test_parse_timestamp_already_millis() {
        let v: serde_json::Value = serde_json::json!({"timestamp": 1710000000000u64});
        assert_eq!(parse_timestamp_ms(&v), 1710000000000);
    }

    #[test]
    fn test_whale_filter() {
        // Simulate handle_message with min_size_usd filtering
        let (tx, mut rx) = broadcast::channel(100);

        // Small trade — should be filtered at $5000 threshold
        let small_trade = serde_json::json!({
            "event_type": "trade",
            "asset_id": "0xabc",
            "price": "0.50",
            "size": "100",
            "side": "BUY",
            "timestamp": 1710000000
        });
        PolymarketTradesWs::handle_message(&tx, &small_trade.to_string(), 5000.0);
        assert!(rx.try_recv().is_err()); // filtered out

        // Large trade — should pass
        let big_trade = serde_json::json!({
            "event_type": "trade",
            "asset_id": "0xabc",
            "price": "0.50",
            "size": "20000",
            "side": "BUY",
            "timestamp": 1710000000
        });
        PolymarketTradesWs::handle_message(&tx, &big_trade.to_string(), 5000.0);
        let event = rx.try_recv().unwrap();
        assert_eq!(event.asset_id, "0xabc");
        assert!((event.size_usd - 10000.0).abs() < 0.01);
    }
}
