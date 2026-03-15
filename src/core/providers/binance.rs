use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{debug, warn};

const BINANCE_BASE_URL: &str = "https://api.binance.com";

/// Supported Binance trading pairs.
const SUPPORTED_SYMBOLS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT",
];

/// Cache TTL — avoid spamming Binance when the background poller runs frequently.
const CACHE_TTL: Duration = Duration::from_secs(2);

#[derive(Debug, Deserialize)]
struct TickerPrice {
    symbol: String,
    #[serde(deserialize_with = "deserialize_string_f64")]
    price: f64,
}

fn deserialize_string_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: &str = serde::Deserialize::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone)]
struct CachedPrices {
    prices: HashMap<String, f64>,
    fetched_at: Instant,
}

/// Simple REST client for Binance spot prices with a short TTL cache.
pub struct BinanceClient {
    http: reqwest::Client,
    cache: Arc<RwLock<Option<CachedPrices>>>,
}

impl BinanceClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("failed to build reqwest client"),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Get the spot price for a single symbol (e.g. "BTCUSDT").
    /// Returns the cached value if fresh, otherwise fetches all prices and caches them.
    pub async fn get_spot_price(&self, symbol: &str) -> Result<f64> {
        let prices = self.get_all_spot_prices().await?;
        prices
            .get(symbol)
            .copied()
            .with_context(|| format!("No price found for {symbol}"))
    }

    /// Get spot prices for all 6 supported assets.
    /// Uses a TTL cache to avoid excessive API calls.
    pub async fn get_all_spot_prices(&self) -> Result<HashMap<String, f64>> {
        // Check cache
        {
            let cache = self.cache.read().await;
            if let Some(ref cached) = *cache
                && cached.fetched_at.elapsed() < CACHE_TTL
            {
                debug!("Returning cached Binance prices");
                return Ok(cached.prices.clone());
            }
        }

        // Cache miss or stale — fetch fresh prices
        let prices = self.fetch_all_prices().await?;

        // Update cache
        {
            let mut cache = self.cache.write().await;
            *cache = Some(CachedPrices {
                prices: prices.clone(),
                fetched_at: Instant::now(),
            });
        }

        Ok(prices)
    }

    /// Get the close price for a symbol at a specific timestamp using Binance klines.
    /// Fetches the 1-minute kline that contains the given timestamp.
    /// Returns (open_price, close_price) of that candle.
    pub async fn get_kline_at(
        &self,
        symbol: &str,
        timestamp_ms: i64,
    ) -> Result<(f64, f64)> {
        let url = format!(
            "{BINANCE_BASE_URL}/api/v3/klines?symbol={symbol}&interval=1m&startTime={timestamp_ms}&limit=1"
        );

        let resp: Vec<serde_json::Value> = self
            .http
            .get(&url)
            .send()
            .await
            .context("Binance kline request failed")?
            .error_for_status()
            .context("Binance kline returned error")?
            .json()
            .await
            .context("Failed to parse Binance kline response")?;

        let kline = resp
            .first()
            .context("No kline data returned for timestamp")?;
        let arr = kline
            .as_array()
            .context("Kline entry is not an array")?;

        // Kline format: [open_time, open, high, low, close, volume, ...]
        let open: f64 = arr
            .get(1)
            .and_then(|v| v.as_str())
            .context("Missing open price")?
            .parse()
            .context("Invalid open price")?;
        let close: f64 = arr
            .get(4)
            .and_then(|v| v.as_str())
            .context("Missing close price")?
            .parse()
            .context("Invalid close price")?;

        Ok((open, close))
    }

    /// Fetch 1-minute klines for a time range in bulk.
    /// Binance allows up to 1000 klines per request.
    /// Returns a sorted Vec of (open_time_ms, open, close) tuples.
    pub async fn get_klines_range(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<(i64, f64, f64)>> {
        let mut all_klines = Vec::new();
        let mut cursor = start_ms;
        let limit = 1000;

        while cursor < end_ms {
            let url = format!(
                "{BINANCE_BASE_URL}/api/v3/klines?symbol={symbol}&interval=1m&startTime={cursor}&endTime={end_ms}&limit={limit}"
            );

            let resp: Vec<serde_json::Value> = self
                .http
                .get(&url)
                .send()
                .await
                .context("Binance klines range request failed")?
                .error_for_status()
                .context("Binance klines range returned error")?
                .json()
                .await
                .context("Failed to parse Binance klines range response")?;

            if resp.is_empty() {
                break;
            }

            for kline in &resp {
                let arr = kline.as_array().context("Kline not an array")?;
                let open_time = arr.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let open: f64 = arr
                    .get(1)
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0.0);
                let close: f64 = arr
                    .get(4)
                    .and_then(|v| v.as_str())
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0.0);
                all_klines.push((open_time, open, close));
            }

            // Move cursor past the last kline we got
            if let Some(last) = resp.last() {
                let last_open = last
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_i64())
                    .unwrap_or(end_ms);
                cursor = last_open + 60_000; // next minute
            } else {
                break;
            }

            // Rate limit if fetching many chunks
            if all_klines.len() >= limit {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        debug!(
            symbol,
            klines = all_klines.len(),
            "Fetched bulk Binance klines"
        );
        Ok(all_klines)
    }

    /// Fetch all ticker prices from Binance and filter to supported symbols.
    async fn fetch_all_prices(&self) -> Result<HashMap<String, f64>> {
        let url = format!("{BINANCE_BASE_URL}/api/v3/ticker/price");

        let tickers: Vec<TickerPrice> = self
            .http
            .get(&url)
            .send()
            .await
            .context("Binance ticker/price request failed")?
            .error_for_status()
            .context("Binance returned error status")?
            .json()
            .await
            .context("Failed to parse Binance ticker response")?;

        let prices: HashMap<String, f64> = tickers
            .into_iter()
            .filter(|t| SUPPORTED_SYMBOLS.contains(&t.symbol.as_str()))
            .map(|t| (t.symbol, t.price))
            .collect();

        if prices.len() < SUPPORTED_SYMBOLS.len() {
            let missing: Vec<_> = SUPPORTED_SYMBOLS
                .iter()
                .filter(|s| !prices.contains_key(**s))
                .collect();
            warn!(?missing, "Some Binance symbols missing from response");
        }

        debug!(count = prices.len(), "Fetched Binance spot prices");
        Ok(prices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_symbols() {
        assert_eq!(SUPPORTED_SYMBOLS.len(), 6);
        assert!(SUPPORTED_SYMBOLS.contains(&"BTCUSDT"));
        assert!(SUPPORTED_SYMBOLS.contains(&"DOGEUSDT"));
    }

    #[test]
    fn test_deserialize_ticker() {
        let json = r#"{"symbol":"BTCUSDT","price":"84123.45"}"#;
        let ticker: TickerPrice = serde_json::from_str(json).unwrap();
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert!((ticker.price - 84123.45).abs() < 0.01);
    }
}
