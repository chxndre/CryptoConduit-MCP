// Extracted from market-scout src/polymarket/client.rs
// Polymarket CLOB API client with circuit breaker, dead-token tracking, retry logic.
// CRITICAL: Do NOT modify the retry logic or dead-token handling — battle-tested.

use crate::core::infrastructure::recovery::CircuitBreaker;
use crate::core::types::OrderBook;
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

const DEAD_TOKEN_THRESHOLD: u32 = 3;
const DEAD_TOKEN_RETRY_SECS: u64 = 3600;

#[derive(Debug, Clone)]
struct DeadTokenState {
    consecutive_failures: u32,
    disabled_at: Option<Instant>,
}

/// Client for Polymarket CLOB API.
/// Thread-safe: CircuitBreaker is internally Arc<Mutex<...>> and Clone.
/// Dead tokens tracked via Arc<RwLock<HashMap<...>>>.
pub struct PolymarketClient {
    http_client: Client,
    base_url: String,
    circuit_breaker: CircuitBreaker,
    dead_tokens: Arc<RwLock<HashMap<String, DeadTokenState>>>,
}

impl PolymarketClient {
    pub fn new(base_url: Option<String>, timeout_secs: Option<u64>) -> Self {
        let base_url = base_url.unwrap_or_else(|| "https://clob.polymarket.com".to_string());
        let timeout = Duration::from_secs(timeout_secs.unwrap_or(2));

        let http_client = Client::builder()
            .timeout(timeout)
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .expect("Failed to create HTTP client");

        let circuit_breaker = CircuitBreaker::with_config(
            3,
            2,
            Duration::from_secs(30),
        );

        info!(
            base_url = %base_url,
            timeout_secs = ?timeout.as_secs(),
            "Polymarket client initialized"
        );

        Self {
            http_client,
            base_url,
            circuit_breaker,
            dead_tokens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get order book for a token ID. Returns (OrderBook, api_latency_ms).
    pub async fn get_order_book_timed(&self, token_id: &str) -> Result<(OrderBook, f64)> {
        let start = Instant::now();
        let book = self.get_order_book(token_id).await?;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
        Ok((book, latency_ms))
    }

    pub async fn get_order_book(&self, token_id: &str) -> Result<OrderBook> {
        // Check if token is disabled
        {
            let dead = self.dead_tokens.read().await;
            if let Some(state) = dead.get(token_id)
                && state.consecutive_failures >= DEAD_TOKEN_THRESHOLD
                && let Some(disabled_at) = state.disabled_at
            {
                if disabled_at.elapsed().as_secs() < DEAD_TOKEN_RETRY_SECS {
                    anyhow::bail!("Token disabled (dead/expired): {}", token_id);
                }
                debug!(token_id = %token_id, "Retrying disabled token after cooldown");
            }
        }

        // Check circuit breaker (no extra lock — CB is internally thread-safe)
        if !self.circuit_breaker.allow_request() {
            anyhow::bail!("Circuit breaker is open - Polymarket API unavailable");
        }

        let url = format!("{}/book?token_id={}", self.base_url, token_id);
        debug!(token_id = %token_id, "Fetching order book");

        let mut last_error = None;
        for attempt in 1..=3u32 {
            match self.http_client.get(&url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<OrderBook>().await {
                            Ok(book) => {
                                self.circuit_breaker.record_success();

                                // Clear dead token state on success with non-empty book
                                if !book.bids.is_empty() || !book.asks.is_empty() {
                                    let mut dead = self.dead_tokens.write().await;
                                    if dead.remove(token_id).is_some() {
                                        info!(token_id = %token_id, "Token re-enabled");
                                    }
                                } else {
                                    // Empty book — track for auto-disable
                                    let mut dead = self.dead_tokens.write().await;
                                    let state = dead.entry(token_id.to_string()).or_insert(DeadTokenState {
                                        consecutive_failures: 0,
                                        disabled_at: None,
                                    });
                                    state.consecutive_failures += 1;
                                    if state.consecutive_failures >= DEAD_TOKEN_THRESHOLD && state.disabled_at.is_none() {
                                        state.disabled_at = Some(Instant::now());
                                        warn!(token_id = %token_id, "Token disabled due to empty order book");
                                    }
                                }

                                return Ok(book);
                            }
                            Err(e) => {
                                last_error = Some(anyhow::anyhow!("Failed to parse JSON: {}", e));
                            }
                        }
                    } else {
                        let status = response.status();
                        let error_text = response.text().await.unwrap_or_default();
                        last_error = Some(anyhow::anyhow!("HTTP error {}: {}", status, error_text));

                        if status.is_client_error() {
                            if status.as_u16() == 404 {
                                let mut dead = self.dead_tokens.write().await;
                                let state = dead.entry(token_id.to_string()).or_insert(DeadTokenState {
                                    consecutive_failures: 0,
                                    disabled_at: None,
                                });
                                state.consecutive_failures += 1;
                                if state.consecutive_failures >= DEAD_TOKEN_THRESHOLD && state.disabled_at.is_none() {
                                    state.disabled_at = Some(Instant::now());
                                    warn!(token_id = %token_id, "Token disabled after 404s");
                                }
                            }
                            break;
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Request failed: {}", e));
                }
            }

            if attempt < 3 {
                let delay_ms = 100 * (2_u64.pow(attempt - 1));
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }

        self.circuit_breaker.record_failure();

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Unknown error")))
    }

    pub async fn is_operational(&self) -> bool {
        self.circuit_breaker.allow_request()
    }

    pub async fn dead_token_count(&self) -> usize {
        let dead = self.dead_tokens.read().await;
        dead.values()
            .filter(|s| s.consecutive_failures >= DEAD_TOKEN_THRESHOLD && s.disabled_at.is_some())
            .count()
    }

    /// Fetch historical midpoint prices from the CLOB prices-history endpoint.
    /// Returns a vec of (timestamp_ms, midpoint_price) sorted by time.
    pub async fn get_prices_history(&self, token_id: &str) -> Result<Vec<(i64, f64)>> {
        let url = format!(
            "{}/prices-history?market={}&interval=1d&fidelity=1",
            self.base_url, token_id
        );
        debug!(token_id = %token_id, "Fetching prices history");

        let response = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Prices history request failed: {}", e))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Prices history HTTP {}: {}", status, body);
        }

        let raw: serde_json::Value = response
            .json()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to parse prices history JSON: {}", e))?;

        let history = raw.get("history")
            .and_then(|v| v.as_array())
            .or_else(|| raw.as_array());

        let Some(arr) = history else {
            anyhow::bail!("Unexpected prices-history response shape: {}", raw);
        };

        let mut points = Vec::with_capacity(arr.len());
        for item in arr {
            let ts = item.get("t")
                .or_else(|| item.get("time"))
                .or_else(|| item.get("timestamp"))
                .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));

            let price = item.get("p")
                .or_else(|| item.get("price"))
                .and_then(|v| {
                    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                });

            if let (Some(t), Some(p)) = (ts, price) {
                // Normalize to milliseconds: if timestamp looks like seconds (< 1e12), convert
                let t_ms = if t < 1_000_000_000_000 { t * 1000 } else { t };
                points.push((t_ms, p));
            }
        }

        points.sort_by_key(|(t, _)| *t);
        Ok(points)
    }

    /// Convenience: fetch prices history filtered to a time range [start_ms, end_ms].
    pub async fn get_prices_history_for_range(
        &self,
        token_id: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<(i64, f64)>> {
        let all = self.get_prices_history(token_id).await?;
        Ok(all
            .into_iter()
            .filter(|(t, _)| *t >= start_ms && *t <= end_ms)
            .collect())
    }
}
