use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::core::types::{MarketConfig, OrderBook, ShortTermMarket};

/// Maximum number of alerts to keep in the ring buffer.
const MAX_ALERTS: usize = 100;
/// Maximum number of whale trades to keep (roughly 1 hour at typical volume).
const MAX_WHALE_TRADES: usize = 500;
/// Maximum history entries (~4 hours at 20 snapshots/min).
const MAX_HISTORY: usize = 4800;

/// Timestamped order book snapshot stored in the cache.
#[derive(Debug, Clone)]
pub struct TimestampedOrderBook {
    pub book: OrderBook,
    pub fetched_at: Instant,
    pub fetched_at_utc: DateTime<Utc>,
}

impl TimestampedOrderBook {
    pub fn new(book: OrderBook) -> Self {
        Self {
            book,
            fetched_at: Instant::now(),
            fetched_at_utc: Utc::now(),
        }
    }

    pub fn age_secs(&self) -> f64 {
        self.fetched_at.elapsed().as_secs_f64()
    }
}

/// Timestamped spot price.
#[derive(Debug, Clone)]
pub struct TimestampedPrice {
    pub price: f64,
    pub fetched_at: Instant,
    pub fetched_at_utc: DateTime<Utc>,
}

impl TimestampedPrice {
    pub fn new(price: f64) -> Self {
        Self {
            price,
            fetched_at: Instant::now(),
            fetched_at_utc: Utc::now(),
        }
    }

    pub fn age_secs(&self) -> f64 {
        self.fetched_at.elapsed().as_secs_f64()
    }
}

/// A snapshot of a single market at a point in time, for the history ring buffer.
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub token_id: String,
    /// Stable key for history continuity across token rotations.
    /// Format: "{asset}_{interval}m_{side}" for short-term (e.g., "btc_5m_up"),
    /// or token_id for daily/monthly (stable IDs).
    pub market_key: String,
    pub timestamp: DateTime<Utc>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread_pct: Option<f64>,
    pub bid_depth_1pct: f64,
    pub ask_depth_1pct: f64,
}

/// Alert types emitted by the background monitor.
#[derive(Debug, Clone)]
pub enum AlertKind {
    SpreadNarrowing {
        token_id: String,
        market_name: String,
        old_spread_pct: f64,
        new_spread_pct: f64,
    },
    DepthSpike {
        token_id: String,
        market_name: String,
        side: String,
        old_depth: f64,
        new_depth: f64,
        multiplier: f64,
    },
    WhaleTrade {
        token_id: String,
        market_name: String,
        side: String,
        size_usd: f64,
        price: f64,
    },
    WindowApproaching {
        asset: String,
        interval: u32,
        window_start_ts: i64,
        seconds_until: i64,
    },
}

impl std::fmt::Display for AlertKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertKind::SpreadNarrowing {
                market_name,
                new_spread_pct,
                ..
            } => write!(
                f,
                "Spread narrowed to {:.1}% on {}",
                new_spread_pct * 100.0,
                market_name
            ),
            AlertKind::DepthSpike {
                market_name,
                side,
                new_depth,
                multiplier,
                ..
            } => write!(
                f,
                "Depth spike {:.1}x on {} {} (${:.0})",
                multiplier, market_name, side, new_depth
            ),
            AlertKind::WhaleTrade {
                market_name,
                side,
                size_usd,
                ..
            } => write!(
                f,
                "Whale {} ${:.0} on {}",
                side, size_usd, market_name
            ),
            AlertKind::WindowApproaching {
                asset,
                interval,
                seconds_until,
                ..
            } => write!(
                f,
                "{} {}m window starting in {}s",
                asset, interval, seconds_until
            ),
        }
    }
}

/// A timestamped alert.
#[derive(Debug, Clone)]
pub struct Alert {
    pub timestamp: DateTime<Utc>,
    pub kind: AlertKind,
}

impl Alert {
    pub fn new(kind: AlertKind) -> Self {
        Self {
            timestamp: Utc::now(),
            kind,
        }
    }
}

/// A whale trade record for the activity feed.
#[derive(Debug, Clone)]
pub struct WhaleTrade {
    pub token_id: String,
    pub market_name: String,
    pub side: String,
    pub price: f64,
    pub size_usd: f64,
    pub timestamp: DateTime<Utc>,
}

/// Central shared state for the background monitor.
/// All fields are behind Arc<RwLock<...>> for concurrent access from
/// background tasks and MCP tool handlers.
pub struct SharedState {
    inner: Arc<RwLock<ServerState>>,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ServerState::default())),
        }
    }

    pub fn inner(&self) -> &Arc<RwLock<ServerState>> {
        &self.inner
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, ServerState> {
        self.inner.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, ServerState> {
        self.inner.write().await
    }
}

impl Clone for SharedState {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// The actual server state stored behind the RwLock.
pub struct ServerState {
    /// Short-term markets keyed by "{asset}_{interval}m" (e.g. "btc_5m", "eth_15m").
    pub short_term_markets: HashMap<String, Vec<ShortTermMarket>>,
    /// Daily multi-strike markets (all assets).
    pub daily_markets: Vec<MarketConfig>,
    /// Monthly multi-strike markets (all assets).
    pub monthly_markets: Vec<MarketConfig>,
    /// Cached order books keyed by token_id.
    pub order_books: HashMap<String, TimestampedOrderBook>,
    /// Cached spot prices keyed by Binance symbol (e.g. "BTCUSDT").
    pub spot_prices: HashMap<String, TimestampedPrice>,
    /// Ring buffer of market snapshots for historical analysis (last ~4 hours).
    pub history: VecDeque<MarketSnapshot>,
    /// Recent alerts (last 100).
    pub alerts: VecDeque<Alert>,
    /// Recent whale trades (last ~1 hour).
    pub whale_trades: VecDeque<WhaleTrade>,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            short_term_markets: HashMap::new(),
            daily_markets: Vec::new(),
            monthly_markets: Vec::new(),
            order_books: HashMap::new(),
            spot_prices: HashMap::new(),
            history: VecDeque::with_capacity(MAX_HISTORY),
            alerts: VecDeque::with_capacity(MAX_ALERTS),
            whale_trades: VecDeque::with_capacity(MAX_WHALE_TRADES),
        }
    }
}

/// Minimum seconds between depth spike alerts for the same token+side.
const DEPTH_SPIKE_COOLDOWN_SECS: i64 = 300;

impl ServerState {
    /// Push an alert, evicting the oldest if at capacity.
    /// Depth spike alerts are de-duplicated: same token+side can't fire within cooldown.
    pub fn push_alert(&mut self, alert: Alert) {
        // De-duplicate depth spike alerts
        if let AlertKind::DepthSpike {
            ref token_id,
            ref side,
            ..
        } = alert.kind
        {
            let dominated = self.alerts.iter().rev().any(|prev| {
                if let AlertKind::DepthSpike {
                    token_id: ref prev_tid,
                    side: ref prev_side,
                    ..
                } = prev.kind
                {
                    prev_tid == token_id
                        && prev_side == side
                        && (alert.timestamp - prev.timestamp).num_seconds()
                            < DEPTH_SPIKE_COOLDOWN_SECS
                } else {
                    false
                }
            });
            if dominated {
                return;
            }
        }

        if self.alerts.len() >= MAX_ALERTS {
            self.alerts.pop_front();
        }
        self.alerts.push_back(alert);
    }

    /// Push a whale trade, evicting the oldest if at capacity.
    pub fn push_whale_trade(&mut self, trade: WhaleTrade) {
        if self.whale_trades.len() >= MAX_WHALE_TRADES {
            self.whale_trades.pop_front();
        }
        self.whale_trades.push_back(trade);
    }

    /// Push a market snapshot to the history ring buffer.
    pub fn push_history(&mut self, snapshot: MarketSnapshot) {
        if self.history.len() >= MAX_HISTORY {
            self.history.pop_front();
        }
        self.history.push_back(snapshot);
    }

    /// Get all active short-term token IDs (UP + DOWN) for polling.
    pub fn active_short_term_token_ids(&self) -> Vec<String> {
        let now = Utc::now().timestamp();
        let mut ids = Vec::new();
        for markets in self.short_term_markets.values() {
            for m in markets {
                let interval_secs = m.interval as i64 * 60;
                let window_end = m.window_start_ts + interval_secs;
                // Include if window hasn't expired yet (with 30s grace for settlement)
                if now < window_end + 30 {
                    ids.push(m.up_token_id.clone());
                    ids.push(m.down_token_id.clone());
                }
            }
        }
        ids
    }

    /// Get top daily/monthly token IDs by volume for order book polling.
    /// Returns at most `limit` token IDs.
    pub fn top_daily_monthly_token_ids(&self, limit: usize) -> Vec<String> {
        let mut markets_with_volume: Vec<(&MarketConfig, f64)> = self
            .daily_markets
            .iter()
            .chain(self.monthly_markets.iter())
            .filter(|m| !m.is_expired())
            .map(|m| (m, m.volume_usd))
            .collect();

        markets_with_volume.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut ids = Vec::new();
        for (m, _) in markets_with_volume.into_iter().take(limit) {
            ids.push(m.yes_token_id.clone());
            ids.push(m.no_token_id.clone());
        }
        ids
    }

    /// Prune expired whale trades (older than 1 hour).
    pub fn prune_whale_trades(&mut self) {
        let cutoff = Utc::now() - chrono::Duration::hours(1);
        while let Some(front) = self.whale_trades.front() {
            if front.timestamp < cutoff {
                self.whale_trades.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_state_clone_shares_inner() {
        let state = SharedState::new();
        let state2 = state.clone();
        // Both should point to the same Arc
        assert!(Arc::ptr_eq(state.inner(), state2.inner()));
    }

    #[test]
    fn test_alert_ring_buffer_capacity() {
        let mut state = ServerState::default();
        for i in 0..150 {
            state.push_alert(Alert::new(AlertKind::WindowApproaching {
                asset: "BTC".into(),
                interval: 5,
                window_start_ts: i,
                seconds_until: 60,
            }));
        }
        assert_eq!(state.alerts.len(), MAX_ALERTS);
        // Oldest should have been evicted — first remaining should be #50
        if let AlertKind::WindowApproaching { window_start_ts, .. } = &state.alerts.front().unwrap().kind {
            assert_eq!(*window_start_ts, 50);
        }
    }

    #[test]
    fn test_whale_trade_ring_buffer() {
        let mut state = ServerState::default();
        for _ in 0..MAX_WHALE_TRADES + 10 {
            state.push_whale_trade(WhaleTrade {
                token_id: "0xabc".into(),
                market_name: "BTC 5m UP".into(),
                side: "BUY".into(),
                price: 0.65,
                size_usd: 10000.0,
                timestamp: Utc::now(),
            });
        }
        assert_eq!(state.whale_trades.len(), MAX_WHALE_TRADES);
    }

    #[test]
    fn test_history_ring_buffer() {
        let mut state = ServerState::default();
        for _ in 0..MAX_HISTORY + 10 {
            state.push_history(MarketSnapshot {
                token_id: "0xabc".into(),
                market_key: "btc_5m_up".into(),
                timestamp: Utc::now(),
                best_bid: Some(0.45),
                best_ask: Some(0.55),
                spread_pct: Some(0.20),
                bid_depth_1pct: 100.0,
                ask_depth_1pct: 150.0,
            });
        }
        assert_eq!(state.history.len(), MAX_HISTORY);
    }

    #[test]
    fn test_timestamped_order_book_age() {
        use crate::core::types::OrderBook;
        let book = OrderBook {
            timestamp: 1710000000,
            market: "test".into(),
            asset_id: "0xabc".into(),
            bids: vec![],
            asks: vec![],
        };
        let tsb = TimestampedOrderBook::new(book);
        // Age should be very small (just created)
        assert!(tsb.age_secs() < 1.0);
    }

    #[test]
    fn test_timestamped_price_age() {
        let tsp = TimestampedPrice::new(84000.0);
        assert!(tsp.age_secs() < 1.0);
        assert!((tsp.price - 84000.0).abs() < 0.01);
    }

    #[test]
    fn test_top_daily_monthly_token_ids() {
        use chrono::Duration;
        let mut state = ServerState::default();
        let future = Utc::now() + Duration::hours(24);

        // Add 3 daily markets with different volumes
        for (i, vol) in [(1, 5000.0), (2, 15000.0), (3, 10000.0)] {
            state.daily_markets.push(MarketConfig {
                name: format!("Market {}", i),
                condition_id: format!("cond_{}", i),
                yes_token_id: format!("yes_{}", i),
                no_token_id: format!("no_{}", i),
                strike_price: 80000.0 + i as f64 * 1000.0,
                expiry: future,
                underlying: "BTCUSDT".into(),
                volume_usd: vol,
            });
        }

        // Limit to top 2 markets = 4 token IDs (yes + no each)
        let ids = state.top_daily_monthly_token_ids(2);
        assert_eq!(ids.len(), 4);
        // Highest volume (15000) should come first
        assert_eq!(ids[0], "yes_2");
        assert_eq!(ids[1], "no_2");
    }

    #[test]
    fn test_prune_whale_trades() {
        let mut state = ServerState::default();
        // Add an old trade (2 hours ago)
        state.push_whale_trade(WhaleTrade {
            token_id: "old".into(),
            market_name: "OLD".into(),
            side: "BUY".into(),
            price: 0.5,
            size_usd: 10000.0,
            timestamp: Utc::now() - chrono::Duration::hours(2),
        });
        // Add a recent trade
        state.push_whale_trade(WhaleTrade {
            token_id: "new".into(),
            market_name: "NEW".into(),
            side: "SELL".into(),
            price: 0.6,
            size_usd: 8000.0,
            timestamp: Utc::now(),
        });

        state.prune_whale_trades();
        assert_eq!(state.whale_trades.len(), 1);
        assert_eq!(state.whale_trades.front().unwrap().token_id, "new");
    }

    #[test]
    fn test_active_short_term_token_ids() {
        let mut state = ServerState::default();
        let now = Utc::now().timestamp();

        // Active window (starts now, 5 min duration)
        state.short_term_markets.insert(
            "btc_5m".into(),
            vec![ShortTermMarket {
                asset: "BTC".into(),
                interval: 5,
                window_start_ts: now,
                up_token_id: "up_active".into(),
                down_token_id: "down_active".into(),
                condition_id: "cond".into(),
                slug: "btc-updown-5m-123".into(),
                start_spot_price: None,
            }],
        );

        // Expired window (started 10 min ago, 5 min duration, well past 30s grace)
        state.short_term_markets.insert(
            "eth_5m".into(),
            vec![ShortTermMarket {
                asset: "ETH".into(),
                interval: 5,
                window_start_ts: now - 600,
                up_token_id: "up_expired".into(),
                down_token_id: "down_expired".into(),
                condition_id: "cond2".into(),
                slug: "eth-updown-5m-old".into(),
                start_spot_price: None,
            }],
        );

        let ids = state.active_short_term_token_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"up_active".to_string()));
        assert!(ids.contains(&"down_active".to_string()));
    }
}
