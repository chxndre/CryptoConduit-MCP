use std::collections::HashMap;
use std::time::Instant;

use chrono::Utc;
use tracing::debug;

use super::state::{
    Alert, AlertKind, MarketSnapshot, TimestampedOrderBook, WhaleTrade,
};
use crate::core::types::ShortTermMarket;

/// Configuration for alert thresholds.
#[derive(Debug, Clone)]
pub struct AlertConfig {
    /// Alert when spread drops below this percentage (e.g. 10.0 = 10%).
    pub spread_narrow_threshold: f64,
    /// Alert when depth increases by this factor vs previous snapshot.
    pub depth_spike_multiplier: f64,
    /// Minimum USD value for whale trade alerts.
    pub whale_trade_min_usd: f64,
    /// Seconds before a window starts to trigger "approaching" alert.
    pub window_approach_secs: i64,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            spread_narrow_threshold: 10.0,
            depth_spike_multiplier: 5.0, // 5x spike — reduces noise on volatile short-term books
            whale_trade_min_usd: 5000.0,
            window_approach_secs: 30,
        }
    }
}

/// Check for spread narrowing alerts by comparing current order books against
/// previous snapshots in the history buffer.
pub fn check_spread_alerts(
    current_books: &HashMap<String, TimestampedOrderBook>,
    previous_books: &HashMap<String, TimestampedOrderBook>,
    token_names: &HashMap<String, String>,
    threshold_pct: f64,
) -> Vec<Alert> {
    let mut alerts = Vec::new();
    let threshold = threshold_pct / 100.0; // convert from percentage to fraction

    for (token_id, current) in current_books {
        let current_spread = match current.book.spread_pct() {
            Some(s) => s,
            None => continue,
        };

        // Only alert when spread drops below threshold
        if current_spread >= threshold {
            continue;
        }

        // Check if previous spread was above threshold (transition)
        if let Some(prev) = previous_books.get(token_id) {
            let prev_spread = prev.book.spread_pct().unwrap_or(1.0);
            if prev_spread > threshold && current_spread < threshold {
                let name = token_names
                    .get(token_id)
                    .cloned()
                    .unwrap_or_else(|| token_id[..8].to_string());
                alerts.push(Alert::new(AlertKind::SpreadNarrowing {
                    token_id: token_id.clone(),
                    market_name: name,
                    old_spread_pct: prev_spread,
                    new_spread_pct: current_spread,
                }));
            }
        }
    }

    alerts
}

/// Minimum previous depth (USD) to consider for depth spike alerts.
/// Thin books below this threshold produce too much noise.
const DEPTH_SPIKE_MIN_PREV_USD: f64 = 100.0;

/// Cooldown period for depth spike alerts per (token_id, side).
const DEPTH_SPIKE_COOLDOWN_SECS: u64 = 300; // 5 minutes

/// Maximum depth (USD) considered realistic for short-term markets.
/// Values above this are data artefacts (momentary book glitches), not real liquidity.
/// Real depth on 5m/15m markets is typically $10-$500.
pub const SHORT_TERM_DEPTH_CAP_USD: f64 = 100_000.0;

/// Check for depth spike alerts by comparing current depth against previous snapshots.
/// `cooldowns` tracks the last alert time per (token_id, side) to enforce a 5-minute
/// cooldown window. Pass the same map across calls to maintain state.
/// `max_depth_usd` caps alerts — spikes above this value are treated as data artefacts.
/// Pass `f64::MAX` to disable the cap (e.g., for monthly markets).
pub fn check_depth_alerts(
    current_books: &HashMap<String, TimestampedOrderBook>,
    previous_books: &HashMap<String, TimestampedOrderBook>,
    token_names: &HashMap<String, String>,
    multiplier: f64,
    cooldowns: &mut HashMap<(String, String), Instant>,
    max_depth_usd: f64,
) -> Vec<Alert> {
    let mut alerts = Vec::new();
    let now = Instant::now();
    let cooldown = std::time::Duration::from_secs(DEPTH_SPIKE_COOLDOWN_SECS);

    for (token_id, current) in current_books {
        let Some(prev) = previous_books.get(token_id) else {
            continue;
        };

        let name = token_names
            .get(token_id)
            .cloned()
            .unwrap_or_else(|| token_id[..8.min(token_id.len())].to_string());

        // Check bid side
        let cur_bid = current.book.bid_depth_within(0.03);
        let prev_bid = prev.book.bid_depth_within(0.03);
        if prev_bid > DEPTH_SPIKE_MIN_PREV_USD
            && cur_bid >= prev_bid * multiplier
            && cur_bid <= max_depth_usd
        {
            let key = (token_id.clone(), "BID".to_string());
            let in_cooldown = cooldowns
                .get(&key)
                .map_or(false, |last| now.duration_since(*last) < cooldown);
            if !in_cooldown {
                cooldowns.insert(key, now);
                alerts.push(Alert::new(AlertKind::DepthSpike {
                    token_id: token_id.clone(),
                    market_name: name.clone(),
                    side: "BID".into(),
                    old_depth: prev_bid,
                    new_depth: cur_bid,
                    multiplier: cur_bid / prev_bid,
                }));
            }
        }

        // Check ask side
        let cur_ask = current.book.ask_depth_within(0.03);
        let prev_ask = prev.book.ask_depth_within(0.03);
        if prev_ask > DEPTH_SPIKE_MIN_PREV_USD
            && cur_ask >= prev_ask * multiplier
            && cur_ask <= max_depth_usd
        {
            let key = (token_id.clone(), "ASK".to_string());
            let in_cooldown = cooldowns
                .get(&key)
                .map_or(false, |last| now.duration_since(*last) < cooldown);
            if !in_cooldown {
                cooldowns.insert(key, now);
                alerts.push(Alert::new(AlertKind::DepthSpike {
                    token_id: token_id.clone(),
                    market_name: name,
                    side: "ASK".into(),
                    old_depth: prev_ask,
                    new_depth: cur_ask,
                    multiplier: cur_ask / prev_ask,
                }));
            }
        }
    }

    alerts
}

/// Check for approaching 5m/15m windows that haven't started yet.
pub fn check_window_approaching(
    short_term_markets: &HashMap<String, Vec<ShortTermMarket>>,
    approach_secs: i64,
) -> Vec<Alert> {
    let now = Utc::now().timestamp();
    let mut alerts = Vec::new();

    for markets in short_term_markets.values() {
        for m in markets {
            let secs_until = m.window_start_ts - now;
            // Alert if window is approaching but not yet started
            if secs_until > 0 && secs_until <= approach_secs {
                alerts.push(Alert::new(AlertKind::WindowApproaching {
                    asset: m.asset.clone(),
                    interval: m.interval,
                    window_start_ts: m.window_start_ts,
                    seconds_until: secs_until,
                }));
            }
        }
    }

    alerts
}

/// Create a MarketSnapshot from a TimestampedOrderBook for history tracking.
/// `market_key` provides a stable identifier that survives token ID rotations
/// (e.g., "btc_5m_up" for short-term, or the token_id itself for daily/monthly).
pub fn snapshot_from_book(token_id: &str, market_key: &str, tsb: &TimestampedOrderBook) -> MarketSnapshot {
    MarketSnapshot {
        token_id: token_id.to_string(),
        market_key: market_key.to_string(),
        timestamp: tsb.fetched_at_utc,
        best_bid: tsb.book.best_bid(),
        best_ask: tsb.book.best_ask(),
        spread_pct: tsb.book.spread_pct(),
        bid_depth_1pct: tsb.book.bid_depth_within(0.01),
        ask_depth_1pct: tsb.book.ask_depth_within(0.01),
    }
}

/// Process a whale trade event and generate alerts + WhaleTrade records.
/// Returns (alert, whale_trade) if the trade meets the threshold.
///
/// Filters out settlement artifacts (price >= 0.95) — these are post-resolution
/// redemptions/cleanups, not genuine directional bets. Analysis of 25.8M trades
/// shows most large trades at p=0.99-1.00 are noise.
pub fn process_whale_trade(
    token_id: &str,
    market_name: &str,
    side: &str,
    price: f64,
    size_usd: f64,
    min_usd: f64,
) -> Option<(Alert, WhaleTrade)> {
    if size_usd < min_usd {
        return None;
    }

    // Filter settlement artifacts: trades at price >= 0.95 are almost always
    // post-resolution redemptions, not predictive signals.
    if price >= 0.95 {
        return None;
    }

    debug!(
        token_id = %token_id,
        side = %side,
        size_usd = size_usd,
        "Whale trade detected"
    );

    let alert = Alert::new(AlertKind::WhaleTrade {
        token_id: token_id.to_string(),
        market_name: market_name.to_string(),
        side: side.to_string(),
        size_usd,
        price,
    });

    let trade = WhaleTrade {
        token_id: token_id.to_string(),
        market_name: market_name.to_string(),
        side: side.to_string(),
        price,
        size_usd,
        timestamp: Utc::now(),
    };

    Some((alert, trade))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{OrderBook, PriceLevel};

    fn make_book(bid: f64, ask: f64, size: f64) -> OrderBook {
        OrderBook {
            timestamp: 1710000000,
            market: "test".into(),
            asset_id: "0xabc".into(),
            bids: vec![PriceLevel {
                price: bid.to_string(),
                size: size.to_string(),
            }],
            asks: vec![PriceLevel {
                price: ask.to_string(),
                size: size.to_string(),
            }],
        }
    }

    fn make_tsb(book: OrderBook) -> TimestampedOrderBook {
        TimestampedOrderBook {
            book,
            fetched_at: Instant::now(),
            fetched_at_utc: Utc::now(),
        }
    }

    #[test]
    fn test_spread_narrowing_alert() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let mut names = HashMap::new();

        // Previous: wide spread (20%)
        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.40, 0.60, 100.0)),
        );
        // Current: narrow spread (5%)
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.48, 0.52, 100.0)),
        );
        names.insert("token1".to_string(), "BTC 5m UP".to_string());

        let alerts = check_spread_alerts(&curr, &prev, &names, 10.0);
        assert_eq!(alerts.len(), 1);

        if let AlertKind::SpreadNarrowing {
            market_name,
            new_spread_pct,
            ..
        } = &alerts[0].kind
        {
            assert_eq!(market_name, "BTC 5m UP");
            assert!(*new_spread_pct < 0.10); // less than 10%
        } else {
            panic!("Expected SpreadNarrowing alert");
        }
    }

    #[test]
    fn test_no_spread_alert_when_still_wide() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let names = HashMap::new();

        // Both wide
        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.30, 0.70, 100.0)),
        );
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.35, 0.65, 100.0)),
        );

        let alerts = check_spread_alerts(&curr, &prev, &names, 10.0);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_depth_spike_alert() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let mut names = HashMap::new();

        // Previous: depth above $100 minimum threshold (size 250 at price 0.50 = $125 each side)
        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 250.0)),
        );
        // Current: 4x depth
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 1000.0)),
        );
        names.insert("token1".to_string(), "ETH 15m DOWN".to_string());

        let alerts = check_depth_alerts(&curr, &prev, &names, 3.0, &mut HashMap::new(), f64::MAX);
        // Should trigger on at least one side
        assert!(!alerts.is_empty());

        let has_depth_spike = alerts.iter().any(|a| matches!(&a.kind, AlertKind::DepthSpike { .. }));
        assert!(has_depth_spike);
    }

    #[test]
    fn test_no_depth_spike_small_change() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let names = HashMap::new();

        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 250.0)),
        );
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 300.0)),
        );

        let alerts = check_depth_alerts(&curr, &prev, &names, 3.0, &mut HashMap::new(), f64::MAX);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_no_depth_spike_below_min_threshold() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let names = HashMap::new();

        // Previous depth below $100 minimum — should not trigger even with 10x spike
        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 50.0)),
        );
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 500.0)),
        );

        let alerts = check_depth_alerts(&curr, &prev, &names, 3.0, &mut HashMap::new(), f64::MAX);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_window_approaching_alert() {
        let now = Utc::now().timestamp();
        let mut markets = HashMap::new();
        markets.insert(
            "btc_5m".to_string(),
            vec![
                // Window starting in 20 seconds
                ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now + 20,
                    up_token_id: "up1".into(),
                    down_token_id: "down1".into(),
                    condition_id: "cond1".into(),
                    slug: "btc-updown-5m-1".into(),
                    start_spot_price: None,
                },
                // Window starting in 5 minutes (too far)
                ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now + 300,
                    up_token_id: "up2".into(),
                    down_token_id: "down2".into(),
                    condition_id: "cond2".into(),
                    slug: "btc-updown-5m-2".into(),
                    start_spot_price: None,
                },
            ],
        );

        let alerts = check_window_approaching(&markets, 30);
        assert_eq!(alerts.len(), 1);

        if let AlertKind::WindowApproaching {
            asset,
            interval,
            seconds_until,
            ..
        } = &alerts[0].kind
        {
            assert_eq!(asset, "BTC");
            assert_eq!(*interval, 5);
            assert!(*seconds_until <= 30);
        } else {
            panic!("Expected WindowApproaching alert");
        }
    }

    #[test]
    fn test_window_approaching_ignores_started() {
        let now = Utc::now().timestamp();
        let mut markets = HashMap::new();
        markets.insert(
            "btc_5m".to_string(),
            vec![ShortTermMarket {
                asset: "BTC".into(),
                interval: 5,
                window_start_ts: now - 60, // already started
                up_token_id: "up1".into(),
                down_token_id: "down1".into(),
                condition_id: "cond1".into(),
                slug: "btc-updown-5m-old".into(),
                start_spot_price: None,
            }],
        );

        let alerts = check_window_approaching(&markets, 30);
        assert!(alerts.is_empty());
    }

    #[test]
    fn test_process_whale_trade_above_threshold() {
        let result = process_whale_trade("0xabc", "BTC 5m UP", "BUY", 0.65, 10000.0, 5000.0);
        assert!(result.is_some());
        let (alert, trade) = result.unwrap();

        if let AlertKind::WhaleTrade { size_usd, side, .. } = &alert.kind {
            assert!((size_usd - 10000.0).abs() < 0.01);
            assert_eq!(side, "BUY");
        } else {
            panic!("Expected WhaleTrade alert");
        }
        assert_eq!(trade.token_id, "0xabc");
        assert_eq!(trade.market_name, "BTC 5m UP");
    }

    #[test]
    fn test_process_whale_trade_below_threshold() {
        let result = process_whale_trade("0xabc", "BTC Daily Above 85k", "BUY", 0.65, 3000.0, 5000.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_process_whale_trade_settlement_artifact() {
        // Large trade at p=0.99 is a post-resolution redemption, not a signal
        let result = process_whale_trade("0xabc", "BTC Monthly", "BUY", 0.99, 50000.0, 5000.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_process_whale_trade_high_price_boundary() {
        // p=0.94 is still below the 0.95 cutoff — should pass
        let result = process_whale_trade("0xabc", "BTC Daily Above 85k", "BUY", 0.94, 10000.0, 5000.0);
        assert!(result.is_some());

        // p=0.95 is at the boundary — should be filtered
        let result = process_whale_trade("0xabc", "BTC Daily Above 85k", "BUY", 0.95, 10000.0, 5000.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_from_book() {
        let book = make_book(0.45, 0.55, 200.0);
        let tsb = make_tsb(book);
        let snap = snapshot_from_book("token1", "btc_5m_up", &tsb);

        assert_eq!(snap.token_id, "token1");
        assert_eq!(snap.market_key, "btc_5m_up");
        assert!((snap.best_bid.unwrap() - 0.45).abs() < 0.001);
        assert!((snap.best_ask.unwrap() - 0.55).abs() < 0.001);
        assert!(snap.spread_pct.is_some());
    }

    #[test]
    fn test_alert_display() {
        let alert = Alert::new(AlertKind::SpreadNarrowing {
            token_id: "0x123".into(),
            market_name: "BTC 5m UP".into(),
            old_spread_pct: 0.15,
            new_spread_pct: 0.05,
        });
        let display = format!("{}", alert.kind);
        assert!(display.contains("5.0%"));
        assert!(display.contains("BTC 5m UP"));
    }

    #[test]
    fn test_depth_spike_cooldown() {
        let mut prev = HashMap::new();
        let mut curr = HashMap::new();
        let mut names = HashMap::new();

        // Previous: depth above $100 minimum threshold
        prev.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 250.0)),
        );
        // Current: 4x depth (above 3.0x multiplier)
        curr.insert(
            "token1".to_string(),
            make_tsb(make_book(0.50, 0.52, 1000.0)),
        );
        names.insert("token1".to_string(), "ETH 15m DOWN".to_string());

        let mut cooldowns = HashMap::new();

        // First call should fire alerts
        let alerts1 = check_depth_alerts(&curr, &prev, &names, 3.0, &mut cooldowns, f64::MAX);
        assert!(!alerts1.is_empty(), "First call should produce alerts");

        // Second call with same cooldown map should be suppressed
        let alerts2 = check_depth_alerts(&curr, &prev, &names, 3.0, &mut cooldowns, f64::MAX);
        assert!(alerts2.is_empty(), "Second call within cooldown should be suppressed");

        // Verify cooldown map has entries
        assert!(!cooldowns.is_empty());
    }
}
