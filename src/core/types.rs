use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

// --- Polymarket Order Book Types ---
// Extracted from market-scout src/polymarket/types.rs
// CRITICAL: Do NOT modify the deserialization logic or iteration order.
// These handle Polymarket API quirks (mixed types, reversed sort order).

fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de;

    struct StringOrNumber;

    impl<'de> de::Visitor<'de> for StringOrNumber {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or number representing a u64")
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v)
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(v as u64)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            v.parse::<u64>().map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_any(StringOrNumber)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    pub fn price_f64(&self) -> Result<f64, std::num::ParseFloatError> {
        self.price.parse()
    }

    pub fn size_f64(&self) -> Result<f64, std::num::ParseFloatError> {
        self.size.parse()
    }
}

/// Order book for a specific token.
/// IMPORTANT: Polymarket returns bids ascending, asks descending.
/// Best prices are at the END of the arrays, not the beginning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    #[serde(deserialize_with = "deserialize_string_or_number")]
    pub timestamp: u64,
    pub market: String,
    pub asset_id: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl OrderBook {
    /// Best bid (highest buy) — last element due to Polymarket ascending sort
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.last().and_then(|b| b.price_f64().ok())
    }

    /// Best ask (lowest sell) — last element due to Polymarket descending sort
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.last().and_then(|a| a.price_f64().ok())
    }

    pub fn midpoint(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    pub fn spread_pct(&self) -> Option<f64> {
        match (self.spread(), self.midpoint()) {
            (Some(spread), Some(mid)) if mid > 0.0 => Some(spread / mid),
            _ => None,
        }
    }

    /// Total USD depth on ask side within slippage tolerance.
    /// Size is in contracts; USD value = size * price.
    pub fn ask_depth_within(&self, max_slippage_pct: f64) -> f64 {
        let Some(best) = self.best_ask() else {
            return 0.0;
        };
        let max_price = best * (1.0 + max_slippage_pct);
        self.asks
            .iter()
            .filter_map(|l| {
                let price = l.price_f64().ok()?;
                let size = l.size_f64().ok()?;
                if price <= max_price {
                    Some(size * price)
                } else {
                    None
                }
            })
            .sum()
    }

    /// Total USD depth on bid side within slippage tolerance.
    /// Size is in contracts; USD value = size * price.
    pub fn bid_depth_within(&self, max_slippage_pct: f64) -> f64 {
        let Some(best) = self.best_bid() else {
            return 0.0;
        };
        let min_price = best * (1.0 - max_slippage_pct);
        self.bids
            .iter()
            .filter_map(|l| {
                let price = l.price_f64().ok()?;
                let size = l.size_f64().ok()?;
                if price >= min_price {
                    Some(size * price)
                } else {
                    None
                }
            })
            .sum()
    }

    /// Get depth on the relevant side for a given order side
    pub fn depth_for_side(&self, side: Side, max_slippage_pct: f64) -> f64 {
        match side {
            Side::Buy => self.ask_depth_within(max_slippage_pct),
            Side::Sell => self.bid_depth_within(max_slippage_pct),
        }
    }

    /// Simulate a fill by walking the order book with a USD budget.
    /// Iterates in reverse since Polymarket API returns best prices at END.
    pub fn simulate_fill_usd(&self, side: Side, budget_usd: f64) -> Option<FillResult> {
        let levels = match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };

        if levels.is_empty() {
            return None;
        }

        let best_price = levels.last()?.price_f64().ok()?;
        let mut remaining_usd = budget_usd;
        let mut total_contracts = 0.0;
        let mut total_cost = 0.0;
        let mut levels_crossed = 0;

        for level in levels.iter().rev() {
            if remaining_usd <= 0.0 {
                break;
            }
            let price = level.price_f64().ok()?;
            let size = level.size_f64().ok()?;

            let level_cost = size * price;
            let (fill_contracts, fill_cost) = if level_cost <= remaining_usd {
                (size, level_cost)
            } else {
                let contracts = remaining_usd / price;
                (contracts, remaining_usd)
            };

            total_contracts += fill_contracts;
            total_cost += fill_cost;
            remaining_usd -= fill_cost;
            levels_crossed += 1;
        }

        if total_contracts <= 0.0 {
            return None;
        }

        let avg_price = total_cost / total_contracts;
        let slippage = (avg_price - best_price).abs();
        let slippage_pct = if best_price > 0.0 {
            slippage / best_price
        } else {
            0.0
        };

        Some(FillResult {
            avg_price,
            filled_qty: total_contracts,
            total_cost,
            slippage_pct,
            levels_crossed,
            best_price,
            fully_filled: remaining_usd <= 0.0,
        })
    }

    /// Simulate a fill by walking the order book for a given contract quantity.
    /// Iterates in reverse since Polymarket API returns best prices at END.
    pub fn simulate_fill(&self, side: Side, quantity: f64) -> Option<FillResult> {
        let levels = match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        };

        if levels.is_empty() {
            return None;
        }

        let best_price = levels.last()?.price_f64().ok()?;
        let mut remaining = quantity;
        let mut total_cost = 0.0;
        let mut levels_crossed = 0;

        for level in levels.iter().rev() {
            if remaining <= 0.0 {
                break;
            }
            let price = level.price_f64().ok()?;
            let size = level.size_f64().ok()?;
            let fill_qty = remaining.min(size);

            total_cost += fill_qty * price;
            remaining -= fill_qty;
            levels_crossed += 1;
        }

        let filled_qty = quantity - remaining;
        if filled_qty <= 0.0 {
            return None;
        }

        let avg_price = total_cost / filled_qty;
        let slippage = (avg_price - best_price).abs();
        let slippage_pct = if best_price > 0.0 {
            slippage / best_price
        } else {
            0.0
        };

        Some(FillResult {
            avg_price,
            filled_qty,
            total_cost,
            slippage_pct,
            levels_crossed,
            best_price,
            fully_filled: remaining <= 0.0,
        })
    }

    /// Check if we can fill a given USD size and calculate expected slippage
    pub fn check_depth(&self, side: Side, size_usd: f64) -> Option<DepthCheck> {
        let fill_result = self.simulate_fill_usd(side, size_usd)?;

        let depth_at_1pct = self.depth_for_side(side, 0.01);
        let depth_at_3pct = self.depth_for_side(side, 0.03);
        let depth_at_5pct = self.depth_for_side(side, 0.05);

        Some(DepthCheck {
            can_fill: fill_result.fully_filled,
            fill_pct: fill_result.filled_qty / size_usd,
            expected_slippage_pct: fill_result.slippage_pct,
            levels_needed: fill_result.levels_crossed,
            depth_at_1pct,
            depth_at_3pct,
            depth_at_5pct,
            best_price: fill_result.best_price,
        })
    }

    /// Simulate a limit order price placement inside the spread
    pub fn limit_order_price(&self, side: Side, offset: f64) -> Option<f64> {
        match side {
            Side::Buy => {
                let best_bid = self.best_bid()?;
                let best_ask = self.best_ask()?;
                let limit_price = best_bid + offset;
                Some(limit_price.min(best_ask - 0.001))
            }
            Side::Sell => {
                let best_bid = self.best_bid()?;
                let best_ask = self.best_ask()?;
                let limit_price = best_ask - offset;
                Some(limit_price.max(best_bid + 0.001))
            }
        }
    }
}

/// Result of a simulated fill
#[derive(Debug, Clone)]
pub struct FillResult {
    pub avg_price: f64,
    pub filled_qty: f64,
    pub total_cost: f64,
    pub slippage_pct: f64,
    pub levels_crossed: usize,
    pub best_price: f64,
    pub fully_filled: bool,
}

/// Result of depth check for a given size
#[derive(Debug, Clone)]
pub struct DepthCheck {
    pub can_fill: bool,
    pub fill_pct: f64,
    pub expected_slippage_pct: f64,
    pub levels_needed: usize,
    pub depth_at_1pct: f64,
    pub depth_at_3pct: f64,
    pub depth_at_5pct: f64,
    pub best_price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

// --- Direction & Trade Types ---
// Extracted from market-scout src/strategy.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Up,
    Down,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Direction::Up => write!(f, "UP"),
            Direction::Down => write!(f, "DOWN"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeMode {
    Taker,
    Maker,
}

impl std::fmt::Display for TradeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeMode::Taker => write!(f, "taker"),
            TradeMode::Maker => write!(f, "maker"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExitReason {
    Settlement,
    StopLoss,
    ProfitTarget,
    UnknownSettlement,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::Settlement => write!(f, "settlement"),
            ExitReason::StopLoss => write!(f, "stop_loss"),
            ExitReason::ProfitTarget => write!(f, "profit_target"),
            ExitReason::UnknownSettlement => write!(f, "unknown_settlement"),
        }
    }
}

// --- Market Config ---
// Extracted from market-scout src/config/markets.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    pub name: String,
    pub condition_id: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub strike_price: f64,
    pub expiry: DateTime<Utc>,
    pub underlying: String,
    #[serde(default)]
    pub volume_usd: f64,
}

impl MarketConfig {
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expiry
    }

    pub fn is_upside(&self) -> bool {
        let name = self.name.to_lowercase();
        !(name.contains("below") || name.contains("dip") || name.contains("fall"))
    }

    pub fn otm_percent(&self, current_price: f64) -> f64 {
        if current_price <= 0.0 {
            return f64::MAX;
        }
        ((self.strike_price - current_price) / current_price) * 100.0
    }
}

// --- Short-Term Market ---
// Extracted from market-scout src/discovery.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShortTermMarket {
    pub asset: String,
    pub interval: u32,
    pub window_start_ts: i64,
    pub up_token_id: String,
    pub down_token_id: String,
    pub condition_id: String,
    pub slug: String,
    /// Spot price at the time this window was discovered/opened.
    pub start_spot_price: Option<f64>,
}

// --- Resolved Market Summary ---
// Used by the historical market query fallback in gamma.rs.
// When JSONL logs are empty/disabled, resolved markets on Gamma provide
// volume, outcome, and activity data retroactively.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedMarketSummary {
    pub slug: String,
    pub asset: String,
    pub timeframe: String,
    pub timestamp: DateTime<Utc>,
    pub volume_usd: f64,
    pub outcome: Option<String>,
    pub was_active: bool,
}

// --- Spot Price History ---
// Extracted from market-scout src/strategy.rs
// Ring buffer with binary search for historical price lookups.

pub struct SpotPriceHistory {
    entries: VecDeque<(i64, f64)>, // (timestamp_ms, price)
    max_age_ms: i64,
}

impl SpotPriceHistory {
    pub fn new(max_age_secs: u64) -> Self {
        Self {
            entries: VecDeque::with_capacity((max_age_secs * 2) as usize),
            max_age_ms: max_age_secs as i64 * 1000,
        }
    }

    pub fn record(&mut self, timestamp_ms: i64, price: f64) {
        let cutoff = timestamp_ms - self.max_age_ms;
        while let Some(&(ts, _)) = self.entries.front() {
            if ts < cutoff {
                self.entries.pop_front();
            } else {
                break;
            }
        }
        self.entries.push_back((timestamp_ms, price));
    }

    pub fn price_at(&self, target_ms: i64) -> Option<f64> {
        if self.entries.is_empty() {
            return None;
        }

        let mut best_price = None;
        let mut best_diff = i64::MAX;

        let idx = self.entries.partition_point(|(ts, _)| *ts < target_ms);

        for i in [idx.saturating_sub(1), idx] {
            if let Some(&(ts, price)) = self.entries.get(i) {
                let diff = (ts - target_ms).abs();
                if diff < best_diff {
                    best_diff = diff;
                    best_price = Some(price);
                }
            }
        }

        best_price
    }

    pub fn price_at_checked(&self, target_ms: i64, max_diff_ms: i64) -> Option<f64> {
        if self.entries.is_empty() {
            return None;
        }

        let idx = self.entries.partition_point(|(ts, _)| *ts < target_ms);

        let mut best_price = None;
        let mut best_diff = i64::MAX;

        for i in [idx.saturating_sub(1), idx] {
            if let Some(&(ts, price)) = self.entries.get(i) {
                let diff = (ts - target_ms).abs();
                if diff < best_diff {
                    best_diff = diff;
                    best_price = Some(price);
                }
            }
        }

        if best_diff <= max_diff_ms {
            best_price
        } else {
            None
        }
    }

    pub fn latest_with_ts(&self) -> Option<(i64, f64)> {
        self.entries.back().copied()
    }

    pub fn latest(&self) -> Option<f64> {
        self.entries.back().map(|(_, p)| *p)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
