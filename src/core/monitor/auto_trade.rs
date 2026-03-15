// Autonomous 5m/15m trading engine.
// Checks entry conditions each polling tick during the entry zone.
// Executes via paper engine or live executor.
// Persists config + recent trade log to auto_trade.json in config directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::core::analysis::microstructure::{self, TradeabilityRating};
use crate::core::monitor::state::SharedState;
use crate::core::paper::engine;
use crate::core::paper::portfolio::Portfolio;

/// Maximum recent auto-trades to keep in state (ring buffer).
const MAX_RECENT_TRADES: usize = 200;

// ──────────────────────────── Config ────────────────────────────

/// Execution mode for auto-trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoTradeMode {
    /// Open paper positions (default).
    Paper,
    /// Place real orders via live executor.
    Live,
}

impl std::fmt::Display for AutoTradeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutoTradeMode::Paper => write!(f, "paper"),
            AutoTradeMode::Live => write!(f, "live"),
        }
    }
}

/// Side override for auto-trade direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoTradeSide {
    /// Automatically determine side from spot price direction (default).
    Auto,
    /// Always buy UP.
    Up,
    /// Always buy DOWN.
    Down,
}

impl std::fmt::Display for AutoTradeSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutoTradeSide::Auto => write!(f, "auto"),
            AutoTradeSide::Up => write!(f, "up"),
            AutoTradeSide::Down => write!(f, "down"),
        }
    }
}

/// Per-asset/window auto-trade configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTradeConfig {
    /// Asset ticker (lowercase: "btc", "eth", etc.)
    pub asset: String,
    /// Window type: 5 or 15 (minutes)
    pub window: u32,
    /// Whether this config is active.
    pub enabled: bool,
    /// Minimum elapsed percentage before entering (e.g. 85.0 for 85%).
    pub entry_pct: f64,
    /// Minimum spot price move percentage to trigger entry (e.g. 0.07 for 0.07%).
    pub min_move_pct: f64,
    /// Maximum acceptable entry price (e.g. 0.75).
    pub max_entry_price: f64,
    /// Position size in USD per trade.
    pub position_size_usd: f64,
    /// Execution mode.
    pub mode: AutoTradeMode,
    /// Maximum spread percentage to accept (e.g. 5.0 for 5%). None = no limit.
    #[serde(default)]
    pub max_spread_pct: Option<f64>,
    /// Minimum ask-side depth in USD required (e.g. 50.0). None = no limit.
    #[serde(default)]
    pub min_depth_usd: Option<f64>,
    /// Side override: "auto" (default), "up", or "down".
    #[serde(default = "default_side")]
    pub side: AutoTradeSide,
    /// Maximum total exposure across all open auto-trade positions in USD. None = no limit.
    #[serde(default)]
    pub max_total_exposure_usd: Option<f64>,
}

fn default_side() -> AutoTradeSide {
    AutoTradeSide::Auto
}

impl AutoTradeConfig {
    /// Key for dedup: "btc_5m", "eth_15m", etc.
    pub fn key(&self) -> String {
        format!("{}_{}", self.asset, self.window)
    }
}

// ──────────────────────────── Trade Record ────────────────────────────

/// Record of an auto-trade execution (or skip).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoTradeRecord {
    pub timestamp: DateTime<Utc>,
    pub asset: String,
    pub window: u32,
    pub side: String,
    pub entry_price: f64,
    pub size_usd: f64,
    pub mode: AutoTradeMode,
    pub position_id: Option<u64>,
    pub spot_price: f64,
    pub spot_move_pct: f64,
    pub elapsed_pct: f64,
    /// Condition ID for tracking settlement
    pub condition_id: String,
}

// ──────────────────────────── State ────────────────────────────

/// Persisted auto-trade state: configs + recent trades.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AutoTradeState {
    pub configs: Vec<AutoTradeConfig>,
    pub recent_trades: Vec<AutoTradeRecord>,
    /// Track which condition_ids we've already traded in (to avoid double entry).
    pub traded_conditions: HashMap<String, DateTime<Utc>>,
    #[serde(skip)]
    file_path: PathBuf,
}

impl AutoTradeState {
    /// Load from config directory or create fresh.
    pub fn load() -> Result<Self> {
        let path = auto_trade_path()?;
        Self::load_from_path(path)
    }

    pub fn load_from_path(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            debug!(path = %path.display(), "No auto-trade state found, starting fresh");
            return Ok(Self {
                file_path: path,
                ..Self::default()
            });
        }

        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read auto-trade state: {}", path.display()))?;

        let mut state: AutoTradeState = serde_json::from_str(&data)
            .with_context(|| format!("Failed to parse auto-trade JSON: {}", path.display()))?;

        state.file_path = path;
        info!(
            configs = state.configs.len(),
            trades = state.recent_trades.len(),
            "Auto-trade state loaded"
        );

        Ok(state)
    }

    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self)
            .context("Failed to serialize auto-trade state")?;

        crate::core::infrastructure::atomic::atomic_write(&self.file_path, json.as_bytes())?;

        debug!(path = %self.file_path.display(), "Auto-trade state saved");
        Ok(())
    }

    /// Set or update a config for an asset+window. Returns the updated config.
    pub fn set_config(&mut self, config: AutoTradeConfig) -> &AutoTradeConfig {
        let key = config.key();
        if let Some(existing) = self.configs.iter_mut().find(|c| c.key() == key) {
            *existing = config;
        } else {
            self.configs.push(config);
        }
        self.configs.iter().find(|c| c.key() == key).unwrap()
    }

    /// Get config for an asset+window.
    pub fn get_config(&self, asset: &str, window: u32) -> Option<&AutoTradeConfig> {
        let key = format!("{}_{}", asset.to_lowercase(), window);
        self.configs.iter().find(|c| c.key() == key)
    }

    /// Get all enabled configs.
    pub fn enabled_configs(&self) -> Vec<&AutoTradeConfig> {
        self.configs.iter().filter(|c| c.enabled).collect()
    }

    /// Record a trade, trimming the ring buffer.
    pub fn push_trade(&mut self, record: AutoTradeRecord) {
        self.traded_conditions
            .insert(record.condition_id.clone(), record.timestamp);
        self.recent_trades.push(record);
        while self.recent_trades.len() > MAX_RECENT_TRADES {
            self.recent_trades.remove(0);
        }
    }

    /// Check if we already traded this condition_id.
    pub fn already_traded(&self, condition_id: &str) -> bool {
        self.traded_conditions.contains_key(condition_id)
    }

    /// Prune old traded_conditions entries (older than 2 hours).
    pub fn prune_old_conditions(&mut self) {
        let cutoff = Utc::now() - chrono::Duration::hours(2);
        self.traded_conditions.retain(|_, ts| *ts > cutoff);
    }

    /// Get recent trades for a specific asset+window (or all if None).
    pub fn recent_trades_for(
        &self,
        asset: Option<&str>,
        window: Option<u32>,
    ) -> Vec<&AutoTradeRecord> {
        self.recent_trades
            .iter()
            .filter(|t| {
                if let Some(a) = asset
                    && t.asset.to_lowercase() != a.to_lowercase()
                {
                    return false;
                }
                if let Some(w) = window
                    && t.window != w
                {
                    return false;
                }
                true
            })
            .collect()
    }
}

fn auto_trade_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir()
        .context("Could not determine config directory")?
        .join("crypto-conduit");
    Ok(config_dir.join("auto_trade.json"))
}

// ──────────────────────────── Entry Condition Check ────────────────────────────

/// Result of checking entry conditions for a specific window.
#[derive(Debug)]
pub struct EntryCheckResult {
    pub should_enter: bool,
    pub side: String,
    pub entry_price: f64,
    pub token_id: String,
    pub condition_id: String,
    pub market_name: String,
    pub spot_price: f64,
    pub spot_move_pct: f64,
    pub elapsed_pct: f64,
    pub reason_skip: Option<String>,
}

/// Microstructure data for one side (UP or DOWN) of a market.
#[derive(Debug, Clone, Default)]
pub struct SideBookData {
    pub best_ask: Option<f64>,
    pub tradeability: Option<TradeabilityRating>,
    pub spread_pct: Option<f64>,
    pub ask_depth_usd: Option<f64>,
}

/// Check entry conditions for one active window given an auto-trade config.
/// Uses the current time. For backtesting with historical timestamps, use `check_entry_conditions_at`.
pub fn check_entry_conditions(
    config: &AutoTradeConfig,
    window_start_ts: i64,
    interval_secs: i64,
    up_token_id: &str,
    down_token_id: &str,
    condition_id: &str,
    up_data: &SideBookData,
    down_data: &SideBookData,
    spot_price: f64,
    spot_at_window_start: Option<f64>,
    already_traded: bool,
) -> EntryCheckResult {
    check_entry_conditions_at(
        config,
        Utc::now().timestamp(),
        window_start_ts,
        interval_secs,
        up_token_id,
        down_token_id,
        condition_id,
        up_data,
        down_data,
        spot_price,
        spot_at_window_start,
        already_traded,
    )
}

/// Check entry conditions at a specific timestamp (for backtesting).
/// Same logic as `check_entry_conditions` but takes an explicit `now_ts` instead of `Utc::now()`.
pub fn check_entry_conditions_at(
    config: &AutoTradeConfig,
    now_ts: i64,
    window_start_ts: i64,
    interval_secs: i64,
    up_token_id: &str,
    down_token_id: &str,
    condition_id: &str,
    up_data: &SideBookData,
    down_data: &SideBookData,
    spot_price: f64,
    spot_at_window_start: Option<f64>,
    already_traded: bool,
) -> EntryCheckResult {
    let elapsed = now_ts - window_start_ts;
    let elapsed_pct = (elapsed as f64 / interval_secs as f64) * 100.0;

    // Already traded this condition?
    if already_traded {
        return EntryCheckResult {
            should_enter: false,
            side: String::new(),
            entry_price: 0.0,
            token_id: String::new(),
            condition_id: condition_id.to_string(),
            market_name: String::new(),
            spot_price,
            spot_move_pct: 0.0,
            elapsed_pct,
            reason_skip: Some("Already traded this window".into()),
        };
    }

    // 1. Time check — elapsed % must meet threshold
    if elapsed_pct < config.entry_pct {
        return EntryCheckResult {
            should_enter: false,
            side: String::new(),
            entry_price: 0.0,
            token_id: String::new(),
            condition_id: condition_id.to_string(),
            market_name: String::new(),
            spot_price,
            spot_move_pct: 0.0,
            elapsed_pct,
            reason_skip: Some(format!(
                "Elapsed {:.0}% < {:.0}% threshold",
                elapsed_pct, config.entry_pct
            )),
        };
    }

    // 2. Spot move check
    let spot_move_pct = if let Some(start_price) = spot_at_window_start {
        if start_price > 0.0 {
            ((spot_price - start_price) / start_price).abs() * 100.0
        } else {
            0.0
        }
    } else {
        // No reference price — skip
        return EntryCheckResult {
            should_enter: false,
            side: String::new(),
            entry_price: 0.0,
            token_id: String::new(),
            condition_id: condition_id.to_string(),
            market_name: String::new(),
            spot_price,
            spot_move_pct: 0.0,
            elapsed_pct,
            reason_skip: Some("No spot price at window start".into()),
        };
    };

    if spot_move_pct < config.min_move_pct {
        return EntryCheckResult {
            should_enter: false,
            side: String::new(),
            entry_price: 0.0,
            token_id: String::new(),
            condition_id: condition_id.to_string(),
            market_name: String::new(),
            spot_price,
            spot_move_pct,
            elapsed_pct,
            reason_skip: Some(format!(
                "Spot move {:.3}% < {:.3}% threshold",
                spot_move_pct, config.min_move_pct
            )),
        };
    }

    // Determine direction based on side config
    let spot_up = spot_price > spot_at_window_start.unwrap_or(0.0);
    let (side, token_id, book_data) = match config.side {
        AutoTradeSide::Up => ("UP", up_token_id, up_data),
        AutoTradeSide::Down => ("DOWN", down_token_id, down_data),
        AutoTradeSide::Auto => {
            if spot_up {
                ("UP", up_token_id, up_data)
            } else {
                ("DOWN", down_token_id, down_data)
            }
        }
    };

    let market_name = format!(
        "{} {}m {}",
        config.asset.to_uppercase(),
        config.window,
        side
    );

    // 3. Entry price check
    let entry_price = match book_data.best_ask {
        Some(p) => p,
        None => {
            return EntryCheckResult {
                should_enter: false,
                side: side.to_string(),
                entry_price: 0.0,
                token_id: token_id.to_string(),
                condition_id: condition_id.to_string(),
                market_name,
                spot_price,
                spot_move_pct,
                elapsed_pct,
                reason_skip: Some("No ask price available".into()),
            };
        }
    };

    if entry_price > config.max_entry_price {
        return EntryCheckResult {
            should_enter: false,
            side: side.to_string(),
            entry_price,
            token_id: token_id.to_string(),
            condition_id: condition_id.to_string(),
            market_name,
            spot_price,
            spot_move_pct,
            elapsed_pct,
            reason_skip: Some(format!(
                "Entry price {:.3} > {:.3} max",
                entry_price, config.max_entry_price
            )),
        };
    }

    // 4. Tradeability check
    if let Some(rating) = book_data.tradeability
        && rating == TradeabilityRating::Untradeable
    {
            return EntryCheckResult {
                should_enter: false,
                side: side.to_string(),
                entry_price,
                token_id: token_id.to_string(),
                condition_id: condition_id.to_string(),
                market_name,
                spot_price,
                spot_move_pct,
                elapsed_pct,
                reason_skip: Some("Market is untradeable".into()),
            };
    }

    // 5. Spread check
    if let Some(max_spread) = config.max_spread_pct {
        if let Some(spread) = book_data.spread_pct {
            if spread > max_spread {
                return EntryCheckResult {
                    should_enter: false,
                    side: side.to_string(),
                    entry_price,
                    token_id: token_id.to_string(),
                    condition_id: condition_id.to_string(),
                    market_name,
                    spot_price,
                    spot_move_pct,
                    elapsed_pct,
                    reason_skip: Some(format!(
                        "Spread {:.1}% > {:.1}% max",
                        spread, max_spread
                    )),
                };
            }
        }
    }

    // 6. Depth check
    if let Some(min_depth) = config.min_depth_usd {
        if let Some(depth) = book_data.ask_depth_usd {
            if depth < min_depth {
                return EntryCheckResult {
                    should_enter: false,
                    side: side.to_string(),
                    entry_price,
                    token_id: token_id.to_string(),
                    condition_id: condition_id.to_string(),
                    market_name,
                    spot_price,
                    spot_move_pct,
                    elapsed_pct,
                    reason_skip: Some(format!(
                        "Depth ${:.0} < ${:.0} min",
                        depth, min_depth
                    )),
                };
            }
        }
    }

    // All conditions passed
    EntryCheckResult {
        should_enter: true,
        side: side.to_string(),
        entry_price,
        token_id: token_id.to_string(),
        condition_id: condition_id.to_string(),
        market_name,
        spot_price,
        spot_move_pct,
        elapsed_pct,
        reason_skip: None,
    }
}

// ──────────────────────────── Execution ────────────────────────────

/// Shared auto-trade state type for use across the server.
pub type SharedAutoTradeState = Arc<Mutex<AutoTradeState>>;

/// Execute an auto-trade based on an entry check result.
/// Returns a trade record (or None if skipped due to mode).
///
/// For live mode, pass an optional LiveExecutor + RiskManager.
/// These are type-erased via trait objects to avoid leaking the live-trading
/// feature flag into this module's public API.
pub async fn execute_auto_trade(
    config: &AutoTradeConfig,
    check: &EntryCheckResult,
    auto_state: &SharedAutoTradeState,
    portfolio: &Arc<Mutex<Portfolio>>,
    live_executor: Option<&crate::core::execution::live::LiveExecutor>,
    risk_manager: Option<&Arc<Mutex<crate::core::execution::risk::RiskManager>>>,
) -> Option<AutoTradeRecord> {
    if !check.should_enter {
        return None;
    }

    let is_upside = check.side == "UP";

    match config.mode {
        AutoTradeMode::Paper => {
            let underlying = format!(
                "{}USDT",
                config.asset.to_uppercase()
            );

            // Compute window timestamps for expiry tracking and Binance fallback settlement
            let now_ts = Utc::now().timestamp();
            let remaining_secs = ((100.0 - check.elapsed_pct) / 100.0 * (config.window as f64 * 60.0)) as i64;
            let window_end_ts = Some(now_ts + remaining_secs);
            let window_start_ts = Some(now_ts + remaining_secs - (config.window as i64 * 60));

            let mut port = portfolio.lock().await;
            match engine::open_position(
                &mut port,
                check.market_name.clone(),
                check.token_id.clone(),
                check.condition_id.clone(),
                check.entry_price,
                config.position_size_usd,
                check.spot_price,
                0.0, // strike_price not applicable for 5m/15m
                underlying,
                is_upside,
                true, // holding_yes = aligned with direction
                window_start_ts,
                window_end_ts,
            ) {
                Ok(id) => {
                    info!(
                        id = id,
                        asset = %config.asset,
                        window = config.window,
                        side = %check.side,
                        price = check.entry_price,
                        size = config.position_size_usd,
                        "Auto-trade paper position opened"
                    );

                    let record = AutoTradeRecord {
                        timestamp: Utc::now(),
                        asset: config.asset.clone(),
                        window: config.window,
                        side: check.side.clone(),
                        entry_price: check.entry_price,
                        size_usd: config.position_size_usd,
                        mode: AutoTradeMode::Paper,
                        position_id: Some(id),
                        spot_price: check.spot_price,
                        spot_move_pct: check.spot_move_pct,
                        elapsed_pct: check.elapsed_pct,
                        condition_id: check.condition_id.clone(),
                    };

                    drop(port);
                    let mut state = auto_state.lock().await;
                    state.push_trade(record.clone());
                    if let Err(e) = state.save() {
                        warn!(error = %e, "Failed to save auto-trade state");
                    }

                    Some(record)
                }
                Err(e) => {
                    warn!(error = %e, "Auto-trade paper position failed");
                    None
                }
            }
        }

        AutoTradeMode::Live => {
            let executor = match live_executor {
                Some(e) => e,
                None => {
                    warn!("Live auto-trade: no LiveExecutor available. Skipping.");
                    return None;
                }
            };

            // Risk check
            if let Some(rm) = risk_manager {
                let rm_guard = rm.lock().await;
                if let Err(rejection) = rm_guard.can_trade() {
                    warn!(reason = %rejection, "Live auto-trade blocked by risk manager");
                    return None;
                }
            }

            // Preload token metadata for faster order building
            if let Err(e) = executor.preload_token_metadata(&check.token_id).await {
                warn!(error = %e, "Failed to preload token metadata");
            }

            // Place entry order
            let size_tokens = config.position_size_usd / check.entry_price;
            match executor.place_entry_order(
                &check.token_id,
                check.entry_price,
                size_tokens,
                None,
                false, // live mode = real execution
            ).await {
                Ok(result) if result.filled => {
                    info!(
                        order_id = %result.order_id,
                        asset = %config.asset,
                        window = config.window,
                        side = %check.side,
                        price = result.avg_price,
                        size = config.position_size_usd,
                        "Live auto-trade order filled"
                    );

                    // Record in risk manager
                    if let Some(rm) = risk_manager {
                        rm.lock().await.record_position_opened();
                    }

                    let record = AutoTradeRecord {
                        timestamp: Utc::now(),
                        asset: config.asset.clone(),
                        window: config.window,
                        side: check.side.clone(),
                        entry_price: result.avg_price,
                        size_usd: config.position_size_usd,
                        mode: AutoTradeMode::Live,
                        position_id: None, // Live orders tracked by order_id, not paper position
                        spot_price: check.spot_price,
                        spot_move_pct: check.spot_move_pct,
                        elapsed_pct: check.elapsed_pct,
                        condition_id: check.condition_id.clone(),
                    };

                    let mut state = auto_state.lock().await;
                    state.push_trade(record.clone());
                    if let Err(e) = state.save() {
                        warn!(error = %e, "Failed to save auto-trade state");
                    }

                    Some(record)
                }
                Ok(result) => {
                    warn!(
                        order_id = %result.order_id,
                        status = %result.status,
                        "Live auto-trade order not filled"
                    );
                    None
                }
                Err(e) => {
                    warn!(error = %e, "Live auto-trade order failed");
                    None
                }
            }
        }
    }
}

// ──────────────────────────── Window Start Spot Cache ────────────────────────────

use std::sync::OnceLock;
use tokio::sync::Mutex as TokioMutex;

/// Global cache for spot prices at window start times.
/// Key: "{asset}_{window_start_ts}", Value: spot price at (or near) window start.
static WINDOW_SPOT_CACHE: OnceLock<TokioMutex<HashMap<String, f64>>> = OnceLock::new();

fn window_spot_cache() -> &'static TokioMutex<HashMap<String, f64>> {
    WINDOW_SPOT_CACHE.get_or_init(|| TokioMutex::new(HashMap::new()))
}

/// Record the current spot price for a window if we haven't already.
/// Called early in the window (before entry zone) so we have a reference price.
pub async fn cache_window_spot_price(asset: &str, window_start_ts: i64, spot_price: f64) {
    let key = format!("{}_{}", asset, window_start_ts);
    let mut cache = window_spot_cache().lock().await;
    cache.entry(key).or_insert(spot_price);

    // Prune old entries (older than 30 minutes)
    let cutoff_ts = Utc::now().timestamp() - 1800;
    cache.retain(|k, _| {
        k.rsplit('_')
            .next()
            .and_then(|ts| ts.parse::<i64>().ok())
            .map(|ts| ts > cutoff_ts)
            .unwrap_or(false)
    });
}

/// Get cached spot price at window start.
pub async fn get_window_spot_price(asset: &str, window_start_ts: i64) -> Option<f64> {
    let key = format!("{}_{}", asset, window_start_ts);
    let cache = window_spot_cache().lock().await;
    cache.get(&key).copied()
}

/// Enhanced version of run_auto_trade_checks that uses the window spot cache.
pub async fn run_auto_trade_checks_with_cache(
    state: &SharedState,
    auto_state: &SharedAutoTradeState,
    portfolio: &Arc<Mutex<Portfolio>>,
    live_executor: Option<&crate::core::execution::live::LiveExecutor>,
    risk_manager: Option<&Arc<Mutex<crate::core::execution::risk::RiskManager>>>,
) {
    let configs: Vec<AutoTradeConfig> = {
        let at = auto_state.lock().await;
        at.enabled_configs().into_iter().cloned().collect()
    };

    if configs.is_empty() {
        return;
    }

    // First pass: cache spot prices for all active windows
    {
        let s = state.read().await;
        for config in &configs {
            let key_with_m = format!("{}_{}m", config.asset, config.window);
            let now = Utc::now().timestamp();

            if let Some(markets) = s.short_term_markets.get(&key_with_m) {
                for market in markets {
                    let end = market.window_start_ts + (market.interval as i64 * 60);
                    if now >= market.window_start_ts && now < end {
                        let spot_sym = format!("{}USDT", config.asset.to_uppercase());
                        if let Some(sp) = s.spot_prices.get(&spot_sym) {
                            cache_window_spot_price(
                                &config.asset,
                                market.window_start_ts,
                                sp.price,
                            )
                            .await;
                        }
                    }
                }
            }
        }
    }

    // Second pass: check entry conditions
    let mut s = state.read().await;
    let now = Utc::now().timestamp();

    for config in &configs {
        let key_with_m = format!("{}_{}m", config.asset, config.window);

        let markets = match s.short_term_markets.get(&key_with_m) {
            Some(m) => m,
            None => continue,
        };

        let active = markets.iter().find(|m| {
            let end = m.window_start_ts + (m.interval as i64 * 60);
            now >= m.window_start_ts && now < end
        });

        let market = match active {
            Some(m) => m,
            None => continue,
        };

        let already_traded = {
            let at = auto_state.lock().await;
            at.already_traded(&market.condition_id)
        };

        let up_book = s.order_books.get(&market.up_token_id);
        let down_book = s.order_books.get(&market.down_token_id);

        let make_book_data = |book: Option<&crate::core::monitor::state::TimestampedOrderBook>| -> SideBookData {
            match book {
                Some(b) => {
                    let assessment = microstructure::assess_tradeability(&b.book);
                    SideBookData {
                        best_ask: b.book.best_ask(),
                        tradeability: assessment.as_ref().map(|a| a.rating),
                        spread_pct: assessment.as_ref().map(|a| a.spread_pct),
                        ask_depth_usd: assessment.as_ref().map(|a| a.ask_depth_usd),
                    }
                }
                None => SideBookData::default(),
            }
        };

        let up_data = make_book_data(up_book);
        let down_data = make_book_data(down_book);

        let spot_sym = format!("{}USDT", config.asset.to_uppercase());
        let spot_price = match s.spot_prices.get(&spot_sym) {
            Some(p) => p.price,
            None => continue,
        };

        let spot_at_start =
            get_window_spot_price(&config.asset, market.window_start_ts).await;

        let interval_secs = market.interval as i64 * 60;

        // Check total exposure if configured
        if let Some(max_exposure) = config.max_total_exposure_usd {
            let port = portfolio.lock().await;
            let open_exposure: f64 = port
                .open_positions()
                .iter()
                .map(|p| p.size_usd)
                .sum();
            if open_exposure + config.position_size_usd > max_exposure {
                debug!(
                    asset = %config.asset,
                    window = config.window,
                    open_exposure = format!("${:.0}", open_exposure),
                    max = format!("${:.0}", max_exposure),
                    "Auto-trade skip: would exceed max total exposure"
                );
                continue;
            }
        }

        let check = check_entry_conditions(
            config,
            market.window_start_ts,
            interval_secs,
            &market.up_token_id,
            &market.down_token_id,
            &market.condition_id,
            &up_data,
            &down_data,
            spot_price,
            spot_at_start,
            already_traded,
        );

        if check.should_enter {
            info!(
                asset = %config.asset,
                window = config.window,
                side = %check.side,
                price = check.entry_price,
                elapsed = format!("{:.0}%", check.elapsed_pct),
                spot_move = format!("{:.3}%", check.spot_move_pct),
                "Auto-trade entry conditions met"
            );

            // Must drop the read lock before execute_auto_trade acquires the portfolio lock
            drop(s);
            execute_auto_trade(
                config,
                &check,
                auto_state,
                portfolio,
                live_executor,
                risk_manager,
            ).await;
            // Re-acquire read lock for remaining configs
            s = state.read().await;
        } else if let Some(reason) = &check.reason_skip {
            debug!(
                asset = %config.asset,
                window = config.window,
                reason = %reason,
                "Auto-trade skip"
            );
        }
    }

    // Periodic cleanup
    {
        let mut at = auto_state.lock().await;
        at.prune_old_conditions();
    }
}

// ──────────────────────────── Tests ────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> AutoTradeConfig {
        AutoTradeConfig {
            asset: "btc".to_string(),
            window: 5,
            enabled: true,
            entry_pct: 85.0,
            min_move_pct: 0.07,
            max_entry_price: 0.75,
            position_size_usd: 100.0,
            mode: AutoTradeMode::Paper,
            max_spread_pct: None,
            min_depth_usd: None,
            side: AutoTradeSide::Auto,
            max_total_exposure_usd: None,
        }
    }

    fn good_book(ask: f64) -> SideBookData {
        SideBookData {
            best_ask: Some(ask),
            tradeability: Some(TradeabilityRating::Good),
            spread_pct: Some(2.0),
            ask_depth_usd: Some(200.0),
        }
    }

    #[test]
    fn test_check_entry_all_pass() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.65),
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            false,
        );

        assert!(result.should_enter);
        assert_eq!(result.side, "UP");
        assert_eq!(result.token_id, "up_token");
        assert_eq!(result.entry_price, 0.65);
        assert!(result.elapsed_pct >= 85.0);
        assert!(result.spot_move_pct >= 0.07);
    }

    #[test]
    fn test_check_entry_too_early() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 120;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.65),
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Elapsed"));
    }

    #[test]
    fn test_check_entry_insufficient_move() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.52),
            &good_book(0.48),
            84010.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Spot move"));
    }

    #[test]
    fn test_check_entry_price_too_high() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.85),
            &good_book(0.15),
            84200.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Entry price"));
    }

    #[test]
    fn test_check_entry_untradeable() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let up_data = SideBookData {
            best_ask: Some(0.65),
            tradeability: Some(TradeabilityRating::Untradeable),
            spread_pct: Some(2.0),
            ask_depth_usd: Some(200.0),
        };

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &up_data,
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("untradeable"));
    }

    #[test]
    fn test_check_entry_already_traded() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.65),
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            true,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Already traded"));
    }

    #[test]
    fn test_check_entry_down_direction() {
        let config = default_config();
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.35),
            &good_book(0.65),
            83900.0,
            Some(84000.0),
            false,
        );

        assert!(result.should_enter);
        assert_eq!(result.side, "DOWN");
        assert_eq!(result.token_id, "down_token");
        assert_eq!(result.entry_price, 0.65);
    }

    #[test]
    fn test_check_entry_spread_too_wide() {
        let mut config = default_config();
        config.max_spread_pct = Some(3.0);
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let up_data = SideBookData {
            best_ask: Some(0.65),
            tradeability: Some(TradeabilityRating::Good),
            spread_pct: Some(5.0), // wider than 3% max
            ask_depth_usd: Some(200.0),
        };

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &up_data,
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Spread"));
    }

    #[test]
    fn test_check_entry_depth_too_low() {
        let mut config = default_config();
        config.min_depth_usd = Some(100.0);
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        let up_data = SideBookData {
            best_ask: Some(0.65),
            tradeability: Some(TradeabilityRating::Good),
            spread_pct: Some(2.0),
            ask_depth_usd: Some(30.0), // less than $100 min
        };

        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &up_data,
            &good_book(0.35),
            84100.0,
            Some(84000.0),
            false,
        );

        assert!(!result.should_enter);
        assert!(result.reason_skip.unwrap().contains("Depth"));
    }

    #[test]
    fn test_check_entry_forced_side_up() {
        let mut config = default_config();
        config.side = AutoTradeSide::Up;
        let now = Utc::now().timestamp();
        let window_start = now - 270;

        // Spot moved DOWN, but side forced to UP
        let result = check_entry_conditions(
            &config,
            window_start,
            300,
            "up_token",
            "down_token",
            "cond_123",
            &good_book(0.65),
            &good_book(0.35),
            83900.0, // spot moved down
            Some(84000.0),
            false,
        );

        assert!(result.should_enter);
        assert_eq!(result.side, "UP"); // forced UP despite spot down
    }

    #[test]
    fn test_auto_trade_config_key() {
        let config = AutoTradeConfig {
            asset: "eth".to_string(),
            window: 15,
            enabled: true,
            entry_pct: 90.0,
            min_move_pct: 0.05,
            max_entry_price: 0.70,
            position_size_usd: 50.0,
            mode: AutoTradeMode::Paper,
            max_spread_pct: None,
            min_depth_usd: None,
            side: AutoTradeSide::Auto,
            max_total_exposure_usd: None,
        };
        assert_eq!(config.key(), "eth_15");
    }

    #[test]
    fn test_auto_trade_state_set_config() {
        let mut state = AutoTradeState::default();

        let config1 = default_config();
        state.set_config(config1.clone());
        assert_eq!(state.configs.len(), 1);

        // Update same key
        let mut config2 = default_config();
        config2.position_size_usd = 200.0;
        state.set_config(config2);
        assert_eq!(state.configs.len(), 1);
        assert_eq!(state.configs[0].position_size_usd, 200.0);

        // Different key
        let mut config3 = default_config();
        config3.asset = "eth".to_string();
        state.set_config(config3);
        assert_eq!(state.configs.len(), 2);
    }

    #[test]
    fn test_auto_trade_state_already_traded() {
        let mut state = AutoTradeState::default();
        assert!(!state.already_traded("cond_1"));

        state
            .traded_conditions
            .insert("cond_1".to_string(), Utc::now());
        assert!(state.already_traded("cond_1"));
        assert!(!state.already_traded("cond_2"));
    }

    #[test]
    fn test_auto_trade_state_prune_conditions() {
        let mut state = AutoTradeState::default();
        let old_time = Utc::now() - chrono::Duration::hours(3);
        let recent_time = Utc::now();

        state
            .traded_conditions
            .insert("old".to_string(), old_time);
        state
            .traded_conditions
            .insert("recent".to_string(), recent_time);

        state.prune_old_conditions();
        assert!(!state.already_traded("old"));
        assert!(state.already_traded("recent"));
    }

    #[test]
    fn test_auto_trade_state_persistence() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("auto_trade.json");

        let mut state = AutoTradeState::load_from_path(path.clone()).unwrap();
        state.set_config(default_config());
        state.push_trade(AutoTradeRecord {
            timestamp: Utc::now(),
            asset: "btc".to_string(),
            window: 5,
            side: "UP".to_string(),
            entry_price: 0.65,
            size_usd: 100.0,
            mode: AutoTradeMode::Paper,
            position_id: Some(1),
            spot_price: 84000.0,
            spot_move_pct: 0.12,
            elapsed_pct: 90.0,
            condition_id: "cond_1".to_string(),
        });
        state.save().unwrap();

        // Reload
        let loaded = AutoTradeState::load_from_path(path).unwrap();
        assert_eq!(loaded.configs.len(), 1);
        assert_eq!(loaded.recent_trades.len(), 1);
        assert_eq!(loaded.recent_trades[0].side, "UP");
        assert!(loaded.already_traded("cond_1"));
    }

    #[test]
    fn test_recent_trades_ring_buffer() {
        let mut state = AutoTradeState::default();
        for i in 0..MAX_RECENT_TRADES + 50 {
            state.push_trade(AutoTradeRecord {
                timestamp: Utc::now(),
                asset: "btc".to_string(),
                window: 5,
                side: "UP".to_string(),
                entry_price: 0.60,
                size_usd: 100.0,
                mode: AutoTradeMode::Paper,
                position_id: Some(i as u64),
                spot_price: 84000.0,
                spot_move_pct: 0.1,
                elapsed_pct: 90.0,
                condition_id: format!("cond_{i}"),
            });
        }
        assert_eq!(state.recent_trades.len(), MAX_RECENT_TRADES);
    }

    #[test]
    fn test_enabled_configs() {
        let mut state = AutoTradeState::default();

        let mut c1 = default_config();
        c1.enabled = true;
        state.set_config(c1);

        let mut c2 = default_config();
        c2.asset = "eth".to_string();
        c2.enabled = false;
        state.set_config(c2);

        assert_eq!(state.enabled_configs().len(), 1);
        assert_eq!(state.enabled_configs()[0].asset, "btc");
    }

    #[test]
    fn test_auto_trade_mode_display() {
        assert_eq!(format!("{}", AutoTradeMode::Paper), "paper");
        assert_eq!(format!("{}", AutoTradeMode::Live), "live");
        assert_eq!(format!("{}", AutoTradeMode::Live), "live");
    }

    #[test]
    fn test_recent_trades_for_filter() {
        let mut state = AutoTradeState::default();
        let make_record = |asset: &str, window: u32| AutoTradeRecord {
            timestamp: Utc::now(),
            asset: asset.to_string(),
            window,
            side: "UP".to_string(),
            entry_price: 0.60,
            size_usd: 100.0,
            mode: AutoTradeMode::Paper,
            position_id: Some(1),
            spot_price: 84000.0,
            spot_move_pct: 0.1,
            elapsed_pct: 90.0,
            condition_id: "c".to_string(),
        };

        state.push_trade(make_record("btc", 5));
        state.push_trade(make_record("btc", 15));
        state.push_trade(make_record("eth", 5));

        assert_eq!(state.recent_trades_for(None, None).len(), 3);
        assert_eq!(state.recent_trades_for(Some("btc"), None).len(), 2);
        assert_eq!(state.recent_trades_for(Some("btc"), Some(5)).len(), 1);
        assert_eq!(state.recent_trades_for(None, Some(5)).len(), 2);
        assert_eq!(state.recent_trades_for(Some("sol"), None).len(), 0);
    }
}
