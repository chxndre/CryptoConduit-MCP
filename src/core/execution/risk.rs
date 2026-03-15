// Risk manager for live trading.
// Tracks daily P&L, consecutive losses, drawdown, open positions, and balance.
// Persists state to disk across restarts.
// Extracted from market-scout with platform-appropriate paths.

use std::fmt;
use std::path::PathBuf;
use tracing::{info, warn};

use super::config;

/// Why a trade was rejected by the risk manager.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskRejection {
    DailyLossLimitHit { daily_pnl: f64, limit: f64 },
    ConsecutiveLossesExceeded { count: u32, limit: u32 },
    MaxPositionsReached { current: u32, limit: u32 },
    InsufficientBalance { balance: f64, required: f64 },
    ManuallyHalted { reason: String },
    DrawdownLimitHit { drawdown: f64, limit: f64 },
}

impl fmt::Display for RiskRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RiskRejection::DailyLossLimitHit { daily_pnl, limit } => {
                write!(f, "Daily loss limit hit: ${:.2} (limit: -${:.2})", daily_pnl, limit.abs())
            }
            RiskRejection::ConsecutiveLossesExceeded { count, limit } => {
                write!(f, "Consecutive losses exceeded: {} (limit: {})", count, limit)
            }
            RiskRejection::MaxPositionsReached { current, limit } => {
                write!(f, "Max positions reached: {} (limit: {})", current, limit)
            }
            RiskRejection::InsufficientBalance { balance, required } => {
                write!(f, "Insufficient balance: ${:.2} (need: ${:.2})", balance, required)
            }
            RiskRejection::ManuallyHalted { reason } => {
                write!(f, "Manually halted: {}", reason)
            }
            RiskRejection::DrawdownLimitHit { drawdown, limit } => {
                write!(f, "Drawdown from peak: ${:.2} (limit: ${:.2})", drawdown, limit)
            }
        }
    }
}

/// Configuration for risk controls.
#[derive(Debug, Clone)]
pub struct RiskConfig {
    /// Stop trading if daily P&L drops below this (negative value, e.g., -50.0)
    pub daily_loss_limit: f64,
    /// Halt after this many consecutive losses
    pub max_consecutive_losses: u32,
    /// Maximum concurrent open positions
    pub max_open_positions: u32,
    /// Maximum position size in USD
    pub max_position_size_usd: f64,
    /// Minimum balance required to trade
    pub min_balance_usd: f64,
    /// Max drawdown from cumulative P&L peak (0.0 = disabled)
    pub max_drawdown: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            daily_loss_limit: -50.0,
            max_consecutive_losses: 5,
            max_open_positions: 3,
            max_position_size_usd: 500.0,
            min_balance_usd: 10.0,
            max_drawdown: 0.0,
        }
    }
}

impl From<config::RiskLimits> for RiskConfig {
    fn from(limits: config::RiskLimits) -> Self {
        Self {
            daily_loss_limit: -(limits.daily_loss_limit.abs()),
            max_consecutive_losses: limits.max_consecutive_losses,
            max_open_positions: limits.max_open_positions,
            max_position_size_usd: 500.0, // not exposed in RiskLimits, use default
            min_balance_usd: limits.min_balance_usd,
            max_drawdown: limits.max_drawdown,
        }
    }
}

/// Persisted risk state — survives restarts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedState {
    /// UTC date string (YYYY-MM-DD) — daily fields only valid for this day
    date: String,
    daily_pnl: f64,
    consecutive_losses: u32,
    total_trades_today: u32,
    total_wins_today: u32,
    #[serde(default)]
    cumulative_pnl: f64,
    #[serde(default)]
    cumulative_trades: u32,
    #[serde(default)]
    cumulative_wins: u32,
    #[serde(default)]
    peak_cumulative_pnl: f64,
}

/// Tracks trading risk state and enforces limits.
pub struct RiskManager {
    config: RiskConfig,
    daily_pnl: f64,
    consecutive_losses: u32,
    open_positions: u32,
    current_balance: f64,
    halted: bool,
    halt_reason: String,
    total_trades_today: u32,
    total_wins_today: u32,
    cumulative_pnl: f64,
    cumulative_trades: u32,
    cumulative_wins: u32,
    peak_cumulative_pnl: f64,
    state_file: PathBuf,
}

impl RiskManager {
    /// Create a new RiskManager with the default state file path.
    pub fn new(config: RiskConfig, initial_balance: f64) -> Self {
        let state_file = config::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("risk_state.json");
        Self::with_state_file(config, initial_balance, state_file)
    }

    /// Create with a custom state file path (useful for testing).
    pub fn with_state_file(config: RiskConfig, initial_balance: f64, state_file: PathBuf) -> Self {
        let mut rm = Self {
            config,
            daily_pnl: 0.0,
            consecutive_losses: 0,
            open_positions: 0,
            current_balance: initial_balance,
            halted: false,
            halt_reason: String::new(),
            total_trades_today: 0,
            total_wins_today: 0,
            cumulative_pnl: 0.0,
            cumulative_trades: 0,
            cumulative_wins: 0,
            peak_cumulative_pnl: 0.0,
            state_file,
        };
        rm.load_state();

        info!(
            daily_loss_limit = rm.config.daily_loss_limit,
            max_consecutive_losses = rm.config.max_consecutive_losses,
            max_open_positions = rm.config.max_open_positions,
            min_balance = rm.config.min_balance_usd,
            max_drawdown = rm.config.max_drawdown,
            initial_balance = initial_balance,
            restored_daily_pnl = format!("{:.2}", rm.daily_pnl),
            restored_cumulative_pnl = format!("{:.2}", rm.cumulative_pnl),
            "RiskManager initialized"
        );
        rm
    }

    /// Load persisted state from disk.
    /// Daily fields only restored if same UTC day. Cumulative fields always restored.
    fn load_state(&mut self) {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        if !self.state_file.exists() {
            return;
        }
        match std::fs::read_to_string(&self.state_file) {
            Ok(content) => {
                match serde_json::from_str::<PersistedState>(&content) {
                    Ok(state) => {
                        // Cumulative fields are ALWAYS restored
                        self.cumulative_pnl = state.cumulative_pnl;
                        self.cumulative_trades = state.cumulative_trades;
                        self.cumulative_wins = state.cumulative_wins;
                        self.peak_cumulative_pnl = state.peak_cumulative_pnl;

                        if state.date == today {
                            self.daily_pnl = state.daily_pnl;
                            self.consecutive_losses = state.consecutive_losses;
                            self.total_trades_today = state.total_trades_today;
                            self.total_wins_today = state.total_wins_today;
                            info!(
                                daily_pnl = format!("{:.2}", state.daily_pnl),
                                cumulative_pnl = format!("{:.2}", state.cumulative_pnl),
                                "Restored risk state (same day)"
                            );
                        } else {
                            info!(
                                state_date = %state.date,
                                today = %today,
                                cumulative_pnl = format!("{:.2}", state.cumulative_pnl),
                                "New day — daily counters reset, cumulative P&L restored"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to parse risk state — starting fresh");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to read risk state — starting fresh");
            }
        }
    }

    /// Persist current state to disk.
    fn save_state(&self) {
        let state = PersistedState {
            date: chrono::Utc::now().format("%Y-%m-%d").to_string(),
            daily_pnl: self.daily_pnl,
            consecutive_losses: self.consecutive_losses,
            total_trades_today: self.total_trades_today,
            total_wins_today: self.total_wins_today,
            cumulative_pnl: self.cumulative_pnl,
            cumulative_trades: self.cumulative_trades,
            cumulative_wins: self.cumulative_wins,
            peak_cumulative_pnl: self.peak_cumulative_pnl,
        };
        match serde_json::to_string_pretty(&state) {
            Ok(json) => {
                if let Err(e) = crate::core::infrastructure::atomic::atomic_write(
                    &self.state_file,
                    json.as_bytes(),
                ) {
                    warn!(error = %e, "Failed to save risk state");
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to serialize risk state");
            }
        }
    }

    /// Check if a new trade is allowed. Returns Ok(()) or the reason it's blocked.
    pub fn can_trade(&self) -> Result<(), RiskRejection> {
        if self.halted {
            return Err(RiskRejection::ManuallyHalted {
                reason: self.halt_reason.clone(),
            });
        }

        if self.daily_pnl <= self.config.daily_loss_limit {
            return Err(RiskRejection::DailyLossLimitHit {
                daily_pnl: self.daily_pnl,
                limit: self.config.daily_loss_limit.abs(),
            });
        }

        if self.config.max_drawdown > 0.0 {
            let drawdown = self.peak_cumulative_pnl - self.cumulative_pnl;
            if drawdown >= self.config.max_drawdown {
                return Err(RiskRejection::DrawdownLimitHit {
                    drawdown,
                    limit: self.config.max_drawdown,
                });
            }
        }

        if self.consecutive_losses >= self.config.max_consecutive_losses {
            return Err(RiskRejection::ConsecutiveLossesExceeded {
                count: self.consecutive_losses,
                limit: self.config.max_consecutive_losses,
            });
        }

        if self.open_positions >= self.config.max_open_positions {
            return Err(RiskRejection::MaxPositionsReached {
                current: self.open_positions,
                limit: self.config.max_open_positions,
            });
        }

        if self.current_balance < self.config.min_balance_usd {
            return Err(RiskRejection::InsufficientBalance {
                balance: self.current_balance,
                required: self.config.min_balance_usd,
            });
        }

        Ok(())
    }

    /// Record that a new position was opened.
    pub fn record_position_opened(&mut self) {
        self.open_positions += 1;
    }

    /// Record that a position was closed with the given P&L.
    pub fn record_position_closed(&mut self, pnl: f64) {
        self.open_positions = self.open_positions.saturating_sub(1);
        self.daily_pnl += pnl;
        self.current_balance += pnl;
        self.total_trades_today += 1;
        self.cumulative_pnl += pnl;
        self.cumulative_trades += 1;

        if self.cumulative_pnl > self.peak_cumulative_pnl {
            self.peak_cumulative_pnl = self.cumulative_pnl;
        }

        if pnl >= 0.0 {
            self.consecutive_losses = 0;
            self.total_wins_today += 1;
            self.cumulative_wins += 1;
        } else {
            self.consecutive_losses += 1;
            if self.consecutive_losses >= self.config.max_consecutive_losses {
                warn!(
                    consecutive_losses = self.consecutive_losses,
                    "Consecutive loss limit reached — halting trading"
                );
            }
        }

        if self.daily_pnl <= self.config.daily_loss_limit {
            warn!(
                daily_pnl = format!("{:.2}", self.daily_pnl),
                limit = format!("{:.2}", self.config.daily_loss_limit),
                "Daily loss limit reached — halting trading until reset"
            );
        }

        if self.config.max_drawdown > 0.0 {
            let drawdown = self.peak_cumulative_pnl - self.cumulative_pnl;
            if drawdown >= self.config.max_drawdown {
                warn!(
                    drawdown = format!("{:.2}", drawdown),
                    limit = format!("{:.2}", self.config.max_drawdown),
                    "Drawdown limit reached — halting trading"
                );
            }
        }

        self.save_state();
    }

    /// Manually halt trading.
    pub fn halt(&mut self, reason: &str) {
        self.halted = true;
        self.halt_reason = reason.to_string();
        warn!(reason = reason, "Trading manually halted");
    }

    /// Reset daily counters (call at midnight UTC).
    pub fn daily_reset(&mut self) {
        info!(
            previous_daily_pnl = format!("{:.2}", self.daily_pnl),
            trades = self.total_trades_today,
            wins = self.total_wins_today,
            "Daily risk counters reset"
        );
        self.daily_pnl = 0.0;
        self.total_trades_today = 0;
        self.total_wins_today = 0;
        // consecutive_losses is NOT reset (intentional)
        self.save_state();
    }

    /// Get the current balance.
    pub fn balance(&self) -> f64 {
        self.current_balance
    }

    /// Update the current balance (e.g., from API query).
    pub fn update_balance(&mut self, balance: f64) {
        self.current_balance = balance;
    }

    /// Get whether trading is halted.
    pub fn is_halted(&self) -> bool {
        self.halted
            || self.daily_pnl <= self.config.daily_loss_limit
            || self.consecutive_losses >= self.config.max_consecutive_losses
            || (self.config.max_drawdown > 0.0
                && (self.peak_cumulative_pnl - self.cumulative_pnl) >= self.config.max_drawdown)
    }

    /// Get the current daily P&L.
    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    /// Get the cumulative (all-time) P&L.
    pub fn cumulative_pnl(&self) -> f64 {
        self.cumulative_pnl
    }

    /// Get cumulative win rate as a percentage (0-100).
    pub fn cumulative_win_rate(&self) -> f64 {
        if self.cumulative_trades == 0 {
            return 0.0;
        }
        (self.cumulative_wins as f64 / self.cumulative_trades as f64) * 100.0
    }

    /// Get the peak cumulative P&L (high-water mark).
    pub fn peak_cumulative_pnl(&self) -> f64 {
        self.peak_cumulative_pnl
    }

    /// Get the current drawdown from peak.
    pub fn current_drawdown(&self) -> f64 {
        self.peak_cumulative_pnl - self.cumulative_pnl
    }

    /// Get the risk config.
    pub fn config(&self) -> &RiskConfig {
        &self.config
    }

    /// Get a summary string for display.
    pub fn status_summary(&self) -> String {
        let drawdown = self.peak_cumulative_pnl - self.cumulative_pnl;
        format!(
            "DayPnL=${:.2} | CumPnL=${:.2} (peak=${:.2}, DD=${:.2}) | {} trades, {:.0}%WR | Consec.Losses={} | Pos={}/{} | Bal=${:.2} | Halted={}",
            self.daily_pnl,
            self.cumulative_pnl,
            self.peak_cumulative_pnl,
            drawdown,
            self.cumulative_trades,
            self.cumulative_win_rate(),
            self.consecutive_losses,
            self.open_positions,
            self.config.max_open_positions,
            self.current_balance,
            self.is_halted()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn test_config() -> RiskConfig {
        RiskConfig {
            daily_loss_limit: -25.0,
            max_consecutive_losses: 3,
            max_open_positions: 2,
            max_position_size_usd: 25.0,
            min_balance_usd: 50.0,
            max_drawdown: 0.0,
        }
    }

    fn test_rm(balance: f64) -> RiskManager {
        RiskManager::with_state_file(test_config(), balance, unique_state_path())
    }

    #[test]
    fn test_can_trade_all_clear() {
        let rm = test_rm(100.0);
        assert!(rm.can_trade().is_ok());
    }

    #[test]
    fn test_daily_loss_limit() {
        let mut rm = test_rm(100.0);
        rm.record_position_closed(-10.0);
        assert!(rm.can_trade().is_ok());
        rm.record_position_closed(-10.0);
        assert!(rm.can_trade().is_ok());
        rm.record_position_closed(-10.0);
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::DailyLossLimitHit { .. })
        ));
    }

    #[test]
    fn test_consecutive_losses() {
        let mut rm = test_rm(100.0);
        rm.record_position_closed(-1.0);
        rm.record_position_closed(-1.0);
        assert!(rm.can_trade().is_ok());
        rm.record_position_closed(-1.0);
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::ConsecutiveLossesExceeded { .. })
        ));
    }

    #[test]
    fn test_consecutive_losses_reset_on_win() {
        let mut rm = test_rm(100.0);
        rm.record_position_closed(-1.0);
        rm.record_position_closed(-1.0);
        rm.record_position_closed(5.0);
        assert_eq!(rm.consecutive_losses, 0);
        assert!(rm.can_trade().is_ok());
    }

    #[test]
    fn test_max_positions() {
        let mut rm = test_rm(100.0);
        rm.record_position_opened();
        assert!(rm.can_trade().is_ok());
        rm.record_position_opened();
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::MaxPositionsReached { .. })
        ));
        rm.record_position_closed(1.0);
        assert!(rm.can_trade().is_ok());
    }

    #[test]
    fn test_insufficient_balance() {
        let rm = test_rm(30.0);
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::InsufficientBalance { .. })
        ));
    }

    #[test]
    fn test_manual_halt() {
        let mut rm = test_rm(100.0);
        rm.halt("testing");
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::ManuallyHalted { .. })
        ));
        assert!(rm.is_halted());
    }

    #[test]
    fn test_daily_reset() {
        let mut rm = test_rm(100.0);
        rm.record_position_closed(-5.0);
        rm.record_position_closed(-5.0);
        assert_eq!(rm.daily_pnl(), -10.0);
        assert_eq!(rm.total_trades_today, 2);

        rm.daily_reset();
        assert_eq!(rm.daily_pnl(), 0.0);
        assert_eq!(rm.total_trades_today, 0);
        // Consecutive losses NOT reset
        assert_eq!(rm.consecutive_losses, 2);
    }

    #[test]
    fn test_balance_updates_with_pnl() {
        let mut rm = test_rm(100.0);
        rm.record_position_closed(10.0);
        assert_eq!(rm.current_balance, 110.0);
        rm.record_position_closed(-5.0);
        assert_eq!(rm.current_balance, 105.0);
    }

    #[test]
    fn test_is_halted_checks_all_conditions() {
        let mut rm = test_rm(100.0);
        assert!(!rm.is_halted());
        rm.record_position_closed(-30.0);
        assert!(rm.is_halted());
    }

    #[test]
    fn test_status_summary() {
        let rm = test_rm(100.0);
        let summary = rm.status_summary();
        assert!(summary.contains("DayPnL=$0.00"));
        assert!(summary.contains("CumPnL=$0.00"));
        assert!(summary.contains("Consec.Losses=0"));
        assert!(summary.contains("Pos=0/2"));
        assert!(summary.contains("Bal=$100.00"));
        assert!(summary.contains("Halted=false"));
    }

    #[test]
    fn test_state_persistence_same_day() {
        let mut tmp = NamedTempFile::new().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let state = format!(
            r#"{{"date":"{}","daily_pnl":-12.50,"consecutive_losses":2,"total_trades_today":5,"total_wins_today":3,"cumulative_pnl":45.00,"cumulative_trades":20,"cumulative_wins":12}}"#,
            today
        );
        write!(tmp, "{}", state).unwrap();

        let rm = RiskManager::with_state_file(test_config(), 100.0, tmp.path().to_path_buf());
        assert_eq!(rm.daily_pnl, -12.50);
        assert_eq!(rm.consecutive_losses, 2);
        assert_eq!(rm.cumulative_pnl, 45.00);
        assert_eq!(rm.cumulative_trades, 20);
    }

    #[test]
    fn test_state_persistence_different_day() {
        let mut tmp = NamedTempFile::new().unwrap();
        let state = r#"{"date":"2020-01-01","daily_pnl":-50.0,"consecutive_losses":10,"total_trades_today":20,"total_wins_today":5,"cumulative_pnl":88.50,"cumulative_trades":40,"cumulative_wins":25}"#;
        write!(tmp, "{}", state).unwrap();

        let rm = RiskManager::with_state_file(test_config(), 100.0, tmp.path().to_path_buf());
        assert_eq!(rm.daily_pnl, 0.0);
        assert_eq!(rm.consecutive_losses, 0);
        assert_eq!(rm.cumulative_pnl, 88.50);
        assert_eq!(rm.cumulative_trades, 40);
    }

    #[test]
    fn test_state_save_and_reload() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let mut rm = RiskManager::with_state_file(test_config(), 100.0, path.clone());
        rm.record_position_closed(-8.0);
        rm.record_position_closed(-3.0);
        rm.record_position_closed(5.0);

        let rm2 = RiskManager::with_state_file(test_config(), 100.0, path);
        assert_eq!(rm2.daily_pnl, -6.0);
        assert_eq!(rm2.consecutive_losses, 0);
        assert_eq!(rm2.total_trades_today, 3);
        assert_eq!(rm2.cumulative_pnl, -6.0);
    }

    // Drawdown tests

    fn unique_state_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "crypto-conduit-test-risk-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn dd_only_config(max_dd: f64) -> RiskConfig {
        RiskConfig {
            daily_loss_limit: -10000.0,
            max_consecutive_losses: 999,
            min_balance_usd: 0.0,
            max_drawdown: max_dd,
            ..test_config()
        }
    }

    #[test]
    fn test_drawdown_halt_triggers() {
        let mut rm = RiskManager::with_state_file(dd_only_config(50.0), 1000.0, unique_state_path());
        rm.record_position_closed(-50.0);
        assert!(matches!(
            rm.can_trade(),
            Err(RiskRejection::DrawdownLimitHit { .. })
        ));
        assert!(rm.is_halted());
    }

    #[test]
    fn test_drawdown_disabled_when_zero() {
        let mut rm = RiskManager::with_state_file(dd_only_config(0.0), 10000.0, unique_state_path());
        rm.record_position_closed(-500.0);
        assert!(rm.can_trade().is_ok());
    }

    #[test]
    fn test_peak_updates_correctly() {
        let mut rm = RiskManager::with_state_file(dd_only_config(100.0), 1000.0, unique_state_path());
        rm.record_position_closed(30.0);
        assert_eq!(rm.peak_cumulative_pnl, 30.0);
        rm.record_position_closed(20.0);
        assert_eq!(rm.peak_cumulative_pnl, 50.0);
        rm.record_position_closed(-40.0);
        assert_eq!(rm.peak_cumulative_pnl, 50.0);
        assert_eq!(rm.cumulative_pnl, 10.0);
        assert_eq!(rm.current_drawdown(), 40.0);
        assert!(rm.can_trade().is_ok());
    }

    #[test]
    fn test_drawdown_with_recovery() {
        let mut rm = RiskManager::with_state_file(dd_only_config(100.0), 10000.0, unique_state_path());
        rm.record_position_closed(50.0);
        rm.record_position_closed(-30.0);
        rm.record_position_closed(40.0);
        assert_eq!(rm.peak_cumulative_pnl, 60.0);
        assert_eq!(rm.current_drawdown(), 0.0);
    }

    #[test]
    fn test_from_risk_limits() {
        let limits = config::RiskLimits {
            daily_loss_limit: 75.0,
            max_consecutive_losses: 3,
            max_open_positions: 2,
            min_balance_usd: 25.0,
            max_drawdown: 100.0,
        };
        let config: RiskConfig = limits.into();
        assert_eq!(config.daily_loss_limit, -75.0);
        assert_eq!(config.max_consecutive_losses, 3);
        assert_eq!(config.max_open_positions, 2);
        assert_eq!(config.min_balance_usd, 25.0);
        assert_eq!(config.max_drawdown, 100.0);
    }
}
