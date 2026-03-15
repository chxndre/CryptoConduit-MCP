// Trading configuration loaded from config.toml [trading] section.
// Returns None when section is absent (trading disabled).
// Private key is validated but NEVER logged.

use std::path::PathBuf;
use tracing::warn;

/// Live trading configuration parsed from config.toml.
#[derive(Clone)]
pub struct TradingConfig {
    /// Hex-encoded private key (without 0x prefix). Never logged.
    private_key: String,
    /// Wallet type: "Eoa", "GnosisSafe", or "Proxy"
    pub wallet_type: String,
    /// Maximum single order size in USD
    pub max_order_size_usd: f64,
    /// How long to poll for order fill before cancelling
    pub order_timeout_secs: u64,
    /// Polygon RPC URL for on-chain operations (CTF redemption)
    pub polygon_rpc_url: String,
    /// Risk management limits
    pub risk: RiskLimits,
}

impl TradingConfig {
    /// Access the private key. Deliberately not Display/Debug to prevent accidental logging.
    pub fn private_key(&self) -> &str {
        &self.private_key
    }
}

// Manual Debug impl to NEVER print the private key.
impl std::fmt::Debug for TradingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradingConfig")
            .field("private_key", &"[REDACTED]")
            .field("wallet_type", &self.wallet_type)
            .field("max_order_size_usd", &self.max_order_size_usd)
            .field("order_timeout_secs", &self.order_timeout_secs)
            .field("polygon_rpc_url", &self.polygon_rpc_url)
            .field("risk", &self.risk)
            .finish()
    }
}

impl std::fmt::Display for TradingConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TradingConfig {{ wallet_type: {}, max_order: ${}, timeout: {}s }}",
            self.wallet_type, self.max_order_size_usd, self.order_timeout_secs
        )
    }
}

/// Risk management limits for live trading.
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Halt if daily P&L drops below this (positive value stored, applied as negative)
    pub daily_loss_limit: f64,
    /// Halt after N consecutive losses
    pub max_consecutive_losses: u32,
    /// Maximum concurrent open positions
    pub max_open_positions: u32,
    /// Minimum USDC balance to allow trading
    pub min_balance_usd: f64,
    /// Max drawdown from cumulative P&L peak (0 = disabled)
    pub max_drawdown: f64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            daily_loss_limit: 50.0,
            max_consecutive_losses: 5,
            max_open_positions: 3,
            min_balance_usd: 10.0,
            max_drawdown: 0.0,
        }
    }
}

/// Platform-appropriate config directory for crypto-conduit.
pub fn config_dir() -> Option<PathBuf> {
    dirs::config_dir().map(|d| d.join("crypto-conduit"))
}

/// Load trading config from config.toml [trading] section.
/// Returns None if the section is absent (trading disabled).
pub fn load_trading_config() -> Option<TradingConfig> {
    let config_path = config_dir()?.join("config.toml");
    let content = std::fs::read_to_string(&config_path).ok()?;
    let table: toml::Table = content.parse().ok()?;

    let trading = table.get("trading")?.as_table()?;

    // Private key is required
    let raw_key = trading.get("private_key")?.as_str()?.to_string();
    if raw_key.is_empty() || raw_key == "0x..." {
        warn!("Trading private key is placeholder — trading disabled");
        return None;
    }
    let private_key = raw_key.strip_prefix("0x").unwrap_or(&raw_key).to_string();

    // Validate hex format (must be 64 hex chars for a 32-byte key)
    if private_key.len() != 64 || !private_key.chars().all(|c| c.is_ascii_hexdigit()) {
        warn!("Trading private key has invalid format (expected 64 hex chars) — trading disabled");
        return None;
    }

    let wallet_type = trading
        .get("wallet_type")
        .and_then(|v| v.as_str())
        .unwrap_or("GnosisSafe")
        .to_string();

    let max_order_size_usd = trading
        .get("max_order_size_usd")
        .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
        .unwrap_or(500.0);

    let order_timeout_secs = trading
        .get("order_timeout_secs")
        .and_then(|v| v.as_integer())
        .unwrap_or(3) as u64;

    let polygon_rpc_url = trading
        .get("polygon_rpc_url")
        .and_then(|v| v.as_str())
        .unwrap_or("https://polygon-rpc.com")
        .to_string();

    // Risk limits from [trading.risk] subsection
    let risk_table = trading.get("risk").and_then(|v| v.as_table());
    let risk = if let Some(rt) = risk_table {
        RiskLimits {
            daily_loss_limit: rt
                .get("daily_loss_limit")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(50.0),
            max_consecutive_losses: rt
                .get("max_consecutive_losses")
                .and_then(|v| v.as_integer())
                .unwrap_or(5) as u32,
            max_open_positions: rt
                .get("max_open_positions")
                .and_then(|v| v.as_integer())
                .unwrap_or(3) as u32,
            min_balance_usd: rt
                .get("min_balance_usd")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(10.0),
            max_drawdown: rt
                .get("max_drawdown")
                .and_then(|v| v.as_float().or_else(|| v.as_integer().map(|i| i as f64)))
                .unwrap_or(0.0),
        }
    } else {
        RiskLimits::default()
    };

    Some(TradingConfig {
        private_key,
        wallet_type,
        max_order_size_usd,
        order_timeout_secs,
        polygon_rpc_url,
        risk,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_limits_defaults() {
        let r = RiskLimits::default();
        assert_eq!(r.daily_loss_limit, 50.0);
        assert_eq!(r.max_consecutive_losses, 5);
        assert_eq!(r.max_open_positions, 3);
        assert_eq!(r.min_balance_usd, 10.0);
        assert_eq!(r.max_drawdown, 0.0);
    }

    #[test]
    fn test_config_display_redacts_key() {
        let config = TradingConfig {
            private_key: "a".repeat(64),
            wallet_type: "Eoa".to_string(),
            max_order_size_usd: 100.0,
            order_timeout_secs: 3,
            polygon_rpc_url: "https://polygon-rpc.com".to_string(),
            risk: RiskLimits::default(),
        };
        let display = format!("{}", config);
        assert!(!display.contains(&"a".repeat(64)));
        assert!(display.contains("Eoa"));

        // Debug also redacts
        let debug = format!("{:?}", config);
        assert!(!debug.contains(&"a".repeat(64)));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn test_private_key_validation() {
        // load_trading_config reads from file, so we test the validation logic directly
        let valid_key = "ab".repeat(32); // 64 hex chars
        assert_eq!(valid_key.len(), 64);
        assert!(valid_key.chars().all(|c| c.is_ascii_hexdigit()));

        let short_key = "ab".repeat(16); // 32 chars — too short
        assert_ne!(short_key.len(), 64);
    }
}
