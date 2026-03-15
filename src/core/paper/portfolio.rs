// Paper trading portfolio — JSON persistence and P&L statistics.
// Persists to paper_portfolio.json in the config directory.
// Supports stats by asset, price bucket, and overall.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info};

use super::engine::{PaperPosition, PositionStatus};

/// Portfolio stored on disk as JSON.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Portfolio {
    pub positions: Vec<PaperPosition>,
    #[serde(skip)]
    pub file_path: PathBuf,
}

impl Portfolio {
    /// Create a new portfolio using the platform config directory.
    pub fn new() -> Result<Self> {
        let path = config_portfolio_path()?;
        Ok(Self::new_with_path(path))
    }

    /// Create a portfolio at a specific path (used for testing).
    pub fn new_with_path(path: PathBuf) -> Self {
        Self {
            positions: Vec::new(),
            file_path: path,
        }
    }

    /// Load portfolio from the platform config directory.
    /// Returns empty portfolio if the file doesn't exist yet.
    pub fn load() -> Result<Self> {
        let path = config_portfolio_path()?;
        Self::load_from_path(path)
    }

    /// Load from a specific path. Returns empty portfolio if file missing.
    pub fn load_from_path(path: PathBuf) -> Result<Self> {
        if !path.exists() {
            debug!(path = %path.display(), "No portfolio file found, starting fresh");
            return Ok(Self::new_with_path(path));
        }

        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read portfolio: {}", path.display()))?;

        let mut portfolio: Portfolio = serde_json::from_str(&data)
            .with_context(|| format!("Failed to parse portfolio JSON: {}", path.display()))?;

        portfolio.file_path = path;
        info!(
            positions = portfolio.positions.len(),
            "Portfolio loaded from disk"
        );

        Ok(portfolio)
    }

    /// Persist portfolio to disk as JSON (atomic write via temp file + rename).
    pub fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self)
            .context("Failed to serialize portfolio")?;

        crate::core::infrastructure::atomic::atomic_write(&self.file_path, json.as_bytes())?;

        debug!(
            path = %self.file_path.display(),
            positions = self.positions.len(),
            "Portfolio saved"
        );

        Ok(())
    }

    // --- Query methods ---

    pub fn open_positions(&self) -> Vec<&PaperPosition> {
        self.positions
            .iter()
            .filter(|p| p.status == PositionStatus::Open)
            .collect()
    }

    pub fn settled_positions(&self) -> Vec<&PaperPosition> {
        self.positions
            .iter()
            .filter(|p| {
                p.status == PositionStatus::SettledWon
                    || p.status == PositionStatus::SettledLost
                    || p.status == PositionStatus::ClosedEarly
            })
            .collect()
    }

    pub fn position_by_id(&self, id: u64) -> Option<&PaperPosition> {
        self.positions.iter().find(|p| p.id == id)
    }

    // --- Statistics ---

    pub fn stats(&self) -> PortfolioStats {
        let settled: Vec<&PaperPosition> = self.settled_positions();

        if settled.is_empty() {
            return PortfolioStats::default();
        }

        let total_trades = settled.len();
        let wins = settled
            .iter()
            .filter(|p| {
                p.status == PositionStatus::SettledWon
                    || (p.status == PositionStatus::ClosedEarly && p.pnl.unwrap_or(0.0) > 0.0)
            })
            .count();
        let win_rate = wins as f64 / total_trades as f64;

        let pnls: Vec<f64> = settled.iter().filter_map(|p| p.pnl).collect();
        let total_pnl: f64 = pnls.iter().sum();
        let avg_pnl = total_pnl / pnls.len() as f64;
        let best_pnl = pnls.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let worst_pnl = pnls.iter().cloned().fold(f64::INFINITY, f64::min);

        let total_volume: f64 = settled.iter().map(|p| p.size_usd).sum();
        let total_fees: f64 = settled.iter().map(|p| p.entry_fee).sum();

        // Stats by asset
        let by_asset = self.stats_by_group(|p| p.underlying.clone());

        // Stats by price bucket (0.0-0.3, 0.3-0.5, 0.5-0.7, 0.7-1.0)
        let by_price_bucket = self.stats_by_group(|p| price_bucket(p.entry_price));

        PortfolioStats {
            total_trades,
            wins,
            win_rate,
            total_pnl,
            avg_pnl,
            best_pnl,
            worst_pnl,
            total_volume,
            total_fees,
            open_count: self.open_positions().len(),
            by_asset,
            by_price_bucket,
        }
    }

    fn stats_by_group<F>(&self, key_fn: F) -> HashMap<String, GroupStats>
    where
        F: Fn(&PaperPosition) -> String,
    {
        let settled = self.settled_positions();
        let mut groups: HashMap<String, Vec<&PaperPosition>> = HashMap::new();

        for pos in &settled {
            groups.entry(key_fn(pos)).or_default().push(pos);
        }

        groups
            .into_iter()
            .map(|(key, positions)| {
                let total = positions.len();
                let wins = positions
                    .iter()
                    .filter(|p| {
                        p.status == PositionStatus::SettledWon
                            || (p.status == PositionStatus::ClosedEarly
                                && p.pnl.unwrap_or(0.0) > 0.0)
                    })
                    .count();
                let pnls: Vec<f64> = positions.iter().filter_map(|p| p.pnl).collect();
                let total_pnl: f64 = pnls.iter().sum();

                (
                    key,
                    GroupStats {
                        total_trades: total,
                        wins,
                        win_rate: if total > 0 {
                            wins as f64 / total as f64
                        } else {
                            0.0
                        },
                        total_pnl,
                    },
                )
            })
            .collect()
    }
}

fn price_bucket(price: f64) -> String {
    if price < 0.3 {
        "0.00-0.30".to_string()
    } else if price < 0.5 {
        "0.30-0.50".to_string()
    } else if price < 0.7 {
        "0.50-0.70".to_string()
    } else {
        "0.70-1.00".to_string()
    }
}

/// Get the config directory path for portfolio file.
fn config_portfolio_path() -> Result<PathBuf> {
    let config_dir = dirs::config_dir()
        .context("Could not determine config directory")?
        .join("crypto-conduit");
    Ok(config_dir.join("paper_portfolio.json"))
}

#[derive(Debug, Clone, Default)]
pub struct PortfolioStats {
    pub total_trades: usize,
    pub wins: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_pnl: f64,
    pub best_pnl: f64,
    pub worst_pnl: f64,
    pub total_volume: f64,
    pub total_fees: f64,
    pub open_count: usize,
    pub by_asset: HashMap<String, GroupStats>,
    pub by_price_bucket: HashMap<String, GroupStats>,
}

#[derive(Debug, Clone)]
pub struct GroupStats {
    pub total_trades: usize,
    pub wins: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::paper::engine::{open_position, PaperPosition, PositionStatus};
    use crate::core::types::{Direction, Side};
    use chrono::Utc;
    use tempfile::TempDir;

    fn test_portfolio(dir: &TempDir) -> Portfolio {
        Portfolio::new_with_path(dir.path().join("paper_portfolio.json"))
    }

    fn make_settled_position(
        id: u64,
        entry_price: f64,
        size_usd: f64,
        underlying: &str,
        won: bool,
    ) -> PaperPosition {
        let quantity = size_usd / entry_price;
        let fee = crate::core::analysis::fees::polymarket_fee(entry_price) * quantity;
        let pnl = if won {
            quantity - size_usd - fee
        } else {
            -(size_usd + fee)
        };
        PaperPosition {
            id,
            market_name: format!("Test Market {id}"),
            token_id: format!("token_{id}"),
            condition_id: format!("cond_{id}"),
            side: Side::Buy,
            entry_price,
            quantity,
            size_usd,
            entry_time: Utc::now(),
            entry_spot_price: 84000.0,
            entry_fee: fee,
            strike_price: 84500.0,
            underlying: underlying.to_string(),
            is_upside: true,
            holding_yes: true,
            status: if won {
                PositionStatus::SettledWon
            } else {
                PositionStatus::SettledLost
            },
            settled_at: Some(Utc::now()),
            settlement_outcome: Some(if won {
                Direction::Up
            } else {
                Direction::Down
            }),
            pnl: Some(pnl),
            exit_price: None,
            exit_fee: None,
            window_start_ts: None,
            window_end_ts: None,
        }
    }

    #[test]
    fn test_empty_portfolio_stats() {
        let dir = TempDir::new().unwrap();
        let portfolio = test_portfolio(&dir);
        let stats = portfolio.stats();
        assert_eq!(stats.total_trades, 0);
        assert_eq!(stats.win_rate, 0.0);
        assert_eq!(stats.total_pnl, 0.0);
    }

    #[test]
    fn test_portfolio_save_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        open_position(
            &mut portfolio,
            "BTC 5m UP".to_string(),
            "tok1".to_string(),
            "cond1".to_string(),
            0.55,
            100.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        open_position(
            &mut portfolio,
            "ETH 5m DOWN".to_string(),
            "tok2".to_string(),
            "cond2".to_string(),
            0.40,
            50.0,
            3000.0,
            3050.0,
            "ETHUSDT".to_string(),
            false,
            false,
            None,
            None,
        )
        .unwrap();

        // Reload
        let loaded =
            Portfolio::load_from_path(dir.path().join("paper_portfolio.json")).unwrap();
        assert_eq!(loaded.positions.len(), 2);
        assert_eq!(loaded.positions[0].id, 1);
        assert_eq!(loaded.positions[1].id, 2);
        assert_eq!(loaded.positions[1].holding_yes, false);
    }

    #[test]
    fn test_stats_with_settled_positions() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        // 3 wins, 2 losses
        portfolio.positions.push(make_settled_position(1, 0.55, 100.0, "BTCUSDT", true));
        portfolio.positions.push(make_settled_position(2, 0.60, 80.0, "BTCUSDT", true));
        portfolio.positions.push(make_settled_position(3, 0.45, 50.0, "ETHUSDT", true));
        portfolio.positions.push(make_settled_position(4, 0.50, 100.0, "BTCUSDT", false));
        portfolio.positions.push(make_settled_position(5, 0.70, 60.0, "ETHUSDT", false));

        let stats = portfolio.stats();
        assert_eq!(stats.total_trades, 5);
        assert_eq!(stats.wins, 3);
        assert!((stats.win_rate - 0.60).abs() < 1e-8);
        assert!(stats.best_pnl > 0.0);
        assert!(stats.worst_pnl < 0.0);
        assert!(stats.total_volume > 0.0);
        assert!(stats.total_fees > 0.0);

        // By asset
        assert!(stats.by_asset.contains_key("BTCUSDT"));
        assert!(stats.by_asset.contains_key("ETHUSDT"));
        let btc_stats = &stats.by_asset["BTCUSDT"];
        assert_eq!(btc_stats.total_trades, 3);
        assert_eq!(btc_stats.wins, 2);

        let eth_stats = &stats.by_asset["ETHUSDT"];
        assert_eq!(eth_stats.total_trades, 2);
        assert_eq!(eth_stats.wins, 1);
    }

    #[test]
    fn test_stats_by_price_bucket() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        portfolio.positions.push(make_settled_position(1, 0.20, 50.0, "BTCUSDT", true));
        portfolio.positions.push(make_settled_position(2, 0.40, 50.0, "BTCUSDT", false));
        portfolio.positions.push(make_settled_position(3, 0.60, 50.0, "BTCUSDT", true));
        portfolio.positions.push(make_settled_position(4, 0.80, 50.0, "BTCUSDT", false));

        let stats = portfolio.stats();
        assert!(stats.by_price_bucket.contains_key("0.00-0.30"));
        assert!(stats.by_price_bucket.contains_key("0.30-0.50"));
        assert!(stats.by_price_bucket.contains_key("0.50-0.70"));
        assert!(stats.by_price_bucket.contains_key("0.70-1.00"));

        assert_eq!(stats.by_price_bucket["0.00-0.30"].total_trades, 1);
        assert_eq!(stats.by_price_bucket["0.00-0.30"].wins, 1);
    }

    #[test]
    fn test_open_and_settled_queries() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        // Add mix of open and settled
        open_position(
            &mut portfolio,
            "Open1".to_string(),
            "t".to_string(),
            "c".to_string(),
            0.55,
            100.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        portfolio.positions.push(make_settled_position(10, 0.60, 80.0, "BTCUSDT", true));

        assert_eq!(portfolio.open_positions().len(), 1);
        assert_eq!(portfolio.settled_positions().len(), 1);
        assert!(portfolio.position_by_id(1).is_some());
        assert!(portfolio.position_by_id(10).is_some());
        assert!(portfolio.position_by_id(99).is_none());
    }

    #[test]
    fn test_price_bucket_boundaries() {
        assert_eq!(price_bucket(0.0), "0.00-0.30");
        assert_eq!(price_bucket(0.29), "0.00-0.30");
        assert_eq!(price_bucket(0.30), "0.30-0.50");
        assert_eq!(price_bucket(0.49), "0.30-0.50");
        assert_eq!(price_bucket(0.50), "0.50-0.70");
        assert_eq!(price_bucket(0.69), "0.50-0.70");
        assert_eq!(price_bucket(0.70), "0.70-1.00");
        assert_eq!(price_bucket(0.99), "0.70-1.00");
    }

    #[test]
    fn test_load_nonexistent_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.json");
        let portfolio = Portfolio::load_from_path(path).unwrap();
        assert!(portfolio.positions.is_empty());
    }
}
