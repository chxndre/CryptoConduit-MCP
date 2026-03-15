// Paper trading engine.
// Opens positions at ask price, calculates fees, persists to disk.
// Uses lazy settlement: on portfolio query, checks Gamma API for resolved outcomes.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::core::analysis::fees::polymarket_fee;
use crate::core::providers::gamma::verify_settlement_outcome;
use crate::core::types::{Direction, Side};

use super::portfolio::Portfolio;

/// Unique position counter — monotonically increasing, persisted with portfolio.
fn next_id(portfolio: &Portfolio) -> u64 {
    portfolio
        .positions
        .iter()
        .map(|p| p.id)
        .max()
        .unwrap_or(0)
        + 1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    Open,
    SettledWon,
    SettledLost,
    ClosedEarly,
    ExpiredPending,
}

impl std::fmt::Display for PositionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PositionStatus::Open => write!(f, "OPEN"),
            PositionStatus::SettledWon => write!(f, "WON"),
            PositionStatus::SettledLost => write!(f, "LOST"),
            PositionStatus::ClosedEarly => write!(f, "CLOSED"),
            PositionStatus::ExpiredPending => write!(f, "EXPIRED_PENDING"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperPosition {
    pub id: u64,
    pub market_name: String,
    pub token_id: String,
    pub condition_id: String,
    pub side: Side,
    pub entry_price: f64,
    pub quantity: f64,
    pub size_usd: f64,
    pub entry_time: DateTime<Utc>,
    pub entry_spot_price: f64,
    pub entry_fee: f64,
    pub strike_price: f64,
    pub underlying: String,
    pub is_upside: bool,
    pub holding_yes: bool,
    pub status: PositionStatus,
    pub settled_at: Option<DateTime<Utc>>,
    pub settlement_outcome: Option<Direction>,
    pub pnl: Option<f64>,
    /// Exit price (only set for early close).
    #[serde(default)]
    pub exit_price: Option<f64>,
    /// Exit fee (taker fee on sell side, only set for early close).
    #[serde(default)]
    pub exit_fee: Option<f64>,
    /// Window start timestamp (Unix seconds) for short-term markets.
    /// Used for Binance fallback settlement.
    #[serde(default)]
    pub window_start_ts: Option<i64>,
    /// Window end timestamp (Unix seconds) for short-term markets.
    /// Used for time-based expiry detection.
    #[serde(default)]
    pub window_end_ts: Option<i64>,
}

impl PaperPosition {
    /// Calculate P&L for a settled position.
    /// Settlement is fee-free. Only entry fee was paid.
    /// If holding YES and outcome is Up (YES wins): payout = quantity * $1.00
    /// If holding YES and outcome is Down (YES loses): payout = 0
    /// Gross cost = size_usd + entry_fee
    fn calculate_pnl(&self, won: bool) -> f64 {
        if won {
            // Payout is $1 per contract, cost was entry_price per contract + fee
            let payout = self.quantity; // $1 * quantity
            let cost = self.size_usd + self.entry_fee;
            payout - cost
        } else {
            // Payout is $0, lose entire cost
            let cost = self.size_usd + self.entry_fee;
            -cost
        }
    }

    /// Did we win based on the settlement outcome?
    /// The YES token wins when the outcome matches the market direction:
    ///   - is_upside=true (UP market): YES wins on Up, loses on Down
    ///   - is_upside=false (DOWN market): YES wins on Down, loses on Up
    /// For NO holdings, it's the inverse.
    fn did_win(&self, outcome: Direction) -> bool {
        let market_won = match (self.is_upside, outcome) {
            (true, Direction::Up) => true,
            (true, Direction::Down) => false,
            (false, Direction::Down) => true,
            (false, Direction::Up) => false,
        };
        if self.holding_yes { market_won } else { !market_won }
    }
}

/// Open a new paper position.
/// Entry at ask price (taker), fee calculated via polymarket_fee().
pub fn open_position(
    portfolio: &mut Portfolio,
    market_name: String,
    token_id: String,
    condition_id: String,
    entry_price: f64,
    size_usd: f64,
    spot_price: f64,
    strike_price: f64,
    underlying: String,
    is_upside: bool,
    holding_yes: bool,
    window_start_ts: Option<i64>,
    window_end_ts: Option<i64>,
) -> Result<u64> {
    if entry_price <= 0.0 || entry_price >= 1.0 {
        anyhow::bail!(
            "Invalid entry price {:.4}: must be between 0 and 1 exclusive",
            entry_price
        );
    }
    if size_usd <= 0.0 {
        anyhow::bail!("Size USD must be positive, got {:.2}", size_usd);
    }

    let quantity = size_usd / entry_price;
    let fee_per_contract = polymarket_fee(entry_price);
    let entry_fee = fee_per_contract * quantity;

    let id = next_id(portfolio);

    let position = PaperPosition {
        id,
        market_name,
        token_id,
        condition_id,
        side: Side::Buy,
        entry_price,
        quantity,
        size_usd,
        entry_time: Utc::now(),
        entry_spot_price: spot_price,
        entry_fee,
        strike_price,
        underlying,
        is_upside,
        holding_yes,
        status: PositionStatus::Open,
        settled_at: None,
        settlement_outcome: None,
        pnl: None,
        exit_price: None,
        exit_fee: None,
        window_start_ts,
        window_end_ts,
    };

    info!(
        id = id,
        market = %position.market_name,
        entry_price = position.entry_price,
        quantity = format!("{:.2}", position.quantity),
        fee = format!("{:.4}", position.entry_fee),
        "Paper position opened"
    );

    portfolio.positions.push(position);
    portfolio.save()?;

    Ok(id)
}

/// Close a position early at the given exit price (sell at bid).
/// Taker fee applies on the sell side. Returns the realized P&L.
pub fn close_position(
    portfolio: &mut Portfolio,
    position_id: u64,
    exit_price: f64,
) -> Result<f64> {
    if exit_price <= 0.0 || exit_price >= 1.0 {
        anyhow::bail!(
            "Invalid exit price {:.4}: must be between 0 and 1 exclusive",
            exit_price
        );
    }

    let pos = portfolio
        .positions
        .iter_mut()
        .find(|p| p.id == position_id)
        .context(format!("Position #{} not found", position_id))?;

    if pos.status != PositionStatus::Open {
        anyhow::bail!(
            "Position #{} is not open (status: {})",
            position_id,
            pos.status
        );
    }

    let exit_fee_per_contract = polymarket_fee(exit_price);
    let exit_fee = exit_fee_per_contract * pos.quantity;

    // P&L = (exit_price - entry_price) * quantity - entry_fee - exit_fee
    let gross_pnl = (exit_price - pos.entry_price) * pos.quantity;
    let pnl = gross_pnl - pos.entry_fee - exit_fee;

    pos.status = PositionStatus::ClosedEarly;
    pos.settled_at = Some(Utc::now());
    pos.exit_price = Some(exit_price);
    pos.exit_fee = Some(exit_fee);
    pos.pnl = Some(pnl);

    info!(
        id = pos.id,
        market = %pos.market_name,
        entry = pos.entry_price,
        exit = exit_price,
        pnl = format!("{:.2}", pnl),
        "Paper position closed early"
    );

    portfolio.save().context("Failed to save portfolio after early close")?;

    Ok(pnl)
}

/// Lazy settlement: check all open positions against Gamma API (Polymarket's authority).
/// Gamma knows the Chainlink-based settlement outcome. For expired positions that Gamma
/// hasn't resolved yet, we retry with more attempts before marking as pending.
/// Returns the number of positions settled in this pass.
pub async fn settle_open_positions(
    portfolio: &mut Portfolio,
    client: &Client,
) -> Result<usize> {
    let open_indices: Vec<usize> = portfolio
        .positions
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.status == PositionStatus::Open || p.status == PositionStatus::ExpiredPending
        })
        .map(|(i, _)| i)
        .collect();

    if open_indices.is_empty() {
        return Ok(0);
    }

    let mut settled_count = 0;
    let now_ts = Utc::now().timestamp();

    for idx in open_indices {
        let token_id = portfolio.positions[idx].token_id.clone();
        let is_expired = portfolio.positions[idx]
            .window_end_ts
            .map(|end_ts| now_ts > end_ts + 120)
            .unwrap_or(false);

        // Try harder for expired positions (they should be resolved on Gamma)
        let (retries, delay) = if is_expired { (3, 1) } else { (1, 0) };

        match verify_settlement_outcome(client, &token_id, retries, delay).await {
            Ok(Some(outcome)) => {
                let pos = &mut portfolio.positions[idx];
                let won = pos.did_win(outcome);
                let pnl = pos.calculate_pnl(won);

                pos.status = if won {
                    PositionStatus::SettledWon
                } else {
                    PositionStatus::SettledLost
                };
                pos.settled_at = Some(Utc::now());
                pos.settlement_outcome = Some(outcome);
                pos.pnl = Some(pnl);

                info!(
                    id = pos.id,
                    market = %pos.market_name,
                    outcome = %outcome,
                    won = won,
                    pnl = format!("{:.2}", pnl),
                    "Paper position settled via Gamma"
                );

                settled_count += 1;
            }
            Ok(None) | Err(_) => {
                let pos = &mut portfolio.positions[idx];
                if is_expired && pos.status == PositionStatus::Open {
                    pos.status = PositionStatus::ExpiredPending;
                    debug!(
                        id = pos.id,
                        market = %pos.market_name,
                        "Window expired, Gamma has no resolution yet — marking pending"
                    );
                    settled_count += 1;
                }
            }
        }
    }

    if settled_count > 0 {
        portfolio.save().context("Failed to save portfolio after settlement")?;
    }

    Ok(settled_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::paper::portfolio::Portfolio;
    use tempfile::TempDir;

    fn test_portfolio(dir: &TempDir) -> Portfolio {
        Portfolio::new_with_path(dir.path().join("paper_portfolio.json"))
    }

    #[test]
    fn test_open_position_basic() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id = open_position(
            &mut portfolio,
            "BTC 5m UP".to_string(),
            "token123".to_string(),
            "cond123".to_string(),
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

        assert_eq!(id, 1);
        assert_eq!(portfolio.positions.len(), 1);

        let pos = &portfolio.positions[0];
        assert_eq!(pos.market_name, "BTC 5m UP");
        assert_eq!(pos.entry_price, 0.55);
        assert!((pos.quantity - 100.0 / 0.55).abs() < 0.01);
        assert!(pos.entry_fee > 0.0);
        assert_eq!(pos.status, PositionStatus::Open);
        assert!(pos.pnl.is_none());
    }

    #[test]
    fn test_open_position_fee_calculation() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let entry_price = 0.50;
        open_position(
            &mut portfolio,
            "ETH 5m UP".to_string(),
            "token456".to_string(),
            "cond456".to_string(),
            entry_price,
            100.0,
            3000.0,
            3050.0,
            "ETHUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        let pos = &portfolio.positions[0];
        let expected_fee_per_contract = polymarket_fee(entry_price);
        let expected_total_fee = expected_fee_per_contract * pos.quantity;
        assert!(
            (pos.entry_fee - expected_total_fee).abs() < 1e-8,
            "Entry fee should match polymarket_fee calculation"
        );
    }

    #[test]
    fn test_open_position_invalid_price() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        // Price = 0 should fail
        let result = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            0.0,
            100.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        );
        assert!(result.is_err());

        // Price = 1 should fail
        let result = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            1.0,
            100.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        );
        assert!(result.is_err());

        // Negative size should fail
        let result = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            0.50,
            -10.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_position_ids_increment() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id1 = open_position(
            &mut portfolio,
            "pos1".to_string(),
            "t1".to_string(),
            "c1".to_string(),
            0.50,
            50.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        let id2 = open_position(
            &mut portfolio,
            "pos2".to_string(),
            "t2".to_string(),
            "c2".to_string(),
            0.60,
            50.0,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn test_pnl_calculation_win() {
        let pos = PaperPosition {
            id: 1,
            market_name: "BTC UP".to_string(),
            token_id: "t".to_string(),
            condition_id: "c".to_string(),
            side: Side::Buy,
            entry_price: 0.60,
            quantity: 100.0,
            size_usd: 60.0,
            entry_time: Utc::now(),
            entry_spot_price: 84000.0,
            entry_fee: 0.90, // approximate
            strike_price: 84500.0,
            underlying: "BTCUSDT".to_string(),
            is_upside: true,
            holding_yes: true,
            status: PositionStatus::Open,
            settled_at: None,
            settlement_outcome: None,
            pnl: None,
            exit_price: None,
            exit_fee: None,
            window_start_ts: None,
            window_end_ts: None,
        };

        // Win: payout = 100 contracts * $1 = $100
        // Cost = $60 (size_usd) + $0.90 (fee)
        // PnL = $100 - $60.90 = $39.10
        let pnl = pos.calculate_pnl(true);
        assert!((pnl - 39.10).abs() < 0.01, "Win PnL should be ~$39.10, got {pnl}");
    }

    #[test]
    fn test_pnl_calculation_loss() {
        let pos = PaperPosition {
            id: 1,
            market_name: "BTC UP".to_string(),
            token_id: "t".to_string(),
            condition_id: "c".to_string(),
            side: Side::Buy,
            entry_price: 0.60,
            quantity: 100.0,
            size_usd: 60.0,
            entry_time: Utc::now(),
            entry_spot_price: 84000.0,
            entry_fee: 0.90,
            strike_price: 84500.0,
            underlying: "BTCUSDT".to_string(),
            is_upside: true,
            holding_yes: true,
            status: PositionStatus::Open,
            settled_at: None,
            settlement_outcome: None,
            pnl: None,
            exit_price: None,
            exit_fee: None,
            window_start_ts: None,
            window_end_ts: None,
        };

        // Loss: payout = $0, cost = $60 + $0.90 = $60.90
        let pnl = pos.calculate_pnl(false);
        assert!((pnl - (-60.90)).abs() < 0.01, "Loss PnL should be ~-$60.90, got {pnl}");
    }

    #[test]
    fn test_did_win_logic() {
        let make_pos = |is_upside: bool, holding_yes: bool| PaperPosition {
            id: 1,
            market_name: "test".to_string(),
            token_id: "t".to_string(),
            condition_id: "c".to_string(),
            side: Side::Buy,
            entry_price: 0.50,
            quantity: 100.0,
            size_usd: 50.0,
            entry_time: Utc::now(),
            entry_spot_price: 84000.0,
            entry_fee: 0.0,
            strike_price: 84500.0,
            underlying: "BTCUSDT".to_string(),
            is_upside,
            holding_yes,
            status: PositionStatus::Open,
            settled_at: None,
            settlement_outcome: None,
            pnl: None,
            exit_price: None,
            exit_fee: None,
            window_start_ts: None,
            window_end_ts: None,
        };

        // UP market, holding YES → wins on Up
        let up_yes = make_pos(true, true);
        assert!(up_yes.did_win(Direction::Up));
        assert!(!up_yes.did_win(Direction::Down));

        // UP market, holding NO → wins on Down
        let up_no = make_pos(true, false);
        assert!(up_no.did_win(Direction::Down));
        assert!(!up_no.did_win(Direction::Up));

        // DOWN market, holding YES → wins on Down
        let down_yes = make_pos(false, true);
        assert!(down_yes.did_win(Direction::Down));
        assert!(!down_yes.did_win(Direction::Up));

        // DOWN market, holding NO → wins on Up
        let down_no = make_pos(false, false);
        assert!(down_no.did_win(Direction::Up));
        assert!(!down_no.did_win(Direction::Down));
    }

    #[test]
    fn test_close_position_basic() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id = open_position(
            &mut portfolio,
            "BTC monthly".to_string(),
            "tok1".to_string(),
            "cond1".to_string(),
            0.40,
            100.0,
            84000.0,
            90000.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        // Close at a profit (exit at 0.60)
        let pnl = close_position(&mut portfolio, id, 0.60).unwrap();

        let pos = portfolio.position_by_id(id).unwrap();
        assert_eq!(pos.status, PositionStatus::ClosedEarly);
        assert!(pos.exit_price == Some(0.60));
        assert!(pos.exit_fee.is_some());
        assert!(pos.exit_fee.unwrap() > 0.0);
        assert!(pos.pnl.is_some());
        assert!(pnl > 0.0, "Should be profitable: entry 0.40 → exit 0.60");
    }

    #[test]
    fn test_close_position_at_loss() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id = open_position(
            &mut portfolio,
            "ETH weekly".to_string(),
            "tok2".to_string(),
            "cond2".to_string(),
            0.60,
            100.0,
            3000.0,
            3500.0,
            "ETHUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        // Close at a loss (exit at 0.40)
        let pnl = close_position(&mut portfolio, id, 0.40).unwrap();
        assert!(pnl < 0.0, "Should be a loss: entry 0.60 → exit 0.40");

        let pos = portfolio.position_by_id(id).unwrap();
        assert_eq!(pos.status, PositionStatus::ClosedEarly);
    }

    #[test]
    fn test_close_position_not_found() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);
        let result = close_position(&mut portfolio, 999, 0.50);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_close_position_already_settled() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            0.50,
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

        // Close it first
        close_position(&mut portfolio, id, 0.60).unwrap();

        // Try to close again
        let result = close_position(&mut portfolio, id, 0.70);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not open"));
    }

    #[test]
    fn test_close_position_invalid_price() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let id = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            0.50,
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

        assert!(close_position(&mut portfolio, id, 0.0).is_err());
        assert!(close_position(&mut portfolio, id, 1.0).is_err());
    }

    #[test]
    fn test_close_position_pnl_math() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        let entry_price = 0.50;
        let exit_price = 0.70;
        let size_usd = 100.0;

        let id = open_position(
            &mut portfolio,
            "test".to_string(),
            "t".to_string(),
            "c".to_string(),
            entry_price,
            size_usd,
            84000.0,
            84500.0,
            "BTCUSDT".to_string(),
            true,
            true,
            None,
            None,
        )
        .unwrap();

        let pnl = close_position(&mut portfolio, id, exit_price).unwrap();
        let pos = portfolio.position_by_id(id).unwrap();

        // Verify: P&L = (exit - entry) * quantity - entry_fee - exit_fee
        let quantity = size_usd / entry_price;
        let entry_fee = crate::core::analysis::fees::polymarket_fee(entry_price) * quantity;
        let exit_fee = crate::core::analysis::fees::polymarket_fee(exit_price) * quantity;
        let expected_pnl = (exit_price - entry_price) * quantity - entry_fee - exit_fee;

        assert!(
            (pnl - expected_pnl).abs() < 0.01,
            "PnL {pnl:.4} should match expected {expected_pnl:.4}"
        );
        assert_eq!(pos.exit_fee.unwrap(), exit_fee);
    }

    #[test]
    fn test_persistence_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut portfolio = test_portfolio(&dir);

        open_position(
            &mut portfolio,
            "BTC 5m UP".to_string(),
            "token_abc".to_string(),
            "cond_abc".to_string(),
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

        // Reload from disk
        let loaded = Portfolio::load_from_path(dir.path().join("paper_portfolio.json")).unwrap();
        assert_eq!(loaded.positions.len(), 1);
        assert_eq!(loaded.positions[0].market_name, "BTC 5m UP");
        assert_eq!(loaded.positions[0].token_id, "token_abc");
        assert_eq!(loaded.positions[0].entry_price, 0.55);
    }
}
