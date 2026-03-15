// Fee model for Polymarket crypto markets (since Mar 6 2026).
// Extracted from market-scout src/strategy.rs, extended with EV calculations.
//
// Key facts:
// - Taker fee per $1 contract: 0.25 * (p * (1-p))^2
// - Maker rebate: 20% of taker fee (paid back to maker)
// - Settlement is FREE — no fee when holding to expiry
// - Early exit (sell before settlement) pays taker fee again

use crate::core::types::TradeMode;

/// Taker fee per $1 contract at a given entry price.
/// Formula: 0.25 * (p * (1-p))^2
/// Max fee is 1.56% at p=0.50, drops to near-zero at extremes.
pub fn polymarket_fee(price: f64) -> f64 {
    let p = price.clamp(0.0, 1.0);
    0.25 * (p * (1.0 - p)).powi(2)
}

/// Maker rebate per $1 contract: 20% of the taker fee.
pub fn maker_rebate(price: f64) -> f64 {
    0.20 * polymarket_fee(price)
}

/// Net fee per $1 contract for a given trade mode.
/// Taker: pays fee. Maker: receives rebate (negative cost).
pub fn net_entry_fee(price: f64, mode: TradeMode) -> f64 {
    match mode {
        TradeMode::Taker => polymarket_fee(price),
        TradeMode::Maker => -maker_rebate(price),
    }
}

/// P&L if the position wins (settles at $1.00), holding to settlement.
/// Win payout = (1.0 - entry_price) * contracts - entry_fee_total
pub fn pnl_if_win(entry_price: f64, size_usd: f64, mode: TradeMode) -> f64 {
    let fee_per_contract = net_entry_fee(entry_price, mode);
    let contracts = size_usd / entry_price;
    let gross_pnl = (1.0 - entry_price) * contracts;
    let total_fee = fee_per_contract * contracts;
    gross_pnl - total_fee
}

/// P&L if the position loses (settles at $0.00), holding to settlement.
/// Loss = -size_usd - entry_fee_total
pub fn pnl_if_loss(entry_price: f64, size_usd: f64, mode: TradeMode) -> f64 {
    let fee_per_contract = net_entry_fee(entry_price, mode);
    let contracts = size_usd / entry_price;
    let total_fee = fee_per_contract * contracts;
    -size_usd - total_fee
}

/// Breakeven win rate for hold-to-settlement strategy.
/// At breakeven: WR * win_pnl + (1-WR) * loss_pnl = 0
/// Solving: WR = -loss_pnl / (win_pnl - loss_pnl)
pub fn breakeven_win_rate(entry_price: f64, mode: TradeMode) -> f64 {
    let size = 100.0; // arbitrary — ratio is size-independent
    let win = pnl_if_win(entry_price, size, mode);
    let loss = pnl_if_loss(entry_price, size, mode);
    let denom = win - loss;
    if denom.abs() < 1e-12 {
        return 1.0;
    }
    (-loss / denom).clamp(0.0, 1.0)
}

/// Expected value per trade at a given win rate, holding to settlement.
pub fn expected_value(entry_price: f64, win_rate: f64, size_usd: f64, mode: TradeMode) -> f64 {
    let win = pnl_if_win(entry_price, size_usd, mode);
    let loss = pnl_if_loss(entry_price, size_usd, mode);
    win_rate * win + (1.0 - win_rate) * loss
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_at_half() {
        let fee = polymarket_fee(0.50);
        assert!((fee - 0.015625).abs() < 1e-8, "Fee at 0.50 should be ~1.56%");
    }

    #[test]
    fn test_fee_at_extremes() {
        assert!(polymarket_fee(0.01) < 0.001);
        assert!(polymarket_fee(0.99) < 0.001);
        assert!((polymarket_fee(0.0)).abs() < 1e-12);
        assert!((polymarket_fee(1.0)).abs() < 1e-12);
    }

    #[test]
    fn test_fee_symmetry() {
        let fee_40 = polymarket_fee(0.40);
        let fee_60 = polymarket_fee(0.60);
        assert!((fee_40 - fee_60).abs() < 1e-12, "Fee should be symmetric around 0.50");
    }

    #[test]
    fn test_maker_rebate() {
        let fee = polymarket_fee(0.50);
        let rebate = maker_rebate(0.50);
        assert!((rebate - fee * 0.20).abs() < 1e-12);
    }

    #[test]
    fn test_breakeven_at_50() {
        let be = breakeven_win_rate(0.50, TradeMode::Taker);
        // At 0.50, fee is 1.56%, so breakeven should be slightly above 50%
        assert!(be > 0.50 && be < 0.52, "Breakeven at 0.50 should be ~50.8%, got {be}");
    }

    #[test]
    fn test_breakeven_at_65() {
        let be = breakeven_win_rate(0.65, TradeMode::Taker);
        // At p=0.65, fee=1.29%, breakeven ~66.3% (above entry price due to fees)
        assert!(be > 0.65 && be < 0.67, "Breakeven at 0.65 should be ~66.3%, got {be}");
    }

    #[test]
    fn test_maker_lower_breakeven() {
        let be_taker = breakeven_win_rate(0.60, TradeMode::Taker);
        let be_maker = breakeven_win_rate(0.60, TradeMode::Maker);
        assert!(be_maker < be_taker, "Maker breakeven should be lower than taker");
    }

    #[test]
    fn test_ev_positive_above_breakeven() {
        let be = breakeven_win_rate(0.60, TradeMode::Taker);
        let ev = expected_value(0.60, be + 0.05, 100.0, TradeMode::Taker);
        assert!(ev > 0.0, "EV should be positive above breakeven WR");
    }

    #[test]
    fn test_ev_negative_below_breakeven() {
        let be = breakeven_win_rate(0.60, TradeMode::Taker);
        let ev = expected_value(0.60, be - 0.05, 100.0, TradeMode::Taker);
        assert!(ev < 0.0, "EV should be negative below breakeven WR");
    }

    #[test]
    fn test_pnl_win_loss_relationship() {
        let win = pnl_if_win(0.60, 100.0, TradeMode::Taker);
        let loss = pnl_if_loss(0.60, 100.0, TradeMode::Taker);
        assert!(win > 0.0, "Win PnL should be positive");
        assert!(loss < 0.0, "Loss PnL should be negative");
        assert!(loss.abs() > win.abs(), "Loss should be larger than win at p>0.50");
    }
}
