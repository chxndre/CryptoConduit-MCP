// MCP tool handlers for order book deep dives and trade simulation.
// Tools: get_order_book, simulate_trade

use rmcp::schemars;
use serde::Deserialize;

use crate::core::analysis::fees;
use crate::core::analysis::microstructure;
use crate::core::monitor::state::SharedState;
use crate::core::providers::polymarket::PolymarketClient;
use crate::core::providers::resolver::{resolve_market, ResolveResult};
use crate::core::types::{Side, TradeMode};
use crate::mcp::formatter::{fmt_pct, fmt_usd};

// ──────────────────────────── get_order_book ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetOrderBookParams {
    /// Direct token ID (64-char hex) — use this OR market description
    pub token_id: Option<String>,
    /// Natural language market description, e.g. "BTC 5m UP" — use this OR token_id
    pub market: Option<String>,
    /// Side to analyze: "buy" or "sell" (default: "buy")
    pub side: Option<String>,
    /// Simulate a fill for this USD amount (e.g. 100)
    pub simulate_usd: Option<f64>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_order_book(
    state: &SharedState,
    polymarket: &PolymarketClient,
    params: GetOrderBookParams,
) -> String {
    // Resolve token ID
    let (token_id, market_name) = if let Some(tid) = params.token_id {
        (tid.clone(), format!("Token {}", &tid[..8.min(tid.len())]))
    } else if let Some(query) = params.market {
        let s = state.read().await;
        let short_term: Vec<(String, Vec<_>)> = s
            .short_term_markets
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let daily = s.daily_markets.clone();
        let monthly = s.monthly_markets.clone();
        drop(s);

        match resolve_market(&query, &short_term, &daily, &monthly) {
            ResolveResult::Found(m) => (m.token_id, m.name),
            ResolveResult::Ambiguous(matches) => {
                let options: Vec<String> = matches
                    .iter()
                    .map(|m| format!("- {} (token: {}...)", m.name, &m.token_id[..8.min(m.token_id.len())]))
                    .collect();
                return format!(
                    "Ambiguous market query '{}'. Did you mean:\n{}",
                    query,
                    options.join("\n")
                );
            }
            ResolveResult::NotFound(msg) => return msg,
        }
    } else {
        return "Please provide either `token_id` or `market` parameter.".to_string();
    };

    // Fetch order book on-demand (this is the one tool that hits the API directly)
    let book = match polymarket.get_order_book(&token_id).await {
        Ok(b) => b,
        Err(e) => {
            return format!("Failed to fetch order book for {market_name}: {e}");
        }
    };

    let side = match params.side.as_deref() {
        Some("sell") => Side::Sell,
        _ => Side::Buy,
    };

    let mut out = format!("## Order Book: {market_name}\n\n");

    // Basic stats
    out.push_str(&format!(
        "Best Bid: {} | Best Ask: {} | Midpoint: {} | Spread: {}\n",
        book.best_bid().map_or("n/a".into(), |p| format!("{:.4}", p)),
        book.best_ask().map_or("n/a".into(), |p| format!("{:.4}", p)),
        book.midpoint().map_or("n/a".into(), |p| format!("{:.4}", p)),
        book.spread_pct().map_or("n/a".into(), fmt_pct),
    ));

    // Depth
    out.push_str(&format!(
        "Ask depth (5%): {} | Bid depth (5%): {}\n",
        fmt_usd(book.ask_depth_within(0.05)),
        fmt_usd(book.bid_depth_within(0.05)),
    ));

    // Tradeability
    if let Some(assess) = microstructure::assess_tradeability(&book) {
        out.push_str(&format!(
            "Tradeability: **{}** | Slippage($100): {}\n",
            assess.rating,
            fmt_pct(assess.slippage_100_pct),
        ));
    }

    // Depth chart (top 5 levels each side)
    out.push_str("\n### Asks (top 5)\n");
    for (i, level) in book.asks.iter().rev().take(5).enumerate() {
        if let (Ok(p), Ok(s)) = (level.price_f64(), level.size_f64()) {
            let usd = s * p;
            out.push_str(&format!(
                "  L{}: {:.4} × {:.1} ({}) \n",
                i + 1,
                p,
                s,
                fmt_usd(usd),
            ));
        }
    }

    out.push_str("\n### Bids (top 5)\n");
    for (i, level) in book.bids.iter().rev().take(5).enumerate() {
        if let (Ok(p), Ok(s)) = (level.price_f64(), level.size_f64()) {
            let usd = s * p;
            out.push_str(&format!(
                "  L{}: {:.4} × {:.1} ({})\n",
                i + 1,
                p,
                s,
                fmt_usd(usd),
            ));
        }
    }

    // Fill simulation
    if let Some(budget) = params.simulate_usd {
        out.push_str(&format!("\n### Fill Simulation ({} {})\n", fmt_usd(budget), if side == Side::Buy { "BUY" } else { "SELL" }));
        if let Some(fill) = book.simulate_fill_usd(side, budget) {
            let fee = fees::polymarket_fee(fill.avg_price);
            out.push_str(&format!(
                "Avg price: {:.4} | Filled: {:.1} contracts | Slippage: {} | Levels crossed: {}\n\
                 Fee: {} per contract | Total fee: {}\n",
                fill.avg_price,
                fill.filled_qty,
                fmt_pct(fill.slippage_pct),
                fill.levels_crossed,
                fmt_pct(fee),
                fmt_usd(fee * fill.filled_qty),
            ));
            if !fill.fully_filled {
                out.push_str(&format!(
                    "⚠ Partial fill — only {} of {} filled\n",
                    fmt_usd(fill.total_cost),
                    fmt_usd(budget),
                ));
            }
        } else {
            out.push_str("No liquidity available for fill simulation.\n");
        }
    }

    out
}

// ──────────────────────────── simulate_trade ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SimulateTradeParams {
    /// Entry price (0.01 to 0.99)
    pub price: f64,
    /// Position size in USD
    pub size_usd: f64,
    /// Trade mode: "taker" (default) or "maker"
    pub mode: Option<String>,
}

pub async fn handle_simulate_trade(params: SimulateTradeParams) -> String {
    if params.price <= 0.0 || params.price >= 1.0 {
        return format!("Invalid price {:.4}: must be between 0 and 1 exclusive.", params.price);
    }
    if params.size_usd <= 0.0 {
        return "Position size must be positive.".to_string();
    }

    let mode = match params.mode.as_deref() {
        Some("maker") => TradeMode::Maker,
        _ => TradeMode::Taker,
    };

    let p = params.price;
    let size = params.size_usd;
    let contracts = size / p;
    let fee_per = fees::net_entry_fee(p, mode);
    let total_fee = fee_per * contracts;
    let win_pnl = fees::pnl_if_win(p, size, mode);
    let loss_pnl = fees::pnl_if_loss(p, size, mode);
    let be_wr = fees::breakeven_win_rate(p, mode);

    let mode_str = match mode {
        TradeMode::Taker => "Taker",
        TradeMode::Maker => "Maker",
    };

    let mut out = format!(
        "## Trade Simulation — {mode_str}\n\n\
         Entry: {:.4} | Size: {} | Contracts: {:.1}\n\
         Fee/contract: {} | Total fee: {}\n\n\
         | Scenario | P&L |\n\
         |----------|-----|\n\
         | **Win** (settles $1.00) | {} |\n\
         | **Loss** (settles $0.00) | {} |\n\n\
         Breakeven win rate: {}\n",
        p,
        fmt_usd(size),
        contracts,
        fmt_pct(fee_per.abs()),
        fmt_usd(total_fee.abs()),
        fmt_usd(win_pnl),
        fmt_usd(loss_pnl),
        fmt_pct(be_wr),
    );

    // EV at various win rates
    out.push_str("\n| Win Rate | EV |\n|----------|----|\n");
    for wr in [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80] {
        let ev = fees::expected_value(p, wr, size, mode);
        out.push_str(&format!(
            "| {} | {} |\n",
            fmt_pct(wr),
            fmt_usd(ev),
        ));
    }

    // Early exit note
    let exit_fee = fees::polymarket_fee(p);
    out.push_str(&format!(
        "\n**Early exit fee:** {} per contract (if sold before settlement)\n",
        fmt_pct(exit_fee),
    ));

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── simulate_trade tests ──

    #[tokio::test]
    async fn test_simulate_trade_taker_basic() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.50,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("## Trade Simulation"));
        assert!(result.contains("Taker"));
        assert!(result.contains("Win"));
        assert!(result.contains("Loss"));
        assert!(result.contains("Breakeven win rate"));
    }

    #[tokio::test]
    async fn test_simulate_trade_maker() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.50,
            size_usd: 100.0,
            mode: Some("maker".into()),
        })
        .await;
        assert!(result.contains("Maker"));
    }

    #[tokio::test]
    async fn test_simulate_trade_invalid_price_zero() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.0,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("Invalid price"));
    }

    #[tokio::test]
    async fn test_simulate_trade_invalid_price_one() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 1.0,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("Invalid price"));
    }

    #[tokio::test]
    async fn test_simulate_trade_negative_price() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: -0.5,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("Invalid price"));
    }

    #[tokio::test]
    async fn test_simulate_trade_negative_size() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.50,
            size_usd: -50.0,
            mode: None,
        })
        .await;
        assert!(result.contains("Position size must be positive"));
    }

    #[tokio::test]
    async fn test_simulate_trade_ev_table() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.40,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("50.0%"));
        assert!(result.contains("80.0%"));
        assert!(result.contains("Early exit fee"));
    }

    #[tokio::test]
    async fn test_simulate_trade_extreme_low_price() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.05,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("## Trade Simulation"));
    }

    #[tokio::test]
    async fn test_simulate_trade_extreme_high_price() {
        let result = handle_simulate_trade(SimulateTradeParams {
            price: 0.95,
            size_usd: 100.0,
            mode: None,
        })
        .await;
        assert!(result.contains("## Trade Simulation"));
    }

    // ── get_order_book parameter validation ──

    #[tokio::test]
    async fn test_get_order_book_no_params() {
        let state = SharedState::new();
        let client = PolymarketClient::new(None, None);
        let result = handle_get_order_book(
            &state,
            &client,
            GetOrderBookParams {
                token_id: None,
                market: None,
                side: None,
                simulate_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("Please provide either"));
    }

    #[tokio::test]
    async fn test_get_order_book_not_found_market() {
        let state = SharedState::new();
        let client = PolymarketClient::new(None, None);
        let result = handle_get_order_book(
            &state,
            &client,
            GetOrderBookParams {
                token_id: None,
                market: Some("nonexistent xyz market".into()),
                side: None,
                simulate_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No markets matching"));
    }
}
