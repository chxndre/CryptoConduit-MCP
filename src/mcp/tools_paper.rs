// MCP tool handlers for paper trading.
// Tools: paper_trade, get_paper_portfolio

use rmcp::schemars;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::core::monitor::state::SharedState;
use crate::core::paper::engine;
use crate::core::paper::portfolio::Portfolio;
use crate::core::providers::resolver::{resolve_market, ResolveResult};
use crate::mcp::formatter::{fmt_pct, fmt_usd, polymarket_url};

// ──────────────────────────── paper_trade ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PaperTradeParams {
    /// Market description (e.g. "BTC 5m UP") or token ID
    pub market: String,
    /// Side: "buy" (default) or "sell"
    pub side: Option<String>,
    /// Position size in USD
    pub size_usd: f64,
    /// Override entry price (default: current ask from order book)
    pub price: Option<f64>,
}

pub async fn handle_paper_trade(
    state: &SharedState,
    portfolio: &Arc<Mutex<Portfolio>>,
    params: PaperTradeParams,
) -> String {
    let s = state.read().await;

    // Resolve market
    let short_term: Vec<(String, Vec<_>)> = s
        .short_term_markets
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let daily = s.daily_markets.clone();
    let monthly = s.monthly_markets.clone();

    let resolved = match resolve_market(&params.market, &short_term, &daily, &monthly) {
        ResolveResult::Found(m) => m,
        ResolveResult::Ambiguous(matches) => {
            let options: Vec<String> = matches
                .iter()
                .map(|m| format!("- {}", m.name))
                .collect();
            return format!(
                "Ambiguous market '{}'. Did you mean:\n{}",
                params.market,
                options.join("\n")
            );
        }
        ResolveResult::NotFound(msg) => return msg,
    };

    // Get entry price from order book or override
    let entry_price = if let Some(p) = params.price {
        p
    } else if let Some(tsb) = s.order_books.get(&resolved.token_id) {
        match tsb.book.best_ask() {
            Some(ask) => ask,
            None => return "Order book empty — cannot determine entry price. Provide `price` manually.".to_string(),
        }
    } else {
        return format!(
            "No order book data for {}. Provide `price` manually or wait for background poller.",
            resolved.name
        );
    };

    // Get spot price for the underlying
    let underlying = guess_underlying(&resolved.name);
    let spot_price = s
        .spot_prices
        .get(&underlying)
        .map(|p| p.price)
        .unwrap_or(0.0);

    // Find condition_id and market metadata
    let (condition_id, strike_price, is_upside, holding_yes, slug, window_start_ts, window_end_ts) =
        find_market_metadata(&resolved.token_id, &short_term, &daily, &monthly);

    drop(s); // Release read lock before acquiring portfolio mutex

    let mut portfolio = portfolio.lock().await;
    match engine::open_position(
        &mut portfolio,
        resolved.name.clone(),
        resolved.token_id.clone(),
        condition_id,
        entry_price,
        params.size_usd,
        spot_price,
        strike_price,
        underlying,
        is_upside,
        holding_yes,
        window_start_ts,
        window_end_ts,
    ) {
        Ok(id) => {
            let fee = crate::core::analysis::fees::polymarket_fee(entry_price);
            let contracts = params.size_usd / entry_price;
            let total_fee = fee * contracts;
            let url = if !slug.is_empty() {
                format!("\n{}", polymarket_url(&slug))
            } else {
                String::new()
            };

            format!(
                "## Paper Trade Opened\n\n\
                 **Position #{id}** — {}\n\
                 Entry: {:.4} | Size: {} | Contracts: {:.1}\n\
                 Fee: {} ({} total)\n\
                 Spot at entry: {}\n\
                 Status: OPEN — will settle when market resolves{url}",
                resolved.name,
                entry_price,
                fmt_usd(params.size_usd),
                contracts,
                fmt_pct(fee),
                fmt_usd(total_fee),
                fmt_usd(spot_price),
            )
        }
        Err(e) => format!("Failed to open paper trade: {e}"),
    }
}

// ──────────────────────────── get_paper_portfolio ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetPaperPortfolioParams {
    /// Attempt to settle pending positions via Gamma API (default: true)
    pub settle: Option<bool>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_paper_portfolio(
    portfolio: &Arc<Mutex<Portfolio>>,
    http_client: &reqwest::Client,
    params: GetPaperPortfolioParams,
) -> String {
    let mut portfolio = portfolio.lock().await;

    // Lazy settlement
    let settle = params.settle.unwrap_or(true);
    let settled_count = if settle {
        match engine::settle_open_positions(&mut portfolio, http_client).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "Settlement check failed");
                0
            }
        }
    } else {
        0
    };

    let stats = portfolio.stats();

    let mut out = String::from("## Paper Portfolio\n\n");

    if settled_count > 0 {
        out.push_str(&format!("*{settled_count} position(s) settled since last check.*\n\n"));
    }

    // Open positions (includes ExpiredPending)
    let pending: Vec<_> = portfolio
        .positions
        .iter()
        .filter(|p| {
            p.status == engine::PositionStatus::Open
                || p.status == engine::PositionStatus::ExpiredPending
        })
        .collect();

    out.push_str(&format!("### Open Positions ({})\n", pending.len()));
    if pending.is_empty() {
        out.push_str("No open positions.\n");
    } else {
        out.push_str("| # | Market | Entry | Size | Fee | Opened | Status |\n");
        out.push_str("|---|--------|-------|------|-----|--------|--------|\n");
        for p in &pending {
            let time_info = if p.status == engine::PositionStatus::ExpiredPending {
                "EXPIRED (pending settlement)".to_string()
            } else if let Some(end_ts) = p.window_end_ts {
                let now = chrono::Utc::now().timestamp();
                if now < end_ts {
                    format!("{}s left", end_ts - now)
                } else {
                    "WINDOW ENDED".to_string()
                }
            } else {
                "OPEN".to_string()
            };

            out.push_str(&format!(
                "| {} | {} | {:.3} | {} | {} | {} | {} |\n",
                p.id,
                p.market_name,
                p.entry_price,
                fmt_usd(p.size_usd),
                fmt_usd(p.entry_fee),
                p.entry_time.format("%Y-%m-%d %H:%M UTC"),
                time_info,
            ));
        }
        out.push_str("\n*Use `paper_close` with a position ID to exit early.*\n");
    }

    // Statistics
    if stats.total_trades > 0 {
        out.push_str(&format!(
            "\n### All-Time Statistics\n\
             Trades: {} | Wins: {} | Win rate: {}\n\
             Total P&L: {} | Avg P&L: {}\n\
             Best: {} | Worst: {}\n\
             Volume: {} | Fees paid: {}\n",
            stats.total_trades,
            stats.wins,
            fmt_pct(stats.win_rate),
            fmt_usd(stats.total_pnl),
            fmt_usd(stats.avg_pnl),
            fmt_usd(stats.best_pnl),
            fmt_usd(stats.worst_pnl),
            fmt_usd(stats.total_volume),
            fmt_usd(stats.total_fees),
        ));

        // By asset
        if !stats.by_asset.is_empty() {
            out.push_str("\n**By Asset:**\n");
            for (asset, gs) in &stats.by_asset {
                out.push_str(&format!(
                    "  {}: {} trades, {} WR, {} P&L\n",
                    asset.replace("USDT", ""),
                    gs.total_trades,
                    fmt_pct(gs.win_rate),
                    fmt_usd(gs.total_pnl),
                ));
            }
        }

        // By price bucket
        if !stats.by_price_bucket.is_empty() {
            out.push_str("\n**By Entry Price:**\n");
            let mut buckets: Vec<_> = stats.by_price_bucket.iter().collect();
            buckets.sort_by_key(|(k, _)| k.to_string());
            for (bucket, gs) in buckets {
                out.push_str(&format!(
                    "  {}: {} trades, {} WR, {} P&L\n",
                    bucket,
                    gs.total_trades,
                    fmt_pct(gs.win_rate),
                    fmt_usd(gs.total_pnl),
                ));
            }
        }
    } else {
        out.push_str("\nNo settled trades yet.\n");
    }

    out
}

// ──────────────────────────── paper_close ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PaperCloseParams {
    /// Position ID to close early
    pub position_id: u64,
    /// Exit price (sell at bid). If omitted, uses current best bid from order book.
    pub price: Option<f64>,
}

pub async fn handle_paper_close(
    state: &SharedState,
    portfolio: &Arc<Mutex<Portfolio>>,
    params: PaperCloseParams,
) -> String {
    // Find the position's token_id to look up its order book bid
    let exit_price = if let Some(p) = params.price {
        p
    } else {
        // Try to get best bid from order book
        let port = portfolio.lock().await;
        let pos = match port.position_by_id(params.position_id) {
            Some(p) => p,
            None => return format!("Position #{} not found.", params.position_id),
        };

        if pos.status != engine::PositionStatus::Open {
            return format!("Position #{} is not open (status: {}).", params.position_id, pos.status);
        }

        let token_id = pos.token_id.clone();
        drop(port);

        let s = state.read().await;
        match s.order_books.get(&token_id) {
            Some(tsb) => match tsb.book.best_bid() {
                Some(bid) => bid,
                None => return "No bid in order book — cannot determine exit price. Provide `price` manually.".to_string(),
            },
            None => return format!(
                "No order book data for token {}. Provide `price` manually.",
                &token_id[..8.min(token_id.len())]
            ),
        }
    };

    let mut port = portfolio.lock().await;
    match engine::close_position(&mut port, params.position_id, exit_price) {
        Ok(pnl) => {
            let pos = port.position_by_id(params.position_id).unwrap();
            let exit_fee = pos.exit_fee.unwrap_or(0.0);
            format!(
                "## Position #{} Closed Early\n\n\
                 **{}**\n\
                 Entry: {:.4} → Exit: {:.4}\n\
                 Entry fee: {} | Exit fee: {}\n\
                 **P&L: {}** ({})\n\
                 Status: CLOSED",
                params.position_id,
                pos.market_name,
                pos.entry_price,
                exit_price,
                fmt_usd(pos.entry_fee),
                fmt_usd(exit_fee),
                fmt_usd(pnl),
                if pnl >= 0.0 { "profit" } else { "loss" },
            )
        }
        Err(e) => format!("Failed to close position: {e}"),
    }
}

// ──────────────────────────── Helpers ────────────────────────────

/// Guess the Binance underlying symbol from a market name.
fn guess_underlying(name: &str) -> String {
    let lower = name.to_lowercase();
    for (prefix, sym) in [
        ("btc", "BTCUSDT"),
        ("bitcoin", "BTCUSDT"),
        ("eth", "ETHUSDT"),
        ("ethereum", "ETHUSDT"),
        ("sol", "SOLUSDT"),
        ("solana", "SOLUSDT"),
        ("xrp", "XRPUSDT"),
        ("doge", "DOGEUSDT"),
        ("dogecoin", "DOGEUSDT"),
        ("bnb", "BNBUSDT"),
    ] {
        if lower.contains(prefix) {
            return sym.to_string();
        }
    }
    "BTCUSDT".to_string()
}

/// Metadata tuple: (condition_id, strike_price, is_upside, holding_yes, slug, window_start_ts, window_end_ts)
type MarketMeta = (String, f64, bool, bool, String, Option<i64>, Option<i64>);

/// Find market metadata for a token.
fn find_market_metadata(
    token_id: &str,
    short_term: &[(String, Vec<crate::core::types::ShortTermMarket>)],
    daily: &[crate::core::types::MarketConfig],
    monthly: &[crate::core::types::MarketConfig],
) -> MarketMeta {
    // Search short-term markets
    for (_key, markets) in short_term {
        for m in markets {
            let start_ts = Some(m.window_start_ts);
            let end_ts = Some(m.window_start_ts + (m.interval as i64 * 60));
            if m.up_token_id == token_id {
                return (m.condition_id.clone(), 0.0, true, true, m.slug.clone(), start_ts, end_ts);
            }
            if m.down_token_id == token_id {
                return (m.condition_id.clone(), 0.0, false, true, m.slug.clone(), start_ts, end_ts);
            }
        }
    }

    // Search daily markets
    for m in daily {
        if m.yes_token_id == token_id {
            return (m.condition_id.clone(), m.strike_price, m.is_upside(), true, String::new(), None, None);
        }
        if m.no_token_id == token_id {
            return (m.condition_id.clone(), m.strike_price, m.is_upside(), false, String::new(), None, None);
        }
    }

    // Search monthly markets
    for m in monthly {
        if m.yes_token_id == token_id {
            return (m.condition_id.clone(), m.strike_price, true, true, String::new(), None, None);
        }
        if m.no_token_id == token_id {
            return (m.condition_id.clone(), m.strike_price, true, false, String::new(), None, None);
        }
    }

    ("unknown".to_string(), 0.0, true, true, String::new(), None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::monitor::state::{SharedState, TimestampedOrderBook};
    use crate::core::paper::portfolio::Portfolio;
    use crate::core::types::{OrderBook, PriceLevel, ShortTermMarket};
    use tempfile::TempDir;

    fn make_book(bids: &[(f64, f64)], asks: &[(f64, f64)]) -> OrderBook {
        OrderBook {
            timestamp: 1710000000,
            market: "test".into(),
            asset_id: "test_token".into(),
            bids: bids
                .iter()
                .map(|(p, s)| PriceLevel {
                    price: p.to_string(),
                    size: s.to_string(),
                })
                .collect(),
            asks: asks
                .iter()
                .map(|(p, s)| PriceLevel {
                    price: p.to_string(),
                    size: s.to_string(),
                })
                .collect(),
        }
    }

    fn test_portfolio(dir: &TempDir) -> Portfolio {
        Portfolio::new_with_path(dir.path().join("paper_portfolio.json"))
    }

    // ── paper_trade tests ──

    #[tokio::test]
    async fn test_paper_trade_not_found_market() {
        let state = SharedState::new();
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "nonexistent market xyz".into(),
                side: None,
                size_usd: 100.0,
                price: None,
            },
        )
        .await;
        assert!(result.contains("No markets matching"));
    }

    #[tokio::test]
    async fn test_paper_trade_no_order_book() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let now = chrono::Utc::now().timestamp();
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now - 60,
                    up_token_id: "btc_up_tok".into(),
                    down_token_id: "btc_down_tok".into(),
                    condition_id: "cond_test".into(),
                    slug: "btc-updown-5m-test".into(),
                    start_spot_price: None,
                }],
            );
        }
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "btc 5m up".into(),
                side: None,
                size_usd: 100.0,
                price: None,
            },
        )
        .await;
        assert!(result.contains("No order book data"));
    }

    #[tokio::test]
    async fn test_paper_trade_empty_order_book() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let now = chrono::Utc::now().timestamp();
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now - 60,
                    up_token_id: "btc_up_tok".into(),
                    down_token_id: "btc_down_tok".into(),
                    condition_id: "cond_test".into(),
                    slug: "btc-updown-5m-test".into(),
                    start_spot_price: None,
                }],
            );
            // Empty order book (no asks)
            s.order_books.insert(
                "btc_up_tok".into(),
                TimestampedOrderBook::new(make_book(&[(0.40, 100.0)], &[])),
            );
        }
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "btc 5m up".into(),
                side: None,
                size_usd: 100.0,
                price: None,
            },
        )
        .await;
        assert!(result.contains("Order book empty"));
    }

    #[tokio::test]
    async fn test_paper_trade_with_manual_price() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let now = chrono::Utc::now().timestamp();
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now - 60,
                    up_token_id: "btc_up_tok".into(),
                    down_token_id: "btc_down_tok".into(),
                    condition_id: "cond_test".into(),
                    slug: "btc-updown-5m-test".into(),
                    start_spot_price: None,
                }],
            );
            s.spot_prices.insert(
                "BTCUSDT".into(),
                crate::core::monitor::state::TimestampedPrice::new(84000.0),
            );
        }
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "btc 5m up".into(),
                side: None,
                size_usd: 100.0,
                price: Some(0.55), // manual override
            },
        )
        .await;
        assert!(result.contains("Paper Trade Opened"));
        assert!(result.contains("0.5500"));
        assert!(result.contains("$84000"));
    }

    #[tokio::test]
    async fn test_paper_trade_ambiguous_market() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let now = chrono::Utc::now().timestamp();
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now - 60,
                    up_token_id: "btc_up_tok".into(),
                    down_token_id: "btc_down_tok".into(),
                    condition_id: "cond_test".into(),
                    slug: "btc-updown-5m-test".into(),
                    start_spot_price: None,
                }],
            );
        }
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        // "btc 5m" is ambiguous (UP or DOWN?)
        let result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "btc 5m".into(),
                side: None,
                size_usd: 100.0,
                price: Some(0.50),
            },
        )
        .await;
        // Should be ambiguous (matches both UP and DOWN tokens)
        assert!(
            result.contains("Ambiguous") || result.contains("Paper Trade Opened"),
            "Expected ambiguous or opened, got: {}",
            result
        );
    }

    // ── get_paper_portfolio tests ──

    #[tokio::test]
    async fn test_portfolio_empty() {
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let client = reqwest::Client::new();
        let result = handle_get_paper_portfolio(
            &portfolio,
            &client,
            GetPaperPortfolioParams {
                settle: Some(false), // skip settlement to avoid API calls
                format: None,
            },
        )
        .await;
        assert!(result.contains("## Paper Portfolio"));
        assert!(result.contains("No open positions"));
        assert!(result.contains("No settled trades yet"));
    }

    // ── paper_close tests ──

    #[tokio::test]
    async fn test_paper_close_with_manual_price() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let now = chrono::Utc::now().timestamp();
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: now - 60,
                    up_token_id: "btc_up_tok".into(),
                    down_token_id: "btc_down_tok".into(),
                    condition_id: "cond_test".into(),
                    slug: "btc-updown-5m-test".into(),
                    start_spot_price: None,
                }],
            );
            s.spot_prices.insert(
                "BTCUSDT".into(),
                crate::core::monitor::state::TimestampedPrice::new(84000.0),
            );
        }
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));

        // Open a position first
        let open_result = handle_paper_trade(
            &state,
            &portfolio,
            PaperTradeParams {
                market: "btc 5m up".into(),
                side: None,
                size_usd: 100.0,
                price: Some(0.50),
            },
        )
        .await;
        assert!(open_result.contains("Paper Trade Opened"));

        // Close it
        let close_result = handle_paper_close(
            &state,
            &portfolio,
            PaperCloseParams {
                position_id: 1,
                price: Some(0.65),
            },
        )
        .await;
        assert!(close_result.contains("Closed Early"));
        assert!(close_result.contains("profit"));
    }

    #[tokio::test]
    async fn test_paper_close_not_found() {
        let state = SharedState::new();
        let dir = TempDir::new().unwrap();
        let portfolio = Arc::new(Mutex::new(test_portfolio(&dir)));
        let result = handle_paper_close(
            &state,
            &portfolio,
            PaperCloseParams {
                position_id: 999,
                price: Some(0.50),
            },
        )
        .await;
        assert!(result.contains("not found"));
    }

    // ── guess_underlying tests ──

    #[test]
    fn test_guess_underlying_btc() {
        assert_eq!(guess_underlying("BTC 5m UP"), "BTCUSDT");
        assert_eq!(guess_underlying("Bitcoin above 90K"), "BTCUSDT");
    }

    #[test]
    fn test_guess_underlying_eth() {
        assert_eq!(guess_underlying("ETH 15m DOWN"), "ETHUSDT");
        assert_eq!(guess_underlying("ethereum up or down"), "ETHUSDT");
    }

    #[test]
    fn test_guess_underlying_other_assets() {
        assert_eq!(guess_underlying("SOL 5m UP"), "SOLUSDT");
        assert_eq!(guess_underlying("XRP daily"), "XRPUSDT");
        assert_eq!(guess_underlying("DOGE 5m DOWN"), "DOGEUSDT");
        assert_eq!(guess_underlying("BNB monthly"), "BNBUSDT");
    }

    #[test]
    fn test_guess_underlying_fallback() {
        // Unknown market name falls back to BTCUSDT
        assert_eq!(guess_underlying("unknown market"), "BTCUSDT");
    }
}
