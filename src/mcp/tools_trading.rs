// MCP tool handlers for live trading (Phase 2).
// Tools: get_balance, place_order, cancel_order, get_positions, redeem_winnings
//
// Param types and handler functions for live trading tools.

use rmcp::schemars;
use serde::Deserialize;

// ──────────────────────────── Param types (always available) ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetBalanceParams {
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PlaceOrderParams {
    /// Market description (e.g. "BTC 5m UP") or token ID
    pub market: String,
    /// "buy" (default) or "sell"
    pub side: Option<String>,
    /// Position size in USD
    pub size_usd: f64,
    /// Override price (default: best ask for buy, best bid for sell)
    pub price: Option<f64>,
    /// Dry run mode (default: true). Set to false for real execution.
    pub dry_run: Option<bool>,
    /// Must be true to execute a real order when dry_run=false. Safety confirmation.
    pub confirm: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CancelOrderParams {
    /// Order ID to cancel. Use "all" to cancel all open orders.
    pub order_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetPositionsParams {
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ApproveExchangeParams {
    /// Check approval status only (no transactions). Default: false.
    pub check_only: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RedeemWinningsParams {
    /// Condition ID of the settled market to redeem (hex, with or without 0x prefix)
    pub condition_id: String,
    /// Whether this is a NegRisk market (5m/15m markets are NegRisk). Default: true.
    pub neg_risk: Option<bool>,
}

// ──────────────────────────── Handlers ────────────────────────────

pub mod handlers {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::core::execution::live::LiveExecutor;
    use crate::core::execution::risk::RiskManager;
    use crate::core::monitor::state::SharedState;
    use crate::core::providers::resolver::{resolve_market_strict, ResolveResult};
    use crate::mcp::formatter::polymarket_url;

    use super::*;

    pub async fn handle_get_balance(
        executor: &Arc<LiveExecutor>,
        risk_manager: &Arc<Mutex<RiskManager>>,
        _params: GetBalanceParams,
    ) -> String {
        let balance = match executor.get_balance().await {
            Ok(b) => b,
            Err(e) => return format!("Failed to query balance: {}", e),
        };

        let rm = risk_manager.lock().await;

        format!(
            "## Wallet Balance\n\n\
             Address: `{}`\n\
             USDC Balance: **${:.2}**\n\n\
             ## Risk Status\n\
             {}",
            executor.address(),
            balance,
            rm.status_summary(),
        )
    }

    pub async fn handle_place_order(
        state: &SharedState,
        executor: &Arc<LiveExecutor>,
        risk_manager: &Arc<Mutex<RiskManager>>,
        params: PlaceOrderParams,
    ) -> String {
        let is_sell = params
            .side
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("sell"))
            .unwrap_or(false);
        let dry_run = params.dry_run.unwrap_or(true);
        let confirm = params.confirm.unwrap_or(false);

        // Safety: real execution requires explicit opt-in
        if !dry_run && !confirm {
            return "**Safety check:** To place a real order, set both `dry_run: false` AND `confirm: true`. \
                    This is a safety measure to prevent accidental trades."
                .to_string();
        }

        // Resolve market
        let s = state.read().await;
        let short_term: Vec<(String, Vec<_>)> = s
            .short_term_markets
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let daily = s.daily_markets.clone();
        let monthly = s.monthly_markets.clone();

        let resolved = match resolve_market_strict(&params.market, &short_term, &daily, &monthly) {
            ResolveResult::Found(m) => m,
            ResolveResult::Ambiguous(matches) => {
                let options: Vec<String> =
                    matches.iter().map(|m| format!("- {}", m.name)).collect();
                return format!(
                    "Ambiguous market '{}'. Did you mean:\n{}",
                    params.market,
                    options.join("\n")
                );
            }
            ResolveResult::NotFound(msg) => return msg,
        };

        // Get price from order book or override
        let price = if let Some(p) = params.price {
            p
        } else if let Some(tsb) = s.order_books.get(&resolved.token_id) {
            if is_sell {
                match tsb.book.best_bid() {
                    Some(bid) => bid,
                    None => {
                        return "Order book empty — cannot determine price. Provide `price` manually."
                            .to_string()
                    }
                }
            } else {
                match tsb.book.best_ask() {
                    Some(ask) => ask,
                    None => {
                        return "Order book empty — cannot determine price. Provide `price` manually."
                            .to_string()
                    }
                }
            }
        } else {
            return format!(
                "No order book data for {}. Provide `price` manually or wait for background poller.",
                resolved.name
            );
        };

        let slug = find_slug(&resolved.token_id, &short_term, &daily, &monthly);
        drop(s);

        // Validate price range
        if price <= 0.0 || price >= 1.0 {
            return format!(
                "Invalid price {:.4} — must be between 0.01 and 0.99",
                price
            );
        }

        // Validate size
        if params.size_usd <= 0.0 || !params.size_usd.is_finite() {
            return format!(
                "Invalid size ${:.2} — must be a positive finite number",
                params.size_usd
            );
        }

        let size_tokens = params.size_usd / price;

        // Validate order size
        if params.size_usd > executor.max_order_size_usd() {
            return format!(
                "Order size ${:.2} exceeds configured max ${:.2}",
                params.size_usd,
                executor.max_order_size_usd()
            );
        }

        // Risk check
        {
            let rm = risk_manager.lock().await;
            if let Err(rejection) = rm.can_trade() {
                return format!("**Risk manager blocked trade:** {}", rejection);
            }
        }

        // Build URL
        let url = if !slug.is_empty() {
            format!("\n{}", polymarket_url(&slug))
        } else {
            String::new()
        };

        if dry_run {
            let fee = crate::core::analysis::fees::polymarket_fee(price);
            let total_fee = fee * size_tokens;
            return format!(
                "## Dry Run — Order Preview\n\n\
                 Market: {}\n\
                 Side: {}\n\
                 Price: {:.4}\n\
                 Size: ${:.2} ({:.1} contracts)\n\
                 Est. Fee: ${:.4}\n\
                 Token: `{}`{}\n\n\
                 *Set `dry_run: false` and `confirm: true` to execute.*",
                resolved.name,
                if is_sell { "SELL" } else { "BUY" },
                price,
                params.size_usd,
                size_tokens,
                total_fee,
                resolved.token_id,
                url,
            );
        }

        // Pre-warm SDK metadata cache for this token (fee_rate, tick_size, neg_risk).
        // Non-fatal — first order will just be slightly slower if this fails.
        if let Err(e) = executor.preload_token_metadata(&resolved.token_id).await {
            tracing::debug!(error = %e, "Token metadata preload failed (non-fatal)");
        }

        // Execute (dry_run from tool params, not config)
        let result = if is_sell {
            executor
                .place_exit_order(&resolved.token_id, size_tokens, price, None, dry_run)
                .await
        } else {
            executor
                .place_entry_order(&resolved.token_id, price, size_tokens, None, dry_run)
                .await
        };

        match result {
            Ok(order) => {
                if order.filled {
                    risk_manager.lock().await.record_position_opened();
                }
                format!(
                    "## Order {}\n\n\
                     Market: {}\n\
                     Side: {}\n\
                     Order ID: `{}`\n\
                     Status: {}\n\
                     Filled: {} ({:.1} contracts @ {:.4}){}\n",
                    if order.filled { "Filled" } else { "Submitted" },
                    resolved.name,
                    if is_sell { "SELL" } else { "BUY" },
                    order.order_id,
                    order.status,
                    order.filled,
                    order.filled_size,
                    order.avg_price,
                    url,
                )
            }
            Err(e) => format!("**Order failed:** {}", e),
        }
    }

    pub async fn handle_cancel_order(
        executor: &Arc<LiveExecutor>,
        params: CancelOrderParams,
    ) -> String {
        if params.order_id.eq_ignore_ascii_case("all") {
            match executor.cancel_all().await {
                Ok(()) => "All open orders cancelled.".to_string(),
                Err(e) => format!("Failed to cancel all orders: {}", e),
            }
        } else {
            match executor.cancel_order(&params.order_id).await {
                Ok(()) => format!("Order `{}` cancelled.", params.order_id),
                Err(e) => format!("Failed to cancel order `{}`: {}", params.order_id, e),
            }
        }
    }

    pub async fn handle_get_positions(
        executor: &Arc<LiveExecutor>,
        risk_manager: &Arc<Mutex<RiskManager>>,
        _params: GetPositionsParams,
    ) -> String {
        let balance = match executor.get_balance().await {
            Ok(b) => b,
            Err(e) => return format!("Failed to query balance: {}", e),
        };

        let rm = risk_manager.lock().await;

        format!(
            "## Live Trading Status\n\n\
             Address: `{}`\n\
             USDC Balance: **${:.2}**\n\n\
             ## Risk Status\n\
             {}\n\n\
             *Note: For detailed position tracking, use paper trading portfolio \
             or check polymarket.com directly. On-chain position queries coming in a future update.*",
            executor.address(),
            balance,
            rm.status_summary(),
        )
    }

    pub async fn handle_approve_exchange(
        executor: &Arc<LiveExecutor>,
        params: ApproveExchangeParams,
    ) -> String {
        let check_only = params.check_only.unwrap_or(false);

        let result = if check_only {
            executor.check_approvals().await
        } else {
            executor.approve_exchange().await
        };

        match result {
            Ok(approval) => {
                let mut out = String::new();
                if check_only {
                    out.push_str("## Exchange Approval Status\n\n");
                } else if approval.tx_hashes.is_empty() {
                    out.push_str("## Exchange Approvals (Already Set)\n\n");
                } else {
                    out.push_str("## Exchange Approvals Set\n\n");
                }

                out.push_str(&format!("Address: `{}`\n\n", executor.address()));

                out.push_str("| Contract | USDC | CTF |\n|----------|------|-----|\n");
                for s in &approval.statuses {
                    out.push_str(&format!(
                        "| {} | {} | {} |\n",
                        s.contract_name,
                        if s.usdc_approved {
                            format!("Approved ({})", s.usdc_allowance)
                        } else {
                            "NOT APPROVED".to_string()
                        },
                        if s.ctf_approved { "Approved" } else { "NOT APPROVED" },
                    ));
                }

                if !approval.tx_hashes.is_empty() {
                    out.push_str(&format!(
                        "\n**{} transaction(s) confirmed:**\n",
                        approval.tx_hashes.len()
                    ));
                    for tx in &approval.tx_hashes {
                        out.push_str(&format!("- `{}`\n", tx));
                    }
                }

                if !approval.all_approved && check_only {
                    out.push_str(
                        "\n**Some approvals missing.** Run `approve_exchange` with `check_only: false` \
                         to set them (requires POL for gas).",
                    );
                }

                out
            }
            Err(e) => format!("**Approval failed:** {}", e),
        }
    }

    pub async fn handle_redeem_winnings(
        executor: &Arc<LiveExecutor>,
        params: RedeemWinningsParams,
    ) -> String {
        let is_neg_risk = params.neg_risk.unwrap_or(true);

        match executor.redeem(&params.condition_id, is_neg_risk).await {
            Ok(tx_hash) => {
                format!(
                    "## Redemption Successful\n\n\
                     Condition: `{}`\n\
                     Type: {}\n\
                     TX: `{}`\n\n\
                     USDC should appear in your wallet shortly.",
                    params.condition_id,
                    if is_neg_risk { "NegRisk" } else { "Standard" },
                    tx_hash,
                )
            }
            Err(e) => format!("**Redemption failed:** {}", e),
        }
    }

    /// Find the slug for a token ID across all market collections.
    fn find_slug(
        token_id: &str,
        short_term: &[(String, Vec<crate::core::types::ShortTermMarket>)],
        daily: &[crate::core::types::MarketConfig],
        monthly: &[crate::core::types::MarketConfig],
    ) -> String {
        for (_key, markets) in short_term {
            for m in markets {
                if m.up_token_id == token_id || m.down_token_id == token_id {
                    return m.slug.clone();
                }
            }
        }
        for m in daily.iter().chain(monthly.iter()) {
            if m.yes_token_id == token_id || m.no_token_id == token_id {
                return m.name.clone();
            }
        }
        String::new()
    }
}
