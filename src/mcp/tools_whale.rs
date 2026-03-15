// MCP tool: get_whale_activity — view recent large trades on Polymarket daily/monthly crypto markets.
// Only tracks daily/monthly markets. 5m/15m trades are too small (median ~$4) for whale detection.
// Settlement artifacts (price >= 0.95) are filtered out at the alert layer.

use chrono::Utc;
use rmcp::schemars;
use serde::Deserialize;

use crate::core::monitor::state::SharedState;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetWhaleActivityParams {
    /// Filter by asset ticker (e.g. "btc", "eth"). Omit for all assets.
    pub asset: Option<String>,
    /// Lookback in minutes (default: 60).
    pub since_minutes: Option<u64>,
    /// Minimum trade size in USD (default: 10000). Daily/monthly markets only.
    pub min_size_usd: Option<f64>,
    /// Output format: "text" (default) or "json".
    pub format: Option<String>,
}

pub async fn handle_get_whale_activity(state: &SharedState, params: GetWhaleActivityParams) -> String {
    let since_minutes = params.since_minutes.unwrap_or(60);
    let min_size = params.min_size_usd.unwrap_or(10000.0);
    let cutoff = Utc::now() - chrono::Duration::minutes(since_minutes as i64);
    let json_mode = params.format.as_deref() == Some("json");

    let state = state.read().await;

    let trades: Vec<_> = state
        .whale_trades
        .iter()
        .filter(|t| t.timestamp >= cutoff && t.size_usd >= min_size)
        .filter(|t| {
            if let Some(ref asset) = params.asset {
                t.market_name
                    .to_lowercase()
                    .contains(&asset.to_lowercase())
            } else {
                true
            }
        })
        .collect();

    if trades.is_empty() {
        let asset_note = params
            .asset
            .as_deref()
            .map(|a| format!(" for {}", a.to_uppercase()))
            .unwrap_or_default();
        return format!(
            "No whale activity{} in the last {} minutes (threshold: ${:.0}).",
            asset_note, since_minutes, min_size
        );
    }

    // Compute summary stats
    let total_count = trades.len();
    let mut total_volume = 0.0;
    let mut buy_volume = 0.0;
    let mut sell_volume = 0.0;
    let mut largest_trade: Option<&crate::core::monitor::state::WhaleTrade> = None;

    for t in &trades {
        total_volume += t.size_usd;
        if t.side.to_uppercase() == "BUY" {
            buy_volume += t.size_usd;
        } else {
            sell_volume += t.size_usd;
        }
        if largest_trade.map_or(true, |prev| t.size_usd > prev.size_usd) {
            largest_trade = Some(t);
        }
    }

    let net_flow = buy_volume - sell_volume;
    let net_label = if net_flow >= 0.0 { "buy" } else { "sell" };

    if json_mode {
        let trade_records: Vec<serde_json::Value> = trades
            .iter()
            .rev()
            .map(|t| {
                serde_json::json!({
                    "timestamp": t.timestamp.to_rfc3339(),
                    "market": t.market_name,
                    "side": t.side,
                    "size_usd": t.size_usd,
                    "price": t.price,
                    "token_id": t.token_id,
                })
            })
            .collect();

        let result = serde_json::json!({
            "trades": trade_records,
            "summary": {
                "count": total_count,
                "total_volume_usd": total_volume,
                "buy_volume_usd": buy_volume,
                "sell_volume_usd": sell_volume,
                "net_flow_usd": net_flow,
                "net_direction": net_label,
                "since_minutes": since_minutes,
                "min_size_usd": min_size,
            }
        });
        return serde_json::to_string_pretty(&result).unwrap_or_default();
    }

    // Text format
    let mut out = String::new();
    let asset_note = params
        .asset
        .as_deref()
        .map(|a| format!(" — {}", a.to_uppercase()))
        .unwrap_or_default();

    out.push_str(&format!(
        "## Whale Activity (last {}m{})\n\n",
        since_minutes, asset_note
    ));

    // Summary
    out.push_str(&format!(
        "**{} trades** | Total: ${:.0} | Buy: ${:.0} | Sell: ${:.0} | Net: ${:.0} {}\n\n",
        total_count, total_volume, buy_volume, sell_volume, net_flow.abs(), net_label
    ));

    if let Some(biggest) = largest_trade {
        out.push_str(&format!(
            "**Largest:** {} ${:.0} on {} @ {:.3}\n\n",
            biggest.side, biggest.size_usd, biggest.market_name, biggest.price
        ));
    }

    // Trade table (most recent first)
    out.push_str("| Time (UTC) | Market | Side | Size | Price |\n");
    out.push_str("|------------|--------|------|------|-------|\n");

    for t in trades.iter().rev().take(50) {
        let time = t.timestamp.format("%H:%M:%S");
        out.push_str(&format!(
            "| {} | {} | {} | ${:.0} | {:.3} |\n",
            time, t.market_name, t.side, t.size_usd, t.price
        ));
    }

    if total_count > 50 {
        out.push_str(&format!("\n*Showing 50 of {} trades.*\n", total_count));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::monitor::state::WhaleTrade;

    async fn make_state_with_trades(trades: Vec<WhaleTrade>) -> SharedState {
        let state = SharedState::new();
        let mut s = state.write().await;
        for t in trades {
            s.push_whale_trade(t);
        }
        drop(s);
        state
    }

    fn make_trade(market: &str, side: &str, size: f64, mins_ago: i64) -> WhaleTrade {
        WhaleTrade {
            token_id: "0xabc".into(),
            market_name: market.into(),
            side: side.into(),
            price: 0.65,
            size_usd: size,
            timestamp: Utc::now() - chrono::Duration::minutes(mins_ago),
        }
    }

    #[tokio::test]
    async fn test_empty_whale_activity() {
        let state = SharedState::new();
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: None,
                since_minutes: None,
                min_size_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No whale activity"));
    }

    #[tokio::test]
    async fn test_filter_by_asset() {
        let state = make_state_with_trades(vec![
            make_trade("BTC Daily Above 85k", "BUY", 15000.0, 5),
            make_trade("ETH Daily Above 2500", "SELL", 12000.0, 3),
        ])
        .await;
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: Some("btc".into()),
                since_minutes: None,
                min_size_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("BTC Daily Above 85k"));
        assert!(!result.contains("ETH Daily Above 2500"));
    }

    #[tokio::test]
    async fn test_filter_by_min_size() {
        let state = make_state_with_trades(vec![
            make_trade("BTC Daily Above 85k", "BUY", 5000.0, 5),
            make_trade("BTC Daily Above 90k", "SELL", 15000.0, 3),
        ])
        .await;
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: None,
                since_minutes: None,
                min_size_usd: Some(10000.0),
                format: None,
            },
        )
        .await;
        assert!(result.contains("1 trades"));
        assert!(result.contains("BTC Daily Above 90k"));
    }

    #[tokio::test]
    async fn test_filter_by_time() {
        let state = make_state_with_trades(vec![
            make_trade("BTC Daily Above 85k", "BUY", 15000.0, 120), // 2 hours ago
            make_trade("BTC Daily Above 90k", "SELL", 12000.0, 3),
        ])
        .await;
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: None,
                since_minutes: Some(30),
                min_size_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("1 trades"));
    }

    #[tokio::test]
    async fn test_summary_stats() {
        let state = make_state_with_trades(vec![
            make_trade("BTC Daily Above 85k", "BUY", 15000.0, 5),
            make_trade("BTC Daily Above 90k", "SELL", 12000.0, 3),
        ])
        .await;
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: None,
                since_minutes: None,
                min_size_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("2 trades"));
        assert!(result.contains("Buy: $15000"));
        assert!(result.contains("Sell: $12000"));
    }

    #[tokio::test]
    async fn test_json_output() {
        let state =
            make_state_with_trades(vec![make_trade("BTC Daily Above 85k", "BUY", 15000.0, 5)])
                .await;
        let result = handle_get_whale_activity(
            &state,
            GetWhaleActivityParams {
                asset: None,
                since_minutes: None,
                min_size_usd: None,
                format: Some("json".into()),
            },
        )
        .await;
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["summary"]["count"], 1);
        assert_eq!(parsed["summary"]["buy_volume_usd"], 15000.0);
    }
}
