// MCP tool handlers for auto-trade configuration and status.
// Tools: set_auto_trade, get_auto_trade_status

use rmcp::schemars;
use serde::Deserialize;

use crate::core::monitor::auto_trade::{
    AutoTradeConfig, AutoTradeMode, AutoTradeSide, SharedAutoTradeState,
};
use crate::mcp::formatter::fmt_usd;

// ──────────────────────────── set_auto_trade ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetAutoTradeParams {
    /// Asset ticker: btc, eth, sol, xrp, doge, bnb
    pub asset: String,
    /// Window type: "5m" or "15m"
    pub window: String,
    /// Enable or disable auto-trade for this asset+window
    pub enabled: bool,
    /// Minimum elapsed percentage before entry zone (50-99). Required when enabling.
    pub entry_pct: Option<f64>,
    /// Minimum spot price move percentage to trigger entry. Required when enabling.
    pub min_move_pct: Option<f64>,
    /// Maximum entry price (0-1 exclusive). Required when enabling.
    pub max_entry_price: Option<f64>,
    /// Position size in USD per trade. Required when enabling.
    pub position_size_usd: Option<f64>,
    /// Execution mode: "paper" (default) or "live". Live places real orders.
    pub mode: Option<String>,
    /// Maximum spread percentage to accept. Optional — omit for no limit.
    pub max_spread_pct: Option<f64>,
    /// Minimum ask-side depth in USD required. Optional — omit for no limit.
    pub min_depth_usd: Option<f64>,
    /// Side override: "auto" (default — follows spot direction), "up", or "down".
    pub side: Option<String>,
    /// Maximum total exposure across all open auto-trade positions in USD. Optional — omit for no limit.
    pub max_total_exposure_usd: Option<f64>,
}

pub async fn handle_set_auto_trade(
    auto_state: &SharedAutoTradeState,
    params: SetAutoTradeParams,
) -> String {
    let asset = params.asset.to_lowercase();

    let valid_assets = ["btc", "eth", "sol", "xrp", "doge", "bnb"];
    if !valid_assets.contains(&asset.as_str()) {
        return format!(
            "Invalid asset '{}'. Valid: {}",
            asset,
            valid_assets.join(", ")
        );
    }

    let window: u32 = match params.window.as_str() {
        "5m" | "5" => 5,
        "15m" | "15" => 15,
        _ => return format!("Invalid window '{}'. Use '5m' or '15m'.", params.window),
    };

    let mode = match params.mode.as_deref().unwrap_or("paper") {
        "paper" => AutoTradeMode::Paper,
        "live" => AutoTradeMode::Live,
        other => return format!("Invalid mode '{}'. Use 'paper' or 'live'.", other),
    };

    // When enabling, position_size_usd is required. Strategy params have sensible defaults.
    if params.enabled && params.position_size_usd.is_none() {
        return "position_size_usd is required when enabling auto paper trade. \
                How much USD per trade?"
            .to_string();
    }

    let entry_pct = params.entry_pct.unwrap_or(80.0);
    let min_move_pct = params.min_move_pct.unwrap_or(0.03);
    let max_entry_price = params.max_entry_price.unwrap_or(0.80);
    let position_size_usd = params.position_size_usd.unwrap_or(0.0);

    let side = match params.side.as_deref().unwrap_or("auto") {
        "auto" => AutoTradeSide::Auto,
        "up" => AutoTradeSide::Up,
        "down" => AutoTradeSide::Down,
        other => return format!("Invalid side '{}'. Use 'auto', 'up', or 'down'.", other),
    };

    if params.enabled {
        if !(50.0..=99.0).contains(&entry_pct) {
            return format!("entry_pct must be between 50 and 99, got {:.0}", entry_pct);
        }
        if max_entry_price <= 0.0 || max_entry_price >= 1.0 {
            return format!(
                "max_entry_price must be between 0 and 1 exclusive, got {:.3}",
                max_entry_price
            );
        }
        if position_size_usd <= 0.0 {
            return format!(
                "position_size_usd must be positive, got {:.2}",
                position_size_usd
            );
        }
        if let Some(spread) = params.max_spread_pct {
            if spread <= 0.0 {
                return format!("max_spread_pct must be positive, got {:.1}", spread);
            }
        }
        if let Some(depth) = params.min_depth_usd {
            if depth <= 0.0 {
                return format!("min_depth_usd must be positive, got {:.0}", depth);
            }
        }
        if let Some(exposure) = params.max_total_exposure_usd {
            if exposure <= 0.0 {
                return format!("max_total_exposure_usd must be positive, got {:.0}", exposure);
            }
        }
    }

    let config = AutoTradeConfig {
        asset: asset.clone(),
        window,
        enabled: params.enabled,
        entry_pct,
        min_move_pct,
        max_entry_price,
        position_size_usd,
        mode,
        max_spread_pct: params.max_spread_pct,
        min_depth_usd: params.min_depth_usd,
        side,
        max_total_exposure_usd: params.max_total_exposure_usd,
    };

    let mut state = auto_state.lock().await;
    state.set_config(config);

    if let Err(e) = state.save() {
        return format!("Auto-trade config set but failed to persist: {e}");
    }

    if params.enabled {
        let mut extra = String::new();
        if let Some(s) = params.max_spread_pct {
            extra.push_str(&format!("Max spread: {:.1}%\n", s));
        }
        if let Some(d) = params.min_depth_usd {
            extra.push_str(&format!("Min depth: {}\n", fmt_usd(d)));
        }
        if side != AutoTradeSide::Auto {
            extra.push_str(&format!("Side: {} (forced)\n", side));
        }
        if let Some(e) = params.max_total_exposure_usd {
            extra.push_str(&format!("Max total exposure: {}\n", fmt_usd(e)));
        }

        format!(
            "## Auto-Trade Enabled\n\n\
             **{} {}m** — {}\n\
             Entry zone: >{:.0}% elapsed\n\
             Min spot move: {:.3}%\n\
             Max entry price: {:.3}\n\
             Position size: {}\n\
             {}\
             \nThe background monitor will check entry conditions every ~5s during the entry zone \
             and execute automatically when all conditions are met.\n\n\
             To stop: `set_auto_trade` with `enabled: false`\n\
             To check: `get_auto_trade_status`",
            asset.to_uppercase(),
            window,
            mode,
            entry_pct,
            min_move_pct,
            max_entry_price,
            fmt_usd(position_size_usd),
            extra,
        )
    } else {
        format!(
            "## Auto-Trade Disabled\n\n\
             **{} {}m** — stopped.\n\
             No new trades will be opened. Existing positions will settle normally.",
            asset.to_uppercase(),
            window,
        )
    }
}

// ──────────────────────────── get_auto_trade_status ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetAutoTradeStatusParams {
    /// Filter by asset (optional, default: all)
    pub asset: Option<String>,
    /// Maximum recent trades to show (default: 10)
    pub limit: Option<usize>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_auto_trade_status(
    auto_state: &SharedAutoTradeState,
    params: GetAutoTradeStatusParams,
) -> String {
    let state = auto_state.lock().await;
    let limit = params.limit.unwrap_or(10);
    let asset_filter = params.asset.as_deref().map(|a| a.to_lowercase());

    let mut out = String::from("## Auto-Trade Status\n\n");

    // Active configs
    let configs: Vec<_> = state
        .configs
        .iter()
        .filter(|c| {
            if let Some(ref a) = asset_filter {
                c.asset == *a
            } else {
                true
            }
        })
        .collect();

    let active_count = configs.iter().filter(|c| c.enabled).count();

    if configs.is_empty() {
        out.push_str("No auto-trade configs set. Use `set_auto_trade` to enable.\n");
        return out;
    }

    out.push_str(&format!(
        "### Configs ({} active, {} total)\n\n",
        active_count,
        configs.len()
    ));

    for config in &configs {
        let status = if config.enabled { "ACTIVE" } else { "OFF" };
        let mut extras = Vec::new();
        if let Some(s) = config.max_spread_pct {
            extras.push(format!("Max spread: {:.1}%", s));
        }
        if let Some(d) = config.min_depth_usd {
            extras.push(format!("Min depth: {}", fmt_usd(d)));
        }
        if config.side != AutoTradeSide::Auto {
            extras.push(format!("Side: {}", config.side));
        }
        if let Some(e) = config.max_total_exposure_usd {
            extras.push(format!("Max exposure: {}", fmt_usd(e)));
        }
        let extra_str = if extras.is_empty() {
            String::new()
        } else {
            format!("\n  {}", extras.join(" | "))
        };

        out.push_str(&format!(
            "**{} {}m** — {} ({})\n  \
             Entry: >{:.0}% elapsed | Min move: {:.3}% | Max price: {:.3} | Size: {}{}\n\n",
            config.asset.to_uppercase(),
            config.window,
            status,
            config.mode,
            config.entry_pct,
            config.min_move_pct,
            config.max_entry_price,
            fmt_usd(config.position_size_usd),
            extra_str,
        ));
    }

    // Recent trades
    let recent: Vec<_> = state
        .recent_trades
        .iter()
        .filter(|t| {
            if let Some(ref a) = asset_filter {
                t.asset.to_lowercase() == *a
            } else {
                true
            }
        })
        .rev()
        .take(limit)
        .collect();

    if !recent.is_empty() {
        out.push_str(&format!("### Recent Trades (last {})\n\n", recent.len()));
        for trade in &recent {
            let mode_tag = match trade.mode {
                AutoTradeMode::Live => " [LIVE]",
                AutoTradeMode::Paper => "",
            };
            let pos_str = trade
                .position_id
                .map(|id| format!(" → #{id}"))
                .unwrap_or_default();

            out.push_str(&format!(
                "- **{}** {} {}m {} @ {:.3} ({}){}{}\n  \
                 Spot: {} | Move: {:.3}% | Elapsed: {:.0}%\n",
                trade.timestamp.format("%H:%M:%S UTC"),
                trade.asset.to_uppercase(),
                trade.window,
                trade.side,
                trade.entry_price,
                fmt_usd(trade.size_usd),
                mode_tag,
                pos_str,
                fmt_usd(trade.spot_price),
                trade.spot_move_pct,
                trade.elapsed_pct,
            ));
        }
    } else {
        out.push_str("### Recent Trades\n\nNo auto-trades executed yet.\n");
    }

    // Summary stats from recent trades
    let all_trades: Vec<_> = state
        .recent_trades
        .iter()
        .filter(|t| {
            if let Some(ref a) = asset_filter {
                t.asset.to_lowercase() == *a
            } else {
                true
            }
        })
        .collect();

    if !all_trades.is_empty() {
        let total = all_trades.len();
        let total_volume: f64 = all_trades.iter().map(|t| t.size_usd).sum();
        let avg_price: f64 =
            all_trades.iter().map(|t| t.entry_price).sum::<f64>() / total as f64;

        out.push_str(&format!(
            "\n### Summary\n\
             Total auto-trades: {} | Volume: {} | Avg entry: {:.3}\n",
            total,
            fmt_usd(total_volume),
            avg_price,
        ));
    }

    out
}
