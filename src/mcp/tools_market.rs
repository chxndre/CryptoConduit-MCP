// MCP tool handlers for market discovery and spot prices.
// Tools: search_crypto_markets, get_active_window, get_window_briefing, get_spot_price

use rmcp::schemars;
use serde::Deserialize;

use crate::core::analysis::fees;
use crate::core::analysis::microstructure;
use crate::core::monitor::state::SharedState;
use crate::core::types::TradeMode;
use crate::mcp::formatter::{fmt_age, fmt_pct, fmt_usd, fmt_window_time, polymarket_url};

/// Format spot price move: "Start: $73,280 → Now: $73,303 (+0.03%)"
fn format_spot_move(start: Option<f64>, current: Option<f64>) -> String {
    match (start, current) {
        (Some(s), Some(c)) => {
            let delta_pct = (c - s) / s * 100.0;
            let sign = if delta_pct >= 0.0 { "+" } else { "" };
            format!(
                " | Start: {} → Now: {} ({sign}{:.2}%)",
                fmt_usd(s),
                fmt_usd(c),
                delta_pct
            )
        }
        (None, Some(c)) => format!(" | Spot: {}", fmt_usd(c)),
        (Some(s), None) => format!(" | Start: {}", fmt_usd(s)),
        (None, None) => String::new(),
    }
}

// ──────────────────────────── search_crypto_markets ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchCryptoMarketsParams {
    /// Asset filter: btc, eth, sol, xrp, doge, bnb, or "all" (default: "all")
    pub asset: Option<String>,
    /// Market type filter: 5m, 15m, daily, monthly, or "all" (default: "all")
    pub market_type: Option<String>,
    /// Sort by: "spread" (tightest first), "volume" (highest first), or "default" (no sort). Default: "default"
    pub sort_by: Option<String>,
    /// Maximum number of results per table (default: 10)
    pub limit: Option<usize>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

/// Intermediate entry for sortable market results.
#[derive(Clone)]
struct MarketEntry {
    label: String,
    spread_pct: Option<f64>,
    volume_usd: f64,
    /// Structured data for JSON output
    json_data: serde_json::Value,
}

impl MarketEntry {
    fn format_line(&self) -> &str {
        &self.label
    }
}

pub async fn handle_search_crypto_markets(
    state: &SharedState,
    params: SearchCryptoMarketsParams,
) -> String {
    let s = state.read().await;
    let asset_filter = params.asset.unwrap_or_else(|| "all".into()).to_lowercase();
    let type_filter = params.market_type.unwrap_or_else(|| "all".into()).to_lowercase();
    let sort_by = params.sort_by.unwrap_or_else(|| "default".into()).to_lowercase();
    let limit = params.limit.unwrap_or(10);
    let json_mode = params.format.as_deref() == Some("json");
    let mut entries: Vec<MarketEntry> = Vec::new();

    // Short-term markets
    if type_filter == "all" || type_filter == "5m" || type_filter == "15m" {
        for (key, markets) in &s.short_term_markets {
            for market in markets {
                if asset_filter != "all" && market.asset.to_lowercase() != asset_filter {
                    continue;
                }
                if type_filter != "all" {
                    let expected_interval: u32 = if type_filter == "5m" { 5 } else { 15 };
                    if market.interval != expected_interval {
                        continue;
                    }
                }

                let up_book = s.order_books.get(&market.up_token_id);
                let down_book = s.order_books.get(&market.down_token_id);

                let up_spread = up_book.and_then(|b| b.book.spread_pct());
                let down_spread = down_book.and_then(|b| b.book.spread_pct());
                let up_ask = up_book.and_then(|b| b.book.best_ask());
                let down_ask = down_book.and_then(|b| b.book.best_ask());

                // Best (tightest) spread of the two sides
                let best_spread = match (up_spread, down_spread) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    _ => None,
                };

                // Use order book depth (10% slippage) as volume proxy for short-term markets
                let depth: f64 = [up_book, down_book]
                    .iter()
                    .filter_map(|b| b.as_ref())
                    .map(|b| b.book.bid_depth_within(10.0) + b.book.ask_depth_within(10.0))
                    .sum();

                let url = polymarket_url(&market.slug);
                let elapsed = chrono::Utc::now().timestamp() - market.window_start_ts;
                let remaining = ((market.interval as i64 * 60) - elapsed).max(0);

                // Spot price context
                let underlying = crate::core::providers::gamma::asset_to_underlying(&market.asset);
                let current_spot = s.spot_prices.get(&underlying).map(|p| p.price);
                let spot_line = format_spot_move(market.start_spot_price, current_spot);

                let window_time = fmt_window_time(market.window_start_ts);

                let time_label = if remaining == 0 {
                    "expired".to_string()
                } else {
                    format!("{remaining}s remaining")
                };

                entries.push(MarketEntry {
                    label: format!(
                        "**{} {}m ({window_time})** [{key}] — {time_label}{}\n  \
                         UP: ask={} spread={} | DOWN: ask={} spread={}\n  \
                         {url}",
                        market.asset,
                        market.interval,
                        spot_line,
                        up_ask.map_or("n/a".into(), |p| format!("{:.2}", p)),
                        up_spread.map_or("n/a".into(), fmt_pct),
                        down_ask.map_or("n/a".into(), |p| format!("{:.2}", p)),
                        down_spread.map_or("n/a".into(), fmt_pct),
                    ),
                    spread_pct: best_spread,
                    volume_usd: depth,
                    json_data: serde_json::json!({
                        "type": format!("{}m", market.interval),
                        "asset": market.asset,
                        "slug": market.slug,
                        "remaining_secs": remaining,
                        "start_spot_price": market.start_spot_price,
                        "current_spot_price": current_spot,
                        "up": { "ask": up_ask, "spread_pct": up_spread },
                        "down": { "ask": down_ask, "spread_pct": down_spread },
                        "best_spread_pct": best_spread,
                        "depth_usd": depth,
                        "url": url,
                    }),
                });
            }
        }
    }

    // Daily markets
    if type_filter == "all" || type_filter == "daily" {
        for market in &s.daily_markets {
            if asset_filter != "all" && !market.underlying.to_lowercase().starts_with(&asset_filter) {
                continue;
            }
            let yes_book = s.order_books.get(&market.yes_token_id);
            let spread = yes_book.and_then(|b| b.book.spread_pct());
            let ask = yes_book.and_then(|b| b.book.best_ask());

            entries.push(MarketEntry {
                label: format!(
                    "**{}** — strike {} | ask={} spread={} | vol={}",
                    market.name,
                    fmt_usd(market.strike_price),
                    ask.map_or("n/a".into(), |p| format!("{:.2}", p)),
                    spread.map_or("n/a".into(), fmt_pct),
                    fmt_usd(market.volume_usd),
                ),
                spread_pct: spread,
                volume_usd: market.volume_usd,
                json_data: serde_json::json!({
                    "type": "daily",
                    "name": market.name,
                    "underlying": market.underlying,
                    "strike_price": market.strike_price,
                    "ask": ask,
                    "spread_pct": spread,
                    "volume_usd": market.volume_usd,
                    "condition_id": market.condition_id,
                }),
            });
        }
    }

    // Monthly markets
    if type_filter == "all" || type_filter == "monthly" {
        for market in &s.monthly_markets {
            if asset_filter != "all" && !market.underlying.to_lowercase().starts_with(&asset_filter) {
                continue;
            }
            let yes_book = s.order_books.get(&market.yes_token_id);
            let spread = yes_book.and_then(|b| b.book.spread_pct());

            entries.push(MarketEntry {
                label: format!(
                    "**{}** — strike {} | spread={} | vol={}",
                    market.name,
                    fmt_usd(market.strike_price),
                    spread.map_or("n/a".into(), fmt_pct),
                    fmt_usd(market.volume_usd),
                ),
                spread_pct: spread,
                volume_usd: market.volume_usd,
                json_data: serde_json::json!({
                    "type": "monthly",
                    "name": market.name,
                    "underlying": market.underlying,
                    "strike_price": market.strike_price,
                    "spread_pct": spread,
                    "volume_usd": market.volume_usd,
                    "condition_id": market.condition_id,
                }),
            });
        }
    }

    if entries.is_empty() {
        let available = "BTC, ETH, SOL, XRP, DOGE, BNB";
        return format!("No markets found for asset={asset_filter}, type={type_filter}. Available assets: {available}");
    }

    // Helper to render a slice of entries
    let render = |entries: &[MarketEntry], heading: &str| -> String {
        if json_mode {
            let arr: Vec<_> = entries.iter().map(|e| &e.json_data).collect();
            serde_json::to_string_pretty(&serde_json::json!({
                "heading": heading,
                "count": entries.len(),
                "markets": arr,
            }))
            .unwrap_or_else(|_| "[]".into())
        } else {
            format!(
                "## {heading} ({} results)\n\n{}",
                entries.len(),
                entries.iter().map(|e| e.format_line()).collect::<Vec<_>>().join("\n\n")
            )
        }
    };

    match sort_by.as_str() {
        "spread" => {
            entries.sort_by(|a, b| {
                let sa = a.spread_pct.unwrap_or(f64::MAX);
                let sb = b.spread_pct.unwrap_or(f64::MAX);
                sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
            });
            entries.truncate(limit);
            render(&entries, "Crypto Markets — Tightest Spread")
        }
        "volume" => {
            entries.sort_by(|a, b| {
                b.volume_usd.partial_cmp(&a.volume_usd).unwrap_or(std::cmp::Ordering::Equal)
            });
            entries.truncate(limit);
            render(&entries, "Crypto Markets — Highest Volume")
        }
        "both" => {
            let mut by_spread = entries.clone();
            by_spread.sort_by(|a, b| {
                let sa = a.spread_pct.unwrap_or(f64::MAX);
                let sb = b.spread_pct.unwrap_or(f64::MAX);
                sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
            });
            by_spread.truncate(limit);

            entries.sort_by(|a, b| {
                b.volume_usd.partial_cmp(&a.volume_usd).unwrap_or(std::cmp::Ordering::Equal)
            });
            entries.truncate(limit);

            let spread_out = render(&by_spread, "Tightest Spread");
            let volume_out = render(&entries, "Highest Volume");

            if json_mode {
                format!("[{spread_out},{volume_out}]")
            } else {
                format!("{spread_out}\n\n---\n\n{volume_out}")
            }
        }
        _ => {
            entries.truncate(limit);
            render(&entries, "Crypto Markets")
        }
    }
}

// ──────────────────────────── get_active_window ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetActiveWindowParams {
    /// Asset ticker: btc, eth, sol, xrp, doge, bnb
    pub asset: String,
    /// Window type: "5m" or "15m"
    pub window: String,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_active_window(
    state: &SharedState,
    params: GetActiveWindowParams,
) -> String {
    let s = state.read().await;
    let asset = params.asset.to_lowercase();
    let key = format!("{}_{}", asset, params.window.to_lowercase());

    let markets = match s.short_term_markets.get(&key) {
        Some(m) => m,
        None => {
            return format!(
                "No active {} {} window found. Markets may not have been discovered yet.",
                asset.to_uppercase(),
                params.window
            );
        }
    };

    let now = chrono::Utc::now().timestamp();
    // Find the most recent active window
    let active = markets.iter().find(|m| {
        let end = m.window_start_ts + (m.interval as i64 * 60);
        now < end + 30 // 30s grace period
    });

    let market = match active {
        Some(m) => m,
        None => {
            return format!(
                "No active {} {} window right now. Waiting for next window.",
                asset.to_uppercase(),
                params.window
            );
        }
    };

    let elapsed = now - market.window_start_ts;
    let remaining = ((market.interval as i64 * 60) - elapsed).max(0);
    let elapsed_pct = (elapsed as f64 / (market.interval as f64 * 60.0)).min(1.0) * 100.0;

    let up_book = s.order_books.get(&market.up_token_id);
    let down_book = s.order_books.get(&market.down_token_id);

    let spot = s
        .spot_prices
        .get(&format!("{}USDT", asset.to_uppercase()))
        .map(|p| (p.price, p.age_secs()));

    let url = polymarket_url(&market.slug);

    let window_time = fmt_window_time(market.window_start_ts);
    let time_label = if remaining == 0 {
        "expired".to_string()
    } else {
        format!("{remaining}s remaining")
    };
    let mut out = format!(
        "## {} {}m Window ({window_time})\n\n\
         **Time:** {:.0}% elapsed ({elapsed}s / {}s), {time_label}\n",
        asset.to_uppercase(),
        market.interval,
        elapsed_pct,
        market.interval * 60,
    );

    if let Some((price, age)) = spot {
        if let Some(start_price) = market.start_spot_price {
            let delta_pct = (price - start_price) / start_price * 100.0;
            let sign = if delta_pct >= 0.0 { "+" } else { "" };
            out.push_str(&format!(
                "**Spot:** {} → {} ({sign}{:.2}%) (data age: {})\n",
                fmt_usd(start_price),
                fmt_usd(price),
                delta_pct,
                fmt_age(age)
            ));
        } else {
            out.push_str(&format!(
                "**Spot:** {} (data age: {})\n",
                fmt_usd(price),
                fmt_age(age)
            ));
        }
    }

    // UP side
    out.push_str("\n### UP side\n");
    if let Some(tsb) = up_book {
        let b = &tsb.book;
        out.push_str(&format!(
            "Ask: {} | Bid: {} | Spread: {} | Data age: {}\n",
            b.best_ask().map_or("n/a".into(), |p| format!("{:.3}", p)),
            b.best_bid().map_or("n/a".into(), |p| format!("{:.3}", p)),
            b.spread_pct().map_or("n/a".into(), fmt_pct),
            fmt_age(tsb.age_secs()),
        ));
        if let Some(assess) = microstructure::assess_tradeability(b) {
            out.push_str(&format!(
                "Tradeability: **{}** | Ask depth: {} | Slippage($100): {}\n",
                assess.rating,
                fmt_usd(assess.ask_depth_usd),
                fmt_pct(assess.slippage_100_pct),
            ));
        }
    } else {
        out.push_str("No order book data available\n");
    }

    // DOWN side
    out.push_str("\n### DOWN side\n");
    if let Some(tsb) = down_book {
        let b = &tsb.book;
        out.push_str(&format!(
            "Ask: {} | Bid: {} | Spread: {} | Data age: {}\n",
            b.best_ask().map_or("n/a".into(), |p| format!("{:.3}", p)),
            b.best_bid().map_or("n/a".into(), |p| format!("{:.3}", p)),
            b.spread_pct().map_or("n/a".into(), fmt_pct),
            fmt_age(tsb.age_secs()),
        ));
        if let Some(assess) = microstructure::assess_tradeability(b) {
            out.push_str(&format!(
                "Tradeability: **{}** | Ask depth: {} | Slippage($100): {}\n",
                assess.rating,
                fmt_usd(assess.ask_depth_usd),
                fmt_pct(assess.slippage_100_pct),
            ));
        }
    } else {
        out.push_str("No order book data available\n");
    }

    out.push_str(&format!("\n{url}"));
    out
}

// ──────────────────────────── get_window_briefing ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetWindowBriefingParams {
    /// Asset ticker: btc, eth, sol, xrp, doge, bnb
    pub asset: String,
    /// Window type: "5m" or "15m"
    pub window: String,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_window_briefing(
    state: &SharedState,
    binance: &crate::core::providers::binance::BinanceClient,
    polymarket: &crate::core::providers::polymarket::PolymarketClient,
    params: GetWindowBriefingParams,
) -> String {
    let asset = params.asset.to_lowercase();
    let key = format!("{}_{}", asset, params.window.to_lowercase());

    // Find the active market and check if books need fetching
    let (up_tid, down_tid) = {
        let s = state.read().await;
        let markets = match s.short_term_markets.get(&key) {
            Some(m) => m,
            None => {
                return format!("No {} {} markets discovered yet.", asset.to_uppercase(), params.window);
            }
        };
        let now = chrono::Utc::now().timestamp();
        let active = markets.iter().find(|m| {
            let end = m.window_start_ts + (m.interval as i64 * 60);
            now < end + 30
        });
        match active {
            Some(m) => {
                let need_up = !s.order_books.contains_key(&m.up_token_id);
                let need_down = !s.order_books.contains_key(&m.down_token_id);
                let up = if need_up { Some(m.up_token_id.clone()) } else { None };
                let dn = if need_down { Some(m.down_token_id.clone()) } else { None };
                (up, dn)
            }
            None => {
                return format!("No active {} {} window. Waiting for next window.", asset.to_uppercase(), params.window);
            }
        }
    };

    // Fetch order books on-demand if not cached
    for tid in [&up_tid, &down_tid].into_iter().flatten() {
        if let Ok(book) = polymarket.get_order_book(tid).await {
            let tsb = crate::core::monitor::state::TimestampedOrderBook::new(book);
            let mut s = state.write().await;
            s.order_books.insert(tid.clone(), tsb);
        }
    }

    // Now read state with books available
    let s = state.read().await;
    let markets = s.short_term_markets.get(&key).unwrap();
    let now = chrono::Utc::now().timestamp();
    let market = markets.iter().find(|m| {
        let end = m.window_start_ts + (m.interval as i64 * 60);
        now < end + 30
    }).unwrap();

    let elapsed = now - market.window_start_ts;
    let elapsed_pct = (elapsed as f64 / (market.interval as f64 * 60.0)).min(1.0) * 100.0;
    let remaining = ((market.interval as i64 * 60) - elapsed).max(0);

    let spot_sym = format!("{}USDT", asset.to_uppercase());
    let spot_price = s.spot_prices.get(&spot_sym).map(|p| p.price);

    let up_book = s.order_books.get(&market.up_token_id);
    let down_book = s.order_books.get(&market.down_token_id);

    let up_ask = up_book.and_then(|b| b.book.best_ask());
    let down_ask = down_book.and_then(|b| b.book.best_ask());

    let mut sections = Vec::new();

    // Time
    let time_label = if remaining == 0 {
        "expired".to_string()
    } else {
        format!("{remaining}s remaining")
    };
    sections.push(format!(
        "**Time:** {:.0}% elapsed ({elapsed}s / {}s), {time_label}",
        elapsed_pct,
        market.interval * 60,
    ));

    // Spot price move
    match (market.start_spot_price, spot_price) {
        (Some(start), Some(current)) => {
            let delta_pct = (current - start) / start * 100.0;
            let sign = if delta_pct >= 0.0 { "+" } else { "" };
            sections.push(format!(
                "**Spot:** {} -> {} ({sign}{:.2}%)",
                fmt_usd(start),
                fmt_usd(current),
                delta_pct
            ));
        }
        (None, Some(current)) => {
            sections.push(format!("**Spot:** {}", fmt_usd(current)));
        }
        _ => {}
    }

    // Recent volatility from Binance klines (last 1h of 5m candles)
    let vol_sym = format!("{}USDT", asset.to_uppercase());
    let now_ms = chrono::Utc::now().timestamp_millis();
    let one_hour_ago_ms = now_ms - 3_600_000;
    if let Ok(vol) = fetch_recent_volatility(binance, &vol_sym, one_hour_ago_ms).await {
        sections.push(format!(
            "**Volatility (1h):** avg 5m move: {:.3}% | max: {:.3}% | moves >{:.2}%: {}/12",
            vol.avg_move_pct,
            vol.max_move_pct,
            0.05, // threshold for "significant" moves
            vol.moves_above_threshold,
        ));
    }

    // Both sides summary
    let format_side = |name: &str, book_opt: Option<&crate::core::monitor::state::TimestampedOrderBook>| -> String {
        if let Some(tsb) = book_opt {
            let b = &tsb.book;
            let ask_str = b.best_ask().map_or("n/a".into(), |p| format!("{:.3}", p));
            let bid_str = b.best_bid().map_or("n/a".into(), |p| format!("{:.3}", p));
            let spread_str = b.spread_pct().map_or("n/a".into(), fmt_pct);
            let mut line = format!("**{name}:** ask={ask_str} bid={bid_str} spread={spread_str}");
            if let Some(assess) = microstructure::assess_tradeability(b) {
                line.push_str(&format!(
                    " | {} (depth: {}, slippage $100: {})",
                    assess.rating,
                    fmt_usd(assess.ask_depth_usd),
                    fmt_pct(assess.slippage_100_pct),
                ));
            }
            line
        } else {
            format!("**{name}:** no order book data")
        }
    };

    sections.push(format_side("UP", up_book));
    sections.push(format_side("DOWN", down_book));

    // Fee context for each available side
    let mut fee_lines = Vec::new();
    if let Some(up_price) = up_ask {
        let fee = fees::polymarket_fee(up_price);
        let be = fees::breakeven_win_rate(up_price, TradeMode::Taker);
        fee_lines.push(format!(
            "UP at {:.3}: fee={}, breakeven WR={}",
            up_price, fmt_pct(fee), fmt_pct(be),
        ));
    }
    if let Some(dn_price) = down_ask {
        let fee = fees::polymarket_fee(dn_price);
        let be = fees::breakeven_win_rate(dn_price, TradeMode::Taker);
        fee_lines.push(format!(
            "DOWN at {:.3}: fee={}, breakeven WR={}",
            dn_price, fmt_pct(fee), fmt_pct(be),
        ));
    }
    if !fee_lines.is_empty() {
        sections.push(format!("**Fees (taker):** {}", fee_lines.join(" | ")));
    }

    let url = polymarket_url(&market.slug);
    sections.push(url);

    let window_time = fmt_window_time(market.window_start_ts);
    format!(
        "## {} {}m Briefing ({window_time})\n\n{}",
        asset.to_uppercase(),
        market.interval,
        sections.join("\n"),
    )
}

// ──────────────────────────── get_spot_price ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetSpotPriceParams {
    /// Asset ticker: btc, eth, sol, xrp, doge, bnb, or "all" (default: "all")
    pub asset: Option<String>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_spot_price(
    state: &SharedState,
    params: GetSpotPriceParams,
) -> String {
    let s = state.read().await;
    let asset_filter = params.asset.unwrap_or_else(|| "all".into()).to_lowercase();

    let symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT"];

    let mut lines = vec!["| Asset | Price | Data Age |".to_string()];
    lines.push("|-------|-------|----------|".to_string());

    let mut found = false;
    for sym in &symbols {
        if asset_filter != "all" {
            let sym_asset = sym.replace("USDT", "").to_lowercase();
            if sym_asset != asset_filter {
                continue;
            }
        }

        if let Some(tp) = s.spot_prices.get(*sym) {
            lines.push(format!(
                "| {} | {} | {} |",
                sym.replace("USDT", ""),
                fmt_usd(tp.price),
                fmt_age(tp.age_secs()),
            ));
            found = true;
        } else {
            lines.push(format!(
                "| {} | — | no data |",
                sym.replace("USDT", ""),
            ));
        }
    }

    if !found && asset_filter != "all" {
        return format!(
            "No spot price data for '{}'. Available: BTC, ETH, SOL, XRP, DOGE, BNB. Background poller may not have started yet.",
            asset_filter.to_uppercase()
        );
    }

    format!("## Spot Prices\n\n{}", lines.join("\n"))
}

// ──────────────────────────── Volatility helper ────────────────────────────

struct RecentVolatility {
    avg_move_pct: f64,
    max_move_pct: f64,
    moves_above_threshold: usize,
}

/// Fetch recent 5m klines from Binance and compute average absolute move.
async fn fetch_recent_volatility(
    binance: &crate::core::providers::binance::BinanceClient,
    symbol: &str,
    start_time_ms: i64,
) -> Result<RecentVolatility, ()> {
    // Binance klines endpoint: /api/v3/klines?symbol=X&interval=5m&startTime=Y&limit=12
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&startTime={start_time_ms}&limit=12"
    );

    let resp: Vec<serde_json::Value> = reqwest::get(&url)
        .await
        .map_err(|_| ())?
        .json()
        .await
        .map_err(|_| ())?;

    if resp.is_empty() {
        return Err(());
    }

    let mut moves: Vec<f64> = Vec::new();
    for kline in &resp {
        if let Some(arr) = kline.as_array() {
            let open: f64 = arr.get(1).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            let close: f64 = arr.get(4).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if open > 0.0 {
                moves.push(((close - open) / open * 100.0).abs());
            }
        }
    }

    if moves.is_empty() {
        return Err(());
    }

    let avg_move_pct = moves.iter().sum::<f64>() / moves.len() as f64;
    let max_move_pct = moves.iter().cloned().fold(0.0_f64, f64::max);
    let moves_above_threshold = moves.iter().filter(|&&m| m > 0.05).count();

    Ok(RecentVolatility {
        avg_move_pct,
        max_move_pct,
        moves_above_threshold,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::monitor::state::{SharedState, TimestampedOrderBook, TimestampedPrice};
    use crate::core::types::{MarketConfig, OrderBook, PriceLevel, ShortTermMarket};
    use chrono::Utc;

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

    fn make_active_market(asset: &str, interval: u32) -> ShortTermMarket {
        let now = chrono::Utc::now().timestamp();
        ShortTermMarket {
            asset: asset.to_uppercase(),
            interval,
            window_start_ts: now - 60, // started 60s ago
            up_token_id: format!("{}_up", asset),
            down_token_id: format!("{}_down", asset),
            condition_id: "cond_test".into(),
            slug: format!("{}-updown-{}m-test", asset, interval),
            start_spot_price: None,
        }
    }

    // ── get_spot_price tests ──

    #[tokio::test]
    async fn test_spot_price_empty_state() {
        let state = SharedState::new();
        let result = handle_get_spot_price(
            &state,
            GetSpotPriceParams {
                asset: Some("all".into()),
                format: None,
            },
        )
        .await;
        assert!(result.contains("## Spot Prices"));
        assert!(result.contains("no data"));
    }

    #[tokio::test]
    async fn test_spot_price_populated() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.spot_prices
                .insert("BTCUSDT".into(), TimestampedPrice::new(84000.0));
            s.spot_prices
                .insert("ETHUSDT".into(), TimestampedPrice::new(3200.0));
        }
        let result = handle_get_spot_price(
            &state,
            GetSpotPriceParams {
                asset: Some("all".into()),
                format: None,
            },
        )
        .await;
        assert!(result.contains("$84000"));
        assert!(result.contains("$3200"));
    }

    #[tokio::test]
    async fn test_spot_price_single_asset() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.spot_prices
                .insert("BTCUSDT".into(), TimestampedPrice::new(84000.0));
        }
        let result = handle_get_spot_price(
            &state,
            GetSpotPriceParams {
                asset: Some("btc".into()),
                format: None,
            },
        )
        .await;
        assert!(result.contains("$84000"));
        // Should not contain ETH since we filtered for BTC only
        assert!(!result.contains("ETH"));
    }

    #[tokio::test]
    async fn test_spot_price_no_data_for_asset() {
        let state = SharedState::new();
        let result = handle_get_spot_price(
            &state,
            GetSpotPriceParams {
                asset: Some("btc".into()),
                format: None,
            },
        )
        .await;
        assert!(result.contains("No spot price data"));
        assert!(result.contains("Background poller"));
    }

    // ── search_crypto_markets tests ──

    #[tokio::test]
    async fn test_search_empty_state() {
        let state = SharedState::new();
        let result = handle_search_crypto_markets(
            &state,
            SearchCryptoMarketsParams {
                asset: None,
                market_type: None,
                sort_by: None,
                limit: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No markets found"));
    }

    #[tokio::test]
    async fn test_search_with_short_term_markets() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.short_term_markets
                .insert("btc_5m".into(), vec![make_active_market("btc", 5)]);
        }
        let result = handle_search_crypto_markets(
            &state,
            SearchCryptoMarketsParams {
                asset: Some("btc".into()),
                market_type: Some("5m".into()),
                sort_by: None,
                limit: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("## Crypto Markets"));
        assert!(result.contains("BTC 5m"));
    }

    #[tokio::test]
    async fn test_search_filter_excludes_wrong_asset() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.short_term_markets
                .insert("btc_5m".into(), vec![make_active_market("btc", 5)]);
        }
        let result = handle_search_crypto_markets(
            &state,
            SearchCryptoMarketsParams {
                asset: Some("eth".into()),
                market_type: None,
                sort_by: None,
                limit: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No markets found"));
    }

    #[tokio::test]
    async fn test_search_daily_markets() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.daily_markets.push(MarketConfig {
                name: "Bitcoin above $90,000".into(),
                condition_id: "cond_1".into(),
                yes_token_id: "yes_tok".into(),
                no_token_id: "no_tok".into(),
                strike_price: 90000.0,
                expiry: Utc::now() + chrono::Duration::hours(12),
                underlying: "BTCUSDT".into(),
                volume_usd: 25000.0,
            });
        }
        let result = handle_search_crypto_markets(
            &state,
            SearchCryptoMarketsParams {
                asset: None,
                market_type: Some("daily".into()),
                sort_by: None,
                limit: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("Bitcoin above $90,000"));
        assert!(result.contains("$90000"));
    }

    #[tokio::test]
    async fn test_search_limit() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            for i in 0..5 {
                s.daily_markets.push(MarketConfig {
                    name: format!("Market {}", i),
                    condition_id: format!("cond_{}", i),
                    yes_token_id: format!("yes_{}", i),
                    no_token_id: format!("no_{}", i),
                    strike_price: 80000.0 + i as f64 * 1000.0,
                    expiry: Utc::now() + chrono::Duration::hours(12),
                    underlying: "BTCUSDT".into(),
                    volume_usd: 10000.0,
                });
            }
        }
        let result = handle_search_crypto_markets(
            &state,
            SearchCryptoMarketsParams {
                asset: None,
                market_type: Some("daily".into()),
                sort_by: None,
                limit: Some(2),
                format: None,
            },
        )
        .await;
        assert!(result.contains("2 results"));
    }

    // ── get_active_window tests ──

    #[tokio::test]
    async fn test_active_window_no_markets() {
        let state = SharedState::new();
        let result = handle_get_active_window(
            &state,
            GetActiveWindowParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        assert!(result.contains("No active BTC 5m window found"));
    }

    #[tokio::test]
    async fn test_active_window_with_order_books() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            let market = make_active_market("btc", 5);
            let up_id = market.up_token_id.clone();
            let down_id = market.down_token_id.clone();
            s.short_term_markets
                .insert("btc_5m".into(), vec![market]);
            s.order_books.insert(
                up_id,
                TimestampedOrderBook::new(make_book(&[(0.40, 100.0)], &[(0.60, 100.0)])),
            );
            s.order_books.insert(
                down_id,
                TimestampedOrderBook::new(make_book(&[(0.35, 100.0)], &[(0.55, 100.0)])),
            );
            s.spot_prices
                .insert("BTCUSDT".into(), TimestampedPrice::new(84000.0));
        }
        let result = handle_get_active_window(
            &state,
            GetActiveWindowParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        assert!(result.contains("## BTC 5m Window"));
        assert!(result.contains("UP side"));
        assert!(result.contains("DOWN side"));
        assert!(result.contains("$84000"));
    }

    #[tokio::test]
    async fn test_active_window_no_order_book_data() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.short_term_markets
                .insert("btc_5m".into(), vec![make_active_market("btc", 5)]);
        }
        let result = handle_get_active_window(
            &state,
            GetActiveWindowParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        assert!(result.contains("No order book data available"));
    }

    // ── get_window_briefing tests ──

    #[tokio::test]
    async fn test_briefing_no_markets() {
        let state = SharedState::new();
        let binance = crate::core::providers::binance::BinanceClient::new();
        let polymarket = crate::core::providers::polymarket::PolymarketClient::new(None, None);
        let result = handle_get_window_briefing(
            &state,
            &binance,
            &polymarket,
            GetWindowBriefingParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        assert!(result.contains("No BTC 5m markets discovered"));
    }

    #[tokio::test]
    async fn test_briefing_no_order_book() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.short_term_markets
                .insert("btc_5m".into(), vec![make_active_market("btc", 5)]);
        }
        let binance = crate::core::providers::binance::BinanceClient::new();
        let polymarket = crate::core::providers::polymarket::PolymarketClient::new(None, None);
        let result = handle_get_window_briefing(
            &state,
            &binance,
            &polymarket,
            GetWindowBriefingParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        // On-demand fetch will try but fail (test tokens don't exist on Polymarket)
        // so we still expect no order book data
        assert!(result.contains("no order book data"));
    }

    #[tokio::test]
    async fn test_briefing_tradeable() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            // Create a market that started 4m50s ago (96.7% elapsed)
            let now = chrono::Utc::now().timestamp();
            let market = ShortTermMarket {
                asset: "BTC".into(),
                interval: 5,
                window_start_ts: now - 290, // 96.7% elapsed
                up_token_id: "btc_up".into(),
                down_token_id: "btc_down".into(),
                condition_id: "cond_test".into(),
                slug: "btc-updown-5m-test".into(),
                start_spot_price: None,
            };
            s.short_term_markets
                .insert("btc_5m".into(), vec![market]);
            // Good order book with tight spread
            s.order_books.insert(
                "btc_up".into(),
                TimestampedOrderBook::new(make_book(
                    &[(0.58, 200.0), (0.59, 200.0)],
                    &[(0.62, 200.0), (0.61, 200.0)],
                )),
            );
            s.order_books.insert(
                "btc_down".into(),
                TimestampedOrderBook::new(make_book(
                    &[(0.38, 200.0), (0.39, 200.0)],
                    &[(0.42, 200.0), (0.41, 200.0)],
                )),
            );
        }
        let binance = crate::core::providers::binance::BinanceClient::new();
        let polymarket = crate::core::providers::polymarket::PolymarketClient::new(None, None);
        let result = handle_get_window_briefing(
            &state,
            &binance,
            &polymarket,
            GetWindowBriefingParams {
                asset: "btc".into(),
                window: "5m".into(),
                format: None,
            },
        )
        .await;
        assert!(result.contains("Briefing"));
        assert!(result.contains("UP:"));
        assert!(result.contains("DOWN:"));
        assert!(result.contains("breakeven WR"));
    }

}
