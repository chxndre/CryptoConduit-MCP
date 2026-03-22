// MCP tool: get_market_analysis — quantitative market analysis with tabulated output.

use std::sync::Arc;

use chrono::Utc;
use rmcp::schemars;
use serde::Deserialize;

use crate::core::analysis::fair_value::{
    self, build_empirical_distribution, check_multi_strike_consistency, estimate_empirical,
    estimate_vol_cdf, realized_vol_from_klines, FairValueCache, FairValueEstimate,
};
use crate::core::analysis::{book_dynamics, momentum};
use crate::core::monitor::state::{SharedState, TimestampedOrderBook};
use crate::core::providers::binance::BinanceClient;
use crate::core::providers::gamma::{asset_to_underlying, query_resolved_markets};
use crate::core::providers::polymarket::PolymarketClient;
use crate::core::types::ResolvedMarketSummary;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetMarketAnalysisParams {
    /// Asset ticker: btc, eth, sol, xrp, doge, bnb
    pub asset: String,
    /// Market type: "5m", "15m", "daily", "monthly" (optional — defaults to "5m")
    pub market_type: Option<String>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

fn fmt_pct(val: f64) -> String {
    if val >= 0.0 {
        format!("+{:.3}%", val)
    } else {
        format!("{:.3}%", val)
    }
}

fn fmt_usd(val: f64) -> String {
    if val >= 1_000_000.0 {
        format!("${:.1}M", val / 1_000_000.0)
    } else if val >= 1_000.0 {
        format!("${:.0}k", val / 1_000.0)
    } else {
        format!("${:.0}", val)
    }
}

/// Format a price with appropriate decimal places based on magnitude.
fn fmt_price(val: f64) -> String {
    if val >= 1000.0 {
        format!("${:.0}", val)
    } else if val >= 1.0 {
        format!("${:.2}", val)
    } else if val >= 0.01 {
        format!("${:.4}", val)
    } else {
        format!("${:.6}", val)
    }
}

/// Context gathered from state for divergence analysis.
struct MarketContext {
    up_ask: Option<f64>,
    down_ask: Option<f64>,
    /// For short-term: spot move since window open (%)
    spot_move_pct: Option<f64>,
    /// For short-term: fraction elapsed (0.0 to 1.0)
    elapsed_fraction: Option<f64>,
    /// For short-term: window duration in minutes
    window_minutes: Option<u32>,
    /// For short-term: current spot price
    current_price: Option<f64>,
    /// For short-term: start spot price (window open)
    start_price: Option<f64>,
    /// For daily/monthly: strike prices and current asks (for multi-strike check)
    daily_strikes: Vec<(f64, f64)>, // (strike, yes_ask)
    /// For daily/monthly: ATM strike price (closest to spot)
    atm_strike: Option<f64>,
    /// For daily/monthly: minutes remaining until expiry
    minutes_remaining: Option<f64>,
}

pub async fn handle_get_market_analysis(
    state: &SharedState,
    binance: &BinanceClient,
    polymarket: &PolymarketClient,
    fair_value_cache: &Arc<FairValueCache>,
    params: GetMarketAnalysisParams,
) -> String {
    let asset = params.asset.to_lowercase();
    let json_mode = params.format.as_deref() == Some("json");
    let symbol = asset_to_underlying(&asset);
    let mt = params
        .market_type
        .as_deref()
        .unwrap_or("5m")
        .to_lowercase();

    // ── 1. Spot momentum ──
    let now_ms = Utc::now().timestamp_millis();
    let momentum_start = now_ms - 20 * 60 * 1000;

    let momentum_result = match binance.get_klines_range(&symbol, momentum_start, now_ms).await {
        Ok(klines) => {
            let prices: Vec<(i64, f64)> = klines.iter().map(|&(ts, _o, c)| (ts, c)).collect();
            Some(momentum::calculate_momentum(&prices))
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch klines for momentum");
            None
        }
    };

    // ── 2. Order book dynamics (with extended historical stats) ──

    // For daily/monthly: fetch order books on-demand if not cached by the passive poller.
    // Collect token IDs that need fetching before acquiring the read lock for analysis.
    if mt == "daily" || mt == "monthly" {
        let tokens_to_fetch: Vec<String> = {
            let st = state.read().await;
            let markets: Vec<_> = if mt == "daily" {
                st.daily_markets.iter().filter(|m| m.underlying == symbol && !m.is_expired()).collect()
            } else {
                st.monthly_markets.iter().filter(|m| m.underlying == symbol && !m.is_expired()).collect()
            };
            markets.iter().flat_map(|m| {
                let mut ids = Vec::new();
                if !st.order_books.contains_key(&m.yes_token_id) {
                    ids.push(m.yes_token_id.clone());
                }
                if !st.order_books.contains_key(&m.no_token_id) {
                    ids.push(m.no_token_id.clone());
                }
                ids
            }).collect()
        };

        if !tokens_to_fetch.is_empty() {
            tracing::info!(count = tokens_to_fetch.len(), "Fetching on-demand order books for daily/monthly");
            for tid in &tokens_to_fetch {
                match polymarket.get_order_book(tid).await {
                    Ok(book) => {
                        let tsb = TimestampedOrderBook::new(book);
                        let mut s = state.write().await;
                        s.order_books.insert(tid.clone(), tsb);
                    }
                    Err(e) => {
                        tracing::warn!(token_id = %&tid[..20.min(tid.len())], error = %e, "On-demand book fetch failed");
                    }
                }
            }
        }
    }

    let (up_book, down_book, market_ctx) = {
        let st = state.read().await;

        let (up_token, down_token, ctx) = match mt.as_str() {
            "5m" | "15m" => {
                let key = format!("{}_{}", asset, mt);
                let now_ts = Utc::now().timestamp();
                let market = st.short_term_markets.get(&key).and_then(|v| {
                    v.iter().find(|m| {
                        let end = m.window_start_ts + m.interval as i64 * 60;
                        now_ts < end + 30
                    })
                });
                match market {
                    Some(m) => {
                        let elapsed_secs = (now_ts - m.window_start_ts).max(0) as f64;
                        let total_secs = m.interval as f64 * 60.0;
                        let elapsed_frac = (elapsed_secs / total_secs).clamp(0.0, 1.0);

                        let current_spot = st.spot_prices.get(&symbol).map(|p| p.price);
                        let move_pct = match (m.start_spot_price, current_spot) {
                            (Some(start), Some(current)) if start > 0.0 => {
                                Some(((current - start) / start) * 100.0)
                            }
                            _ => None,
                        };

                        let ctx = MarketContext {
                            up_ask: None, // filled below
                            down_ask: None,
                            spot_move_pct: move_pct,
                            elapsed_fraction: Some(elapsed_frac),
                            window_minutes: Some(m.interval),
                            current_price: current_spot,
                            start_price: m.start_spot_price,
                            daily_strikes: Vec::new(),
                            atm_strike: None,
                            minutes_remaining: None,
                        };
                        (
                            Some(m.up_token_id.clone()),
                            Some(m.down_token_id.clone()),
                            ctx,
                        )
                    }
                    None => (
                        None,
                        None,
                        MarketContext {
                            up_ask: None,
                            down_ask: None,
                            spot_move_pct: None,
                            elapsed_fraction: None,
                            window_minutes: None,
                            current_price: st.spot_prices.get(&symbol).map(|p| p.price),
                            start_price: None,
                            daily_strikes: Vec::new(),
                            atm_strike: None,
                            minutes_remaining: None,
                        },
                    ),
                }
            }
            "daily" | "monthly" => {
                let markets: Vec<_> = if mt == "daily" {
                    st.daily_markets
                        .iter()
                        .filter(|m| m.underlying == symbol && !m.is_expired())
                        .collect()
                } else {
                    st.monthly_markets
                        .iter()
                        .filter(|m| m.underlying == symbol && !m.is_expired())
                        .collect()
                };

                // Pick the ATM (at-the-money) strike — closest to current spot price
                let current_spot = st.spot_prices.get(&symbol).map(|p| p.price);
                let atm = match current_spot {
                    Some(spot) => markets
                        .iter()
                        .min_by(|a, b| {
                            let da = (a.strike_price - spot).abs();
                            let db = (b.strike_price - spot).abs();
                            da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .copied(),
                    None => markets.first().copied(),
                };
                let (up_tok, down_tok) = match atm {
                    Some(m) => (Some(m.yes_token_id.clone()), Some(m.no_token_id.clone())),
                    None => (None, None),
                };

                // Collect all strikes for multi-strike consistency
                let daily_strikes: Vec<(f64, f64)> = markets
                    .iter()
                    .filter_map(|m| {
                        let ask = st.order_books.get(&m.yes_token_id)?.book.best_ask()?;
                        Some((m.strike_price, ask))
                    })
                    .collect();

                // Compute actual minutes remaining from ATM market's expiry
                let atm_strike_val = atm.map(|m| m.strike_price);
                let mins_remaining = atm.map(|m| {
                    let now = Utc::now();
                    let secs = (m.expiry - now).num_seconds().max(0) as f64;
                    secs / 60.0
                });

                let ctx = MarketContext {
                    up_ask: None,
                    down_ask: None,
                    spot_move_pct: None,
                    elapsed_fraction: None,
                    window_minutes: None,
                    current_price: st.spot_prices.get(&symbol).map(|p| p.price),
                    start_price: None,
                    daily_strikes,
                    atm_strike: atm_strike_val,
                    minutes_remaining: mins_remaining,
                };
                (up_tok, down_tok, ctx)
            }
            _ => (
                None,
                None,
                MarketContext {
                    up_ask: None,
                    down_ask: None,
                    spot_move_pct: None,
                    elapsed_fraction: None,
                    window_minutes: None,
                    current_price: None,
                    start_price: None,
                    daily_strikes: Vec::new(),
                    atm_strike: None,
                    minutes_remaining: None,
                },
            ),
        };

        // Extended book analysis with historical data
        // Use market_key for history lookups — survives token ID rotations
        let analyze_side = |tid: &Option<String>, mkey: &str| {
            tid.as_ref().and_then(|tid| {
                let book = st.order_books.get(tid)?;
                let current_spread = book.book.spread_pct().unwrap_or(0.0);
                let bid_depth = book.book.bid_depth_within(0.01);
                let ask_depth = book.book.ask_depth_within(0.01);

                // Collect all historical data by market_key (stable across window rotations)
                let all_spreads: Vec<(i64, f64)> = st
                    .history
                    .iter()
                    .filter(|s| s.market_key == mkey)
                    .filter_map(|s| Some((s.timestamp.timestamp_millis(), s.spread_pct?)))
                    .collect();

                let historical_depths: Vec<(i64, f64, f64)> = st
                    .history
                    .iter()
                    .filter(|s| s.market_key == mkey)
                    .map(|s| {
                        (
                            s.timestamp.timestamp_millis(),
                            s.bid_depth_1pct,
                            s.ask_depth_1pct,
                        )
                    })
                    .collect();

                // Use recent portion for trend analysis
                let recent_len = all_spreads.len().min(100);
                let recent_start = all_spreads.len().saturating_sub(recent_len);
                let recent_spreads = &all_spreads[recent_start..];

                Some(book_dynamics::analyze_book_extended(
                    current_spread,
                    recent_spreads,
                    &all_spreads,
                    bid_depth,
                    ask_depth,
                    &historical_depths,
                ))
            })
        };

        // Build market keys for history lookups
        let (up_mkey, down_mkey) = match mt.as_str() {
            "5m" | "15m" => (
                format!("{}_{}_up", asset, mt),
                format!("{}_{}_down", asset, mt),
            ),
            _ => (
                up_token.clone().unwrap_or_default(),
                down_token.clone().unwrap_or_default(),
            ),
        };

        let up_analysis = analyze_side(&up_token, &up_mkey);
        let down_analysis = analyze_side(&down_token, &down_mkey);

        let up_ask = up_token
            .as_ref()
            .and_then(|tid| st.order_books.get(tid)?.book.best_ask());
        let down_ask = down_token
            .as_ref()
            .and_then(|tid| st.order_books.get(tid)?.book.best_ask());

        let mut ctx = ctx;
        ctx.up_ask = up_ask;
        ctx.down_ask = down_ask;

        (up_analysis, down_analysis, ctx)
    };

    // ── 2b. Historical fallback via resolved markets (Gamma API) ──
    // When local order book history is empty and market type is short-term,
    // query Gamma for resolved markets to provide historical context.
    let resolved_context: Option<Vec<ResolvedMarketSummary>> = {
        let history_empty = up_book
            .as_ref()
            .map_or(true, |b| b.snapshot_count_24h == 0)
            && down_book
                .as_ref()
                .map_or(true, |b| b.snapshot_count_24h == 0);

        if history_empty && (mt == "5m" || mt == "15m") {
            let http_client = reqwest::Client::new();
            match query_resolved_markets(&http_client, &asset, &mt, 24, 4).await {
                Ok(markets) if !markets.is_empty() => Some(markets),
                Ok(_) => None,
                Err(e) => {
                    tracing::debug!(error = %e, "Resolved market fallback failed");
                    None
                }
            }
        } else {
            None
        }
    };

    // ── 3. Probability divergence (tiered model) ──
    let divergence_analysis = compute_divergence(
        &mt,
        &symbol,
        &market_ctx,
        binance,
        fair_value_cache,
    )
    .await;

    // ── 4. Whale flow (last 30 minutes) ──
    let (whale_buy_vol, whale_sell_vol, whale_count) = {
        let st = state.read().await;
        let cutoff = Utc::now() - chrono::Duration::minutes(30);
        let mut buy_vol = 0.0;
        let mut sell_vol = 0.0;
        let mut count = 0u32;
        for t in st.whale_trades.iter() {
            if t.timestamp >= cutoff && t.market_name.to_lowercase().contains(&asset) {
                count += 1;
                if t.side.to_uppercase() == "BUY" {
                    buy_vol += t.size_usd;
                } else {
                    sell_vol += t.size_usd;
                }
            }
        }
        (buy_vol, sell_vol, count)
    };

    // ── 5. Recent alerts (last 10 minutes) ──
    let recent_alerts: Vec<String> = {
        let st = state.read().await;
        let cutoff = Utc::now() - chrono::Duration::minutes(10);
        st.alerts
            .iter()
            .filter(|a| a.timestamp >= cutoff)
            .filter(|a| {
                let kind_str = format!("{}", a.kind);
                kind_str.to_lowercase().contains(&asset)
            })
            .map(|a| format!("{} — {}", a.timestamp.format("%H:%M:%S"), a.kind))
            .collect()
    };

    // ── Format output ──
    if json_mode {
        return format_json(
            &asset,
            &mt,
            &momentum_result,
            &up_book,
            &down_book,
            &divergence_analysis,
            whale_count,
            whale_buy_vol,
            whale_sell_vol,
            &recent_alerts,
            &resolved_context,
        );
    }

    format_tabulated(
        &asset,
        &mt,
        &momentum_result,
        &up_book,
        &down_book,
        &divergence_analysis,
        whale_count,
        whale_buy_vol,
        whale_sell_vol,
        &recent_alerts,
        &market_ctx,
        &resolved_context,
    )
}

// ──────────────────────────── Divergence computation ────────────────────────────

/// Divergence result for one side (UP or DOWN).
struct SideDivergence {
    market_price: f64,
    estimate: FairValueEstimate,
    divergence: f64,
}

/// Full divergence analysis result.
struct DivergenceAnalysis {
    up: Option<SideDivergence>,
    down: Option<SideDivergence>,
    multi_strike_warning: Option<String>,
}

async fn compute_divergence(
    mt: &str,
    symbol: &str,
    ctx: &MarketContext,
    binance: &BinanceClient,
    cache: &Arc<FairValueCache>,
) -> Option<DivergenceAnalysis> {
    match mt {
        "5m" | "15m" => compute_divergence_short_term(mt, symbol, ctx, binance, cache).await,
        "daily" | "monthly" => compute_divergence_long_term(symbol, ctx, binance).await,
        _ => None,
    }
}

async fn compute_divergence_short_term(
    _mt: &str,
    symbol: &str,
    ctx: &MarketContext,
    binance: &BinanceClient,
    cache: &Arc<FairValueCache>,
) -> Option<DivergenceAnalysis> {
    let current_price = ctx.current_price?;
    let wm = ctx.window_minutes?;
    let elapsed = ctx.elapsed_fraction.unwrap_or(0.5);
    let minutes_remaining = (1.0 - elapsed) * wm as f64;

    let now_ms = Utc::now().timestamp_millis();

    // Always build/fetch empirical distribution — it only needs kline history, not start_price.
    // The start_price is only needed for the lookup (to compute move_pct).
    let cache_key = format!("{}_{}", symbol, wm);
    let lookback_days: i64 = if wm <= 5 { 7 } else { 30 };
    let move_bucket_width = if wm <= 5 { 0.025 } else { 0.05 };

    let (dist, vol_klines) = match cache.get(&cache_key).await {
        Some((d, cached_vol)) => {
            // Distribution cached — still need recent klines for vol-CDF fallback
            let vol_klines = binance
                .get_klines_range(symbol, now_ms - 60 * 60 * 1000, now_ms)
                .await
                .unwrap_or_default();
            let _ = cached_vol; // vol already in the distribution context
            (Some(d), vol_klines)
        }
        None => {
            // Fetch kline history for empirical distribution (7d for 5m, 30d for 15m)
            let start_ms = now_ms - lookback_days * 24 * 60 * 60 * 1000;
            match binance.get_klines_range(symbol, start_ms, now_ms).await {
                Ok(klines) => {
                    let d = build_empirical_distribution(&klines, wm, move_bucket_width);
                    let vol = realized_vol_from_klines(&klines);
                    tracing::info!(
                        symbol = %symbol,
                        window = wm,
                        total_windows = d.total_windows,
                        klines = klines.len(),
                        "Built empirical distribution"
                    );
                    cache.put(cache_key, d.clone(), vol).await;
                    // Use tail of fetched klines as vol_klines (last hour)
                    let one_hour_ago = now_ms - 60 * 60 * 1000;
                    let vol_klines: Vec<_> = klines
                        .into_iter()
                        .filter(|(ts, _, _)| *ts >= one_hour_ago)
                        .collect();
                    (Some(d), vol_klines)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to fetch klines for empirical dist");
                    // Fall back to just 1h of klines for vol-CDF
                    let vol_klines = binance
                        .get_klines_range(symbol, now_ms - 60 * 60 * 1000, now_ms)
                        .await
                        .unwrap_or_default();
                    (None, vol_klines)
                }
            }
        }
    };

    // Build the estimate function — tries empirical, falls back to vol-CDF
    let make_up_estimate = |move_pct: Option<f64>,
                            start_price: Option<f64>,
                            dist: Option<&fair_value::EmpiricalDistribution>|
     -> FairValueEstimate {
        if let (Some(mp), Some(sp), Some(d)) = (move_pct, start_price, dist) {
            estimate_empirical(d, mp, elapsed, current_price, sp, minutes_remaining, &vol_klines, 20)
        } else {
            // Fall back to vol-CDF: strike = start price or current price (ATM ~ 50%)
            let strike = start_price.unwrap_or(current_price);
            estimate_vol_cdf(current_price, strike, minutes_remaining, &vol_klines)
        }
    };

    // UP side: P(finish up)
    let up_div = ctx.up_ask.map(|ask| {
        let est = make_up_estimate(ctx.spot_move_pct, ctx.start_price, dist.as_ref());
        let div = est.probability - ask;
        SideDivergence {
            market_price: ask,
            estimate: est,
            divergence: div,
        }
    });

    // DOWN side: P(finish down) = 1 - P(finish up)
    let down_div = ctx.down_ask.map(|ask| {
        let up_est = make_up_estimate(ctx.spot_move_pct, ctx.start_price, dist.as_ref());
        let down_prob = 1.0 - up_est.probability;
        let est = FairValueEstimate {
            probability: down_prob,
            method: up_est.method.clone(),
            confidence: up_est.confidence.clone(),
            detail: up_est.detail.clone(),
        };
        let div = down_prob - ask;
        SideDivergence {
            market_price: ask,
            estimate: est,
            divergence: div,
        }
    });

    Some(DivergenceAnalysis {
        up: up_div,
        down: down_div,
        multi_strike_warning: None,
    })
}

async fn compute_divergence_long_term(
    symbol: &str,
    ctx: &MarketContext,
    binance: &BinanceClient,
) -> Option<DivergenceAnalysis> {
    let current_price = ctx.current_price?;

    // Need at least one strike to compute vol-CDF
    if ctx.daily_strikes.is_empty() && ctx.up_ask.is_none() {
        return None;
    }

    // Fetch 2h of klines for vol estimation
    let now_ms = Utc::now().timestamp_millis();
    let klines = binance
        .get_klines_range(symbol, now_ms - 2 * 60 * 60 * 1000, now_ms)
        .await
        .ok()?;

    if klines.len() < 10 {
        return None;
    }

    // For daily/monthly with strikes: use ATM strike for divergence
    if !ctx.daily_strikes.is_empty() {
        // Use the ATM strike from MarketContext, or find it from daily_strikes
        let strike = ctx.atm_strike.unwrap_or_else(|| {
            ctx.daily_strikes
                .iter()
                .min_by(|(a, _), (b, _)| {
                    (a - current_price).abs().partial_cmp(&(b - current_price).abs())
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|(s, _)| *s)
                .unwrap_or(ctx.daily_strikes[0].0)
        });
        // Use actual minutes remaining from market expiry
        let minutes_remaining = ctx.minutes_remaining.unwrap_or(8.0 * 60.0);

        let up_est = estimate_vol_cdf(current_price, strike, minutes_remaining, &klines);
        let up_div = ctx.up_ask.map(|ask| {
            let div = up_est.probability - ask;
            SideDivergence {
                market_price: ask,
                estimate: up_est.clone(),
                divergence: div,
            }
        });

        let down_div = ctx.down_ask.map(|ask| {
            let down_prob = 1.0 - up_est.probability;
            let est = FairValueEstimate {
                probability: down_prob,
                method: up_est.method.clone(),
                confidence: up_est.confidence.clone(),
                detail: up_est.detail.clone(),
            };
            let div = down_prob - ask;
            SideDivergence {
                market_price: ask,
                estimate: est,
                divergence: div,
            }
        });

        // Multi-strike consistency check
        let mut strike_probs: Vec<(f64, f64)> = ctx
            .daily_strikes
            .iter()
            .map(|&(strike, _ask)| {
                let est =
                    fair_value::vol_cdf_probability(current_price, strike, minutes_remaining, {
                        realized_vol_from_klines(&klines).unwrap_or(0.001)
                    });
                (strike, est)
            })
            .collect();
        strike_probs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        let multi_strike_warning = check_multi_strike_consistency(&strike_probs);

        Some(DivergenceAnalysis {
            up: up_div,
            down: down_div,
            multi_strike_warning,
        })
    } else {
        None
    }
}

// ──────────────────────────── Formatting ────────────────────────────

#[allow(clippy::too_many_arguments)]
fn format_json(
    asset: &str,
    mt: &str,
    momentum_result: &Option<momentum::MomentumAnalysis>,
    up_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    down_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    divergence_analysis: &Option<DivergenceAnalysis>,
    whale_count: u32,
    whale_buy_vol: f64,
    whale_sell_vol: f64,
    recent_alerts: &[String],
    resolved_context: &Option<Vec<ResolvedMarketSummary>>,
) -> String {
    let mut result = serde_json::Map::new();
    result.insert("asset".into(), serde_json::json!(asset.to_uppercase()));
    result.insert("market_type".into(), serde_json::json!(mt));

    if let Some(mom) = momentum_result {
        result.insert(
            "momentum".into(),
            serde_json::json!({
                "current_price": mom.current_price,
                "move_1m_pct": mom.move_1m_pct,
                "move_5m_pct": mom.move_5m_pct,
                "move_15m_pct": mom.move_15m_pct,
                "acceleration": mom.acceleration,
                "volatility_5m": mom.volatility_5m,
                "trend": mom.trend_label,
            }),
        );
    }

    for (label, analysis) in [("up", up_book), ("down", down_book)] {
        if let Some(a) = analysis {
            let mut book_json = serde_json::json!({
                "spread_pct": a.current_spread_pct,
                "avg_spread_pct": a.avg_spread_pct,
                "spread_trend": format!("{}", a.spread_trend),
                "bid_depth_usd": a.bid_depth_usd,
                "ask_depth_usd": a.ask_depth_usd,
                "depth_asymmetry": a.depth_asymmetry,
                "imbalance": a.book_imbalance_label,
                "snapshot_count_24h": a.snapshot_count_24h,
            });
            if let Some(p10) = a.p10_spread {
                book_json["p10_spread"] = serde_json::json!(p10);
            }
            if let Some(p90) = a.p90_spread {
                book_json["p90_spread"] = serde_json::json!(p90);
            }
            if let Some(ref dt) = a.depth_trend {
                book_json["depth_trend"] = serde_json::json!(format!("{}", dt));
            }
            result.insert(format!("book_{}", label), book_json);
        }
    }

    if let Some(div_analysis) = divergence_analysis {
        let mut div_obj = serde_json::Map::new();
        for (label, side) in [("up", &div_analysis.up), ("down", &div_analysis.down)] {
            if let Some(sd) = side {
                div_obj.insert(
                    label.into(),
                    serde_json::json!({
                        "market_price": sd.market_price,
                        "fair_estimate": sd.estimate.probability,
                        "divergence": sd.divergence,
                        "method": format!("{}", sd.estimate.method),
                        "confidence": format!("{}", sd.estimate.confidence),
                        "detail": sd.estimate.detail,
                    }),
                );
            }
        }
        if let Some(ref warning) = div_analysis.multi_strike_warning {
            div_obj.insert(
                "multi_strike_warning".into(),
                serde_json::json!(warning),
            );
        }
        result.insert("divergence".into(), serde_json::Value::Object(div_obj));
    }

    result.insert(
        "whale_flow_30m".into(),
        serde_json::json!({
            "count": whale_count,
            "buy_volume_usd": whale_buy_vol,
            "sell_volume_usd": whale_sell_vol,
            "net_flow_usd": whale_buy_vol - whale_sell_vol,
        }),
    );
    result.insert("recent_alerts".into(), serde_json::json!(recent_alerts));

    if let Some(resolved) = resolved_context {
        let resolved_json: Vec<serde_json::Value> = resolved
            .iter()
            .map(|r| {
                serde_json::json!({
                    "timestamp": r.timestamp.to_rfc3339(),
                    "volume_usd": r.volume_usd,
                    "outcome": r.outcome,
                    "was_active": r.was_active,
                })
            })
            .collect();
        let active_count = resolved.iter().filter(|r| r.was_active).count();
        let total_volume: f64 = resolved.iter().map(|r| r.volume_usd).sum();
        result.insert(
            "historical_context".into(),
            serde_json::json!({
                "source": "gamma_resolved_markets",
                "note": "Local order book history unavailable — showing resolved market data from Gamma API",
                "samples": resolved.len(),
                "active_count": active_count,
                "total_volume_usd": total_volume,
                "markets": resolved_json,
            }),
        );
    }

    serde_json::to_string_pretty(&serde_json::Value::Object(result))
        .unwrap_or_else(|_| "Failed to serialize analysis".to_string())
}

#[allow(clippy::too_many_arguments)]
fn format_tabulated(
    asset: &str,
    mt: &str,
    momentum_result: &Option<momentum::MomentumAnalysis>,
    up_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    down_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    divergence_analysis: &Option<DivergenceAnalysis>,
    whale_count: u32,
    whale_buy_vol: f64,
    whale_sell_vol: f64,
    recent_alerts: &[String],
    market_ctx: &MarketContext,
    resolved_context: &Option<Vec<ResolvedMarketSummary>>,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "## {} Market Analysis ({})\n\n",
        asset.to_uppercase(),
        mt
    ));

    // ── Spot Momentum ──
    out.push_str("### Spot Momentum\n\n");
    if let Some(mom) = momentum_result {
        out.push_str(&format!(
            "  {:<12} {:>12}    {:<12} {}\n",
            "Price",
            format!("${:.2}", mom.current_price),
            "Trend",
            mom.trend_label,
        ));
        out.push_str(&format!(
            "  {:<12} {:>12}    {:<12} {}\n",
            "1m move",
            mom.move_1m_pct.map(fmt_pct).unwrap_or_else(|| "n/a".into()),
            "5m vol",
            mom.volatility_5m.map(|v| format!("{:.4}%", v)).unwrap_or_else(|| "n/a".into()),
        ));
        out.push_str(&format!(
            "  {:<12} {:>12}    {:<12} {}\n",
            "5m move",
            mom.move_5m_pct.map(fmt_pct).unwrap_or_else(|| "n/a".into()),
            "Accel",
            mom.acceleration
                .map(|a| {
                    if a > 0.01 { format!("accel ({:+.4})", a) }
                    else if a < -0.01 { format!("decel ({:+.4})", a) }
                    else { format!("steady ({:.4})", a.abs()) }
                })
                .unwrap_or_else(|| "n/a".into()),
        ));
        out.push_str(&format!(
            "  {:<12} {:>12}\n",
            "15m move",
            mom.move_15m_pct.map(fmt_pct).unwrap_or_else(|| "n/a".into()),
        ));

        // Summary
        out.push_str(&format!("\n{}\n", summarize_momentum(mom)));
    } else {
        out.push_str("Spot data unavailable.\n");
    }
    out.push('\n');

    // ── Order Book Dynamics ──
    out.push_str("### Order Book Dynamics\n\n");
    let (label_a, label_b) = if mt == "daily" || mt == "monthly" {
        ("YES", "NO")
    } else {
        ("UP", "DOWN")
    };
    if up_book.is_some() || down_book.is_some() {
        // For daily/monthly, show which strike is being analyzed
        if mt == "daily" || mt == "monthly" {
            if let Some(spot) = market_ctx.current_price {
                // Find ATM strike from daily_strikes
                if let Some(&(strike, _)) = market_ctx.daily_strikes.iter()
                    .min_by(|(a, _), (b, _)| {
                        (a - spot).abs().partial_cmp(&(b - spot).abs()).unwrap_or(std::cmp::Ordering::Equal)
                    })
                {
                    let distance_pct = ((strike - spot) / spot * 100.0).abs();
                    if distance_pct > 5.0 {
                        out.push_str(&format!(
                            "  ATM strike: {} (spot: {}) — {:.1}% away, no near-ATM strike available\n\n",
                            fmt_price(strike), fmt_price(spot), distance_pct
                        ));
                    } else {
                        out.push_str(&format!(
                            "  ATM strike: {} (spot: {})\n\n",
                            fmt_price(strike), fmt_price(spot)
                        ));
                    }
                }
            }
        }
        out.push_str(&format!(
            "  {:<6} {:>8} {:>8} {:>11} {:>10} {:>10}  {}\n",
            "Side", "Spread", "Median", "Trend", "BidDep$", "AskDep$", "Imbalance"
        ));
        out.push_str(&format!("  {}\n", "-".repeat(72)));
        for (label, analysis) in [(label_a, &up_book), (label_b, &down_book)] {
            if let Some(a) = analysis {
                let imbalance = if a.current_spread_pct > 0.50 {
                    "⚠ dead book".to_string()
                } else {
                    a.book_imbalance_label.clone()
                };
                out.push_str(&format!(
                    "  {:<6} {:>7.1}% {:>7.1}% {:>11} {:>10} {:>10}  {}\n",
                    label,
                    a.current_spread_pct * 100.0,
                    a.avg_spread_pct * 100.0,
                    format!("{}", a.spread_trend),
                    fmt_usd(a.bid_depth_usd),
                    fmt_usd(a.ask_depth_usd),
                    imbalance,
                ));
            }
        }

        // Extended historical stats
        let any_extended = up_book
            .as_ref()
            .or(down_book.as_ref())
            .map_or(false, |b| b.snapshot_count_24h > 0);
        if any_extended {
            out.push('\n');
            out.push_str(&format!(
                "  {:<6} {:>9} {:>10} {:>10} {:>12} {:>12} {:>11}\n",
                "Side", "Snapshots", "Sprd P10", "Sprd P90", "MedBidDep", "MedAskDep", "Depth Trnd"
            ));
            out.push_str(&format!("  {}\n", "-".repeat(76)));
            for (label, analysis) in [(label_a, &up_book), (label_b, &down_book)] {
                if let Some(a) = analysis {
                    if a.snapshot_count_24h > 0 {
                        let p10 = a.p10_spread.map(|v| format!("{:.1}%", v * 100.0)).unwrap_or_else(|| "n/a".into());
                        let p90 = a.p90_spread.map(|v| format!("{:.1}%", v * 100.0)).unwrap_or_else(|| "n/a".into());
                        let med_bid = a.avg_bid_depth_24h.map(fmt_usd).unwrap_or_else(|| "n/a".into());
                        let med_ask = a.avg_ask_depth_24h.map(fmt_usd).unwrap_or_else(|| "n/a".into());
                        let dt = a.depth_trend.as_ref().map(|t| format!("{}", t)).unwrap_or_else(|| "n/a".into());
                        out.push_str(&format!(
                            "  {:<6} {:>9} {:>10} {:>10} {:>12} {:>12} {:>11}\n",
                            label, a.snapshot_count_24h, p10, p90, med_bid, med_ask, dt,
                        ));
                    }
                }
            }
        }

        // Summary
        out.push_str(&format!("\n{}\n", summarize_book_dynamics(up_book, down_book, label_a, label_b)));
    } else {
        out.push_str(&format!(
            "No active {} markets found for {}.\n",
            mt,
            asset.to_uppercase()
        ));
    }
    out.push('\n');

    // ── Probability Divergence ──
    out.push_str("### Probability Divergence\n\n");
    if let Some(div_analysis) = divergence_analysis {
        if div_analysis.up.is_some() || div_analysis.down.is_some() {
            out.push_str(&format!(
                "  {:<6} {:>8} {:>10}  {:<20} {:<12} {:<10} {}\n",
                "Side", "Market", "Fair Est.", "Divergence", "Method", "Conf", "Detail"
            ));
            out.push_str(&format!("  {}\n", "-".repeat(82)));
            for (label, side) in [(label_a, &div_analysis.up), (label_b, &div_analysis.down)] {
                if let Some(sd) = side {
                    let signal = if sd.divergence > 0.05 {
                        "underpriced"
                    } else if sd.divergence < -0.05 {
                        "overpriced"
                    } else {
                        "fair"
                    };
                    let div_str = format!("{:+.3} ({})", sd.divergence, signal);
                    out.push_str(&format!(
                        "  {:<6} {:>8.3} {:>10.3}  {:<20} {:<12} {:<10} {}\n",
                        label,
                        sd.market_price,
                        sd.estimate.probability,
                        div_str,
                        format!("{}", sd.estimate.method),
                        format!("{}", sd.estimate.confidence),
                        sd.estimate.detail,
                    ));
                }
            }
            if let Some(ref warning) = div_analysis.multi_strike_warning {
                out.push_str(&format!("\n  ! {}\n", warning));
            }

            // Summary
            out.push_str(&format!("\n{}\n", summarize_divergence(div_analysis, label_a, label_b)));
        } else {
            out.push_str("No market prices available for divergence analysis.\n");
        }
    } else {
        out.push_str("Insufficient data for divergence calculation.\n");
    }
    out.push('\n');

    // ── Whale Flow ──
    out.push_str("### Whale Flow (30m)\n\n");
    if whale_count > 0 {
        let net = whale_buy_vol - whale_sell_vol;
        out.push_str(&format!(
            "  {:<8} {:>10} {:>10} {:>12}\n",
            "Trades", "Buy Vol", "Sell Vol", "Net Flow"
        ));
        out.push_str(&format!("  {}\n", "-".repeat(44)));
        out.push_str(&format!(
            "  {:<8} {:>10} {:>10} {:>12}\n",
            whale_count,
            fmt_usd(whale_buy_vol),
            fmt_usd(whale_sell_vol),
            format!(
                "{}{}",
                if net >= 0.0 { "+" } else { "-" },
                fmt_usd(net.abs())
            ),
        ));

        // Summary
        out.push_str(&format!(
            "\n{}\n",
            summarize_whale_flow(whale_count, whale_buy_vol, whale_sell_vol)
        ));
    } else {
        out.push_str(&format!(
            "No whale trades for {} in the last 30 minutes.\n",
            asset.to_uppercase()
        ));
    }
    out.push('\n');

    // ── Recent Alerts ──
    out.push_str("### Recent Alerts (10m)\n\n");
    if recent_alerts.is_empty() {
        out.push_str(&format!(
            "No recent alerts for {}.\n",
            asset.to_uppercase()
        ));
    } else {
        for alert_str in recent_alerts {
            out.push_str(&format!("- {}\n", alert_str));
        }
        out.push_str(&format!(
            "\n{} alert(s) in the last 10 minutes.\n",
            recent_alerts.len()
        ));
    }

    // ── Historical Context (fallback from resolved markets) ──
    if let Some(resolved) = resolved_context {
        out.push('\n');
        out.push_str("### Historical Context (Gamma Resolved Markets)\n\n");
        out.push_str("  _Local order book history unavailable — showing resolved market data._\n\n");

        let active_count = resolved.iter().filter(|r| r.was_active).count();
        let total_volume: f64 = resolved.iter().map(|r| r.volume_usd).sum();
        let avg_volume = if !resolved.is_empty() {
            total_volume / resolved.len() as f64
        } else {
            0.0
        };

        out.push_str(&format!(
            "  {:<12} {:>8}    {:<12} {}\n",
            "Samples", resolved.len(), "Active", active_count,
        ));
        out.push_str(&format!(
            "  {:<12} {:>8}    {:<12} {}\n",
            "Total Vol", fmt_usd(total_volume), "Avg Vol", fmt_usd(avg_volume),
        ));
        out.push('\n');

        out.push_str(&format!(
            "  {:<28} {:>12} {:>10} {:>8}\n",
            "Window", "Volume", "Outcome", "Active"
        ));
        out.push_str(&format!("  {}\n", "-".repeat(62)));
        for r in resolved {
            out.push_str(&format!(
                "  {:<28} {:>12} {:>10} {:>8}\n",
                r.timestamp.format("%Y-%m-%d %H:%M UTC"),
                fmt_usd(r.volume_usd),
                r.outcome.as_deref().unwrap_or("pending"),
                if r.was_active { "yes" } else { "no" },
            ));
        }

        if active_count > 0 {
            let activity_pct = (active_count as f64 / resolved.len() as f64) * 100.0;
            out.push_str(&format!(
                "\n{} {} {}m markets were active ({:.0}% of sampled windows), avg volume {}.\n",
                asset.to_uppercase(),
                mt,
                mt,
                activity_pct,
                fmt_usd(avg_volume),
            ));
        } else {
            out.push_str(&format!(
                "\nNo active {} {} markets found in the sampled period.\n",
                asset.to_uppercase(),
                mt,
            ));
        }
    }

    out
}

// ──────────────────────────── Plain English Summaries ────────────────────────────

fn summarize_momentum(mom: &momentum::MomentumAnalysis) -> String {
    // Thresholds match label_trend() in momentum.rs: ±0.10 strong, ±0.02 mild
    let direction = match mom.move_5m_pct {
        Some(m) if m > 0.10 => "strong uptrend",
        Some(m) if m > 0.02 => "uptrend",
        Some(m) if m >= -0.02 => "flat",
        Some(m) if m >= -0.10 => "downtrend",
        Some(_) => "strong downtrend",
        None => "flat",
    };
    let accel = match mom.acceleration {
        Some(a) if a > 0.01 => ", accelerating",
        Some(a) if a < -0.01 => ", decelerating",
        _ => "",
    };
    let vol = match mom.volatility_5m {
        Some(v) if v > 0.05 => " High volatility.",
        Some(v) if v > 0.02 => " Moderate volatility.",
        _ => " Low volatility.",
    };
    format!(
        "{} — {}{}.{}",
        fmt_price(mom.current_price), direction, accel, vol
    )
}

/// Minimum total depth (bid + ask) in USD for a side to be considered liquid.
const MIN_LIQUID_DEPTH_USD: f64 = 100.0;

fn summarize_book_dynamics(
    up_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    down_book: &Option<book_dynamics::BookDynamicsAnalysis>,
    label_a: &str,
    label_b: &str,
) -> String {
    match (up_book, down_book) {
        (Some(up), Some(down)) => {
            let up_spread = up.current_spread_pct;
            let down_spread = down.current_spread_pct;
            let up_depth = up.bid_depth_usd + up.ask_depth_usd;
            let down_depth = down.bid_depth_usd + down.ask_depth_usd;

            let up_thin = up_depth < MIN_LIQUID_DEPTH_USD;
            let down_thin = down_depth < MIN_LIQUID_DEPTH_USD;

            // Thin books override spread-based assessment
            if up_thin && down_thin {
                return format!(
                    "Both sides have negligible depth ({}, {}) — not tradeable.",
                    fmt_usd(up_depth), fmt_usd(down_depth)
                );
            }
            if up_thin {
                return format!(
                    "{} side has negligible depth ({}) — {} side is more tradeable.",
                    label_a, fmt_usd(up_depth), label_b
                );
            }
            if down_thin {
                return format!(
                    "{} side has negligible depth ({}) — {} side is more tradeable.",
                    label_b, fmt_usd(down_depth), label_a
                );
            }

            let better_side = if down_spread < up_spread && down_depth > up_depth {
                label_b
            } else if up_spread < down_spread && up_depth > down_depth {
                label_a
            } else {
                ""
            };

            if !better_side.is_empty() {
                format!(
                    "{} side has better liquidity (tighter spread, deeper book).",
                    better_side
                )
            } else if up_spread < 0.03 && down_spread < 0.03 && !up_thin && !down_thin {
                "Both sides have tight spreads and reasonable depth — good liquidity.".to_string()
            } else if up_spread > 0.10 || down_spread > 0.10 {
                "Wide spreads — thin liquidity, entry/exit will be costly.".to_string()
            } else {
                "Mixed liquidity across sides.".to_string()
            }
        }
        (Some(_), None) => format!("Only {} side has order book data.", label_a),
        (None, Some(_)) => format!("Only {} side has order book data.", label_b),
        (None, None) => "No order book data available.".to_string(),
    }
}

fn summarize_divergence(div: &DivergenceAnalysis, label_a: &str, label_b: &str) -> String {
    let mut parts = Vec::new();

    for (label, side) in [(label_a, &div.up), (label_b, &div.down)] {
        if let Some(sd) = side {
            if sd.divergence > 0.05 {
                parts.push(format!("{} looks underpriced ({:+.1}%)", label, sd.divergence * 100.0));
            } else if sd.divergence < -0.05 {
                parts.push(format!("{} looks overpriced ({:+.1}%)", label, sd.divergence * 100.0));
            }
        }
    }

    // When both sides appear overpriced, it's likely the market spread (ask > fair on both sides)
    let both_over = div.up.as_ref().map_or(false, |s| s.divergence < -0.05)
        && div.down.as_ref().map_or(false, |s| s.divergence < -0.05);
    if both_over {
        parts.push("(both asks above fair value — likely reflects market spread/taker cost)".to_string());
    }

    if parts.is_empty() {
        "Market prices are close to fair value estimates.".to_string()
    } else {
        parts.join(". ") + if both_over { "" } else { "." }
    }
}

fn summarize_whale_flow(count: u32, buy_vol: f64, sell_vol: f64) -> String {
    let net = buy_vol - sell_vol;
    let bias = if net > 1000.0 {
        "bullish"
    } else if net < -1000.0 {
        "bearish"
    } else {
        "mixed"
    };
    format!(
        "{} whale trade(s), {} bias — {} buy, {} sell.",
        count,
        bias,
        fmt_usd(buy_vol),
        fmt_usd(sell_vol),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::analysis::fair_value::FairValueCache;
    use crate::core::monitor::state::WhaleTrade;
    use std::time::Duration;

    #[tokio::test]
    async fn test_market_analysis_empty_state() {
        let state = SharedState::new();
        let binance = BinanceClient::new();
        let cache = Arc::new(FairValueCache::new(Duration::from_secs(300)));

        let polymarket = PolymarketClient::new(None, None);
        let result = handle_get_market_analysis(
            &state,
            &binance,
            &polymarket,
            &cache,
            GetMarketAnalysisParams {
                asset: "btc".into(),
                market_type: Some("5m".into()),
                format: None,
            },
        )
        .await;
        assert!(result.contains("BTC"));
        assert!(result.contains("Market Analysis"));
        assert!(result.contains("Spot Momentum"));
        assert!(result.contains("Order Book Dynamics"));
        assert!(result.contains("Probability Divergence"));
    }

    #[tokio::test]
    async fn test_market_analysis_whale_flow_from_state() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.push_whale_trade(WhaleTrade {
                token_id: "0xabc".into(),
                market_name: "BTC Daily Above 85k".into(),
                side: "BUY".into(),
                price: 0.65,
                size_usd: 25000.0,
                timestamp: Utc::now(),
            });
            s.push_whale_trade(WhaleTrade {
                token_id: "0xdef".into(),
                market_name: "BTC Daily Above 90k".into(),
                side: "SELL".into(),
                price: 0.30,
                size_usd: 15000.0,
                timestamp: Utc::now(),
            });
        }

        let binance = BinanceClient::new();
        let polymarket = PolymarketClient::new(None, None);
        let cache = Arc::new(FairValueCache::new(Duration::from_secs(300)));
        let result = handle_get_market_analysis(
            &state,
            &binance,
            &polymarket,
            &cache,
            GetMarketAnalysisParams {
                asset: "btc".into(),
                market_type: Some("daily".into()),
                format: None,
            },
        )
        .await;

        assert!(result.contains("Whale Flow"));
        assert!(result.contains("$25k"));
        assert!(result.contains("$15k"));
    }

    #[tokio::test]
    async fn test_market_analysis_json_format() {
        let state = SharedState::new();
        let binance = BinanceClient::new();
        let polymarket = PolymarketClient::new(None, None);
        let cache = Arc::new(FairValueCache::new(Duration::from_secs(300)));

        let result = handle_get_market_analysis(
            &state,
            &binance,
            &polymarket,
            &cache,
            GetMarketAnalysisParams {
                asset: "eth".into(),
                market_type: Some("5m".into()),
                format: Some("json".into()),
            },
        )
        .await;

        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["asset"], "ETH");
        assert_eq!(parsed["market_type"], "5m");
        assert!(parsed.get("whale_flow_30m").is_some());
    }
}
