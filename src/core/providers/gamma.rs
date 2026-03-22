// Extracted from market-scout src/discovery.rs
// Gamma API client for market discovery, slug building, settlement verification.
// Handles all Polymarket API quirks (stringified JSON arrays, mixed types).
// CRITICAL: Do NOT modify json_str_array() or parse logic — battle-tested.

use anyhow::{Context, Result};
use chrono::{Datelike, Months, NaiveDate, Utc};
use regex::Regex;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashSet;
use tracing::{debug, info, warn};

use crate::core::types::{Direction, MarketConfig, ResolvedMarketSummary, ShortTermMarket};

const GAMMA_API: &str = "https://gamma-api.polymarket.com";

// --- Slug builders ---

/// Build daily "above" slug for a given asset and date.
/// Pattern: `{slug_name}-above-on-{month}-{day}`
pub fn daily_above_slug(asset: &str, date: NaiveDate) -> Option<String> {
    let slug_name = asset_to_slug_name(asset)?;
    let month = month_name(date.month());
    Some(format!("{}-above-on-{}-{}", slug_name, month, date.day()))
}

/// Build daily up/down slug for a given asset and date.
/// Pattern: `{slug_name}-up-or-down-on-{month}-{day}-{year}`
pub fn daily_updown_slug(asset: &str, date: NaiveDate) -> Option<String> {
    let slug_name = asset_to_slug_name(asset)?;
    let month = month_name(date.month());
    Some(format!(
        "{}-up-or-down-on-{}-{}-{}",
        slug_name,
        month,
        date.day(),
        date.year()
    ))
}

/// Build daily range slug for a given asset and date.
/// Pattern: `{slug_name}-price-on-{month}-{day}`
pub fn daily_range_slug(asset: &str, date: NaiveDate) -> Option<String> {
    let slug_name = asset_to_slug_name(asset)?;
    let month = month_name(date.month());
    Some(format!("{}-price-on-{}-{}", slug_name, month, date.day()))
}

/// Build hourly up/down slug for a given asset, date, and hour.
/// Pattern: `{slug_name}-up-or-down-{month}-{day}-{year}-{hour}-et`
pub fn hourly_slug(asset: &str, date: NaiveDate, hour: u32) -> Option<String> {
    let slug_name = asset_to_slug_name(asset)?;
    let month = month_name(date.month());
    Some(format!(
        "{}-up-or-down-{}-{}-{}-{}-et",
        slug_name,
        month,
        date.day(),
        date.year(),
        hour
    ))
}

/// Build weekly range slug for a given asset and date range.
/// Pattern: `what-price-will-{slug_name}-hit-{month}-{day1}-{day2}`
pub fn weekly_slug(asset: &str, month_num: u32, day1: u32, day2: u32) -> Option<String> {
    let slug_name = asset_to_slug_name(asset)?;
    let month = month_name(month_num);
    Some(format!(
        "what-price-will-{}-hit-{}-{}-{}",
        slug_name, month, day1, day2
    ))
}

/// Build short-term (5m/15m) up/down slug.
/// Pattern: `{ticker}-updown-{N}m-{window_start_ts}` (lowercase ticker)
pub fn short_term_slug(asset: &str, interval_secs: u64, window_start: i64) -> String {
    let interval_min = interval_secs / 60;
    format!(
        "{}-updown-{}m-{}",
        asset.to_lowercase(),
        interval_min,
        window_start
    )
}

// --- Name mappings ---

pub fn month_name(month: u32) -> &'static str {
    match month {
        1 => "january",
        2 => "february",
        3 => "march",
        4 => "april",
        5 => "may",
        6 => "june",
        7 => "july",
        8 => "august",
        9 => "september",
        10 => "october",
        11 => "november",
        12 => "december",
        _ => "unknown",
    }
}

fn month_short_name(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "???",
    }
}

/// Map asset ticker to slug name used in Polymarket URLs.
/// Extended for all 6 supported assets.
pub fn asset_to_slug_name(asset: &str) -> Option<&'static str> {
    match asset.to_lowercase().as_str() {
        "btc" => Some("bitcoin"),
        "eth" => Some("ethereum"),
        "sol" => Some("solana"),
        "xrp" => Some("xrp"),
        "doge" => Some("dogecoin"),
        "bnb" => Some("bnb"),
        _ => None,
    }
}

/// Map asset ticker to Binance symbol.
pub fn asset_to_underlying(asset: &str) -> String {
    match asset.to_lowercase().as_str() {
        "btc" => "BTCUSDT".to_string(),
        "eth" => "ETHUSDT".to_string(),
        "sol" => "SOLUSDT".to_string(),
        "xrp" => "XRPUSDT".to_string(),
        "doge" => "DOGEUSDT".to_string(),
        "bnb" => "BNBUSDT".to_string(),
        _ => format!("{}USDT", asset.to_uppercase()),
    }
}

// --- Gamma API helpers ---

/// Fetch a Gamma event by slug as raw JSON Value.
async fn fetch_gamma_event(client: &Client, slug: &str) -> Result<Option<Value>> {
    let url = format!("{}/events?slug={}", GAMMA_API, slug);
    debug!(slug = %slug, "Fetching Gamma event");

    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Failed to fetch Gamma event: {}", slug))?;

    if !resp.status().is_success() {
        debug!(slug = %slug, status = %resp.status(), "Gamma API non-success");
        return Ok(None);
    }

    let value: Value = resp
        .json()
        .await
        .with_context(|| format!("Failed to parse Gamma JSON for: {}", slug))?;

    // API returns either a single object or an array
    match &value {
        Value::Array(arr) if !arr.is_empty() => Ok(Some(arr[0].clone())),
        Value::Object(_) => Ok(Some(value)),
        _ => Ok(None),
    }
}

/// Helper: get string from JSON value
fn json_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

/// Helper: get string array from JSON value.
/// Handles both real arrays AND Gamma API's stringified JSON arrays
/// (e.g., `"[\"id1\", \"id2\"]"` instead of `["id1", "id2"]`).
fn json_str_array(v: &Value, key: &str) -> Option<Vec<String>> {
    let val = v.get(key)?;
    // Try as actual JSON array first
    if let Some(arr) = val.as_array() {
        return arr
            .iter()
            .map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
    }
    // Gamma API returns some arrays as JSON strings: "[\"id1\", \"id2\"]"
    if let Some(s) = val.as_str() {
        let parsed: Vec<String> = serde_json::from_str(s).ok()?;
        return Some(parsed);
    }
    None
}

/// Helper: get bool from JSON value
fn json_bool(v: &Value, key: &str) -> Option<bool> {
    v.get(key).and_then(|v| v.as_bool())
}

/// Parse a strike price from a question like "Will the price of Bitcoin be above $68,000 on February 17?"
pub fn parse_strike_from_question(question: &str) -> Option<f64> {
    let re = Regex::new(r"\$([0-9,]+(?:\.[0-9]+)?k?)").ok()?;
    let caps = re.captures(question)?;
    let price_str = caps.get(1)?.as_str();

    let cleaned = price_str.replace(',', "");
    if cleaned.ends_with('k') {
        let num: f64 = cleaned.trim_end_matches('k').parse().ok()?;
        Some(num * 1000.0)
    } else {
        cleaned.parse().ok()
    }
}

fn is_above_question(question: &str) -> bool {
    question.to_lowercase().contains("above")
}

/// Parse direction from monthly market question text.
/// Monthly markets use "dip"/"fall"/"below" for downside, everything else is upside.
fn parse_monthly_direction(question: &str) -> bool {
    let q = question.to_lowercase();
    !(q.contains("dip") || q.contains("fall") || q.contains("below"))
}

// --- Public discovery API ---

/// Query Gamma API to check which outcome actually won for a resolved market.
/// Returns the winning Direction, or None if the market isn't resolved yet / API error.
/// Retries up to `max_retries` times with `retry_delay_secs` between attempts.
/// Uses `clob_token_ids` for lookup (condition_id filter is unreliable on Gamma).
pub async fn verify_settlement_outcome(
    client: &Client,
    token_id: &str,
    max_retries: u32,
    retry_delay_secs: u64,
) -> Result<Option<Direction>> {
    let url = format!("{}/markets?clob_token_ids={}", GAMMA_API, token_id);

    for attempt in 1..=max_retries {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<Value>().await {
                    Ok(value) => {
                        // Response is an array of market objects
                        let market = match &value {
                            Value::Array(arr) if !arr.is_empty() => &arr[0],
                            _ => {
                                debug!(attempt, "Gamma API returned empty/unexpected response");
                                if attempt < max_retries {
                                    tokio::time::sleep(tokio::time::Duration::from_secs(
                                        retry_delay_secs,
                                    ))
                                    .await;
                                    continue;
                                }
                                return Ok(None);
                            }
                        };

                        // Check if market is closed (resolved)
                        let closed =
                            market.get("closed").and_then(|v| v.as_bool()).unwrap_or(false);
                        if !closed {
                            debug!(attempt, "Market not yet closed, retrying...");
                            if attempt < max_retries {
                                tokio::time::sleep(tokio::time::Duration::from_secs(
                                    retry_delay_secs,
                                ))
                                .await;
                                continue;
                            }
                            return Ok(None);
                        }

                        // Parse outcomes and outcomePrices
                        let outcomes = json_str_array(market, "outcomes");
                        let prices = json_str_array(market, "outcomePrices");

                        if let (Some(outcomes), Some(prices)) = (outcomes, prices)
                            && outcomes.len() == 2 && prices.len() == 2
                        {
                                let p0: f64 = prices[0].parse().unwrap_or(0.5);
                                let p1: f64 = prices[1].parse().unwrap_or(0.5);

                                // Resolved market: winning outcome has price ~1.0, losing ~0.0
                                // If both near 0.5, market isn't resolved yet
                                if (p0 - p1).abs() < 0.5 {
                                    debug!(p0, p1, attempt, "Prices not yet resolved, retrying...");
                                    if attempt < max_retries {
                                        tokio::time::sleep(tokio::time::Duration::from_secs(
                                            retry_delay_secs,
                                        ))
                                        .await;
                                        continue;
                                    }
                                    return Ok(None);
                                }

                                let winner_idx = if p0 > p1 { 0 } else { 1 };
                                let winner = &outcomes[winner_idx];

                                let direction = match winner.to_lowercase().as_str() {
                                    "up" | "yes" => Some(Direction::Up),
                                    "down" | "no" => Some(Direction::Down),
                                    _ => {
                                        warn!(
                                            winner = %winner,
                                            "Unknown outcome label from Gamma API"
                                        );
                                        None
                                    }
                                };

                                return Ok(direction);
                        }

                        debug!(
                            attempt,
                            "Could not parse outcomes/prices from Gamma response"
                        );
                        return Ok(None);
                    }
                    Err(e) => {
                        debug!(attempt, error = %e, "Failed to parse Gamma API JSON");
                    }
                }
            }
            Ok(resp) => {
                debug!(attempt, status = %resp.status(), "Gamma API non-success");
            }
            Err(e) => {
                debug!(attempt, error = %e, "Gamma API request failed");
            }
        }

        if attempt < max_retries {
            tokio::time::sleep(tokio::time::Duration::from_secs(retry_delay_secs)).await;
        }
    }

    Ok(None)
}

/// Fetch a single historical short-term market by slug (for backtesting).
/// Works for both active and resolved markets — the Gamma events API returns by slug regardless of status.
pub async fn fetch_market_by_slug(
    client: &Client,
    slug: &str,
    asset: &str,
    window_start_ts: i64,
    interval_min: u32,
) -> Result<Option<ShortTermMarket>> {
    let (market, _settlement) = match fetch_market_with_settlement(client, slug, asset, window_start_ts, interval_min).await? {
        Some(r) => r,
        None => return Ok(None),
    };
    Ok(Some(market))
}

/// Result of a combined market discovery + settlement fetch.
#[derive(Debug, Clone)]
pub struct MarketWithSettlement {
    pub market: ShortTermMarket,
    pub settlement: Option<Direction>,
}

/// Fetch a historical market AND its settlement outcome in a single Gamma API call.
/// The event response contains both market structure (token IDs, condition ID) and
/// outcome prices (settlement). This eliminates the need for a separate settlement call.
pub async fn fetch_market_with_settlement(
    client: &Client,
    slug: &str,
    asset: &str,
    window_start_ts: i64,
    interval_min: u32,
) -> Result<Option<(ShortTermMarket, Option<Direction>)>> {
    let event = match fetch_gamma_event(client, slug).await? {
        Some(e) => e,
        None => return Ok(None),
    };

    let gamma_markets = event
        .get("markets")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let m = match gamma_markets.first() {
        Some(m) => m,
        None => return Ok(None),
    };

    let condition_id = json_str(m, "conditionId").unwrap_or_default();
    let token_ids = json_str_array(m, "clobTokenIds");
    let outcomes = json_str_array(m, "outcomes");

    if let (Some(ids), Some(outs)) = (token_ids, outcomes) {
        if ids.len() >= 2 && outs.len() >= 2 {
            let (up_idx, down_idx) = if outs[0].to_lowercase().contains("up") {
                (0, 1)
            } else {
                (1, 0)
            };

            // Extract settlement from the same response
            let settlement = extract_settlement(m, &outs);

            let market = ShortTermMarket {
                asset: asset.to_uppercase(),
                interval: interval_min,
                window_start_ts,
                up_token_id: ids[up_idx].clone(),
                down_token_id: ids[down_idx].clone(),
                condition_id,
                slug: slug.to_string(),
                start_spot_price: None,
            };

            return Ok(Some((market, settlement)));
        }
    }

    Ok(None)
}

/// Extract settlement direction from a Gamma market JSON object.
/// Returns None if the market isn't resolved yet.
fn extract_settlement(market: &Value, outcomes: &[String]) -> Option<Direction> {
    let closed = market.get("closed").and_then(|v| v.as_bool()).unwrap_or(false);
    if !closed {
        return None;
    }

    let prices = json_str_array(market, "outcomePrices")?;
    if prices.len() < 2 {
        return None;
    }

    let p0: f64 = prices[0].parse().ok()?;
    let p1: f64 = prices[1].parse().ok()?;

    // Resolved market: winning outcome has price ~1.0, losing ~0.0
    if (p0 - p1).abs() < 0.5 {
        return None; // Not yet resolved
    }

    let winner_idx = if p0 > p1 { 0 } else { 1 };
    let winner = &outcomes[winner_idx];

    match winner.to_lowercase().as_str() {
        "up" | "yes" => Some(Direction::Up),
        "down" | "no" => Some(Direction::Down),
        _ => None,
    }
}

/// Discover daily multi-strike "above" markets for today and tomorrow.
/// Accepts an `assets` parameter to discover for multiple assets.
pub async fn discover_daily_markets(
    client: &Client,
    assets: &[String],
) -> Result<Vec<MarketConfig>> {
    let today = Utc::now().date_naive();
    let tomorrow = today.succ_opt().unwrap_or(today);
    let mut all_markets = Vec::new();

    for asset_key in assets {
        let slug_name = match asset_to_slug_name(asset_key) {
            Some(name) => name,
            None => {
                warn!(asset = %asset_key, "Unknown asset for daily discovery, skipping");
                continue;
            }
        };
        let underlying = asset_to_underlying(asset_key);
        let asset_upper = asset_key.to_uppercase();

        for date in [today, tomorrow] {
            let slug = format!(
                "{}-above-on-{}-{}",
                slug_name,
                month_name(date.month()),
                date.day()
            );
            info!(slug = %slug, date = %date, asset = %asset_key, "Discovering daily markets");

            match fetch_gamma_event(client, &slug).await {
                Ok(Some(event)) => {
                    if let Value::Object(map) = &event {
                        let keys: Vec<&String> = map.keys().collect();
                        debug!(keys = ?keys, "Event top-level keys");
                    }

                    let expiry_str = json_str(&event, "endDate").unwrap_or_default();
                    let expiry = chrono::DateTime::parse_from_rfc3339(&expiry_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| {
                            date.and_hms_opt(17, 0, 0)
                                .unwrap()
                                .and_local_timezone(Utc)
                                .unwrap()
                        });

                    let markets = event
                        .get("markets")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let mut parsed = 0;

                    for m in &markets {
                        let closed = json_bool(m, "closed").unwrap_or(false);
                        let active = json_bool(m, "active").unwrap_or(true);
                        let question = json_str(m, "question");

                        if closed || !active {
                            continue;
                        }

                        let question = match question {
                            Some(q) => q,
                            None => continue,
                        };

                        let strike = match parse_strike_from_question(&question) {
                            Some(s) => s,
                            None => {
                                debug!(question = %question, "Could not parse strike");
                                continue;
                            }
                        };

                        let condition_id = json_str(m, "conditionId").unwrap_or_default();
                        let token_ids = json_str_array(m, "clobTokenIds");

                        let (yes_token_id, no_token_id) = match token_ids {
                            Some(ref ids) if ids.len() >= 2 => (ids[0].clone(), ids[1].clone()),
                            _ => {
                                debug!(question = %question, "Missing token IDs");
                                continue;
                            }
                        };

                        let direction = if is_above_question(&question) {
                            "above"
                        } else {
                            "below"
                        };
                        let strike_display = if strike >= 1000.0 {
                            format!("${:.0}k", strike / 1000.0)
                        } else {
                            format!("${:.0}", strike)
                        };
                        let month_short = &month_name(date.month())[..3];
                        let month_cap = format!(
                            "{}{}",
                            &month_short[..1].to_uppercase(),
                            &month_short[1..]
                        );
                        let name = format!(
                            "{} {} {} {} {}",
                            asset_upper,
                            strike_display,
                            direction,
                            month_cap,
                            date.day()
                        );

                        let volume_usd: f64 = json_str(m, "volume")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0.0);

                        all_markets.push(MarketConfig {
                            name,
                            condition_id,
                            yes_token_id,
                            no_token_id,
                            strike_price: strike,
                            expiry,
                            underlying: underlying.clone(),
                            volume_usd,
                        });
                        parsed += 1;
                    }

                    info!(slug = %slug, parsed = parsed, total = markets.len(), "Daily markets discovered");
                }
                Ok(None) => {
                    info!(slug = %slug, "No daily event found (may not exist yet)");
                }
                Err(e) => {
                    warn!(slug = %slug, error = %e, "Failed to fetch daily event");
                }
            }
        }
    }

    Ok(all_markets)
}

/// Discover short-term (5m/15m) up/down markets for the current and next windows.
pub async fn discover_short_term_markets(
    client: &Client,
    asset: &str,
    interval_secs: u64,
) -> Result<Vec<ShortTermMarket>> {
    let now_ts = Utc::now().timestamp();
    let current_window = (now_ts / interval_secs as i64) * interval_secs as i64;
    let next_window = current_window + interval_secs as i64;
    let interval_min = (interval_secs / 60) as u32;

    let mut markets = Vec::new();

    for window_start in [current_window, next_window] {
        let slug = short_term_slug(asset, interval_secs, window_start);
        debug!(slug = %slug, "Discovering short-term market");

        match fetch_gamma_event(client, &slug).await {
            Ok(Some(event)) => {
                let gamma_markets = event
                    .get("markets")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                if let Some(m) = gamma_markets.first() {
                    let condition_id = json_str(m, "conditionId").unwrap_or_default();
                    let token_ids = json_str_array(m, "clobTokenIds");
                    let outcomes = json_str_array(m, "outcomes");

                    if let (Some(ids), Some(outs)) = (token_ids, outcomes)
                        && ids.len() >= 2 && outs.len() >= 2
                    {
                            let (up_idx, down_idx) =
                                if outs[0].to_lowercase().contains("up") {
                                    (0, 1)
                                } else {
                                    (1, 0)
                                };

                            markets.push(ShortTermMarket {
                                asset: asset.to_uppercase(),
                                interval: interval_min,
                                window_start_ts: window_start,
                                up_token_id: ids[up_idx].clone(),
                                down_token_id: ids[down_idx].clone(),
                                condition_id,
                                slug: slug.clone(),
                                start_spot_price: None, // filled in by poller after discovery
                            });

                            debug!(
                                slug = %slug,
                                asset = %asset,
                                window = window_start,
                                "Short-term market discovered"
                            );
                    }
                }
            }
            Ok(None) => {
                debug!(slug = %slug, "Short-term market not found");
            }
            Err(e) => {
                warn!(slug = %slug, error = %e, "Failed to fetch short-term market");
            }
        }
    }

    Ok(markets)
}

/// Discover monthly multi-strike markets for current and next month.
/// Slug pattern: "what-price-will-{asset}-hit-in-{month}-{year}"
pub async fn discover_monthly_markets(
    client: &Client,
    assets: &[String],
) -> Result<Vec<MarketConfig>> {
    let now = Utc::now();
    let current_month = now.month();
    let current_year = now.year();

    let next = now.date_naive() + Months::new(1);
    let next_month = next.month();
    let next_year = next.year();

    let months = vec![(current_month, current_year), (next_month, next_year)];

    let mut all_markets = Vec::new();
    let mut seen_conditions: HashSet<String> = HashSet::new();

    for asset_key in assets {
        let slug_asset = match asset_to_slug_name(asset_key) {
            Some(name) => name,
            None => {
                warn!(asset = %asset_key, "Unknown asset for monthly discovery, skipping");
                continue;
            }
        };
        let underlying = asset_to_underlying(asset_key);
        let asset_upper = asset_key.to_uppercase();

        for &(month, year) in &months {
            let month_full = month_name(month);
            let slug = format!(
                "what-price-will-{}-hit-in-{}-{}",
                slug_asset, month_full, year
            );
            info!(slug = %slug, asset = %asset_key, "Discovering monthly markets");

            match fetch_gamma_event(client, &slug).await {
                Ok(Some(event)) => {
                    let expiry_str = json_str(&event, "endDate").unwrap_or_default();
                    let expiry = chrono::DateTime::parse_from_rfc3339(&expiry_str)
                        .map(|dt| dt.with_timezone(&Utc))
                        .unwrap_or_else(|_| {
                            let last_day = NaiveDate::from_ymd_opt(year, month, 28)
                                .unwrap_or_else(|| {
                                    NaiveDate::from_ymd_opt(year, month, 1).unwrap()
                                });
                            last_day
                                .and_hms_opt(23, 59, 59)
                                .unwrap()
                                .and_local_timezone(Utc)
                                .unwrap()
                        });

                    let markets = event
                        .get("markets")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    let mut parsed = 0;

                    for m in &markets {
                        let closed = json_bool(m, "closed").unwrap_or(false);
                        let active = json_bool(m, "active").unwrap_or(true);
                        let question = json_str(m, "question");

                        if closed || !active {
                            continue;
                        }

                        let question = match question {
                            Some(q) => q,
                            None => continue,
                        };

                        let strike = match parse_strike_from_question(&question) {
                            Some(s) => s,
                            None => {
                                debug!(question = %question, "Could not parse monthly strike");
                                continue;
                            }
                        };

                        let condition_id = json_str(m, "conditionId").unwrap_or_default();

                        // Deduplicate across current/next month overlap
                        if seen_conditions.contains(&condition_id) {
                            continue;
                        }
                        seen_conditions.insert(condition_id.clone());

                        let token_ids = json_str_array(m, "clobTokenIds");
                        let (yes_token_id, no_token_id) = match token_ids {
                            Some(ref ids) if ids.len() >= 2 => (ids[0].clone(), ids[1].clone()),
                            _ => {
                                debug!(question = %question, "Missing monthly token IDs");
                                continue;
                            }
                        };

                        let is_upside = parse_monthly_direction(&question);
                        let direction = if is_upside { "above" } else { "below" };
                        let strike_display = if strike >= 1000.0 {
                            format!("${:.0}k", strike / 1000.0)
                        } else {
                            format!("${:.0}", strike)
                        };
                        let month_short = month_short_name(month);
                        let name = format!(
                            "{} {} {} {} {}",
                            asset_upper, strike_display, direction, month_short, year
                        );

                        // Monthly markets use volumeNum (f64) not volume (string)
                        let volume_usd: f64 = m
                            .get("volumeNum")
                            .and_then(|v| v.as_f64())
                            .or_else(|| json_str(m, "volume").and_then(|v| v.parse().ok()))
                            .unwrap_or(0.0);

                        all_markets.push(MarketConfig {
                            name,
                            condition_id,
                            yes_token_id,
                            no_token_id,
                            strike_price: strike,
                            expiry,
                            underlying: underlying.clone(),
                            volume_usd,
                        });
                        parsed += 1;
                    }

                    info!(slug = %slug, parsed = parsed, total = markets.len(), "Monthly markets discovered");
                }
                Ok(None) => {
                    info!(slug = %slug, "No monthly event found (may not exist yet)");
                }
                Err(e) => {
                    warn!(slug = %slug, error = %e, "Failed to fetch monthly event");
                }
            }
        }
    }

    Ok(all_markets)
}

/// All token IDs from daily markets (yes + no for each non-expired market).
pub fn daily_token_ids(markets: &[MarketConfig]) -> Vec<(String, String, String)> {
    let mut ids = Vec::new();
    for m in markets {
        if !m.is_expired() {
            ids.push((m.yes_token_id.clone(), m.name.clone(), "YES".to_string()));
            ids.push((m.no_token_id.clone(), m.name.clone(), "NO".to_string()));
        }
    }
    ids
}

/// All token IDs from short-term markets, with underlying for spot price lookup.
pub fn short_term_token_ids(
    markets: &[ShortTermMarket],
) -> Vec<(String, String, String, String)> {
    let mut ids = Vec::new();
    for m in markets {
        let label = format!("{} {}m @{}", m.asset, m.interval, m.window_start_ts);
        let underlying = asset_to_underlying(&m.asset.to_lowercase());
        ids.push((
            m.up_token_id.clone(),
            label.clone(),
            "UP".to_string(),
            underlying.clone(),
        ));
        ids.push((
            m.down_token_id.clone(),
            label,
            "DOWN".to_string(),
            underlying,
        ));
    }
    ids
}

/// All token IDs from monthly markets (yes + no for each), with underlying for spot price lookup.
pub fn monthly_token_ids(markets: &[MarketConfig]) -> Vec<(String, String, String, String)> {
    let mut ids = Vec::new();
    for m in markets {
        if !m.is_expired() {
            ids.push((
                m.yes_token_id.clone(),
                m.name.clone(),
                "YES".to_string(),
                m.underlying.clone(),
            ));
            ids.push((
                m.no_token_id.clone(),
                m.name.clone(),
                "NO".to_string(),
                m.underlying.clone(),
            ));
        }
    }
    ids
}

// --- Historical market query fallback ---

/// Query Gamma API for resolved short-term markets across a time range.
/// Constructs slug patterns at regular intervals and returns volume + metadata
/// from resolved markets. This works retroactively without prior logging since
/// Gamma retains resolved market data indefinitely.
///
/// Parameters:
/// - `asset`: lowercase ticker ("btc", "sol", etc.)
/// - `timeframe`: "5m" or "15m"
/// - `lookback_hours`: how far back to query
/// - `sample_interval_hours`: gap between sample points (e.g., 1 = check one window per hour)
pub async fn query_resolved_markets(
    client: &Client,
    asset: &str,
    timeframe: &str,
    lookback_hours: u64,
    sample_interval_hours: u64,
) -> Result<Vec<ResolvedMarketSummary>> {
    let interval_secs: u64 = match timeframe {
        "5m" => 300,
        "15m" => 900,
        _ => anyhow::bail!("Unsupported timeframe for resolved query: {}", timeframe),
    };
    let interval_min = (interval_secs / 60) as u32;

    let now_ts = Utc::now().timestamp();
    let start_ts = now_ts - (lookback_hours * 3600) as i64;
    let step_secs = (sample_interval_hours * 3600) as i64;

    let mut results = Vec::new();
    let mut ts = start_ts;

    while ts < now_ts {
        // Align to window boundary
        let window_start = (ts / interval_secs as i64) * interval_secs as i64;
        let slug = short_term_slug(asset, interval_secs, window_start);

        // Single API call — extract volume, settlement, and activity from one response
        match fetch_gamma_event(client, &slug).await {
            Ok(Some(event)) => {
                let market_obj = event
                    .get("markets")
                    .and_then(|v| v.as_array())
                    .and_then(|arr| arr.first());

                if let Some(m) = market_obj {
                    let volume = m
                        .get("volumeNum")
                        .and_then(|v| v.as_f64())
                        .or_else(|| json_str(m, "volume").and_then(|v| v.parse().ok()))
                        .unwrap_or(0.0);

                    let outcomes = json_str_array(m, "outcomes").unwrap_or_default();
                    let settlement = extract_settlement(m, &outcomes);
                    let outcome = settlement.map(|d| format!("{}", d));

                    let window_time = chrono::DateTime::from_timestamp(window_start, 0)
                        .unwrap_or_else(|| Utc::now());

                    results.push(ResolvedMarketSummary {
                        slug,
                        asset: asset.to_uppercase(),
                        timeframe: timeframe.to_string(),
                        timestamp: window_time,
                        volume_usd: volume,
                        outcome,
                        was_active: volume > 0.0,
                    });
                }
            }
            Ok(None) => {
                debug!(slug = %slug, "Resolved market not found");
            }
            Err(e) => {
                debug!(slug = %slug, error = %e, "Failed to fetch resolved market");
            }
        }

        ts += step_secs;

        // Small delay between requests to respect rate limits
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    info!(
        asset = %asset,
        timeframe = %timeframe,
        lookback_hours = lookback_hours,
        found = results.len(),
        "Resolved market query complete"
    );

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daily_above_slug() {
        let date = NaiveDate::from_ymd_opt(2026, 2, 17).unwrap();
        assert_eq!(
            daily_above_slug("btc", date),
            Some("bitcoin-above-on-february-17".to_string())
        );
        assert_eq!(
            daily_above_slug("eth", date),
            Some("ethereum-above-on-february-17".to_string())
        );
        assert_eq!(
            daily_above_slug("sol", date),
            Some("solana-above-on-february-17".to_string())
        );
    }

    #[test]
    fn test_daily_updown_slug() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        assert_eq!(
            daily_updown_slug("btc", date),
            Some("bitcoin-up-or-down-on-march-16-2026".to_string())
        );
    }

    #[test]
    fn test_daily_range_slug() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        assert_eq!(
            daily_range_slug("eth", date),
            Some("ethereum-price-on-march-16".to_string())
        );
    }

    #[test]
    fn test_hourly_slug() {
        let date = NaiveDate::from_ymd_opt(2026, 3, 16).unwrap();
        assert_eq!(
            hourly_slug("btc", date, 14),
            Some("bitcoin-up-or-down-march-16-2026-14-et".to_string())
        );
    }

    #[test]
    fn test_weekly_slug() {
        assert_eq!(
            weekly_slug("btc", 3, 10, 16),
            Some("what-price-will-bitcoin-hit-march-10-16".to_string())
        );
    }

    #[test]
    fn test_short_term_slug() {
        assert_eq!(
            short_term_slug("btc", 300, 1771317900),
            "btc-updown-5m-1771317900"
        );
        assert_eq!(
            short_term_slug("eth", 900, 1771317900),
            "eth-updown-15m-1771317900"
        );
    }

    #[test]
    fn test_parse_strike() {
        assert_eq!(
            parse_strike_from_question(
                "Will the price of Bitcoin be above $68,000 on February 17?"
            ),
            Some(68000.0)
        );
        assert_eq!(
            parse_strike_from_question(
                "Will the price of Bitcoin be above $100k on February 17?"
            ),
            Some(100000.0)
        );
        assert_eq!(
            parse_strike_from_question(
                "Will the price of Bitcoin be above $62000 on February 17?"
            ),
            Some(62000.0)
        );
    }

    #[test]
    fn test_is_above() {
        assert!(is_above_question(
            "Will the price of Bitcoin be above $68,000?"
        ));
        assert!(!is_above_question(
            "Will the price of Bitcoin be below $60,000?"
        ));
    }

    #[test]
    fn test_parse_monthly_direction() {
        assert!(parse_monthly_direction("Bitcoin above $100000?"));
        assert!(!parse_monthly_direction("Bitcoin dip to $85,000?"));
        assert!(!parse_monthly_direction("Bitcoin fall to $70,000?"));
        assert!(!parse_monthly_direction("Bitcoin below $60,000?"));
        assert!(parse_monthly_direction(
            "Will Bitcoin hit $120k in March 2026?"
        ));
    }

    #[test]
    fn test_asset_to_slug_name() {
        assert_eq!(asset_to_slug_name("btc"), Some("bitcoin"));
        assert_eq!(asset_to_slug_name("BTC"), Some("bitcoin"));
        assert_eq!(asset_to_slug_name("eth"), Some("ethereum"));
        assert_eq!(asset_to_slug_name("sol"), Some("solana"));
        assert_eq!(asset_to_slug_name("xrp"), Some("xrp"));
        assert_eq!(asset_to_slug_name("doge"), Some("dogecoin"));
        assert_eq!(asset_to_slug_name("bnb"), Some("bnb"));
        assert_eq!(asset_to_slug_name("unknown"), None);
    }

    #[test]
    fn test_asset_to_underlying() {
        assert_eq!(asset_to_underlying("btc"), "BTCUSDT");
        assert_eq!(asset_to_underlying("eth"), "ETHUSDT");
        assert_eq!(asset_to_underlying("sol"), "SOLUSDT");
        assert_eq!(asset_to_underlying("doge"), "DOGEUSDT");
        assert_eq!(asset_to_underlying("bnb"), "BNBUSDT");
        assert_eq!(asset_to_underlying("avax"), "AVAXUSDT");
    }

    #[test]
    fn test_month_short_name() {
        assert_eq!(month_short_name(1), "Jan");
        assert_eq!(month_short_name(3), "Mar");
        assert_eq!(month_short_name(12), "Dec");
    }
}
