// Natural language → token ID market resolution.
// Used by paper_trade, place_order, get_order_book, and any tool accepting market references.
//
// Resolution strategy:
// 1. Direct token ID (64-char hex) → return as-is
// 2. Structured match: parse "{ASSET} {TYPE} {SIDE}" patterns
// 3. Fuzzy search: iterate all known markets, score by query similarity
// 4. If ambiguous, return top matches with disambiguation prompt

use crate::core::types::{MarketConfig, ShortTermMarket};

/// A resolved market reference with enough info to fetch order book or place trade.
#[derive(Debug, Clone)]
pub struct ResolvedMarket {
    pub token_id: String,
    pub name: String,
    pub market_type: MarketType,
    pub side: Option<MarketSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketType {
    ShortTerm5m,
    ShortTerm15m,
    Hourly,
    DailyUpDown,
    DailyAbove,
    DailyRange,
    Weekly,
    Monthly,
}

impl std::fmt::Display for MarketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketType::ShortTerm5m => write!(f, "5m"),
            MarketType::ShortTerm15m => write!(f, "15m"),
            MarketType::Hourly => write!(f, "hourly"),
            MarketType::DailyUpDown => write!(f, "daily_updown"),
            MarketType::DailyAbove => write!(f, "daily_above"),
            MarketType::DailyRange => write!(f, "daily_range"),
            MarketType::Weekly => write!(f, "weekly"),
            MarketType::Monthly => write!(f, "monthly"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSide {
    Up,
    Down,
    Yes,
    No,
}

impl std::fmt::Display for MarketSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketSide::Up => write!(f, "UP"),
            MarketSide::Down => write!(f, "DOWN"),
            MarketSide::Yes => write!(f, "YES"),
            MarketSide::No => write!(f, "NO"),
        }
    }
}

/// Result of market resolution — either a single match, multiple matches, or not found.
#[derive(Debug)]
pub enum ResolveResult {
    /// Exact single match
    Found(ResolvedMarket),
    /// Multiple possible matches — caller should disambiguate
    Ambiguous(Vec<ResolvedMarket>),
    /// No matches found
    NotFound(String),
}

/// Resolve a market query against known markets.
///
/// `short_term_markets`: keyed by "{asset}_{interval}" e.g. "btc_5m"
/// `daily_markets`: all known daily markets
/// `monthly_markets`: all known monthly markets
pub fn resolve_market(
    query: &str,
    short_term_markets: &[(String, Vec<ShortTermMarket>)],
    daily_markets: &[MarketConfig],
    monthly_markets: &[MarketConfig],
) -> ResolveResult {
    let query_lower = query.trim().to_lowercase();

    // 1. Direct token ID (64+ char hex string)
    if query_lower.len() >= 64 && query_lower.chars().all(|c| c.is_ascii_hexdigit()) {
        return ResolveResult::Found(ResolvedMarket {
            token_id: query.trim().to_string(),
            name: format!("Token {}", &query[..8]),
            market_type: MarketType::DailyAbove, // unknown, but doesn't matter for direct ID
            side: None,
        });
    }

    // 2. Structured match: "{ASSET} {TYPE} {SIDE}"
    if let Some(resolved) = try_structured_match(&query_lower, short_term_markets) {
        return ResolveResult::Found(resolved);
    }

    // 3. Fuzzy search across all known markets
    let mut candidates = Vec::new();

    // Search short-term markets — prefer active windows over expired ones
    let now_ts = chrono::Utc::now().timestamp();
    for (key, markets) in short_term_markets {
        for market in markets {
            let score = fuzzy_score(&query_lower, &market.asset, key, &market.slug);
            if score > 0 {
                // Boost active windows so they rank above expired ones
                let end = market.window_start_ts + (market.interval as i64 * 60);
                let active_bonus = if now_ts < end + 30 { 50 } else { 0 };

                // Add UP token
                candidates.push((
                    score + side_bonus(&query_lower, "up") + active_bonus,
                    ResolvedMarket {
                        token_id: market.up_token_id.clone(),
                        name: format!("{} UP", market.slug),
                        market_type: if market.interval == 5 {
                            MarketType::ShortTerm5m
                        } else {
                            MarketType::ShortTerm15m
                        },
                        side: Some(MarketSide::Up),
                    },
                ));
                // Add DOWN token
                candidates.push((
                    score + side_bonus(&query_lower, "down") + active_bonus,
                    ResolvedMarket {
                        token_id: market.down_token_id.clone(),
                        name: format!("{} DOWN", market.slug),
                        market_type: if market.interval == 5 {
                            MarketType::ShortTerm5m
                        } else {
                            MarketType::ShortTerm15m
                        },
                        side: Some(MarketSide::Down),
                    },
                ));
            }
        }
    }

    // Search daily markets
    for market in daily_markets {
        let score = fuzzy_score_config(&query_lower, market);
        if score > 0 {
            candidates.push((
                score + side_bonus(&query_lower, "yes"),
                ResolvedMarket {
                    token_id: market.yes_token_id.clone(),
                    name: format!("{} YES", market.name),
                    market_type: if market.name.to_lowercase().contains("above") {
                        MarketType::DailyAbove
                    } else if market.name.to_lowercase().contains("up or down") {
                        MarketType::DailyUpDown
                    } else {
                        MarketType::DailyRange
                    },
                    side: Some(MarketSide::Yes),
                },
            ));
            candidates.push((
                score + side_bonus(&query_lower, "no"),
                ResolvedMarket {
                    token_id: market.no_token_id.clone(),
                    name: format!("{} NO", market.name),
                    market_type: if market.name.to_lowercase().contains("above") {
                        MarketType::DailyAbove
                    } else if market.name.to_lowercase().contains("up or down") {
                        MarketType::DailyUpDown
                    } else {
                        MarketType::DailyRange
                    },
                    side: Some(MarketSide::No),
                },
            ));
        }
    }

    // Search monthly markets
    for market in monthly_markets {
        let score = fuzzy_score_config(&query_lower, market);
        if score > 0 {
            candidates.push((
                score + side_bonus(&query_lower, "yes"),
                ResolvedMarket {
                    token_id: market.yes_token_id.clone(),
                    name: format!("{} YES", market.name),
                    market_type: MarketType::Monthly,
                    side: Some(MarketSide::Yes),
                },
            ));
        }
    }

    // Sort by score descending
    candidates.sort_by(|a, b| b.0.cmp(&a.0));

    match candidates.len() {
        0 => ResolveResult::NotFound(format!("No markets matching '{query}'")),
        1 => ResolveResult::Found(candidates.into_iter().next().unwrap().1),
        _ => {
            let top_score = candidates[0].0;
            let top_matches: Vec<_> = candidates
                .into_iter()
                .filter(|(s, _)| *s == top_score)
                .map(|(_, m)| m)
                .collect();
            if top_matches.len() == 1 {
                ResolveResult::Found(top_matches.into_iter().next().unwrap())
            } else {
                // Return top 5 for disambiguation
                let truncated: Vec<_> = top_matches.into_iter().take(5).collect();
                ResolveResult::Ambiguous(truncated)
            }
        }
    }
}

/// Strict market resolution — requires the query to contain a recognized asset name.
///
/// Returns `ResolveResult::NotFound` if no asset keyword (btc, eth, sol, etc.) is
/// present in the query, preventing accidental matches on type-only queries like "5m".
/// Direct token IDs (64+ hex chars) bypass this check.
///
/// Use this for live trading where a wrong match has real financial consequences.
pub fn resolve_market_strict(
    query: &str,
    short_term_markets: &[(String, Vec<ShortTermMarket>)],
    daily_markets: &[MarketConfig],
    monthly_markets: &[MarketConfig],
) -> ResolveResult {
    let q = query.trim().to_lowercase();

    // Direct token IDs always pass through
    if q.len() >= 64 && q.chars().all(|c| c.is_ascii_hexdigit()) {
        return resolve_market(query, short_term_markets, daily_markets, monthly_markets);
    }

    // Require at least one recognized asset keyword
    let asset_keywords = [
        "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
        "xrp", "doge", "dogecoin", "bnb",
    ];
    if !asset_keywords.iter().any(|kw| q.contains(kw)) {
        return ResolveResult::NotFound(
            format!("Query '{}' does not contain a recognized asset (btc, eth, sol, xrp, doge, bnb). Please include the asset name.", query)
        );
    }

    resolve_market(query, short_term_markets, daily_markets, monthly_markets)
}

/// Find the best active short-term market from a list.
/// Prefers the most recent window that hasn't expired yet (with 30s grace).
/// Falls back to the market with the latest window_start_ts if none are active.
fn find_active_market(markets: &[ShortTermMarket]) -> Option<&ShortTermMarket> {
    let now = chrono::Utc::now().timestamp();
    // First try: find the most recent active window (not yet expired + 30s grace)
    let active = markets.iter().rev().find(|m| {
        let end = m.window_start_ts + (m.interval as i64 * 60);
        now < end + 30
    });
    // Fallback: latest window_start_ts (better than first() which is oldest)
    active.or_else(|| markets.iter().max_by_key(|m| m.window_start_ts))
}

/// Try to parse a structured query like "BTC 5m UP", "ETH above $3K daily".
fn try_structured_match(
    query: &str,
    short_term_markets: &[(String, Vec<ShortTermMarket>)],
) -> Option<ResolvedMarket> {
    let parts: Vec<&str> = query.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    let asset = normalize_asset(parts[0])?;

    // "{ASSET} 5m UP/DOWN" or "{ASSET} 15m UP/DOWN"
    if parts.len() >= 3 {
        let interval = match parts[1] {
            "5m" => Some(5u32),
            "15m" => Some(15u32),
            _ => None,
        };
        let side = parse_side(parts[2]);

        if let (Some(interval), Some(side)) = (interval, side) {
            let key = format!("{}_{}", asset, if interval == 5 { "5m" } else { "15m" });
            for (k, markets) in short_term_markets {
                if k == &key
                    && let Some(market) = find_active_market(markets)
                {
                        let token_id = match side {
                            MarketSide::Up => market.up_token_id.clone(),
                            MarketSide::Down => market.down_token_id.clone(),
                            _ => return None,
                        };
                        return Some(ResolvedMarket {
                            token_id,
                            name: format!("{} {} {}", asset.to_uppercase(), parts[1], side),
                            market_type: if interval == 5 {
                                MarketType::ShortTerm5m
                            } else {
                                MarketType::ShortTerm15m
                            },
                            side: Some(side),
                        });
                }
            }
        }
    }

    None
}

fn normalize_asset(input: &str) -> Option<String> {
    match input.to_lowercase().as_str() {
        "btc" | "bitcoin" => Some("btc".to_string()),
        "eth" | "ethereum" => Some("eth".to_string()),
        "sol" | "solana" => Some("sol".to_string()),
        "xrp" => Some("xrp".to_string()),
        "doge" | "dogecoin" => Some("doge".to_string()),
        "bnb" => Some("bnb".to_string()),
        _ => None,
    }
}

fn parse_side(input: &str) -> Option<MarketSide> {
    match input.to_lowercase().as_str() {
        "up" | "yes" => Some(MarketSide::Up),
        "down" | "no" => Some(MarketSide::Down),
        _ => None,
    }
}

fn side_bonus(query: &str, side: &str) -> i32 {
    if query.contains(side) { 10 } else { 0 }
}

/// Score how well a query matches a short-term market.
/// Returns 0 unless the asset actually matches (prevents false positives
/// from type-only matches like "fake-coin-5m" matching SOL 5m markets).
fn fuzzy_score(query: &str, asset: &str, key: &str, slug: &str) -> i32 {
    let mut score = 0;
    let mut asset_matched = false;

    // Asset match (ticker)
    if query.contains(&asset.to_lowercase()) {
        score += 20;
        asset_matched = true;
    }
    // Full asset name match
    let full_name = match asset.to_lowercase().as_str() {
        "btc" => "bitcoin",
        "eth" => "ethereum",
        "sol" => "solana",
        "doge" => "dogecoin",
        _ => "",
    };
    if !full_name.is_empty() && query.contains(full_name) {
        score += 20;
        asset_matched = true;
    }

    // Slug match (always counts as asset match since slug contains asset)
    if query.contains(slug) {
        score += 30;
        asset_matched = true;
    }

    // Only add type bonus if asset matched — prevents "fake-5m" matching everything with "5m"
    if asset_matched {
        if key.contains("5m") && query.contains("5m") {
            score += 15;
        }
        if key.contains("15m") && query.contains("15m") {
            score += 15;
        }
    }

    // No asset match → no match at all
    if !asset_matched {
        return 0;
    }

    score
}

/// Score how well a query matches a MarketConfig.
/// Requires at least an asset/underlying match to return non-zero.
fn fuzzy_score_config(query: &str, market: &MarketConfig) -> i32 {
    let mut score = 0;
    let name_lower = market.name.to_lowercase();
    let mut asset_matched = false;

    // Asset/underlying match
    let underlying = market.underlying.to_lowercase();
    let ticker = underlying.replace("usdt", "");
    if query.contains(&underlying) || query.contains(&ticker) {
        score += 15;
        asset_matched = true;
    }

    // Check asset full name in market name
    let full_names = ["bitcoin", "ethereum", "solana", "dogecoin", "xrp", "bnb"];
    for name in &full_names {
        if name_lower.contains(name) && query.contains(name) {
            score += 15;
            asset_matched = true;
        }
    }

    if !asset_matched {
        return 0;
    }

    // Check if query terms appear in market name (bonus, not primary)
    for word in query.split_whitespace() {
        if name_lower.contains(word) {
            score += 10;
        }
    }

    // Strike price match (look for numbers in query)
    for word in query.split_whitespace() {
        let cleaned = word.replace(['$', ',', 'k'], "");
        if let Ok(num) = cleaned.parse::<f64>() {
            let query_strike = if word.contains('k') {
                num * 1000.0
            } else {
                num
            };
            if (query_strike - market.strike_price).abs() / market.strike_price < 0.01 {
                score += 25;
            }
        }
    }

    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_short_term(asset: &str, interval: u32) -> (String, Vec<ShortTermMarket>) {
        let key = format!("{}_{}", asset, if interval == 5 { "5m" } else { "15m" });
        let market = ShortTermMarket {
            asset: asset.to_string(),
            interval,
            window_start_ts: 1710000000,
            up_token_id: format!("{}_up_token", asset),
            down_token_id: format!("{}_down_token", asset),
            condition_id: "cond_123".to_string(),
            slug: format!("{}-updown-{}m-1710000000", asset, interval),
            start_spot_price: None,
        };
        (key, vec![market])
    }

    fn make_daily(name: &str, strike: f64) -> MarketConfig {
        MarketConfig {
            name: name.to_string(),
            condition_id: "cond_daily".to_string(),
            yes_token_id: format!("{}_yes", name),
            no_token_id: format!("{}_no", name),
            strike_price: strike,
            expiry: Utc::now() + chrono::Duration::hours(12),
            underlying: "BTCUSDT".to_string(),
            volume_usd: 10000.0,
        }
    }

    #[test]
    fn test_direct_token_id() {
        let token = "a".repeat(64);
        let result = resolve_market(&token, &[], &[], &[]);
        assert!(matches!(result, ResolveResult::Found(_)));
    }

    #[test]
    fn test_structured_btc_5m_up() {
        let markets = vec![make_short_term("btc", 5)];
        let result = resolve_market("btc 5m up", &markets, &[], &[]);
        match result {
            ResolveResult::Found(m) => {
                assert_eq!(m.token_id, "btc_up_token");
                assert_eq!(m.side, Some(MarketSide::Up));
                assert_eq!(m.market_type, MarketType::ShortTerm5m);
            }
            other => panic!("Expected Found, got {:?}", other),
        }
    }

    #[test]
    fn test_structured_eth_15m_down() {
        let markets = vec![make_short_term("eth", 15)];
        let result = resolve_market("eth 15m down", &markets, &[], &[]);
        match result {
            ResolveResult::Found(m) => {
                assert_eq!(m.token_id, "eth_down_token");
                assert_eq!(m.side, Some(MarketSide::Down));
            }
            other => panic!("Expected Found, got {:?}", other),
        }
    }

    #[test]
    fn test_not_found() {
        let result = resolve_market("xyz something", &[], &[], &[]);
        assert!(matches!(result, ResolveResult::NotFound(_)));
    }

    #[test]
    fn test_fuzzy_daily_above() {
        let daily = vec![make_daily("Bitcoin above $72,000", 72000.0)];
        let result = resolve_market("btc above $72K yes", &[], &daily, &[]);
        match result {
            ResolveResult::Found(m) => {
                assert!(m.token_id.contains("yes"));
            }
            ResolveResult::Ambiguous(matches) => {
                // Also acceptable — multiple YES/NO matches
                assert!(!matches.is_empty());
            }
            other => panic!("Expected Found or Ambiguous, got {:?}", other),
        }
    }

    #[test]
    fn test_normalize_asset() {
        assert_eq!(normalize_asset("bitcoin"), Some("btc".to_string()));
        assert_eq!(normalize_asset("ETH"), Some("eth".to_string()));
        assert_eq!(normalize_asset("solana"), Some("sol".to_string()));
        assert_eq!(normalize_asset("unknown"), None);
    }

    #[test]
    fn test_fake_asset_not_found() {
        // "FAKE-coin-5m" should not match any real markets
        let markets = vec![make_short_term("btc", 5), make_short_term("sol", 5)];
        let daily = vec![make_daily("Bitcoin above $72,000", 72000.0)];
        let result = resolve_market("FAKE-coin-5m", &markets, &daily, &[]);
        assert!(
            matches!(result, ResolveResult::NotFound(_)),
            "Expected NotFound for fake asset, got {:?}",
            result
        );
    }

    #[test]
    fn test_strict_rejects_no_asset() {
        let markets = vec![make_short_term("btc", 5), make_short_term("sol", 5)];
        let result = resolve_market_strict("5m up", &markets, &[], &[]);
        assert!(
            matches!(result, ResolveResult::NotFound(_)),
            "Strict mode should reject queries without an asset name, got {:?}",
            result
        );
    }

    #[test]
    fn test_strict_allows_asset_query() {
        let markets = vec![make_short_term("btc", 5)];
        let result = resolve_market_strict("btc 5m up", &markets, &[], &[]);
        assert!(
            matches!(result, ResolveResult::Found(_)),
            "Strict mode should allow queries with an asset name, got {:?}",
            result
        );
    }

    #[test]
    fn test_strict_allows_direct_token_id() {
        let token = "a".repeat(64);
        let result = resolve_market_strict(&token, &[], &[], &[]);
        assert!(matches!(result, ResolveResult::Found(_)));
    }

    #[test]
    fn test_structured_match_prefers_active_window() {
        // Simulate two windows: an expired one (first) and an active one (second)
        let now = Utc::now().timestamp();
        let expired_window = ShortTermMarket {
            asset: "btc".to_string(),
            interval: 5,
            window_start_ts: now - 600, // started 10 min ago, well expired
            up_token_id: "stale_up_token".to_string(),
            down_token_id: "stale_down_token".to_string(),
            condition_id: "cond_old".to_string(),
            slug: "btc-updown-5m-old".to_string(),
            start_spot_price: None,
        };
        let active_window = ShortTermMarket {
            asset: "btc".to_string(),
            interval: 5,
            window_start_ts: now - 60, // started 1 min ago, still active
            up_token_id: "active_up_token".to_string(),
            down_token_id: "active_down_token".to_string(),
            condition_id: "cond_new".to_string(),
            slug: "btc-updown-5m-new".to_string(),
            start_spot_price: None,
        };
        let markets = vec![("btc_5m".to_string(), vec![expired_window, active_window])];

        // Structured match should pick the active window, not the stale one
        let result = resolve_market("btc 5m down", &markets, &[], &[]);
        match result {
            ResolveResult::Found(m) => {
                assert_eq!(m.token_id, "active_down_token",
                    "Resolver should pick the active window token, not the stale one");
            }
            other => panic!("Expected Found, got {:?}", other),
        }

        let result = resolve_market("btc 5m up", &markets, &[], &[]);
        match result {
            ResolveResult::Found(m) => {
                assert_eq!(m.token_id, "active_up_token",
                    "Resolver should pick the active window token, not the stale one");
            }
            other => panic!("Expected Found, got {:?}", other),
        }
    }
}
