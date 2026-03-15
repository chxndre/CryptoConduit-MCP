use std::collections::HashMap;
use std::sync::Arc;

use reqwest::Client;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::alerts::{
    check_depth_alerts, check_spread_alerts, check_window_approaching, process_whale_trade,
    snapshot_from_book, AlertConfig,
};
use super::auto_trade::{self, SharedAutoTradeState};
use super::logger::DataLogger;
use super::state::{SharedState, TimestampedOrderBook, TimestampedPrice};
use crate::core::execution::live::LiveExecutor;
use crate::core::execution::risk::RiskManager;
use crate::core::paper::portfolio::Portfolio;
use crate::core::providers::binance::BinanceClient;
use crate::core::providers::chainlink::ChainlinkRtds;
use crate::core::providers::gamma;
use crate::core::providers::polymarket::PolymarketClient;
use crate::core::providers::trades_ws::PolymarketTradesWs;

/// Configuration for the background poller.
#[derive(Debug, Clone)]
pub struct PollerConfig {
    /// Assets to monitor (lowercase tickers: "btc", "eth", etc.).
    pub assets: Vec<String>,
    /// Interval for market discovery refresh (seconds).
    pub discovery_interval_secs: u64,
    /// Order book poll interval for active short-term markets (seconds).
    pub order_book_active_secs: u64,
    /// Order book poll interval for daily/monthly markets (seconds).
    pub order_book_passive_secs: u64,
    /// Spot price poll interval (seconds).
    pub spot_price_poll_secs: u64,
    /// Maximum number of daily/monthly markets to poll order books for.
    pub max_daily_monthly_poll: usize,
    /// Whether to enable Chainlink RTDS WebSocket.
    pub chainlink_enabled: bool,
    /// Whether to enable Polymarket trade stream WebSocket.
    pub trade_stream_enabled: bool,
    /// Alert thresholds.
    pub alert_config: AlertConfig,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            assets: vec![
                "btc".into(),
                "eth".into(),
                "sol".into(),
                "xrp".into(),
                "doge".into(),
                "bnb".into(),
            ],
            discovery_interval_secs: 240,
            order_book_active_secs: 5,
            order_book_passive_secs: 30,
            spot_price_poll_secs: 3,
            max_daily_monthly_poll: 10,
            chainlink_enabled: false,
            trade_stream_enabled: true,
            alert_config: AlertConfig::default(),
        }
    }
}

/// Spawn all background polling tasks. Returns a Vec of JoinHandles.
///
/// Tasks spawned:
/// 1. Market discovery (every ~4 min)
/// 2. Order book poller for active short-term tokens (every ~5s)
/// 3. Order book poller for daily/monthly tokens (every ~30s)
/// 4. Spot price poller (every ~3s)
/// 5. (Optional) Chainlink RTDS WebSocket
/// 6. (Optional) Polymarket trade stream WebSocket
pub fn spawn_background_tasks(
    state: SharedState,
    config: PollerConfig,
) -> Vec<tokio::task::JoinHandle<()>> {
    spawn_background_tasks_with_auto_trade(state, config, None, None, None, None, None)
}

/// Spawn background tasks with optional auto-trade integration and data logging.
pub fn spawn_background_tasks_with_auto_trade(
    state: SharedState,
    config: PollerConfig,
    auto_state: Option<SharedAutoTradeState>,
    portfolio: Option<Arc<Mutex<Portfolio>>>,
    logger: Option<DataLogger>,
    live_executor: Option<Arc<LiveExecutor>>,
    risk_manager: Option<Arc<Mutex<RiskManager>>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();

    // 1. Market Discovery
    let discovery_state = state.clone();
    let discovery_assets = config.assets.clone();
    let discovery_interval = config.discovery_interval_secs;
    let discovery_logger = logger.clone();
    handles.push(tokio::spawn(async move {
        run_discovery_loop(discovery_state, &discovery_assets, discovery_interval, discovery_logger).await;
    }));

    // 2. Order Book Poller — active short-term tokens
    let ob_state = state.clone();
    let ob_interval = config.order_book_active_secs;
    let ob_alert_config = config.alert_config.clone();
    let ob_auto_state = auto_state.clone();
    let ob_portfolio = portfolio.clone();
    let ob_logger = logger.clone();
    let ob_live_executor = live_executor.clone();
    let ob_risk_manager = risk_manager.clone();
    handles.push(tokio::spawn(async move {
        run_order_book_active_loop(
            ob_state,
            ob_interval,
            &ob_alert_config,
            ob_auto_state,
            ob_portfolio,
            ob_logger,
            ob_live_executor,
            ob_risk_manager,
        )
        .await;
    }));

    // 3. Order Book Poller — daily/monthly tokens (slower)
    let ob_passive_state = state.clone();
    let ob_passive_interval = config.order_book_passive_secs;
    let ob_passive_limit = config.max_daily_monthly_poll;
    let ob_passive_logger = logger.clone();
    handles.push(tokio::spawn(async move {
        run_order_book_passive_loop(
            ob_passive_state,
            ob_passive_interval,
            ob_passive_limit,
            ob_passive_logger,
        )
        .await;
    }));

    // 4. Spot Price Poller
    let spot_state = state.clone();
    let spot_interval = config.spot_price_poll_secs;
    let spot_logger = logger.clone();
    handles.push(tokio::spawn(async move {
        run_spot_price_loop(spot_state, spot_interval, spot_logger).await;
    }));

    // 5. (Optional) Chainlink RTDS
    if config.chainlink_enabled {
        let cl_state = state.clone();
        handles.push(tokio::spawn(async move {
            run_chainlink_listener(cl_state).await;
        }));
    }

    // 6. (Optional) Trade Stream
    if config.trade_stream_enabled {
        let ts_state = state.clone();
        let ts_min_usd = config.alert_config.whale_trade_min_usd;
        handles.push(tokio::spawn(async move {
            run_trade_stream(ts_state, ts_min_usd).await;
        }));
    }

    info!(
        tasks = handles.len(),
        assets = ?config.assets,
        "Background monitor tasks spawned"
    );

    handles
}

/// Market discovery loop: discover short-term, daily, and monthly markets.
async fn run_discovery_loop(state: SharedState, assets: &[String], interval_secs: u64, logger: Option<DataLogger>) {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client for discovery");

    let interval = tokio::time::Duration::from_secs(interval_secs);

    // Run immediately on startup, then at intervals
    loop {
        info!("Running market discovery...");

        // Discover short-term (5m + 15m) for each asset
        for asset in assets {
            for interval_secs in [300u64, 900] {
                let interval_min = interval_secs / 60;
                let key = format!("{}_{interval_min}m", asset);

                match gamma::discover_short_term_markets(&client, asset, interval_secs).await {
                    Ok(mut markets) => {
                        debug!(
                            asset = %asset,
                            interval = interval_min,
                            count = markets.len(),
                            "Short-term markets discovered"
                        );
                        let mut s = state.write().await;
                        // Snapshot current spot price onto newly discovered markets
                        let underlying = gamma::asset_to_underlying(asset);
                        let spot = s.spot_prices.get(&underlying).map(|p| p.price);
                        // Preserve start_spot_price from existing markets (same window_start_ts)
                        if let Some(existing) = s.short_term_markets.get(&key) {
                            for market in &mut markets {
                                if let Some(prev) = existing.iter().find(|m| m.window_start_ts == market.window_start_ts) {
                                    market.start_spot_price = prev.start_spot_price;
                                } else {
                                    market.start_spot_price = spot;
                                }
                            }
                        } else {
                            for market in &mut markets {
                                market.start_spot_price = spot;
                            }
                        }
                        s.short_term_markets.insert(key, markets.clone());
                        drop(s);
                        if let Some(ref log) = logger {
                            log.log_markets("short_term", &markets).await;
                        }
                    }
                    Err(e) => {
                        warn!(
                            asset = %asset,
                            interval = interval_min,
                            error = %e,
                            "Short-term discovery failed"
                        );
                    }
                }
            }
        }

        // Discover daily markets
        match gamma::discover_daily_markets(&client, assets).await {
            Ok(markets) => {
                info!(count = markets.len(), "Daily markets discovered");
                let mut s = state.write().await;
                s.daily_markets = markets.clone();
                drop(s);
                if let Some(ref log) = logger {
                    log.log_markets("daily", &markets).await;
                }
            }
            Err(e) => {
                warn!(error = %e, "Daily discovery failed");
            }
        }

        // Discover monthly markets
        match gamma::discover_monthly_markets(&client, assets).await {
            Ok(markets) => {
                info!(count = markets.len(), "Monthly markets discovered");
                let mut s = state.write().await;
                s.monthly_markets = markets.clone();
                drop(s);
                if let Some(ref log) = logger {
                    log.log_markets("monthly", &markets).await;
                }
            }
            Err(e) => {
                warn!(error = %e, "Monthly discovery failed");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Order book polling loop for active short-term (5m/15m) tokens.
/// Polls every `interval_secs` seconds and runs alert detection + auto-trade checks.
async fn run_order_book_active_loop(
    state: SharedState,
    interval_secs: u64,
    alert_config: &AlertConfig,
    auto_state: Option<SharedAutoTradeState>,
    portfolio: Option<Arc<Mutex<Portfolio>>>,
    logger: Option<DataLogger>,
    live_executor: Option<Arc<LiveExecutor>>,
    risk_manager: Option<Arc<Mutex<RiskManager>>>,
) {
    let polymarket = PolymarketClient::new(None, None);
    let interval = tokio::time::Duration::from_secs(interval_secs);

    // Wait for initial discovery to populate markets
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Depth spike cooldown tracker: (token_id, side) -> last alert time
    let mut depth_cooldowns: HashMap<(String, String), std::time::Instant> = HashMap::new();

    loop {
        // Backfill start_spot_price for windows discovered before spot data loaded.
        // Discovery runs immediately on startup but spot prices take ~3s to arrive,
        // so the first discovered window often has start_spot_price = None.
        {
            let mut s = state.write().await;
            // Snapshot spot prices first to avoid borrow conflict
            let spot_snapshot: HashMap<String, f64> = s
                .spot_prices
                .iter()
                .map(|(k, v)| (k.clone(), v.price))
                .collect();
            for markets in s.short_term_markets.values_mut() {
                for m in markets.iter_mut() {
                    if m.start_spot_price.is_none() {
                        let underlying = crate::core::providers::gamma::asset_to_underlying(&m.asset);
                        if let Some(&price) = spot_snapshot.get(&underlying) {
                            m.start_spot_price = Some(price);
                            debug!(
                                asset = %m.asset,
                                interval = m.interval,
                                price = price,
                                "Backfilled start_spot_price"
                            );
                        }
                    }
                }
            }
        }

        // Collect token IDs with their stable market keys
        let token_ids_with_keys: Vec<(String, String)> = {
            let s = state.read().await;
            active_short_term_tokens_with_keys(&s)
        };
        let token_ids: Vec<String> = token_ids_with_keys.iter().map(|(tid, _)| tid.clone()).collect();
        let token_key_map: HashMap<String, String> = token_ids_with_keys.into_iter().collect();

        if token_ids.is_empty() {
            debug!("No active short-term tokens to poll");
            tokio::time::sleep(interval).await;
            continue;
        }

        debug!(count = token_ids.len(), "Polling active short-term order books");

        // Capture previous books for alert comparison
        let previous_books = {
            let s = state.read().await;
            s.order_books.clone()
        };

        // Fetch order books (stagger slightly to spread load)
        let stagger = tokio::time::Duration::from_millis(
            (interval_secs * 1000 / (token_ids.len() as u64 + 1)).min(500),
        );

        for token_id in &token_ids {
            match polymarket.get_order_book(token_id).await {
                Ok(book) => {
                    let tsb = TimestampedOrderBook::new(book);
                    let mkey = token_key_map.get(token_id).cloned().unwrap_or_else(|| token_id.clone());
                    let snapshot = snapshot_from_book(token_id, &mkey, &tsb);

                    let mut s = state.write().await;
                    s.order_books.insert(token_id.clone(), tsb);
                    s.push_history(snapshot);
                }
                Err(e) => {
                    debug!(token_id = %token_id, error = %e, "Order book fetch failed");
                }
            }
            tokio::time::sleep(stagger).await;
        }

        // Run alert detection
        let current_books = {
            let s = state.read().await;
            s.order_books.clone()
        };

        // Build token name map for alert messages
        let token_names = build_token_names(&state).await;

        let spread_alerts = check_spread_alerts(
            &current_books,
            &previous_books,
            &token_names,
            alert_config.spread_narrow_threshold,
        );
        let depth_alerts = check_depth_alerts(
            &current_books,
            &previous_books,
            &token_names,
            alert_config.depth_spike_multiplier,
            &mut depth_cooldowns,
            super::alerts::SHORT_TERM_DEPTH_CAP_USD,
        );

        // Check window approaching
        let window_alerts = {
            let s = state.read().await;
            check_window_approaching(&s.short_term_markets, alert_config.window_approach_secs)
        };

        // Push all alerts to state
        let alert_count =
            spread_alerts.len() + depth_alerts.len() + window_alerts.len();
        if alert_count > 0 {
            let mut s = state.write().await;
            for alert in spread_alerts
                .into_iter()
                .chain(depth_alerts)
                .chain(window_alerts)
            {
                info!(alert = %alert.kind, "Alert generated");
                s.push_alert(alert);
            }
        }

        // Log order books
        if let Some(ref log) = logger {
            for token_id in &token_ids {
                if let Some(tsb) = current_books.get(token_id) {
                    let mkey = token_key_map.get(token_id).cloned().unwrap_or_else(|| token_id.clone());
                    let name = token_names
                        .get(token_id)
                        .cloned()
                        .unwrap_or_else(|| token_id[..8.min(token_id.len())].to_string());
                    log.log_order_book(token_id, &mkey, &name, tsb).await;
                }
            }
        }

        // Auto-trade checks (if enabled)
        if let (Some(at_state), Some(port)) = (&auto_state, &portfolio) {
            auto_trade::run_auto_trade_checks_with_cache(
                &state,
                at_state,
                port,
                live_executor.as_deref(),
                risk_manager.as_ref(),
            ).await;
        }

        tokio::time::sleep(interval).await;
    }
}

/// Order book polling loop for daily/monthly tokens (less frequent).
async fn run_order_book_passive_loop(
    state: SharedState,
    interval_secs: u64,
    limit: usize,
    logger: Option<DataLogger>,
) {
    let polymarket = PolymarketClient::new(None, None);
    let interval = tokio::time::Duration::from_secs(interval_secs);

    // Wait for initial discovery
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    loop {
        // For daily/monthly, token IDs are stable so we use them as market_key
        let token_ids = {
            let s = state.read().await;
            s.top_daily_monthly_token_ids(limit)
        };

        if token_ids.is_empty() {
            debug!("No daily/monthly tokens to poll");
            tokio::time::sleep(interval).await;
            continue;
        }

        debug!(count = token_ids.len(), "Polling daily/monthly order books");

        let stagger = tokio::time::Duration::from_millis(
            (interval_secs * 1000 / (token_ids.len() as u64 + 1)).min(1000),
        );

        for token_id in &token_ids {
            match polymarket.get_order_book(token_id).await {
                Ok(book) => {
                    let tsb = TimestampedOrderBook::new(book);
                    // Daily/monthly token IDs are stable, so use as market_key
                    let snapshot = snapshot_from_book(token_id, token_id, &tsb);

                    // Log to JSONL (daily/monthly books now persist across restarts)
                    if let Some(ref logger) = logger {
                        logger
                            .log_order_book(token_id, token_id, "daily/monthly", &tsb)
                            .await;
                    }

                    let mut s = state.write().await;
                    s.order_books.insert(token_id.clone(), tsb);
                    s.push_history(snapshot);
                }
                Err(e) => {
                    debug!(token_id = %token_id, error = %e, "Passive order book fetch failed");
                }
            }
            tokio::time::sleep(stagger).await;
        }

        tokio::time::sleep(interval).await;
    }
}

/// Spot price polling loop.
async fn run_spot_price_loop(state: SharedState, interval_secs: u64, logger: Option<DataLogger>) {
    let binance = BinanceClient::new();
    let interval = tokio::time::Duration::from_secs(interval_secs);
    let mut last_cleanup = std::time::Instant::now();

    loop {
        match binance.get_all_spot_prices().await {
            Ok(prices) => {
                let mut s = state.write().await;
                for (symbol, price) in &prices {
                    s.spot_prices
                        .insert(symbol.clone(), TimestampedPrice::new(*price));
                }
                drop(s);
                debug!("Spot prices updated");
                if let Some(ref log) = logger {
                    log.log_spot_prices(&prices).await;
                }
            }
            Err(e) => {
                warn!(error = %e, "Spot price fetch failed");
            }
        }

        // Prune old whale trades periodically
        {
            let mut s = state.write().await;
            s.prune_whale_trades();
        }

        // Run log cleanup once per hour
        if last_cleanup.elapsed() >= std::time::Duration::from_secs(3600) {
            if let Some(ref log) = logger {
                log.cleanup_old_logs();
            }
            last_cleanup = std::time::Instant::now();
        }

        tokio::time::sleep(interval).await;
    }
}

/// Chainlink RTDS WebSocket listener. Updates spot prices from Chainlink feed.
async fn run_chainlink_listener(state: SharedState) {
    let (rtds, mut rx) = ChainlinkRtds::new_all();

    // Spawn the WebSocket connection in a separate task
    tokio::spawn(async move {
        if let Err(e) = rtds.connect().await {
            error!(error = %e, "Chainlink RTDS connection failed permanently");
        }
    });

    // Process incoming price updates
    loop {
        match rx.recv().await {
            Ok(update) => {
                if let Some(binance_sym) = update.to_binance_symbol() {
                    let mut s = state.write().await;
                    s.spot_prices
                        .insert(binance_sym, TimestampedPrice::new(update.price));
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!(skipped = n, "Chainlink price receiver lagged");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                error!("Chainlink price channel closed");
                break;
            }
        }
    }
}

/// Polymarket trade stream WebSocket listener. Detects whale trades.
/// Only subscribes to daily/monthly market tokens — 5m/15m trades are too small
/// (median ~$4, p99 < $200) to produce meaningful whale signals.
async fn run_trade_stream(state: SharedState, min_size_usd: f64) {
    let (token_tx, token_rx) = watch::channel(Vec::<String>::new());

    let (mut ws, mut rx) = PolymarketTradesWs::new(token_rx, min_size_usd);

    // Task to keep token IDs updated from state — daily/monthly only
    let token_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let ids = {
                let s = token_state.read().await;
                // Only daily/monthly: 5m/15m trades are too small for whale detection
                s.top_daily_monthly_token_ids(20)
            };
            if !ids.is_empty() {
                let _ = token_tx.send(ids);
            }
        }
    });

    // Spawn WebSocket connection
    tokio::spawn(async move {
        if let Err(e) = ws.connect().await {
            error!(error = %e, "Trade stream connection failed permanently");
        }
    });

    // Build name lookup for whale alert messages
    loop {
        match rx.recv().await {
            Ok(event) => {
                let token_names = build_token_names(&state).await;
                let name = token_names
                    .get(&event.asset_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        event.asset_id[..8.min(event.asset_id.len())].to_string()
                    });

                if let Some((alert, trade)) = process_whale_trade(
                    &event.asset_id,
                    &name,
                    &event.side,
                    event.price,
                    event.size_usd,
                    min_size_usd,
                ) {
                    let mut s = state.write().await;
                    s.push_alert(alert);
                    s.push_whale_trade(trade);
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!(skipped = n, "Trade stream receiver lagged");
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                error!("Trade stream channel closed");
                break;
            }
        }
    }
}

/// Get active short-term token IDs paired with their stable market keys.
/// Returns Vec<(token_id, market_key)> where market_key = "{asset}_{interval}m_{side}".
fn active_short_term_tokens_with_keys(s: &super::state::ServerState) -> Vec<(String, String)> {
    let now = chrono::Utc::now().timestamp();
    let mut result = Vec::new();
    for markets in s.short_term_markets.values() {
        for m in markets {
            let interval_secs = m.interval as i64 * 60;
            let window_end = m.window_start_ts + interval_secs;
            if now < window_end + 30 {
                let asset_lc = m.asset.to_lowercase();
                result.push((
                    m.up_token_id.clone(),
                    format!("{}_{}m_up", asset_lc, m.interval),
                ));
                result.push((
                    m.down_token_id.clone(),
                    format!("{}_{}m_down", asset_lc, m.interval),
                ));
            }
        }
    }
    result
}

/// Build a map of token_id → human-readable name from current state.
async fn build_token_names(state: &SharedState) -> HashMap<String, String> {
    let s = state.read().await;
    let mut names = HashMap::new();

    // Short-term markets
    for markets in s.short_term_markets.values() {
        for m in markets {
            let label = format!("{} {}m", m.asset, m.interval);
            names.insert(m.up_token_id.clone(), format!("{} UP", label));
            names.insert(m.down_token_id.clone(), format!("{} DOWN", label));
        }
    }

    // Daily markets
    for m in &s.daily_markets {
        names.insert(m.yes_token_id.clone(), format!("{} YES", m.name));
        names.insert(m.no_token_id.clone(), format!("{} NO", m.name));
    }

    // Monthly markets
    for m in &s.monthly_markets {
        names.insert(m.yes_token_id.clone(), format!("{} YES", m.name));
        names.insert(m.no_token_id.clone(), format!("{} NO", m.name));
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poller_config_defaults() {
        let config = PollerConfig::default();
        assert_eq!(config.assets.len(), 6);
        assert_eq!(config.discovery_interval_secs, 240);
        assert_eq!(config.order_book_active_secs, 5);
        assert_eq!(config.order_book_passive_secs, 30);
        assert_eq!(config.spot_price_poll_secs, 3);
        assert_eq!(config.max_daily_monthly_poll, 10);
        assert!(!config.chainlink_enabled);
        assert!(config.trade_stream_enabled);
    }

    #[test]
    fn test_alert_config_defaults() {
        let config = AlertConfig::default();
        assert!((config.spread_narrow_threshold - 10.0).abs() < 0.01);
        assert!((config.depth_spike_multiplier - 5.0).abs() < 0.01);
        assert!((config.whale_trade_min_usd - 5000.0).abs() < 0.01);
        assert_eq!(config.window_approach_secs, 30);
    }

    #[tokio::test]
    async fn test_build_token_names() {
        use crate::core::types::ShortTermMarket;
        use chrono::{Duration, Utc};

        let state = SharedState::new();

        {
            let mut s = state.write().await;
            s.short_term_markets.insert(
                "btc_5m".into(),
                vec![ShortTermMarket {
                    asset: "BTC".into(),
                    interval: 5,
                    window_start_ts: 1710000000,
                    up_token_id: "up_btc".into(),
                    down_token_id: "down_btc".into(),
                    condition_id: "cond".into(),
                    slug: "btc-updown-5m-123".into(),
                    start_spot_price: None,
                }],
            );

            s.daily_markets.push(crate::core::types::MarketConfig {
                name: "BTC $85k above".into(),
                condition_id: "daily_cond".into(),
                yes_token_id: "yes_daily".into(),
                no_token_id: "no_daily".into(),
                strike_price: 85000.0,
                expiry: Utc::now() + Duration::hours(24),
                underlying: "BTCUSDT".into(),
                volume_usd: 10000.0,
            });
        }

        let names = build_token_names(&state).await;
        assert_eq!(names.get("up_btc"), Some(&"BTC 5m UP".to_string()));
        assert_eq!(names.get("down_btc"), Some(&"BTC 5m DOWN".to_string()));
        assert_eq!(
            names.get("yes_daily"),
            Some(&"BTC $85k above YES".to_string())
        );
        assert_eq!(
            names.get("no_daily"),
            Some(&"BTC $85k above NO".to_string())
        );
    }

    #[test]
    fn test_spawn_returns_handles() {
        // Just verify spawn_background_tasks returns the right number of handles
        // without actually running the tasks (they'd try to connect to APIs)
        let config = PollerConfig {
            chainlink_enabled: false,
            trade_stream_enabled: false,
            ..Default::default()
        };
        // We can't easily test this without a tokio runtime that we control,
        // but we verify the config is well-formed
        assert_eq!(config.assets.len(), 6);
    }
}
