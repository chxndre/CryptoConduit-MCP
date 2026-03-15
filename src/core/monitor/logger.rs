// Data logger: writes market data to JSONL files for historical analysis.
// Integrates with the background poller to capture spot prices, order books, and market discovery.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::state::{MarketSnapshot, TimestampedOrderBook};

// ──────────────────────────── Config ────────────────────────────

/// Configuration for the data logger.
#[derive(Debug, Clone)]
pub struct DataLoggerConfig {
    /// Whether logging is enabled.
    pub enabled: bool,
    /// Root directory for log files.
    pub log_dir: PathBuf,
    /// Whether to log full order book arrays (large) or just summary.
    pub log_full_books: bool,
    /// Number of days to retain log files (0 = keep forever).
    pub retention_days: u32,
}

impl Default for DataLoggerConfig {
    fn default() -> Self {
        let log_dir = dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("crypto-conduit")
            .join("logs");

        Self {
            enabled: false,
            log_dir,
            log_full_books: false,
            retention_days: 30,
        }
    }
}

// ──────────────────────────── Record types ────────────────────────────

#[derive(Serialize)]
struct SpotPriceRecord {
    ts: String,
    ts_epoch_ms: i64,
    symbol: String,
    price: f64,
}

#[derive(Serialize)]
struct OrderBookRecord {
    ts: String,
    ts_epoch_ms: i64,
    token_id: String,
    market_key: String,
    market_name: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread_pct: Option<f64>,
    bid_depth_1pct: f64,
    ask_depth_1pct: f64,
    midpoint: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bids: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    asks: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct MarketRecord {
    ts: String,
    ts_epoch_ms: i64,
    category: String,
    count: usize,
    markets: serde_json::Value,
}

// ──────────────────────────── Writers ────────────────────────────

struct LogWriters {
    current_date: String,
    spot_writer: Option<BufWriter<File>>,
    book_writer: Option<BufWriter<File>>,
    market_writer: Option<BufWriter<File>>,
}

impl LogWriters {
    fn new() -> Self {
        Self {
            current_date: String::new(),
            spot_writer: None,
            book_writer: None,
            market_writer: None,
        }
    }

    fn flush_all(&mut self) {
        if let Some(ref mut w) = self.spot_writer {
            let _ = w.flush();
        }
        if let Some(ref mut w) = self.book_writer {
            let _ = w.flush();
        }
        if let Some(ref mut w) = self.market_writer {
            let _ = w.flush();
        }
    }
}

// ──────────────────────────── DataLogger ────────────────────────────

/// Appends market data to daily JSONL files.
/// Thread-safe — all methods are async and use internal locking.
#[derive(Clone)]
pub struct DataLogger {
    config: DataLoggerConfig,
    enabled: Arc<AtomicBool>,
    writers: Arc<Mutex<LogWriters>>,
}

impl DataLogger {
    pub fn new(config: DataLoggerConfig) -> Self {
        let enabled = Arc::new(AtomicBool::new(config.enabled));
        Self {
            config,
            enabled,
            writers: Arc::new(Mutex::new(LogWriters::new())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if enabled {
            info!(dir = %self.config.log_dir.display(), "Data logging enabled");
        } else {
            info!("Data logging disabled");
        }
    }

    pub fn log_dir(&self) -> &PathBuf {
        &self.config.log_dir
    }

    pub fn log_full_books(&self) -> bool {
        self.config.log_full_books
    }

    pub fn set_log_full_books(&self, full: bool) {
        // This is a minor race but acceptable for a config toggle
        // config is cloned into self, so we'd need interior mutability
        // For now, this is set at construction time
        let _ = full;
    }

    /// Log spot prices from a Binance poll.
    pub async fn log_spot_prices(&self, prices: &HashMap<String, f64>) {
        if !self.is_enabled() {
            return;
        }

        let now = Utc::now();
        let ts = now.to_rfc3339();
        let ts_epoch_ms = now.timestamp_millis();

        let mut writers = self.writers.lock().await;
        self.ensure_date_rotation(&mut writers, "spot_prices").ok();

        if let Some(ref mut w) = writers.spot_writer {
            for (symbol, price) in prices {
                let record = SpotPriceRecord {
                    ts: ts.clone(),
                    ts_epoch_ms,
                    symbol: symbol.clone(),
                    price: *price,
                };
                if let Ok(line) = serde_json::to_string(&record) {
                    let _ = writeln!(w, "{}", line);
                }
            }
        }
    }

    /// Log an order book snapshot.
    pub async fn log_order_book(
        &self,
        token_id: &str,
        market_key: &str,
        market_name: &str,
        tsb: &TimestampedOrderBook,
    ) {
        if !self.is_enabled() {
            return;
        }

        let now = Utc::now();
        let ts = now.to_rfc3339();
        let ts_epoch_ms = now.timestamp_millis();

        let book = &tsb.book;
        let record = OrderBookRecord {
            ts,
            ts_epoch_ms,
            token_id: token_id.to_string(),
            market_key: market_key.to_string(),
            market_name: market_name.to_string(),
            best_bid: book.best_bid(),
            best_ask: book.best_ask(),
            spread_pct: book.spread_pct(),
            bid_depth_1pct: book.bid_depth_within(1.0),
            ask_depth_1pct: book.ask_depth_within(1.0),
            midpoint: book.midpoint(),
            bids: if self.config.log_full_books {
                serde_json::to_value(&book.bids).ok()
            } else {
                None
            },
            asks: if self.config.log_full_books {
                serde_json::to_value(&book.asks).ok()
            } else {
                None
            },
        };

        let mut writers = self.writers.lock().await;
        self.ensure_date_rotation(&mut writers, "order_books").ok();

        if let Some(ref mut w) = writers.book_writer {
            if let Ok(line) = serde_json::to_string(&record) {
                let _ = writeln!(w, "{}", line);
            }
        }
    }

    /// Log market discovery results.
    pub async fn log_markets<T: Serialize>(&self, category: &str, markets: &[T]) {
        if !self.is_enabled() {
            return;
        }

        let now = Utc::now();
        let record = MarketRecord {
            ts: now.to_rfc3339(),
            ts_epoch_ms: now.timestamp_millis(),
            category: category.to_string(),
            count: markets.len(),
            markets: serde_json::to_value(markets).unwrap_or_default(),
        };

        let mut writers = self.writers.lock().await;
        self.ensure_date_rotation(&mut writers, "markets").ok();

        if let Some(ref mut w) = writers.market_writer {
            if let Ok(line) = serde_json::to_string(&record) {
                let _ = writeln!(w, "{}", line);
            }
        }
    }

    /// Flush all buffered writers.
    pub async fn flush(&self) {
        let mut writers = self.writers.lock().await;
        writers.flush_all();
    }

    /// Number of days to retain log files (0 = keep forever).
    pub fn retention_days(&self) -> u32 {
        self.config.retention_days
    }

    /// Delete JSONL log files older than `retention_days`.
    /// Scans all subdirectories (spot_prices, order_books, markets) for date-named files.
    pub fn cleanup_old_logs(&self) {
        let days = self.config.retention_days;
        if days == 0 {
            return;
        }

        let cutoff = Utc::now() - Duration::days(days as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d").to_string();
        let mut removed = 0u32;

        for subdir in &["spot_prices", "order_books", "markets"] {
            let dir = self.config.log_dir.join(subdir);
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(_) => continue,
            };

            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                // Files are named YYYY-MM-DD.jsonl — compare lexicographically
                if name_str.ends_with(".jsonl") {
                    let date_part = name_str.trim_end_matches(".jsonl");
                    if date_part < cutoff_str.as_str() {
                        if fs::remove_file(entry.path()).is_ok() {
                            removed += 1;
                        }
                    }
                }
            }
        }

        if removed > 0 {
            info!(removed, retention_days = days, "Cleaned up old log files");
        }
    }

    /// Ensure writers are open for today's date. Rotates files at midnight UTC.
    fn ensure_date_rotation(
        &self,
        writers: &mut LogWriters,
        _hint: &str,
    ) -> std::io::Result<()> {
        let today = Utc::now().format("%Y-%m-%d").to_string();

        if writers.current_date == today {
            return Ok(());
        }

        // Close old writers
        writers.flush_all();
        writers.spot_writer = None;
        writers.book_writer = None;
        writers.market_writer = None;

        // Open new writers
        let spot_dir = self.config.log_dir.join("spot_prices");
        let book_dir = self.config.log_dir.join("order_books");
        let market_dir = self.config.log_dir.join("markets");

        fs::create_dir_all(&spot_dir)?;
        fs::create_dir_all(&book_dir)?;
        fs::create_dir_all(&market_dir)?;

        writers.spot_writer = Some(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(spot_dir.join(format!("{today}.jsonl")))?,
        ));
        writers.book_writer = Some(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(book_dir.join(format!("{today}.jsonl")))?,
        ));
        writers.market_writer = Some(BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(market_dir.join(format!("{today}.jsonl")))?,
        ));

        writers.current_date = today.clone();
        info!(date = %today, dir = %self.config.log_dir.display(), "Log files rotated");

        Ok(())
    }
}

// ──────────────────────────── JSONL Read-back ────────────────────────────

/// Deserialization target for order book JSONL lines.
#[derive(Deserialize)]
struct OrderBookLogEntry {
    ts_epoch_ms: i64,
    token_id: String,
    /// Stable market key — may be absent in older logs (pre-market_key).
    market_key: Option<String>,
    #[allow(dead_code)]
    market_name: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    spread_pct: Option<f64>,
    bid_depth_1pct: f64,
    ask_depth_1pct: f64,
}

/// Load recent order book snapshots from JSONL files into MarketSnapshots.
///
/// Reads today's and yesterday's files, filters to entries within `max_age`,
/// and returns sorted snapshots capped at `max_entries`.
/// Gracefully returns empty vec if files don't exist or are corrupt.
pub fn load_recent_order_books(
    log_dir: &Path,
    max_age: Duration,
    max_entries: usize,
) -> Vec<MarketSnapshot> {
    let book_dir = log_dir.join("order_books");
    if !book_dir.exists() {
        debug!(dir = %book_dir.display(), "No order_books log directory found");
        return Vec::new();
    }

    let now = Utc::now();
    let cutoff = now - max_age;
    let cutoff_ms = cutoff.timestamp_millis();

    // Collect date strings to try (today + as many past days as max_age covers)
    let days_back = max_age.num_days() + 1;
    let mut date_files: Vec<PathBuf> = Vec::new();
    for d in 0..=days_back {
        let date = (now - Duration::days(d)).format("%Y-%m-%d").to_string();
        let path = book_dir.join(format!("{date}.jsonl"));
        if path.exists() {
            date_files.push(path);
        }
    }

    if date_files.is_empty() {
        debug!("No order book JSONL files found for hydration");
        return Vec::new();
    }

    // Oldest files first so we can take the most recent max_entries at the end
    date_files.reverse();

    let mut snapshots: Vec<MarketSnapshot> = Vec::new();
    let mut parse_errors = 0u32;

    for path in &date_files {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "Failed to open order book log file");
                continue;
            }
        };

        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => {
                    parse_errors += 1;
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            let entry: OrderBookLogEntry = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => {
                    parse_errors += 1;
                    continue;
                }
            };

            if entry.ts_epoch_ms < cutoff_ms {
                continue;
            }

            let timestamp = match DateTime::from_timestamp_millis(entry.ts_epoch_ms) {
                Some(dt) => dt,
                None => continue,
            };

            let market_key = entry.market_key.unwrap_or_else(|| entry.token_id.clone());
            snapshots.push(MarketSnapshot {
                token_id: entry.token_id,
                market_key,
                timestamp,
                best_bid: entry.best_bid,
                best_ask: entry.best_ask,
                spread_pct: entry.spread_pct,
                bid_depth_1pct: entry.bid_depth_1pct,
                ask_depth_1pct: entry.ask_depth_1pct,
            });
        }
    }

    if parse_errors > 0 {
        warn!(errors = parse_errors, "Skipped malformed lines in order book JSONL");
    }

    // Sort by timestamp ascending
    snapshots.sort_by_key(|s| s.timestamp);

    // Keep only the most recent max_entries
    if snapshots.len() > max_entries {
        snapshots = snapshots.split_off(snapshots.len() - max_entries);
    }

    info!(
        entries = snapshots.len(),
        files = date_files.len(),
        "Loaded order book history from JSONL logs"
    );
    snapshots
}

/// Load logging config from config.toml.
pub fn load_logging_config() -> DataLoggerConfig {
    let config_path = dirs::config_dir()
        .map(|d| d.join("crypto-conduit").join("config.toml"));

    let mut config = DataLoggerConfig::default();

    if let Some(path) = config_path {
        if let Ok(content) = fs::read_to_string(&path) {
            if let Ok(table) = content.parse::<toml::Table>() {
                if let Some(logging) = table.get("logging").and_then(|v| v.as_table()) {
                    if let Some(enabled) = logging.get("enabled").and_then(|v| v.as_bool()) {
                        config.enabled = enabled;
                    }
                    if let Some(dir) = logging.get("log_dir").and_then(|v| v.as_str()) {
                        config.log_dir = PathBuf::from(dir);
                    }
                    if let Some(full) = logging.get("log_full_books").and_then(|v| v.as_bool()) {
                        config.log_full_books = full;
                    }
                    if let Some(days) = logging.get("retention_days").and_then(|v| v.as_integer()) {
                        config.retention_days = days.max(0) as u32;
                    }
                }
            }
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{OrderBook, PriceLevel};
    use tempfile::TempDir;

    fn make_book() -> OrderBook {
        OrderBook {
            timestamp: 1710000000,
            market: "test".into(),
            asset_id: "token_1".into(),
            bids: vec![PriceLevel {
                price: "0.50".into(),
                size: "100.0".into(),
            }],
            asks: vec![PriceLevel {
                price: "0.55".into(),
                size: "100.0".into(),
            }],
        }
    }

    #[tokio::test]
    async fn test_logger_disabled_noop() {
        let config = DataLoggerConfig {
            enabled: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);
        // Should not panic or create files
        let mut prices = HashMap::new();
        prices.insert("BTCUSDT".to_string(), 84000.0);
        logger.log_spot_prices(&prices).await;
    }

    #[tokio::test]
    async fn test_logger_writes_spot_prices() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        let mut prices = HashMap::new();
        prices.insert("BTCUSDT".to_string(), 84000.0);
        prices.insert("ETHUSDT".to_string(), 3200.0);

        logger.log_spot_prices(&prices).await;
        logger.flush().await;

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let file_path = dir.path().join("spot_prices").join(format!("{today}.jsonl"));
        assert!(file_path.exists(), "Spot price log file should exist");

        let content = fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "Should have 2 price records");

        // Verify JSON is parseable
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v.get("symbol").is_some());
            assert!(v.get("price").is_some());
            assert!(v.get("ts").is_some());
        }
    }

    #[tokio::test]
    async fn test_logger_writes_order_book() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        let tsb = TimestampedOrderBook::new(make_book());
        logger
            .log_order_book("token_1", "btc_5m_up", "BTC 5m UP", &tsb)
            .await;
        logger.flush().await;

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let file_path = dir.path().join("order_books").join(format!("{today}.jsonl"));
        assert!(file_path.exists());

        let content = fs::read_to_string(&file_path).unwrap();
        let v: serde_json::Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(v["token_id"], "token_1");
        assert_eq!(v["market_name"], "BTC 5m UP");
        // No bids/asks in summary mode
        assert!(v.get("bids").is_none());
    }

    #[tokio::test]
    async fn test_logger_full_books() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: true,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        let tsb = TimestampedOrderBook::new(make_book());
        logger
            .log_order_book("token_1", "btc_5m_up", "BTC 5m UP", &tsb)
            .await;
        logger.flush().await;

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let file_path = dir.path().join("order_books").join(format!("{today}.jsonl"));
        let content = fs::read_to_string(&file_path).unwrap();
        let v: serde_json::Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        // Full mode includes bids/asks
        assert!(v.get("bids").is_some());
        assert!(v.get("asks").is_some());
    }

    #[tokio::test]
    async fn test_logger_toggle() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: false,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        assert!(!logger.is_enabled());
        logger.set_enabled(true);
        assert!(logger.is_enabled());
        logger.set_enabled(false);
        assert!(!logger.is_enabled());
    }

    #[tokio::test]
    async fn test_load_recent_order_books() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        // Write some order book entries
        let tsb = TimestampedOrderBook::new(make_book());
        for i in 0..5 {
            let _ = i;
            logger
                .log_order_book("token_1", "btc_5m_up", "BTC 5m UP", &tsb)
                .await;
        }
        logger.flush().await;

        // Read them back
        let snapshots = load_recent_order_books(
            dir.path(),
            chrono::Duration::hours(24),
            4800,
        );
        assert_eq!(snapshots.len(), 5);
        assert_eq!(snapshots[0].token_id, "token_1");
        assert_eq!(snapshots[0].market_key, "btc_5m_up");
        assert!(snapshots[0].best_bid.is_some());
        assert!(snapshots[0].best_ask.is_some());
    }

    #[tokio::test]
    async fn test_load_recent_order_books_empty_dir() {
        let dir = TempDir::new().unwrap();
        let snapshots = load_recent_order_books(
            dir.path(),
            chrono::Duration::hours(24),
            4800,
        );
        assert!(snapshots.is_empty());
    }

    #[tokio::test]
    async fn test_load_recent_order_books_corrupt_lines() {
        let dir = TempDir::new().unwrap();
        let book_dir = dir.path().join("order_books");
        fs::create_dir_all(&book_dir).unwrap();

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let file_path = book_dir.join(format!("{today}.jsonl"));

        // Write one valid and one corrupt line
        let valid = serde_json::json!({
            "ts": "2026-03-19T00:00:00Z",
            "ts_epoch_ms": Utc::now().timestamp_millis(),
            "token_id": "tok_1",
            "market_name": "BTC 5m UP",
            "best_bid": 0.50,
            "best_ask": 0.55,
            "spread_pct": 0.10,
            "bid_depth_1pct": 100.0,
            "ask_depth_1pct": 150.0,
            "midpoint": 0.525
        });
        let mut content = serde_json::to_string(&valid).unwrap();
        content.push('\n');
        content.push_str("this is not json\n");
        fs::write(&file_path, content).unwrap();

        let snapshots = load_recent_order_books(
            dir.path(),
            chrono::Duration::hours(24),
            4800,
        );
        assert_eq!(snapshots.len(), 1);
    }

    #[tokio::test]
    async fn test_logger_market_discovery() {
        let dir = TempDir::new().unwrap();
        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            ..Default::default()
        };
        let logger = DataLogger::new(config);

        let markets = vec![
            serde_json::json!({"asset": "BTC", "interval": 5}),
            serde_json::json!({"asset": "ETH", "interval": 5}),
        ];
        logger.log_markets("short_term", &markets).await;
        logger.flush().await;

        let today = Utc::now().format("%Y-%m-%d").to_string();
        let file_path = dir.path().join("markets").join(format!("{today}.jsonl"));
        assert!(file_path.exists());

        let content = fs::read_to_string(&file_path).unwrap();
        let v: serde_json::Value = serde_json::from_str(content.lines().next().unwrap()).unwrap();
        assert_eq!(v["category"], "short_term");
        assert_eq!(v["count"], 2);
    }

    #[test]
    fn test_cleanup_old_logs() {
        let dir = TempDir::new().unwrap();

        // Create subdirectories with old and new files
        for subdir in &["spot_prices", "order_books", "markets"] {
            let sub = dir.path().join(subdir);
            fs::create_dir_all(&sub).unwrap();
            // Old file (should be deleted with retention_days=7)
            fs::write(sub.join("2020-01-01.jsonl"), "old data\n").unwrap();
            // Recent file (should be kept)
            let today = Utc::now().format("%Y-%m-%d").to_string();
            fs::write(sub.join(format!("{today}.jsonl")), "new data\n").unwrap();
        }

        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            retention_days: 7,
        };
        let logger = DataLogger::new(config);
        logger.cleanup_old_logs();

        // Old files should be gone
        assert!(!dir.path().join("spot_prices").join("2020-01-01.jsonl").exists());
        assert!(!dir.path().join("order_books").join("2020-01-01.jsonl").exists());
        assert!(!dir.path().join("markets").join("2020-01-01.jsonl").exists());

        // Today's files should still exist
        let today = Utc::now().format("%Y-%m-%d").to_string();
        assert!(dir.path().join("spot_prices").join(format!("{today}.jsonl")).exists());
    }

    #[test]
    fn test_cleanup_zero_retention_keeps_all() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("spot_prices");
        fs::create_dir_all(&sub).unwrap();
        fs::write(sub.join("2020-01-01.jsonl"), "old data\n").unwrap();

        let config = DataLoggerConfig {
            enabled: true,
            log_dir: dir.path().to_path_buf(),
            log_full_books: false,
            retention_days: 0,
        };
        let logger = DataLogger::new(config);
        logger.cleanup_old_logs();

        // File should still exist (retention_days=0 means keep forever)
        assert!(sub.join("2020-01-01.jsonl").exists());
    }
}
