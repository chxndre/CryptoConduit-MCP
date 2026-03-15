// MCP tool handlers for background monitor alerts and data logging.
// Tools: get_alerts, set_data_logging, get_data_logging_status

use rmcp::schemars;
use serde::Deserialize;

use crate::core::monitor::logger::DataLogger;
use crate::core::monitor::state::SharedState;
use crate::mcp::formatter::fmt_usd;

// ──────────────────────────── get_alerts ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetAlertsParams {
    /// Lookback period in minutes (default: 60)
    pub since_minutes: Option<u64>,
    /// Minimum whale trade size in USD to include (default: 5000)
    pub min_trade_usd: Option<f64>,
    /// Output format: "text" (default) or "json"
    pub format: Option<String>,
}

pub async fn handle_get_alerts(
    state: &SharedState,
    params: GetAlertsParams,
) -> String {
    let s = state.read().await;
    let since_minutes = params.since_minutes.unwrap_or(60);
    let cutoff = chrono::Utc::now() - chrono::Duration::minutes(since_minutes as i64);

    // Filter alerts by time
    let recent_alerts: Vec<_> = s
        .alerts
        .iter()
        .filter(|a| a.timestamp >= cutoff)
        .collect();

    // Filter whale trades by time and size
    let min_trade = params.min_trade_usd.unwrap_or(5000.0);
    let recent_whales: Vec<_> = s
        .whale_trades
        .iter()
        .filter(|w| w.timestamp >= cutoff && w.size_usd >= min_trade)
        .collect();

    if recent_alerts.is_empty() && recent_whales.is_empty() {
        return format!(
            "No alerts in the last {since_minutes} minutes. Background monitor is active and watching."
        );
    }

    let mut out = format!("## Alerts (last {since_minutes}m)\n\n");

    // Market alerts
    if !recent_alerts.is_empty() {
        out.push_str(&format!("### Market Events ({})\n", recent_alerts.len()));
        for alert in recent_alerts.iter().rev().take(20) {
            out.push_str(&format!(
                "- **{}** — {}\n",
                alert.timestamp.format("%H:%M:%S UTC"),
                alert.kind,
            ));
        }
    }

    // Whale trades
    if !recent_whales.is_empty() {
        out.push_str(&format!(
            "\n### Whale Trades ({}, ≥{})\n",
            recent_whales.len(),
            fmt_usd(min_trade),
        ));
        for whale in recent_whales.iter().rev().take(20) {
            out.push_str(&format!(
                "- **{}** {} {} @ {:.3} on {}\n",
                whale.timestamp.format("%H:%M:%S UTC"),
                whale.side,
                fmt_usd(whale.size_usd),
                whale.price,
                whale.market_name,
            ));
        }
    }

    out
}

// ──────────────────────────── set_data_logging ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SetDataLoggingParams {
    /// Enable or disable data logging
    pub enabled: bool,
}

pub fn handle_set_data_logging(
    logger: &DataLogger,
    params: SetDataLoggingParams,
) -> String {
    logger.set_enabled(params.enabled);

    if params.enabled {
        format!(
            "## Data Logging Enabled\n\n\
             Log directory: `{}`\n\
             Full order books: {}\n\n\
             Logging: spot prices (~5 MB/day), order book summaries (~50 MB/day), market discovery.\n\
             Data is written as JSONL files, rotated daily by UTC date.\n\n\
             To disable: `set_data_logging` with `enabled: false`",
            logger.log_dir().display(),
            if logger.log_full_books() { "yes (~1 GB/day)" } else { "no (summary only)" },
        )
    } else {
        "## Data Logging Disabled\n\nNo new data will be written. Existing log files are preserved.".to_string()
    }
}

// ──────────────────────────── get_data_logging_status ────────────────────────────

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetDataLoggingStatusParams {
    /// Placeholder (no params needed)
    pub _unused: Option<String>,
}

pub fn handle_get_data_logging_status(
    logger: &DataLogger,
) -> String {
    let enabled = logger.is_enabled();
    let log_dir = logger.log_dir();

    let mut out = format!(
        "## Data Logging Status\n\n\
         **Enabled:** {}\n\
         **Log directory:** `{}`\n\
         **Full order books:** {}\n",
        if enabled { "YES" } else { "NO" },
        log_dir.display(),
        if logger.log_full_books() { "yes" } else { "no" },
    );

    // Check disk usage if directory exists
    if log_dir.exists() {
        let mut total_bytes: u64 = 0;
        let mut file_count: u64 = 0;
        for entry in walkdir(log_dir) {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    total_bytes += metadata.len();
                    file_count += 1;
                }
            }
        }
        let mb = total_bytes as f64 / (1024.0 * 1024.0);
        out.push_str(&format!(
            "**Disk usage:** {:.1} MB across {} files\n",
            mb, file_count,
        ));
    } else {
        out.push_str("**Disk usage:** no log directory yet\n");
    }

    out
}

/// Simple recursive directory walker for disk usage calculation.
fn walkdir(dir: &std::path::Path) -> Vec<std::fs::DirEntry> {
    let mut entries = Vec::new();
    if let Ok(read_dir) = std::fs::read_dir(dir) {
        for entry in read_dir.flatten() {
            if let Ok(ft) = entry.file_type() {
                if ft.is_dir() {
                    entries.extend(walkdir(&entry.path()));
                } else {
                    entries.push(entry);
                }
            }
        }
    }
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::monitor::state::{Alert, AlertKind, SharedState, WhaleTrade};
    use chrono::Utc;

    #[tokio::test]
    async fn test_alerts_empty_state() {
        let state = SharedState::new();
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: None,
                min_trade_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No alerts"));
        assert!(result.contains("Background monitor is active"));
    }

    #[tokio::test]
    async fn test_alerts_with_market_alerts() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.push_alert(Alert::new(AlertKind::SpreadNarrowing {
                token_id: "0xabc".into(),
                market_name: "BTC 5m UP".into(),
                old_spread_pct: 0.15,
                new_spread_pct: 0.05,
            }));
        }
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: Some(60),
                min_trade_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("## Alerts"));
        assert!(result.contains("Market Events"));
        assert!(result.contains("BTC 5m UP"));
    }

    #[tokio::test]
    async fn test_alerts_with_whale_trades() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.push_whale_trade(WhaleTrade {
                token_id: "0xabc".into(),
                market_name: "ETH 5m DOWN".into(),
                side: "BUY".into(),
                price: 0.65,
                size_usd: 10000.0,
                timestamp: Utc::now(),
            });
        }
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: Some(60),
                min_trade_usd: Some(5000.0),
                format: None,
            },
        )
        .await;
        assert!(result.contains("Whale Trades"));
        assert!(result.contains("ETH 5m DOWN"));
        assert!(result.contains("$10000"));
    }

    #[tokio::test]
    async fn test_alerts_whale_filter_by_size() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            // Small whale trade below threshold
            s.push_whale_trade(WhaleTrade {
                token_id: "0xabc".into(),
                market_name: "BTC 5m UP".into(),
                side: "BUY".into(),
                price: 0.50,
                size_usd: 3000.0,
                timestamp: Utc::now(),
            });
        }
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: Some(60),
                min_trade_usd: Some(5000.0),
                format: None,
            },
        )
        .await;
        // Should not show whale trades section (below threshold)
        assert!(result.contains("No alerts"));
    }

    #[tokio::test]
    async fn test_alerts_old_events_filtered() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            // Add alert from 2 hours ago
            let old_alert = Alert {
                timestamp: Utc::now() - chrono::Duration::hours(2),
                kind: AlertKind::SpreadNarrowing {
                    token_id: "0xold".into(),
                    market_name: "OLD ALERT".into(),
                    old_spread_pct: 0.20,
                    new_spread_pct: 0.05,
                },
            };
            s.push_alert(old_alert);
        }
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: Some(60), // only last 60 min
                min_trade_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("No alerts"));
    }

    #[tokio::test]
    async fn test_alerts_custom_lookback() {
        let state = SharedState::new();
        {
            let mut s = state.write().await;
            s.push_alert(Alert::new(AlertKind::WindowApproaching {
                asset: "SOL".into(),
                interval: 5,
                window_start_ts: Utc::now().timestamp() + 20,
                seconds_until: 20,
            }));
        }
        let result = handle_get_alerts(
            &state,
            GetAlertsParams {
                since_minutes: Some(5), // short lookback
                min_trade_usd: None,
                format: None,
            },
        )
        .await;
        assert!(result.contains("## Alerts (last 5m)"));
    }
}
