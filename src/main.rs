mod core;
mod mcp;

use std::sync::Arc;
use tokio::sync::Mutex;

use rmcp::ServiceExt;
use tracing_subscriber::EnvFilter;

use crate::core::execution::config::load_trading_config;
use crate::core::execution::risk::{RiskConfig, RiskManager};
use crate::core::monitor::auto_trade::AutoTradeState;
use crate::core::monitor::logger::{load_logging_config, DataLogger};
use crate::core::monitor::poller::{spawn_background_tasks_with_auto_trade, PollerConfig};
use crate::core::monitor::state::SharedState;
use crate::core::paper::portfolio::Portfolio;

use crate::core::execution::live::LiveExecutor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging to stderr (stdout is reserved for MCP stdio transport)
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("Starting crypto-conduit-mcp server");

    // Shared state for background polling + MCP tool reads
    let state = SharedState::new();

    // Load paper trading portfolio (or create empty)
    let portfolio = Portfolio::load().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Could not load portfolio, starting fresh");
        Portfolio::new().unwrap_or_default()
    });
    let portfolio = Arc::new(Mutex::new(portfolio));

    // Load auto-trade state (or create fresh)
    let auto_state = AutoTradeState::load().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Could not load auto-trade state, starting fresh");
        AutoTradeState::default()
    });
    let auto_state = Arc::new(Mutex::new(auto_state));

    // Initialize data logger from config
    let log_config = load_logging_config();
    let logger = DataLogger::new(log_config);
    tracing::info!(enabled = logger.is_enabled(), dir = %logger.log_dir().display(), "Data logger initialized");

    // Hydrate history from JSONL logs (survives restarts)
    {
        use crate::core::monitor::logger::load_recent_order_books;
        let snapshots = load_recent_order_books(
            logger.log_dir(),
            chrono::Duration::hours(24),
            4800,
        );
        if !snapshots.is_empty() {
            let mut s = state.write().await;
            let count = snapshots.len();
            for snapshot in snapshots {
                s.push_history(snapshot);
            }
            tracing::info!(entries = count, "Hydrated history from JSONL logs");
        }
    }

    // Load trading config (if [trading] section exists in config.toml)
    let trading_config = load_trading_config();

    // Initialize live executor (if configured)
    let live_executor: Option<Arc<LiveExecutor>> = if let Some(ref tc) = trading_config {
        match LiveExecutor::new(tc).await {
            Ok(exec) => {
                tracing::info!("Live trading initialized");
                Some(Arc::new(exec))
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to initialize live trading — continuing without it");
                None
            }
        }
    } else {
        tracing::info!("No [trading] section in config.toml — live trading disabled");
        None
    };

    // Initialize risk manager (works with or without live executor)
    let risk_manager: Option<Arc<Mutex<RiskManager>>> = if let Some(ref tc) = trading_config {
        // Try to get initial balance from live executor
        let initial_balance = if let Some(ref exec) = live_executor {
            exec.get_balance().await.unwrap_or(0.0)
        } else {
            0.0
        };

        let risk_config: RiskConfig = tc.risk.clone().into();
        Some(Arc::new(Mutex::new(RiskManager::new(risk_config, initial_balance))))
    } else {
        None
    };

    // Spawn background polling tasks with auto-trade integration
    let config = PollerConfig::default();
    let _handles = spawn_background_tasks_with_auto_trade(
        state.clone(),
        config,
        Some(auto_state.clone()),
        Some(portfolio.clone()),
        Some(logger.clone()),
        live_executor.clone(),
        risk_manager.clone(),
    );
    tracing::info!("Background polling tasks spawned");

    let server = mcp::CryptoMcpServer::new(
        state,
        portfolio,
        auto_state,
        logger,
        live_executor,
        risk_manager,
    );
    let service = server.serve(rmcp::transport::io::stdio()).await?;

    tracing::info!("Server initialized, waiting for requests");

    service.waiting().await?;

    Ok(())
}
