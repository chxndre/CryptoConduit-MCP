pub mod formatter;
mod tools_analysis;
mod tools_auto;
mod tools_book;
mod tools_market;
mod tools_monitor;
mod tools_paper;
mod tools_trading;
mod tools_whale;

use std::sync::Arc;
use tokio::sync::Mutex;

use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ServerHandler};

use crate::core::analysis::fair_value::FairValueCache;
use crate::core::monitor::auto_trade::SharedAutoTradeState;
use crate::core::monitor::logger::DataLogger;
use crate::core::monitor::state::SharedState;
use crate::core::paper::portfolio::Portfolio;
use crate::core::providers::binance::BinanceClient;
use crate::core::providers::polymarket::PolymarketClient;

use crate::core::execution::live::LiveExecutor;
use crate::core::execution::risk::RiskManager;

pub struct CryptoMcpServer {
    tool_router: ToolRouter<Self>,
    state: SharedState,
    polymarket: PolymarketClient,
    http_client: reqwest::Client,
    binance: BinanceClient,
    portfolio: Arc<Mutex<Portfolio>>,
    auto_state: SharedAutoTradeState,
    logger: DataLogger,
    fair_value_cache: Arc<FairValueCache>,
    live_executor: Option<Arc<LiveExecutor>>,
    risk_manager: Option<Arc<Mutex<RiskManager>>>,
}

const NOT_CONFIGURED_MSG: &str =
    "Live trading not configured. Add a [trading] section to config.toml with your private key and restart the server.";

#[tool_router]
impl CryptoMcpServer {
    // ── Market tools ──

    #[tool(
        name = "search_crypto_markets",
        description = "Search for crypto prediction markets on Polymarket. Filter by asset (btc, eth, sol, xrp, doge, bnb) and market type (5m, 15m, daily, monthly). Sort by 'spread' (tightest), 'volume' (highest), or 'both' (two ranked tables). Returns enriched results with spreads, depth, and tradeability."
    )]
    async fn search_crypto_markets(
        &self,
        Parameters(params): Parameters<tools_market::SearchCryptoMarketsParams>,
    ) -> String {
        tools_market::handle_search_crypto_markets(&self.state, params).await
    }

    #[tool(
        name = "get_active_window",
        description = "Get detailed real-time state of the current 5m or 15m prediction market window for a specific asset. Shows both UP and DOWN sides with spreads, depth, tradeability, and fill simulation."
    )]
    async fn get_active_window(
        &self,
        Parameters(params): Parameters<tools_market::GetActiveWindowParams>,
    ) -> String {
        tools_market::handle_get_active_window(&self.state, params).await
    }

    #[tool(
        name = "get_window_briefing",
        description = "Market briefing for a 5m/15m window. Shows time remaining, spot price move since window open, both UP and DOWN sides with spreads/depth/tradeability, and fee context. Present the data AS-IS to the user — do NOT add trade recommendations, entry opinions, strategy commentary, or verdicts like 'too expensive' or 'priced in'. The user decides."
    )]
    async fn get_window_briefing(
        &self,
        Parameters(params): Parameters<tools_market::GetWindowBriefingParams>,
    ) -> String {
        tools_market::handle_get_window_briefing(&self.state, &self.binance, &self.polymarket, params).await
    }

    #[tool(
        name = "get_spot_price",
        description = "Get current spot prices from Binance for BTC, ETH, SOL, XRP, DOGE, BNB. Shows data freshness."
    )]
    async fn get_spot_price(
        &self,
        Parameters(params): Parameters<tools_market::GetSpotPriceParams>,
    ) -> String {
        tools_market::handle_get_spot_price(&self.state, params).await
    }

    // ── Order book tools ──

    #[tool(
        name = "get_order_book",
        description = "Deep dive into a specific market's order book. Accepts a token ID or natural language description (e.g. 'BTC 5m UP'). Shows depth chart, spread, tradeability, and optional fill simulation."
    )]
    async fn get_order_book(
        &self,
        Parameters(params): Parameters<tools_book::GetOrderBookParams>,
    ) -> String {
        tools_book::handle_get_order_book(&self.state, &self.polymarket, params).await
    }

    #[tool(
        name = "simulate_trade",
        description = "Calculate fee-adjusted P&L for a hypothetical Polymarket trade. Shows win/loss scenarios, breakeven win rate, and EV table at various win rates. Pure arithmetic — no API calls."
    )]
    async fn simulate_trade(
        &self,
        Parameters(params): Parameters<tools_book::SimulateTradeParams>,
    ) -> String {
        tools_book::handle_simulate_trade(params).await
    }

    // ── Paper trading tools ──

    #[tool(
        name = "paper_trade",
        description = "Open a paper trade position at current market prices. Entry at ask (taker), fees calculated, persisted to disk. Position settles lazily when you check the portfolio."
    )]
    async fn paper_trade(
        &self,
        Parameters(params): Parameters<tools_paper::PaperTradeParams>,
    ) -> String {
        tools_paper::handle_paper_trade(&self.state, &self.portfolio, params)
            .await
    }

    #[tool(
        name = "get_paper_portfolio",
        description = "View paper trading positions, P&L, and statistics. Automatically attempts to settle expired positions via Gamma API. Shows win rate by asset and entry price bucket."
    )]
    async fn get_paper_portfolio(
        &self,
        Parameters(params): Parameters<tools_paper::GetPaperPortfolioParams>,
    ) -> String {
        tools_paper::handle_get_paper_portfolio(&self.portfolio, &self.http_client, params).await
    }

    #[tool(
        name = "paper_close",
        description = "Close an open paper position early at the current bid price (or a manual price). Taker fee applies on exit. P&L = (exit - entry) * contracts - entry_fee - exit_fee. Use get_paper_portfolio to see open positions and their IDs."
    )]
    async fn paper_close(
        &self,
        Parameters(params): Parameters<tools_paper::PaperCloseParams>,
    ) -> String {
        tools_paper::handle_paper_close(&self.state, &self.portfolio, params).await
    }

    // ── Monitor tools ──

    #[tool(
        name = "get_alerts",
        description = "Get recent market alerts detected by the background monitor: spread narrowing, depth spikes, whale trades, approaching windows. Configurable lookback and minimum whale size."
    )]
    async fn get_alerts(
        &self,
        Parameters(params): Parameters<tools_monitor::GetAlertsParams>,
    ) -> String {
        tools_monitor::handle_get_alerts(&self.state, params).await
    }

    // ── Data logging tools ──

    #[tool(
        name = "set_data_logging",
        description = "Enable or disable background data logging. When enabled, the monitor writes spot prices, order book snapshots, and market discovery to JSONL files (~55 MB/day summary, ~1 GB/day with full books). Data is rotated daily by UTC date."
    )]
    async fn set_data_logging(
        &self,
        Parameters(params): Parameters<tools_monitor::SetDataLoggingParams>,
    ) -> String {
        tools_monitor::handle_set_data_logging(&self.logger, params)
    }

    #[tool(
        name = "get_data_logging_status",
        description = "Check whether data logging is active, where files are stored, and current disk usage."
    )]
    async fn get_data_logging_status(
        &self,
        Parameters(_params): Parameters<tools_monitor::GetDataLoggingStatusParams>,
    ) -> String {
        tools_monitor::handle_get_data_logging_status(&self.logger)
    }

    // ── Auto-trade tools ──

    #[tool(
        name = "set_auto_paper_trade",
        description = "Enable or disable autonomous paper trading on 5m/15m markets. The background monitor executes paper trades when conditions are met. Required: position_size_usd ($ per trade). Optional with defaults: entry_pct (default 80, when to enter as % elapsed), min_move_pct (default 0.03%), max_entry_price (default 0.80). Optional filters: max_spread_pct, min_depth_usd, side (auto/up/down), max_total_exposure_usd. Modes: paper (default), dry_run."
    )]
    async fn set_auto_trade(
        &self,
        Parameters(params): Parameters<tools_auto::SetAutoTradeParams>,
    ) -> String {
        tools_auto::handle_set_auto_trade(&self.auto_state, params).await
    }

    #[tool(
        name = "get_auto_paper_trade_status",
        description = "Check active auto paper trade configs, recent autonomous trades, and cumulative stats. Shows what's running, what was executed, and performance summary."
    )]
    async fn get_auto_trade_status(
        &self,
        Parameters(params): Parameters<tools_auto::GetAutoTradeStatusParams>,
    ) -> String {
        tools_auto::handle_get_auto_trade_status(&self.auto_state, params).await
    }

    // ── Whale activity tools ──

    #[tool(
        name = "get_whale_activity",
        description = "View recent whale trades (≥$10k default) on Polymarket daily/monthly crypto markets. Shows genuine large directional bets with buy/sell flow summary. Filters out 5m/15m markets (too small) and settlement artifacts (price ≥ 0.95). Filter by asset and time range."
    )]
    async fn get_whale_activity(
        &self,
        Parameters(params): Parameters<tools_whale::GetWhaleActivityParams>,
    ) -> String {
        tools_whale::handle_get_whale_activity(&self.state, params).await
    }

    // ── Analysis tools ──

    #[tool(
        name = "get_market_analysis",
        description = "Comprehensive market analysis for a crypto asset. Combines spot momentum (1m/5m/15m moves, acceleration, volatility), order book dynamics (spread trends, depth asymmetry), spot-vs-implied probability divergence, whale flow summary (30m), and recent alerts (10m). Present the data AS-IS — do NOT add trade recommendations."
    )]
    async fn get_market_analysis(
        &self,
        Parameters(params): Parameters<tools_analysis::GetMarketAnalysisParams>,
    ) -> String {
        tools_analysis::handle_get_market_analysis(
            &self.state,
            &self.binance,
            &self.polymarket,
            &self.fair_value_cache,
            params,
        )
        .await
    }

    // ── Live trading tools ──
    // These return "not configured" if [trading] section is missing from config.toml.

    #[tool(
        name = "approve_exchange",
        description = "One-time setup: approve Polymarket exchange contracts to spend your USDC and manage CTF tokens. Required before any live trading. Sends on-chain transactions (costs gas in POL/MATIC). Use check_only=true to see current approval status without sending transactions. Requires [trading] config."
    )]
    async fn approve_exchange(
        &self,
        Parameters(params): Parameters<tools_trading::ApproveExchangeParams>,
    ) -> String {
        if let Some(exec) = &self.live_executor {
            tools_trading::handlers::handle_approve_exchange(exec, params).await
        } else {
            NOT_CONFIGURED_MSG.to_string()
        }
    }

    #[tool(
        name = "get_balance",
        description = "Get wallet USDC balance and risk manager status for live Polymarket trading. Shows address, balance, and current risk limits (daily P&L, consecutive losses, drawdown). Requires [trading] config."
    )]
    async fn get_balance(
        &self,
        Parameters(params): Parameters<tools_trading::GetBalanceParams>,
    ) -> String {
        match (&self.live_executor, &self.risk_manager) {
            (Some(exec), Some(rm)) => {
                tools_trading::handlers::handle_get_balance(exec, rm, params).await
            }
            _ => NOT_CONFIGURED_MSG.to_string(),
        }
    }

    #[tool(
        name = "place_order",
        description = "Place a buy or sell order on Polymarket. SAFETY: dry_run=true by default (simulates only). To execute a real order, set BOTH dry_run=false AND confirm=true. Risk manager checks are enforced before execution. Accepts natural language market descriptions (e.g. 'BTC 5m UP') or token IDs. Requires [trading] config."
    )]
    async fn place_order(
        &self,
        Parameters(params): Parameters<tools_trading::PlaceOrderParams>,
    ) -> String {
        match (&self.live_executor, &self.risk_manager) {
            (Some(exec), Some(rm)) => {
                tools_trading::handlers::handle_place_order(
                    &self.state, exec, rm, params,
                )
                .await
            }
            _ => NOT_CONFIGURED_MSG.to_string(),
        }
    }

    #[tool(
        name = "cancel_order",
        description = "Cancel a specific open order by order ID, or cancel all open orders with order_id='all'. Requires [trading] config."
    )]
    async fn cancel_order(
        &self,
        Parameters(params): Parameters<tools_trading::CancelOrderParams>,
    ) -> String {
        if let Some(exec) = &self.live_executor {
            tools_trading::handlers::handle_cancel_order(exec, params).await
        } else {
            NOT_CONFIGURED_MSG.to_string()
        }
    }

    #[tool(
        name = "get_positions",
        description = "View live trading wallet status: USDC balance, wallet address, risk manager state. For detailed position tracking, check polymarket.com. Requires [trading] config."
    )]
    async fn get_positions(
        &self,
        Parameters(params): Parameters<tools_trading::GetPositionsParams>,
    ) -> String {
        match (&self.live_executor, &self.risk_manager) {
            (Some(exec), Some(rm)) => {
                tools_trading::handlers::handle_get_positions(exec, rm, params).await
            }
            _ => NOT_CONFIGURED_MSG.to_string(),
        }
    }

    #[tool(
        name = "redeem_winnings",
        description = "Redeem winning positions after market settlement. Converts CTF ERC-1155 tokens back to USDC on-chain. Requires EOA wallet type. For NegRisk markets (5m/15m), set neg_risk=true (default). Provide the condition_id of the settled market. Requires [trading] config."
    )]
    async fn redeem_winnings(
        &self,
        Parameters(params): Parameters<tools_trading::RedeemWinningsParams>,
    ) -> String {
        if let Some(exec) = &self.live_executor {
            tools_trading::handlers::handle_redeem_winnings(exec, params).await
        } else {
            NOT_CONFIGURED_MSG.to_string()
        }
    }

}

impl CryptoMcpServer {
    pub fn new(
        state: SharedState,
        portfolio: Arc<Mutex<Portfolio>>,
        auto_state: SharedAutoTradeState,
        logger: DataLogger,
        live_executor: Option<Arc<LiveExecutor>>,
        risk_manager: Option<Arc<Mutex<RiskManager>>>,
    ) -> Self {
        Self {
            tool_router: Self::tool_router(),
            state,
            polymarket: PolymarketClient::new(None, None),
            http_client: reqwest::Client::new(),
            binance: BinanceClient::new(),
            portfolio,
            auto_state,
            logger,
            fair_value_cache: Arc::new(FairValueCache::new(std::time::Duration::from_secs(300))),
            live_executor,
            risk_manager,
        }
    }
}

#[tool_handler]
impl ServerHandler for CryptoMcpServer {
    fn get_info(&self) -> ServerInfo {
        let capabilities = ServerCapabilities::builder()
            .enable_tools()
            .build();

        let instructions = String::from(
            "Crypto prediction market intelligence from Polymarket, enriched with Binance spot prices. \
             Provides market discovery, order book analysis, fee-adjusted P&L simulation, paper trading \
             (with early exit via paper_close), and real-time alerts. Use search_crypto_markets to find \
             markets, get_window_briefing for market data, paper_trade to open positions, and \
             paper_close to exit early at current bid. IMPORTANT: Present tool data AS-IS without \
             adding trade recommendations or strategy opinions. set_auto_paper_trade only requires \
             position_size_usd from the user — other strategy params have sensible defaults. \
             get_whale_activity shows recent large trades on daily/monthly markets from the live \
             Polymarket trade stream. get_market_analysis provides comprehensive quantitative analysis \
             combining momentum, book dynamics, divergence, whale flow, and alerts. \
             Live trading is available via get_balance, place_order, cancel_order, \
             get_positions, and redeem_winnings. place_order defaults to dry_run=true — \
             set dry_run=false AND confirm=true for real execution.",
        );

        ServerInfo::new(capabilities).with_instructions(&instructions)
    }
}
