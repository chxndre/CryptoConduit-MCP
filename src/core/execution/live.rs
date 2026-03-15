// LiveExecutor — SDK-backed order placement and redemption for live trading.
//
// Handles authentication, order placement, polling, cancellation, balance queries,
// and native on-chain CTF token redemption (EOA wallets only).
// Supports dry-run mode (construct orders but don't submit).
//
// Extracted from market-scout, adapted for config.toml-based configuration.

use anyhow::{bail, Context, Result};
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{LocalSigner, Normal, Signer};
use polymarket_client_sdk::clob::client::{Client, Config};
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::{AssetType, OrderStatusType, Side, SignatureType};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use alloy::primitives::U256;
use alloy::providers::ProviderBuilder;
use alloy::sol;
use polymarket_client_sdk::types::address;
use polymarket_client_sdk::{contract_config, POLYGON};

// Solidity interfaces for on-chain approvals
sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 value) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IERC1155 {
        function setApprovalForAll(address operator, bool approved) external;
        function isApprovedForAll(address account, address operator) external view returns (bool);
    }
}

/// USDC (bridged) on Polygon — used by Polymarket
const USDC_POLYGON: alloy::primitives::Address = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

/// Result of checking approvals for a single contract.
#[derive(Debug, Clone)]
pub struct ApprovalStatus {
    pub contract_name: String,
    pub address: String,
    pub usdc_approved: bool,
    pub usdc_allowance: String,
    pub ctf_approved: bool,
}

/// Result of the full approval check/set operation.
#[derive(Debug, Clone)]
pub struct ApprovalResult {
    pub statuses: Vec<ApprovalStatus>,
    pub tx_hashes: Vec<String>,
    pub all_approved: bool,
}

use super::config::TradingConfig;

/// Concrete signer type returned by LocalSigner::from_str().
type PrivateKeySigner = LocalSigner<k256::ecdsa::SigningKey>;

const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// Result of an order placement attempt.
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub order_id: String,
    pub filled: bool,
    pub avg_price: f64,
    pub filled_size: f64,
    pub status: String,
}

/// Parse wallet_type string into SDK SignatureType.
fn parse_signature_type(wallet_type: &str) -> SignatureType {
    match wallet_type.to_uppercase().as_str() {
        "EOA" => SignatureType::Eoa,
        "PROXY" => SignatureType::Proxy,
        _ => SignatureType::GnosisSafe,
    }
}

/// Live executor backed by the Polymarket SDK.
pub struct LiveExecutor {
    client: Client<Authenticated<Normal>>,
    signer: PrivateKeySigner,
    signature_type: SignatureType,
    order_timeout_secs: u64,
    max_order_size_usd: f64,
    polygon_rpc_url: String,
}

impl LiveExecutor {
    /// Create and authenticate a new LiveExecutor from config.
    ///
    /// API credentials are auto-derived from the private key via the SDK.
    /// For GnosisSafe, the funder address is auto-derived via CREATE2.
    pub async fn new(config: &TradingConfig) -> Result<Self> {
        let pk = config.private_key();
        let signature_type = parse_signature_type(&config.wallet_type);

        let signer = LocalSigner::from_str(pk)
            .map_err(|e| anyhow::anyhow!("Invalid private key: {}", e))?
            .with_chain_id(Some(137)); // Polygon mainnet

        let sdk_config = Config::builder().build();
        let unauth_client = Client::new(CLOB_BASE_URL, sdk_config)
            .map_err(|e| anyhow::anyhow!("Failed to create CLOB client: {}", e))?;

        let normal_client = unauth_client
            .authentication_builder(&signer)
            .signature_type(signature_type)
            .authenticate()
            .await
            .map_err(|e| anyhow::anyhow!("Authentication failed: {}", e))?;

        info!(
            address = %signer.address(),
            signature_type = ?signature_type,
            "LiveExecutor authenticated"
        );

        Ok(Self {
            client: normal_client,
            signer,
            signature_type,
            order_timeout_secs: config.order_timeout_secs,
            max_order_size_usd: config.max_order_size_usd,
            polygon_rpc_url: config.polygon_rpc_url.clone(),
        })
    }

    /// Stop SDK heartbeats.
    pub async fn stop_heartbeats(&mut self) -> Result<()> {
        self.client
            .stop_heartbeats()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to stop heartbeats: {}", e))?;
        Ok(())
    }

    /// Refresh the CLOB's cached view of on-chain balance, then query it.
    /// The refresh is non-fatal — if it fails we still return the cached balance.
    pub async fn get_balance(&self) -> Result<f64> {
        // Ask the CLOB to refresh its view of on-chain state
        let refresh_req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        if let Err(e) = self.client.update_balance_allowance(refresh_req).await {
            debug!(error = %e, "Balance refresh failed (non-fatal, using cached value)");
        }

        let query_req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Collateral)
            .build();
        let response = self.client.balance_allowance(query_req).await
            .map_err(|e| anyhow::anyhow!("Balance query failed: {}", e))?;
        let balance = response
            .balance
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0)
            / 1_000_000.0;
        Ok(balance)
    }

    /// Get the wallet address.
    pub fn address(&self) -> String {
        format!("{}", self.signer.address())
    }

    /// Pre-warm SDK's internal DashMap cache for a token's metadata.
    pub async fn preload_token_metadata(&self, token_id: &str) -> Result<()> {
        let token_u256 = token_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid token_id: {}", e))?;
        let _ = self.client.fee_rate_bps(token_u256).await;
        let _ = self.client.tick_size(token_u256).await;
        let _ = self.client.neg_risk(token_u256).await;
        Ok(())
    }

    /// Place an entry (buy) limit order.
    ///
    /// `dry_run` parameter controls execution: if true, builds+signs but doesn't submit.
    /// Callers (MCP handler, auto-trade) decide the effective dry_run value.
    pub async fn place_entry_order(
        &self,
        token_id: &str,
        price: f64,
        size: f64,
        timeout_secs: Option<u64>,
        dry_run: bool,
    ) -> Result<OrderResult> {
        let timeout = timeout_secs.unwrap_or(self.order_timeout_secs);

        // Validate order size
        let order_usd = price * size;
        if order_usd > self.max_order_size_usd {
            bail!(
                "Order size ${:.2} exceeds max ${:.2}",
                order_usd,
                self.max_order_size_usd
            );
        }

        let price_truncated = (price * 100.0).floor() / 100.0;
        let size_truncated = (size * 100.0).floor() / 100.0;
        let price_dec = Decimal::from_f64(price_truncated).context("Invalid price")?;
        let size_dec = Decimal::from_f64(size_truncated).context("Invalid size")?;
        let token_u256 = token_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid token_id: {}", e))?;

        let build_start = Instant::now();
        let signable = self.client
            .limit_order()
            .token_id(token_u256)
            .side(Side::Buy)
            .price(price_dec)
            .size(size_dec)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Order build failed: {}", e))?;
        let build_ms = build_start.elapsed().as_millis();
        debug!(build_ms = build_ms, "Entry order built");

        if dry_run {
            info!(
                token_id = %token_id,
                price = %price_truncated,
                size = %size_truncated,
                "DRY RUN: Would place BUY order (not submitted)"
            );
            return Ok(OrderResult {
                order_id: "dry-run".to_string(),
                filled: true,
                avg_price: price_truncated,
                filled_size: size_truncated,
                status: "DRY_RUN".to_string(),
            });
        }

        let order_start = Instant::now();
        let signed = self.client.sign(&self.signer, signable).await
            .map_err(|e| anyhow::anyhow!("Order signing failed: {}", e))?;
        let response = self.client.post_order(signed).await
            .map_err(|e| anyhow::anyhow!("Order submission failed: {}", e))?;
        let submit_ms = order_start.elapsed().as_millis();

        if !response.success {
            let msg = response.error_msg.unwrap_or_default();
            bail!("Order rejected by exchange: {}", msg);
        }

        let order_id = response.order_id.clone();
        info!(
            order_id = %order_id,
            status = ?response.status,
            price = %price_truncated,
            size = %size_truncated,
            sign_submit_ms = submit_ms,
            "Entry order submitted"
        );

        if response.status == OrderStatusType::Matched {
            return Ok(OrderResult {
                order_id,
                filled: true,
                avg_price: price_truncated,
                filled_size: size_truncated,
                status: "Matched".to_string(),
            });
        }

        self.poll_order_fill(&order_id, timeout).await
    }

    /// Place an exit (sell) limit order at a given price.
    ///
    /// `dry_run` parameter controls execution: if true, skips submission.
    pub async fn place_exit_order(
        &self,
        token_id: &str,
        size: f64,
        price: f64,
        timeout_secs: Option<u64>,
        dry_run: bool,
    ) -> Result<OrderResult> {
        // Validate inputs
        if size <= 0.0 || !size.is_finite() {
            bail!("Invalid exit size: {}", size);
        }
        if !price.is_finite() {
            bail!("Invalid exit price: {}", price);
        }
        let order_usd = price * size;
        if order_usd > self.max_order_size_usd {
            bail!(
                "Exit order size ${:.2} exceeds max ${:.2}",
                order_usd,
                self.max_order_size_usd
            );
        }

        let timeout = timeout_secs.unwrap_or(self.order_timeout_secs);
        let token_u256 = token_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid token_id: {}", e))?;

        if dry_run {
            info!(
                token_id = %token_id,
                price = %price,
                size = %size,
                "DRY RUN: Would place SELL order (not submitted)"
            );
            return Ok(OrderResult {
                order_id: "dry-run".to_string(),
                filled: true,
                avg_price: price,
                filled_size: size,
                status: "DRY_RUN".to_string(),
            });
        }

        let clamped_price = price.max(0.01);
        let price_truncated = (clamped_price * 100.0).ceil() / 100.0;
        let size_truncated = (size * 100.0).floor() / 100.0;
        let price_dec = Decimal::from_f64(price_truncated).context("Invalid price")?;
        let size_dec = Decimal::from_f64(size_truncated).context("Invalid size")?;

        let signable = self.client
            .limit_order()
            .token_id(token_u256)
            .side(Side::Sell)
            .price(price_dec)
            .size(size_dec)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Exit order build failed: {}", e))?;

        let order_start = Instant::now();
        let signed = self.client.sign(&self.signer, signable).await
            .map_err(|e| anyhow::anyhow!("Exit order signing failed: {}", e))?;
        let response = self.client.post_order(signed).await
            .map_err(|e| anyhow::anyhow!("Exit order submission failed: {}", e))?;
        let submit_ms = order_start.elapsed().as_millis();

        if !response.success {
            let msg = response.error_msg.unwrap_or_default();
            bail!("Exit order rejected: {}", msg);
        }

        let order_id = response.order_id.clone();
        info!(order_id = %order_id, price = %clamped_price, sign_submit_ms = submit_ms, "Exit order submitted");

        if response.status == OrderStatusType::Matched {
            return Ok(OrderResult {
                order_id,
                filled: true,
                avg_price: clamped_price,
                filled_size: size_truncated,
                status: "Matched".to_string(),
            });
        }

        let result = self.poll_order_fill(&order_id, timeout).await?;
        if result.filled {
            return Ok(result);
        }

        // Not filled — cancel so caller can retry with fresh price
        if let Err(e) = self.cancel_order(&order_id).await {
            warn!(error = %e, "Failed to cancel unfilled exit order");
        }
        Ok(result)
    }

    /// Cancel a specific order.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        self.client.cancel_order(order_id).await
            .map_err(|e| anyhow::anyhow!("Cancel failed: {}", e))?;
        debug!(order_id = %order_id, "Order cancelled");
        Ok(())
    }

    /// Cancel all open orders.
    pub async fn cancel_all(&self) -> Result<()> {
        self.client.cancel_all_orders().await
            .map_err(|e| anyhow::anyhow!("Cancel all failed: {}", e))?;
        info!("All open orders cancelled");
        Ok(())
    }

    /// Redeem winning positions after market settlement.
    /// Uses native on-chain CTF redemption (EOA wallets only).
    /// For NegRisk markets (5m/15m), uses redeem_neg_risk.
    /// Non-fatal: returns error instead of panicking.
    pub async fn redeem(&self, condition_id: &str, is_neg_risk: bool) -> Result<String> {
        if self.signature_type != SignatureType::Eoa {
            bail!(
                "On-chain redemption requires EOA wallet. Current wallet type: {:?}. \
                 Redeem manually at polymarket.com or migrate to EOA.",
                self.signature_type
            );
        }

        info!(condition_id = %condition_id, neg_risk = is_neg_risk, "Redeeming positions on-chain...");

        use alloy::providers::ProviderBuilder;
        use polymarket_client_sdk::ctf;
        use polymarket_client_sdk::types::address;

        // USDC on Polygon
        let usdc = address!("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

        // Build a wallet-backed provider (required for write transactions)
        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect(&self.polygon_rpc_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Polygon RPC ({}): {}", self.polygon_rpc_url, e))?;

        let condition_bytes: [u8; 32] = hex_to_bytes32(condition_id)?;
        let condition_b256 = alloy::primitives::B256::from(condition_bytes);

        if is_neg_risk {
            // NegRisk markets need the NegRisk adapter
            let ctf_client = ctf::Client::with_neg_risk(provider, 137)
                .map_err(|e| anyhow::anyhow!("Failed to create CTF client: {}", e))?;

            let request = ctf::types::RedeemNegRiskRequest::builder()
                .condition_id(condition_b256)
                .amounts(vec![])  // Empty = redeem all
                .build();
            let response = ctf_client
                .redeem_neg_risk(&request)
                .await
                .map_err(|e| anyhow::anyhow!("NegRisk redemption failed: {}", e))?;
            let tx_hash = format!("{:?}", response.transaction_hash);
            info!(tx_hash = %tx_hash, "NegRisk redemption successful");
            Ok(tx_hash)
        } else {
            // Standard markets use the convenience method (includes collateral_token + binary index sets)
            let ctf_client = ctf::Client::new(provider, 137)
                .map_err(|e| anyhow::anyhow!("Failed to create CTF client: {}", e))?;

            let request = ctf::types::RedeemPositionsRequest::for_binary_market(
                usdc,
                condition_b256,
            );
            let response = ctf_client
                .redeem_positions(&request)
                .await
                .map_err(|e| anyhow::anyhow!("Redemption failed: {}", e))?;
            let tx_hash = format!("{:?}", response.transaction_hash);
            info!(tx_hash = %tx_hash, "Redemption successful");
            Ok(tx_hash)
        }
    }

    /// Check current approval status for all Polymarket exchange contracts.
    /// Read-only — no gas required.
    pub async fn check_approvals(&self) -> Result<ApprovalResult> {
        let provider = ProviderBuilder::new()
            .connect(&self.polygon_rpc_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Polygon RPC: {}", e))?;

        let config = contract_config(POLYGON, false)
            .ok_or_else(|| anyhow::anyhow!("No contract config for Polygon"))?;
        let neg_risk_config = contract_config(POLYGON, true)
            .ok_or_else(|| anyhow::anyhow!("No neg-risk contract config for Polygon"))?;

        let mut targets: Vec<(&str, alloy::primitives::Address)> = vec![
            ("CTF Exchange", config.exchange),
            ("Neg Risk CTF Exchange", neg_risk_config.exchange),
        ];
        if let Some(adapter) = neg_risk_config.neg_risk_adapter {
            targets.push(("Neg Risk Adapter", adapter));
        }

        let owner = self.signer.address();
        let usdc = IERC20::new(USDC_POLYGON, provider.clone());
        let ctf = IERC1155::new(config.conditional_tokens, provider.clone());

        let mut statuses = Vec::new();
        let mut all_approved = true;

        for (name, target) in &targets {
            let usdc_allowance = usdc.allowance(owner, *target).call().await
                .map_err(|e| anyhow::anyhow!("Failed to check USDC allowance for {}: {}", name, e))?;
            let ctf_approved = ctf.isApprovedForAll(owner, *target).call().await
                .map_err(|e| anyhow::anyhow!("Failed to check CTF approval for {}: {}", name, e))?;

            let usdc_ok = usdc_allowance > U256::ZERO;
            if !usdc_ok || !ctf_approved {
                all_approved = false;
            }

            statuses.push(ApprovalStatus {
                contract_name: name.to_string(),
                address: format!("{}", target),
                usdc_approved: usdc_ok,
                usdc_allowance: format_allowance(usdc_allowance),
                ctf_approved,
            });
        }

        Ok(ApprovalResult {
            statuses,
            tx_hashes: vec![],
            all_approved,
        })
    }

    /// Set all required approvals for Polymarket exchange contracts.
    /// Sends on-chain transactions — costs gas (POL/MATIC).
    /// Only approves contracts that aren't already approved.
    /// NOTE: Ignores dry_run — approvals are one-time wallet setup, not trades.
    pub async fn approve_exchange(&self) -> Result<ApprovalResult> {

        let provider = ProviderBuilder::new()
            .wallet(self.signer.clone())
            .connect(&self.polygon_rpc_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Polygon RPC: {}", e))?;

        let config = contract_config(POLYGON, false)
            .ok_or_else(|| anyhow::anyhow!("No contract config for Polygon"))?;
        let neg_risk_config = contract_config(POLYGON, true)
            .ok_or_else(|| anyhow::anyhow!("No neg-risk contract config for Polygon"))?;

        let mut targets: Vec<(&str, alloy::primitives::Address)> = vec![
            ("CTF Exchange", config.exchange),
            ("Neg Risk CTF Exchange", neg_risk_config.exchange),
        ];
        if let Some(adapter) = neg_risk_config.neg_risk_adapter {
            targets.push(("Neg Risk Adapter", adapter));
        }

        let owner = self.signer.address();
        let usdc = IERC20::new(USDC_POLYGON, provider.clone());
        let ctf = IERC1155::new(config.conditional_tokens, provider.clone());
        let mut tx_hashes = Vec::new();
        let mut statuses = Vec::new();

        for (name, target) in &targets {
            info!(contract = name, address = %target, "Checking and approving...");

            // Check current USDC allowance
            let current_allowance = usdc.allowance(owner, *target).call().await
                .unwrap_or(U256::ZERO);
            let usdc_already_ok = current_allowance > U256::ZERO;

            // Check current CTF approval
            let ctf_already_ok = ctf.isApprovedForAll(owner, *target).call().await
                .unwrap_or(false);

            // Approve USDC if needed
            if !usdc_already_ok {
                info!(contract = name, "Setting USDC approval (unlimited)...");
                let tx_hash = usdc.approve(*target, U256::MAX)
                    .send().await
                    .map_err(|e| anyhow::anyhow!("USDC approve tx failed for {}: {}", name, e))?
                    .watch().await
                    .map_err(|e| anyhow::anyhow!("USDC approve confirmation failed for {}: {}", name, e))?;
                let hash_str = format!("{:?}", tx_hash);
                info!(contract = name, tx = %hash_str, "USDC approved");
                tx_hashes.push(hash_str);
            } else {
                info!(contract = name, "USDC already approved");
            }

            // Approve CTF if needed
            if !ctf_already_ok {
                info!(contract = name, "Setting CTF setApprovalForAll...");
                let tx_hash = ctf.setApprovalForAll(*target, true)
                    .send().await
                    .map_err(|e| anyhow::anyhow!("CTF approve tx failed for {}: {}", name, e))?
                    .watch().await
                    .map_err(|e| anyhow::anyhow!("CTF approve confirmation failed for {}: {}", name, e))?;
                let hash_str = format!("{:?}", tx_hash);
                info!(contract = name, tx = %hash_str, "CTF approved");
                tx_hashes.push(hash_str);
            } else {
                info!(contract = name, "CTF already approved");
            }

            statuses.push(ApprovalStatus {
                contract_name: name.to_string(),
                address: format!("{}", target),
                usdc_approved: true,
                usdc_allowance: "MAX (unlimited)".to_string(),
                ctf_approved: true,
            });
        }

        if tx_hashes.is_empty() {
            info!("All contracts already approved — no transactions needed");
        } else {
            info!(count = tx_hashes.len(), "Approval transactions confirmed");
        }

        Ok(ApprovalResult {
            statuses,
            tx_hashes,
            all_approved: true,
        })
    }

    /// Get max order size.
    pub fn max_order_size_usd(&self) -> f64 {
        self.max_order_size_usd
    }

    /// Smoke test: place a tiny limit order far from market and cancel it.
    pub async fn smoke_test(&self, token_id: &str) -> Result<()> {
        info!(
            "Running order smoke test on {}...",
            &token_id[..8.min(token_id.len())]
        );

        let token_u256 = token_id
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid token_id: {}", e))?;
        let price_dec = Decimal::from_f64(0.01).context("Invalid price")?;
        let size_dec = Decimal::from_f64(5.0).context("Invalid size")?;

        let signable = self.client
            .limit_order()
            .token_id(token_u256)
            .side(Side::Buy)
            .price(price_dec)
            .size(size_dec)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Smoke test: order build failed: {}", e))?;

        let signed = self.client.sign(&self.signer, signable).await
            .map_err(|e| anyhow::anyhow!("Smoke test: signing failed: {}", e))?;

        let response = self.client.post_order(signed).await
            .map_err(|e| anyhow::anyhow!("Smoke test: submission failed: {}", e))?;

        if !response.success {
            let msg = response.error_msg.unwrap_or_default();
            bail!("Smoke test: order rejected by exchange: {}", msg);
        }

        let order_id = response.order_id;
        info!("  Smoke test order placed: {} — cancelling...", order_id);

        self.client.cancel_order(&order_id).await
            .map_err(|e| anyhow::anyhow!("Smoke test: cancel failed: {}", e))?;

        info!("  Smoke test passed: build -> sign -> submit -> cancel");
        Ok(())
    }

    /// Poll an order until it's filled, cancelled, or timeout.
    async fn poll_order_fill(&self, order_id: &str, timeout_secs: u64) -> Result<OrderResult> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
        let poll_interval = Duration::from_millis(200);
        let mut first_check = true;

        loop {
            if tokio::time::Instant::now() >= deadline {
                info!(order_id = %order_id, "Order poll timeout — cancelling");
                if let Err(e) = self.cancel_order(order_id).await {
                    warn!(error = %e, "Failed to cancel timed-out order");
                }
                return Ok(OrderResult {
                    order_id: order_id.to_string(),
                    filled: false,
                    avg_price: 0.0,
                    filled_size: 0.0,
                    status: "Timeout".to_string(),
                });
            }

            if first_check {
                first_check = false;
            } else {
                sleep(poll_interval).await;
            }

            match self.client.order(order_id).await {
                Ok(order) => {
                    let size_matched: f64 =
                        order.size_matched.to_string().parse().unwrap_or(0.0);
                    let original_size: f64 =
                        order.original_size.to_string().parse().unwrap_or(0.0);
                    let price: f64 = order.price.to_string().parse().unwrap_or(0.0);

                    match order.status {
                        OrderStatusType::Matched => {
                            info!(
                                order_id = %order_id,
                                matched = %size_matched,
                                "Order fully matched"
                            );
                            return Ok(OrderResult {
                                order_id: order_id.to_string(),
                                filled: true,
                                avg_price: price,
                                filled_size: size_matched,
                                status: "Matched".to_string(),
                            });
                        }
                        OrderStatusType::Canceled => {
                            warn!(order_id = %order_id, "Order was cancelled externally");
                            return Ok(OrderResult {
                                order_id: order_id.to_string(),
                                filled: size_matched > 0.0,
                                avg_price: price,
                                filled_size: size_matched,
                                status: "Canceled".to_string(),
                            });
                        }
                        OrderStatusType::Live | OrderStatusType::Delayed => {
                            if size_matched > 0.0 {
                                debug!(
                                    order_id = %order_id,
                                    matched = %size_matched,
                                    total = %original_size,
                                    "Partial fill"
                                );
                            }
                        }
                        _ => {
                            debug!(order_id = %order_id, status = ?order.status, "Polling...");
                        }
                    }
                }
                Err(e) => {
                    warn!(order_id = %order_id, error = %e, "Order poll failed, retrying");
                }
            }
        }
    }
}

/// Format a USDC allowance value for display.
fn format_allowance(allowance: U256) -> String {
    if allowance == U256::MAX {
        "MAX (unlimited)".to_owned()
    } else if allowance == U256::ZERO {
        "0".to_owned()
    } else {
        let usdc_decimals = U256::from(1_000_000u64);
        let whole = allowance / usdc_decimals;
        format!("{whole} USDC")
    }
}

/// Parse a hex string (with optional 0x prefix) into a 32-byte array.
fn hex_to_bytes32(hex: &str) -> Result<[u8; 32]> {
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    if hex.len() != 64 {
        bail!("Expected 64 hex chars for condition_id, got {}", hex.len());
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|e| anyhow::anyhow!("Invalid hex at position {}: {}", i * 2, e))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_to_bytes32_valid() {
        let hex = "ab".repeat(32);
        let result = hex_to_bytes32(&hex).unwrap();
        assert_eq!(result, [0xab; 32]);
    }

    #[test]
    fn test_hex_to_bytes32_with_prefix() {
        let hex = format!("0x{}", "00".repeat(32));
        let result = hex_to_bytes32(&hex).unwrap();
        assert_eq!(result, [0u8; 32]);
    }

    #[test]
    fn test_hex_to_bytes32_invalid_length() {
        assert!(hex_to_bytes32("abcdef").is_err());
    }

    #[test]
    fn test_parse_signature_type() {
        assert_eq!(parse_signature_type("EOA"), SignatureType::Eoa);
        assert_eq!(parse_signature_type("eoa"), SignatureType::Eoa);
        assert_eq!(parse_signature_type("GnosisSafe"), SignatureType::GnosisSafe);
        assert_eq!(parse_signature_type("PROXY"), SignatureType::Proxy);
        assert_eq!(parse_signature_type("anything"), SignatureType::GnosisSafe);
    }
}
