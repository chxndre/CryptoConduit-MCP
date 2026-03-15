// Fair value estimation for crypto prediction markets.
// Tiered approach: empirical binning for 5m/15m, vol-scaled CDF for longer horizons.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

// ──────────────────────────── Types ────────────────────────────

/// Result of a fair value estimation.
#[derive(Debug, Clone)]
pub struct FairValueEstimate {
    /// Estimated fair probability (0.0 to 1.0).
    pub probability: f64,
    /// Method used for the estimate.
    pub method: FairValueMethod,
    /// Confidence level.
    pub confidence: Confidence,
    /// Human-readable detail string.
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FairValueMethod {
    /// Empirical binning from historical klines (5m/15m).
    Empirical,
    /// Vol-scaled normal CDF (hourly/daily/monthly).
    VolCdf,
    /// Empirical bin had too few samples, fell back to vol-CDF.
    Fallback,
}

impl std::fmt::Display for FairValueMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FairValueMethod::Empirical => write!(f, "empirical"),
            FairValueMethod::VolCdf => write!(f, "vol-CDF"),
            FairValueMethod::Fallback => write!(f, "fallback"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Confidence {
    High,
    Medium,
    Low,
}

impl std::fmt::Display for Confidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Confidence::High => write!(f, "high"),
            Confidence::Medium => write!(f, "medium"),
            Confidence::Low => write!(f, "low"),
        }
    }
}

// ──────────────────────────── Normal CDF ────────────────────────────

/// Abramowitz & Stegun approximation of the normal CDF.
/// Accurate to ~1e-7. No external crate needed.
pub fn normal_cdf(x: f64) -> f64 {
    let a1 = 0.254_829_592;
    let a2 = -0.284_496_736;
    let a3 = 1.421_413_741;
    let a4 = -1.453_152_027;
    let a5 = 1.061_405_429;
    let p = 0.327_591_1;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x_abs = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * x_abs);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x_abs * x_abs).exp();

    0.5 * (1.0 + sign * y)
}

// ──────────────────────────── Vol-scaled CDF ────────────────────────────

/// Estimate P(price finishes above strike) using realized volatility.
///
/// - `current_price`: current spot price
/// - `strike_price`: target price level
/// - `minutes_remaining`: time until market settlement
/// - `vol_per_minute`: realized std dev of 1-minute returns (as fraction, not %)
///
/// Returns probability in [0.0, 1.0].
pub fn vol_cdf_probability(
    current_price: f64,
    strike_price: f64,
    minutes_remaining: f64,
    vol_per_minute: f64,
) -> f64 {
    if minutes_remaining <= 0.0 {
        // Already expired — deterministic
        return if current_price >= strike_price {
            1.0
        } else {
            0.0
        };
    }
    if vol_per_minute <= 0.0 || current_price <= 0.0 {
        return 0.5; // No data → uninformed
    }

    let vol_remaining = vol_per_minute * minutes_remaining.sqrt();
    if vol_remaining <= 0.0 || !vol_remaining.is_finite() {
        return if current_price >= strike_price {
            1.0
        } else {
            0.0
        };
    }

    // Log-normal model: distance in log space
    let distance = (strike_price / current_price).ln();
    // Drift adjustment: -0.5 * sigma^2 * t (log-normal drift)
    let drift = -0.5 * vol_remaining * vol_remaining;
    let z = (distance - drift) / vol_remaining;

    // Guard against NaN/Inf from extreme z values
    if !z.is_finite() {
        return if current_price >= strike_price {
            1.0
        } else {
            0.0
        };
    }

    // P(price > strike) = P(Z > z) = 1 - Phi(z)
    // Clamp to [0.01, 0.99] — extreme values are legitimate near expiry
    // but avoid exactly 0/1 to prevent log(0) in downstream callers.
    (1.0 - normal_cdf(z)).clamp(0.01, 0.99)
}

/// Compute realized volatility (std dev of 1-minute returns as fraction)
/// from a slice of 1-minute klines: (open_time_ms, open, close).
pub fn realized_vol_from_klines(klines: &[(i64, f64, f64)]) -> Option<f64> {
    if klines.len() < 3 {
        return None;
    }

    // Use close-to-close returns
    let returns: Vec<f64> = klines
        .windows(2)
        .filter_map(|w| {
            let prev_close = w[0].2;
            let curr_close = w[1].2;
            if prev_close > 0.0 {
                Some((curr_close / prev_close).ln())
            } else {
                None
            }
        })
        .collect();

    if returns.len() < 2 {
        return None;
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    Some(variance.sqrt())
}

/// Build a FairValueEstimate using vol-CDF.
pub fn estimate_vol_cdf(
    current_price: f64,
    strike_price: f64,
    minutes_remaining: f64,
    klines: &[(i64, f64, f64)],
) -> FairValueEstimate {
    let vol = realized_vol_from_klines(klines);

    match vol {
        Some(v) if v > 0.0 => {
            let prob = vol_cdf_probability(current_price, strike_price, minutes_remaining, v);
            let implied_vol_remaining = v * minutes_remaining.sqrt();

            let confidence = if klines.len() >= 120 {
                Confidence::High
            } else if klines.len() >= 30 {
                Confidence::Medium
            } else {
                Confidence::Low
            };

            FairValueEstimate {
                probability: prob,
                method: FairValueMethod::VolCdf,
                confidence,
                detail: format!(
                    "vol-CDF: \u{03c3}={:.5}/min, {:.0}min left, \u{03c3}_rem={:.3}%",
                    v,
                    minutes_remaining,
                    implied_vol_remaining * 100.0
                ),
            }
        }
        _ => FairValueEstimate {
            probability: 0.5,
            method: FairValueMethod::VolCdf,
            confidence: Confidence::Low,
            detail: "insufficient kline data for vol estimate".to_string(),
        },
    }
}

// ──────────────────────────── Empirical Distribution ────────────────────────────

/// A single bin in the empirical distribution.
#[derive(Debug, Clone)]
struct EmpiricalBin {
    up_count: u32,
    total_count: u32,
}

/// Empirical distribution built from historical klines.
#[derive(Debug, Clone)]
pub struct EmpiricalDistribution {
    /// Key: (move_bucket_idx, elapsed_bucket_idx) → bin
    bins: HashMap<(i32, u32), EmpiricalBin>,
    /// Width of each move bucket in percent.
    move_bucket_width: f64,
    /// Number of elapsed buckets (e.g., 5 means 20% each).
    elapsed_buckets: u32,
    /// When this distribution was computed.
    computed_at: Instant,
    /// Total windows sampled.
    pub total_windows: usize,
}

/// Non-uniform elapsed bin boundaries.
/// 0-80%: 10% increments (8 bins), 80-90%: 5% increments (2 bins),
/// 90-95%: 1 bin, 95-100%: 1 bin. Total: 12 bins.
/// Finer resolution near expiry where the probability curve is highly non-linear.
const ELAPSED_BUCKET_COUNT: u32 = 12;

/// Map an elapsed fraction (0.0 to 1.0) to a bucket index (0 to 11).
fn elapsed_to_bucket(frac: f64) -> u32 {
    let frac = frac.clamp(0.0, 0.9999);
    if frac < 0.80 {
        (frac / 0.10) as u32 // buckets 0-7
    } else if frac < 0.90 {
        8 + ((frac - 0.80) / 0.05) as u32 // buckets 8-9
    } else if frac < 0.95 {
        10
    } else {
        11
    }
}

/// Return the center fraction for each elapsed bucket (for sampling during build).
fn bucket_center(bucket: u32) -> f64 {
    match bucket {
        0..=7 => bucket as f64 * 0.10 + 0.05,
        8 => 0.825,
        9 => 0.875,
        10 => 0.925,
        11 => 0.975,
        _ => 0.5,
    }
}

/// Build an empirical distribution by sliding windows across 1-minute klines.
///
/// - `klines`: sorted (open_time_ms, open, close) from Binance
/// - `window_minutes`: 5 or 15
/// - `move_bucket_width`: bin width in percent (e.g., 0.05 for 5m, 0.10 for 15m)
pub fn build_empirical_distribution(
    klines: &[(i64, f64, f64)],
    window_minutes: u32,
    move_bucket_width: f64,
) -> EmpiricalDistribution {
    let elapsed_buckets = ELAPSED_BUCKET_COUNT;
    let mut bins: HashMap<(i32, u32), EmpiricalBin> = HashMap::new();
    let mut total_windows = 0usize;

    let wm = window_minutes as usize;
    if klines.len() < wm + 1 {
        return EmpiricalDistribution {
            bins,
            move_bucket_width,
            elapsed_buckets,
            computed_at: Instant::now(),
            total_windows: 0,
        };
    }

    // Slide window across klines
    for start_idx in 0..klines.len().saturating_sub(wm) {
        let end_idx = start_idx + wm;
        if end_idx >= klines.len() {
            break;
        }

        let window_open = klines[start_idx].2; // close of first candle = open reference
        let window_close = klines[end_idx].2; // close of last candle

        if window_open <= 0.0 {
            continue;
        }

        let did_finish_up = window_close > window_open;
        total_windows += 1;

        // Sample at each elapsed checkpoint within the window
        for elapsed_bucket in 0..elapsed_buckets {
            let frac = bucket_center(elapsed_bucket);
            let sample_idx = start_idx + (frac * wm as f64) as usize;
            if sample_idx >= klines.len() {
                continue;
            }

            let price_at_elapsed = klines[sample_idx].2;
            let move_pct = ((price_at_elapsed - window_open) / window_open) * 100.0;
            let move_bucket = (move_pct / move_bucket_width).round() as i32;

            let bin = bins
                .entry((move_bucket, elapsed_bucket))
                .or_insert(EmpiricalBin {
                    up_count: 0,
                    total_count: 0,
                });
            bin.total_count += 1;
            if did_finish_up {
                bin.up_count += 1;
            }
        }
    }

    EmpiricalDistribution {
        bins,
        move_bucket_width,
        elapsed_buckets,
        computed_at: Instant::now(),
        total_windows,
    }
}

/// Lookup the empirical P(finish up) for a given move and elapsed fraction.
///
/// Returns (probability, sample_count) or None if no matching bin.
pub fn lookup_empirical(
    dist: &EmpiricalDistribution,
    current_move_pct: f64,
    elapsed_fraction: f64,
) -> Option<(f64, u32)> {
    let move_bucket = (current_move_pct / dist.move_bucket_width).round() as i32;
    let elapsed_bucket = elapsed_to_bucket(elapsed_fraction);

    // Try exact bin first
    if let Some(bin) = dist.bins.get(&(move_bucket, elapsed_bucket)) {
        if bin.total_count > 0 {
            return Some((bin.up_count as f64 / bin.total_count as f64, bin.total_count));
        }
    }

    // Try neighboring move buckets (±1) for interpolation
    let mut total_up = 0u32;
    let mut total_count = 0u32;
    for dm in -1..=1 {
        if let Some(bin) = dist.bins.get(&(move_bucket + dm, elapsed_bucket)) {
            total_up += bin.up_count;
            total_count += bin.total_count;
        }
    }
    if total_count > 0 {
        Some((total_up as f64 / total_count as f64, total_count))
    } else {
        None
    }
}

/// Build a FairValueEstimate using empirical distribution, falling back to vol-CDF.
///
/// Near expiry (<0.5 min remaining), always uses vol-CDF — it handles the rapid
/// probability convergence better than any empirical bin can, since it uses exact
/// remaining time in the σ calculation.
pub fn estimate_empirical(
    dist: &EmpiricalDistribution,
    current_move_pct: f64,
    elapsed_fraction: f64,
    // Fallback vol-CDF params (used if empirical bin is too thin or near expiry)
    current_price: f64,
    strike_price: f64,
    minutes_remaining: f64,
    klines: &[(i64, f64, f64)],
    min_samples: u32,
) -> FairValueEstimate {
    // Very near expiry (<5 seconds): use spot direction directly.
    // Vol-CDF breaks down here — σ_rem → 0 causes numerical issues, and the
    // outcome is effectively determined by the current spot position.
    if minutes_remaining < 5.0 / 60.0 {
        let noise_threshold = 0.005; // ±0.005% is microstructure noise
        let prob = if current_move_pct > noise_threshold {
            0.95 // spot up → almost certainly finishes up
        } else if current_move_pct < -noise_threshold {
            0.05 // spot down → almost certainly doesn't finish up
        } else {
            0.50 // effectively flat — coin flip
        };
        return FairValueEstimate {
            probability: prob,
            method: FairValueMethod::VolCdf,
            confidence: Confidence::High,
            detail: format!(
                "<5s left, spot {:.3}%, direction-based",
                current_move_pct
            ),
        };
    }

    // Near expiry (<30 seconds), vol-CDF is strictly better — it uses exact remaining
    // time rather than a bin that averages over a range.
    if minutes_remaining < 0.5 {
        let mut est = estimate_vol_cdf(current_price, strike_price, minutes_remaining, klines);
        est.detail = format!("<30s left, using exact time; {}", est.detail);
        return est;
    }

    if let Some((prob, n)) = lookup_empirical(dist, current_move_pct, elapsed_fraction) {
        if n >= min_samples {
            let confidence = if n >= 100 {
                Confidence::High
            } else if n >= 40 {
                Confidence::Medium
            } else {
                Confidence::Low
            };
            return FairValueEstimate {
                probability: prob,
                method: FairValueMethod::Empirical,
                confidence,
                detail: format!(
                    "n={}, move={:.3}%, {:.1}min left",
                    n,
                    current_move_pct,
                    minutes_remaining,
                ),
            };
        }
    }

    // Fallback to vol-CDF
    let mut est = estimate_vol_cdf(current_price, strike_price, minutes_remaining, klines);
    est.method = FairValueMethod::Fallback;
    est.detail = format!("thin bin (<{} samples), {}", min_samples, est.detail);
    est
}

// ──────────────────────────── Multi-strike consistency ────────────────────────────

/// Check if a set of (strike, probability) pairs is monotonically decreasing.
/// Returns a warning string if inconsistencies are found, None if consistent.
pub fn check_multi_strike_consistency(
    estimates: &[(f64, f64)], // (strike, fair_probability) sorted by strike ascending
) -> Option<String> {
    if estimates.len() < 2 {
        return None;
    }

    let mut violations = Vec::new();
    for w in estimates.windows(2) {
        let (strike_lo, prob_lo) = w[0];
        let (strike_hi, prob_hi) = w[1];
        // Higher strike should have lower P(above)
        if prob_hi > prob_lo + 0.02 {
            // 2% tolerance for noise
            violations.push(format!(
                "${:.0}:{:.3} > ${:.0}:{:.3}",
                strike_hi, prob_hi, strike_lo, prob_lo
            ));
        }
    }

    if violations.is_empty() {
        None
    } else {
        Some(format!("Non-monotonic strikes: {}", violations.join(", ")))
    }
}

// ──────────────────────────── Cache ────────────────────────────

/// Cached empirical distributions, keyed by "{symbol}_{window_minutes}".
pub struct FairValueCache {
    distributions: Arc<RwLock<HashMap<String, CachedDist>>>,
    ttl: Duration,
}

struct CachedDist {
    dist: EmpiricalDistribution,
    vol_per_minute: Option<f64>,
}

impl FairValueCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            distributions: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// Get a cached distribution, or None if stale/missing.
    pub async fn get(&self, key: &str) -> Option<(EmpiricalDistribution, Option<f64>)> {
        let cache = self.distributions.read().await;
        cache.get(key).and_then(|c| {
            if c.dist.computed_at.elapsed() < self.ttl {
                Some((c.dist.clone(), c.vol_per_minute))
            } else {
                None
            }
        })
    }

    /// Store a distribution in the cache.
    pub async fn put(
        &self,
        key: String,
        dist: EmpiricalDistribution,
        vol_per_minute: Option<f64>,
    ) {
        let mut cache = self.distributions.write().await;
        cache.insert(key, CachedDist { dist, vol_per_minute });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Normal CDF tests ──

    #[test]
    fn test_normal_cdf_zero() {
        let v = normal_cdf(0.0);
        assert!((v - 0.5).abs() < 1e-6, "Phi(0) should be 0.5, got {v}");
    }

    #[test]
    fn test_normal_cdf_one() {
        let v = normal_cdf(1.0);
        assert!(
            (v - 0.8413).abs() < 0.001,
            "Phi(1) should be ~0.8413, got {v}"
        );
    }

    #[test]
    fn test_normal_cdf_neg_one() {
        let v = normal_cdf(-1.0);
        assert!(
            (v - 0.1587).abs() < 0.001,
            "Phi(-1) should be ~0.1587, got {v}"
        );
    }

    #[test]
    fn test_normal_cdf_two() {
        let v = normal_cdf(2.0);
        assert!(
            (v - 0.9772).abs() < 0.001,
            "Phi(2) should be ~0.9772, got {v}"
        );
    }

    #[test]
    fn test_normal_cdf_symmetry() {
        for &x in &[0.5, 1.0, 1.5, 2.0, 3.0] {
            let sum = normal_cdf(x) + normal_cdf(-x);
            assert!((sum - 1.0).abs() < 1e-6, "Phi(x) + Phi(-x) should be 1.0");
        }
    }

    // ── Vol-CDF tests ──

    #[test]
    fn test_vol_cdf_at_the_money() {
        // At the money, P should be close to 0.5
        let p = vol_cdf_probability(100.0, 100.0, 300.0, 0.001);
        assert!(
            (p - 0.5).abs() < 0.1,
            "ATM should be near 50%, got {:.1}%",
            p * 100.0
        );
    }

    #[test]
    fn test_vol_cdf_deep_itm() {
        // Price well above strike
        let p = vol_cdf_probability(100.0, 80.0, 60.0, 0.001);
        assert!(p > 0.95, "Deep ITM should be >95%, got {:.1}%", p * 100.0);
    }

    #[test]
    fn test_vol_cdf_deep_otm() {
        // Price well below strike
        let p = vol_cdf_probability(80.0, 100.0, 60.0, 0.001);
        assert!(p < 0.05, "Deep OTM should be <5%, got {:.1}%", p * 100.0);
    }

    #[test]
    fn test_vol_cdf_expired() {
        assert_eq!(vol_cdf_probability(100.0, 99.0, 0.0, 0.001), 1.0);
        assert_eq!(vol_cdf_probability(99.0, 100.0, 0.0, 0.001), 0.0);
    }

    #[test]
    fn test_vol_cdf_higher_vol_widens() {
        // Higher vol should push probability toward 50%
        let p_low_vol = vol_cdf_probability(100.0, 105.0, 60.0, 0.001);
        let p_high_vol = vol_cdf_probability(100.0, 105.0, 60.0, 0.01);
        assert!(
            p_high_vol > p_low_vol,
            "Higher vol should increase OTM probability"
        );
    }

    #[test]
    fn test_vol_cdf_more_time_widens() {
        // More time remaining should push probability toward 50%
        let p_short = vol_cdf_probability(100.0, 105.0, 5.0, 0.002);
        let p_long = vol_cdf_probability(100.0, 105.0, 500.0, 0.002);
        assert!(
            p_long > p_short,
            "More time should increase OTM probability"
        );
    }

    // ── Realized vol tests ──

    #[test]
    fn test_realized_vol_flat() {
        let klines: Vec<(i64, f64, f64)> = (0..10)
            .map(|i| (i * 60000, 100.0, 100.0))
            .collect();
        let vol = realized_vol_from_klines(&klines);
        assert!(vol.is_some());
        assert!(vol.unwrap() < 1e-10, "Flat prices should have zero vol");
    }

    #[test]
    fn test_realized_vol_trending() {
        // Steady uptrend: each candle closes 0.1% higher
        let klines: Vec<(i64, f64, f64)> = (0..100)
            .map(|i| {
                let price = 100.0 * 1.001_f64.powi(i);
                (i as i64 * 60000, price * 0.999, price)
            })
            .collect();
        let vol = realized_vol_from_klines(&klines).unwrap();
        // Should be very small (constant return, low variance)
        assert!(vol < 0.01, "Constant trend should have low vol, got {vol}");
    }

    #[test]
    fn test_realized_vol_insufficient_data() {
        assert!(realized_vol_from_klines(&[]).is_none());
        assert!(realized_vol_from_klines(&[(0, 100.0, 100.0)]).is_none());
    }

    // ── Empirical distribution tests ──

    #[test]
    fn test_empirical_always_up() {
        // Construct klines that always go up within each 5-min window
        let mut klines = Vec::new();
        for i in 0..5000 {
            let base = 100.0 + (i / 5) as f64 * 0.1; // step up every 5 candles
            let progress = (i % 5) as f64 / 5.0;
            let price = base + progress * 0.1;
            klines.push((i as i64 * 60000, price - 0.01, price));
        }
        let dist = build_empirical_distribution(&klines, 5, 0.05);
        assert!(dist.total_windows > 0);

        // For any positive move mid-window, P(up) should be high
        if let Some((prob, n)) = lookup_empirical(&dist, 0.05, 0.5) {
            assert!(prob > 0.5, "Uptrend should give P(up)>0.5, got {prob} (n={n})");
        }
    }

    #[test]
    fn test_empirical_random_walk() {
        // Symmetric random walk — P(up) should be near 0.5 for move near 0
        let mut klines = Vec::new();
        let mut price = 100.0;
        // Use a simple deterministic "pseudo-random" pattern
        for i in 0..10000 {
            let change = if (i * 7 + 3) % 11 > 5 {
                0.01
            } else {
                -0.01
            };
            price += change;
            klines.push((i as i64 * 60000, price - change, price));
        }
        let dist = build_empirical_distribution(&klines, 5, 0.05);
        assert!(dist.total_windows > 100);

        // Near zero move, P(up) should be roughly 50%
        if let Some((prob, n)) = lookup_empirical(&dist, 0.0, 0.5) {
            assert!(n >= 10, "Should have decent samples at center");
            assert!(
                (prob - 0.5).abs() < 0.25,
                "Random walk at center should be near 50%, got {prob}"
            );
        }
    }

    #[test]
    fn test_empirical_empty_klines() {
        let dist = build_empirical_distribution(&[], 5, 0.05);
        assert_eq!(dist.total_windows, 0);
        assert!(lookup_empirical(&dist, 0.0, 0.5).is_none());
    }

    #[test]
    fn test_empirical_with_fallback() {
        // Empty distribution should trigger fallback
        let dist = build_empirical_distribution(&[], 5, 0.05);
        let klines: Vec<(i64, f64, f64)> = (0..100)
            .map(|i| (i * 60000, 100.0, 100.0))
            .collect();
        let est = estimate_empirical(&dist, 0.0, 0.5, 100.0, 100.0, 3.0, &klines, 20);
        assert_eq!(est.method, FairValueMethod::Fallback);
    }

    #[test]
    fn test_near_expiry_direction_based() {
        let dist = build_empirical_distribution(&[], 5, 0.025);
        let klines: Vec<(i64, f64, f64)> = (0..60)
            .map(|i| (i * 60000, 100.0, 100.0))
            .collect();

        // < 5 seconds left, spot down -0.098%: UP should be ~0.05
        let est = estimate_empirical(
            &dist, -0.098, 0.99, 100.0, 100.098, 0.05, &klines, 20,
        );
        assert!(
            est.probability < 0.10,
            "Spot down with <5s left should give UP ~0.05, got {:.3}",
            est.probability,
        );
        assert!(est.detail.contains("<5s left"));

        // < 5 seconds left, spot up +0.10%: UP should be ~0.95
        let est_up = estimate_empirical(
            &dist, 0.10, 0.99, 100.1, 100.0, 0.05, &klines, 20,
        );
        assert!(
            est_up.probability > 0.90,
            "Spot up with <5s left should give UP ~0.95, got {:.3}",
            est_up.probability,
        );

        // < 5 seconds left, spot flat: should be 0.50
        let est_flat = estimate_empirical(
            &dist, 0.002, 0.99, 100.0, 100.0, 0.05, &klines, 20,
        );
        assert!(
            (est_flat.probability - 0.50).abs() < 0.01,
            "Flat spot with <5s left should give ~0.50, got {:.3}",
            est_flat.probability,
        );
    }

    #[test]
    fn test_vol_cdf_near_zero_time() {
        // With very little time and price below strike, should be near 0
        let p = vol_cdf_probability(99.9, 100.0, 0.01, 0.001);
        assert!(
            p < 0.10,
            "OTM with 0.6s left should be near 0, got {:.3}",
            p,
        );

        // With very little time and price above strike, should be near 1
        let p_itm = vol_cdf_probability(100.1, 100.0, 0.01, 0.001);
        assert!(
            p_itm > 0.90,
            "ITM with 0.6s left should be near 1, got {:.3}",
            p_itm,
        );
    }

    // ── Multi-strike consistency tests ──

    #[test]
    fn test_multi_strike_consistent() {
        let estimates = vec![(80000.0, 0.90), (85000.0, 0.60), (90000.0, 0.20)];
        assert!(check_multi_strike_consistency(&estimates).is_none());
    }

    #[test]
    fn test_multi_strike_inconsistent() {
        let estimates = vec![
            (80000.0, 0.60),
            (85000.0, 0.70), // higher strike but higher prob — wrong
            (90000.0, 0.20),
        ];
        let warning = check_multi_strike_consistency(&estimates);
        assert!(warning.is_some());
        assert!(warning.unwrap().contains("Non-monotonic"));
    }

    #[test]
    fn test_multi_strike_within_tolerance() {
        // 2% tolerance — small inversion should pass
        let estimates = vec![(80000.0, 0.60), (85000.0, 0.61)];
        assert!(check_multi_strike_consistency(&estimates).is_none());
    }

    #[test]
    fn test_multi_strike_single() {
        let estimates = vec![(85000.0, 0.50)];
        assert!(check_multi_strike_consistency(&estimates).is_none());
    }

    // ── Cache tests ──

    #[tokio::test]
    async fn test_cache_put_get() {
        let cache = FairValueCache::new(Duration::from_secs(300));
        let dist = build_empirical_distribution(&[], 5, 0.05);
        cache.put("BTCUSDT_5".into(), dist, Some(0.001)).await;

        let result = cache.get("BTCUSDT_5").await;
        assert!(result.is_some());
        let (_, vol) = result.unwrap();
        assert_eq!(vol, Some(0.001));
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = FairValueCache::new(Duration::from_secs(300));
        assert!(cache.get("ETHUSDT_5").await.is_none());
    }
}
