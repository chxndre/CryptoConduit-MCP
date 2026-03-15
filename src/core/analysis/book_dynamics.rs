// Order book dynamics analysis — spread trends and depth asymmetry.

/// Spread trend direction.
#[derive(Debug, Clone, PartialEq)]
pub enum SpreadTrend {
    Narrowing,
    Stable,
    Widening,
}

impl std::fmt::Display for SpreadTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpreadTrend::Narrowing => write!(f, "narrowing"),
            SpreadTrend::Stable => write!(f, "stable"),
            SpreadTrend::Widening => write!(f, "widening"),
        }
    }
}

/// Depth trend over time.
#[derive(Debug, Clone, PartialEq)]
pub enum DepthTrend {
    Growing,
    Stable,
    Shrinking,
}

impl std::fmt::Display for DepthTrend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DepthTrend::Growing => write!(f, "growing"),
            DepthTrend::Stable => write!(f, "stable"),
            DepthTrend::Shrinking => write!(f, "shrinking"),
        }
    }
}

/// Order book dynamics analysis result.
#[derive(Debug, Clone)]
pub struct BookDynamicsAnalysis {
    pub current_spread_pct: f64,
    pub avg_spread_pct: f64,
    pub min_spread_pct: f64,
    pub max_spread_pct: f64,
    pub spread_trend: SpreadTrend,
    pub bid_depth_usd: f64,
    pub ask_depth_usd: f64,
    pub depth_asymmetry: f64,
    pub book_imbalance_label: String,
    // Extended 24h stats (None if no historical data)
    pub p10_spread: Option<f64>,
    pub p90_spread: Option<f64>,
    pub avg_bid_depth_24h: Option<f64>,
    pub avg_ask_depth_24h: Option<f64>,
    pub depth_trend: Option<DepthTrend>,
    pub snapshot_count_24h: usize,
}

/// Analyze order book dynamics from current state.
/// `spreads` is a time series of (timestamp_ms, spread_pct) from recent snapshots.
pub fn analyze_book(
    current_spread: f64,
    spreads: &[(i64, f64)],
    bid_depth_usd: f64,
    ask_depth_usd: f64,
) -> BookDynamicsAnalysis {
    let (min_spread_pct, max_spread_pct, avg_spread_pct) = if spreads.is_empty() {
        (current_spread, current_spread, current_spread)
    } else {
        let min = spreads.iter().map(|(_, s)| *s).fold(f64::MAX, f64::min);
        let max = spreads.iter().map(|(_, s)| *s).fold(f64::MIN, f64::max);
        let sum: f64 = spreads.iter().map(|(_, s)| s).sum();
        let avg = sum / spreads.len() as f64;
        (min, max, avg)
    };

    let spread_trend = compute_spread_trend(spreads);

    let total_depth = bid_depth_usd + ask_depth_usd;
    let depth_asymmetry = if total_depth > 0.0 {
        (bid_depth_usd - ask_depth_usd) / total_depth
    } else {
        0.0
    };

    let book_imbalance_label = label_imbalance(depth_asymmetry);

    BookDynamicsAnalysis {
        current_spread_pct: current_spread,
        avg_spread_pct,
        min_spread_pct,
        max_spread_pct,
        spread_trend,
        bid_depth_usd,
        ask_depth_usd,
        depth_asymmetry,
        book_imbalance_label,
        p10_spread: None,
        p90_spread: None,
        avg_bid_depth_24h: None,
        avg_ask_depth_24h: None,
        depth_trend: None,
        snapshot_count_24h: 0,
    }
}

/// Snapshot quality thresholds — exclude degenerate books from historical stats.
const MAX_HEALTHY_SPREAD: f64 = 0.25; // 25% spread = effectively dead book
const MIN_HEALTHY_DEPTH: f64 = 10.0; // $10 total depth = no real liquidity

/// Extended analysis incorporating historical data (e.g., from JSONL hydration).
///
/// - `historical_spreads`: (ts_ms, spread_pct) from JSONL + in-memory history combined
/// - `historical_depths`: (ts_ms, bid_depth, ask_depth) from JSONL + in-memory history
///
/// Uses median for central tendency (robust to outliers from thin books and depth spikes)
/// and filters degenerate snapshots (spread >25% or depth <$10).
pub fn analyze_book_extended(
    current_spread: f64,
    recent_spreads: &[(i64, f64)],
    historical_spreads: &[(i64, f64)],
    bid_depth_usd: f64,
    ask_depth_usd: f64,
    historical_depths: &[(i64, f64, f64)],
) -> BookDynamicsAnalysis {
    // Start with base analysis using recent spreads (for trend)
    let mut result = analyze_book(current_spread, recent_spreads, bid_depth_usd, ask_depth_usd);

    if historical_spreads.is_empty() {
        return result;
    }

    result.snapshot_count_24h = historical_spreads.len();

    // Filter healthy spreads: exclude degenerate books
    let mut healthy_spreads: Vec<f64> = historical_spreads
        .iter()
        .map(|(_, s)| *s)
        .filter(|&s| s <= MAX_HEALTHY_SPREAD)
        .collect();
    healthy_spreads.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = healthy_spreads.len();
    if n >= 5 {
        result.p10_spread = Some(healthy_spreads[n / 10]);
        result.p90_spread = Some(healthy_spreads[n * 9 / 10]);
    }

    if n > 0 {
        // Use median for avg_spread (robust to outliers)
        result.avg_spread_pct = median(&healthy_spreads);
        result.min_spread_pct = healthy_spreads[0];
        result.max_spread_pct = healthy_spreads[n - 1];
    }

    // Filter healthy depths: exclude snapshots with negligible liquidity
    let healthy_depths: Vec<(i64, f64, f64)> = historical_depths
        .iter()
        .filter(|(_, b, a)| b + a >= MIN_HEALTHY_DEPTH)
        .copied()
        .collect();

    if !healthy_depths.is_empty() {
        // Collect and sort bid/ask depths independently
        let mut bid_depths: Vec<f64> = healthy_depths.iter().map(|(_, b, _)| *b).collect();
        let mut ask_depths: Vec<f64> = healthy_depths.iter().map(|(_, _, a)| *a).collect();
        bid_depths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        ask_depths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // IQR-based outlier filtering: exclude depth spikes above Q3 + 3×IQR
        let bid_depths = iqr_filter(&bid_depths);
        let ask_depths = iqr_filter(&ask_depths);

        if !bid_depths.is_empty() {
            result.avg_bid_depth_24h = Some(median(&bid_depths));
        }
        if !ask_depths.is_empty() {
            result.avg_ask_depth_24h = Some(median(&ask_depths));
        }

        // Depth trend: compare first half vs second half total depth (using medians)
        // Use IQR-filtered depths for trend to avoid spike-driven artifacts
        let bid_fence = iqr_upper_fence(&bid_depths);
        let ask_fence = iqr_upper_fence(&ask_depths);
        let filtered_depths: Vec<(i64, f64, f64)> = healthy_depths
            .iter()
            .filter(|(_, b, a)| *b <= bid_fence && *a <= ask_fence)
            .copied()
            .collect();

        let dn = filtered_depths.len();
        if dn >= 4 {
            let mid = dn / 2;
            let older_total: Vec<f64> = filtered_depths[..mid].iter().map(|(_, b, a)| b + a).collect();
            let recent_total: Vec<f64> = filtered_depths[mid..].iter().map(|(_, b, a)| b + a).collect();
            let older_med = median_unsorted(&older_total);
            let recent_med = median_unsorted(&recent_total);

            let change = if older_med > 0.0 {
                (recent_med - older_med) / older_med
            } else {
                0.0
            };

            result.depth_trend = Some(if change > 0.10 {
                DepthTrend::Growing
            } else if change < -0.10 {
                DepthTrend::Shrinking
            } else {
                DepthTrend::Stable
            });
        }
    }

    result
}

/// Compute median of a sorted slice. Returns 0.0 for empty input.
fn median(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

/// Compute median from an unsorted slice (copies and sorts internally).
fn median_unsorted(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median(&sorted)
}

/// Compute the upper fence for IQR-based outlier filtering.
/// Returns Q3 + 3×IQR. Values above this are outliers.
/// Input must be sorted ascending. Returns f64::MAX for empty/tiny inputs.
fn iqr_upper_fence(sorted: &[f64]) -> f64 {
    if sorted.len() < 4 {
        return f64::MAX;
    }
    let q1 = sorted[sorted.len() / 4];
    let q3 = sorted[sorted.len() * 3 / 4];
    let iqr = q3 - q1;
    q3 + 3.0 * iqr
}

/// Filter a sorted slice using IQR: exclude values above Q3 + 3×IQR.
/// Returns a sub-slice (sorted) with outliers removed.
fn iqr_filter(sorted: &[f64]) -> Vec<f64> {
    let fence = iqr_upper_fence(sorted);
    sorted.iter().copied().filter(|&v| v <= fence).collect()
}

/// Compare the average of the last 10 spread entries vs the previous 10.
/// If recent average is lower, spreads are narrowing; if higher, widening.
fn compute_spread_trend(spreads: &[(i64, f64)]) -> SpreadTrend {
    if spreads.len() < 4 {
        return SpreadTrend::Stable;
    }

    let mid = spreads.len() / 2;
    let older = &spreads[..mid];
    let recent = &spreads[mid..];

    let older_avg: f64 = older.iter().map(|(_, s)| s).sum::<f64>() / older.len() as f64;
    let recent_avg: f64 = recent.iter().map(|(_, s)| s).sum::<f64>() / recent.len() as f64;

    let change_pct = if older_avg > 0.0 {
        (recent_avg - older_avg) / older_avg
    } else {
        0.0
    };

    // Threshold: 5% relative change in spread to signal a trend
    if change_pct < -0.05 {
        SpreadTrend::Narrowing
    } else if change_pct > 0.05 {
        SpreadTrend::Widening
    } else {
        SpreadTrend::Stable
    }
}

/// Label the book imbalance based on depth asymmetry.
fn label_imbalance(asymmetry: f64) -> String {
    if asymmetry > 0.3 {
        "Strong bid pressure".to_string()
    } else if asymmetry > 0.1 {
        "Mild bid pressure".to_string()
    } else if asymmetry >= -0.1 {
        "Balanced".to_string()
    } else if asymmetry >= -0.3 {
        "Mild ask pressure".to_string()
    } else {
        "Strong ask pressure".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_spreads() {
        let result = analyze_book(5.0, &[], 100.0, 100.0);
        assert_eq!(result.current_spread_pct, 5.0);
        assert_eq!(result.avg_spread_pct, 5.0);
        assert_eq!(result.min_spread_pct, 5.0);
        assert_eq!(result.max_spread_pct, 5.0);
        assert_eq!(result.spread_trend, SpreadTrend::Stable);
    }

    #[test]
    fn test_balanced_book() {
        let result = analyze_book(3.0, &[], 500.0, 500.0);
        assert!((result.depth_asymmetry).abs() < 1e-10);
        assert_eq!(result.book_imbalance_label, "Balanced");
    }

    #[test]
    fn test_strong_bid_pressure() {
        let result = analyze_book(3.0, &[], 800.0, 200.0);
        assert!(result.depth_asymmetry > 0.3);
        assert_eq!(result.book_imbalance_label, "Strong bid pressure");
    }

    #[test]
    fn test_strong_ask_pressure() {
        let result = analyze_book(3.0, &[], 200.0, 800.0);
        assert!(result.depth_asymmetry < -0.3);
        assert_eq!(result.book_imbalance_label, "Strong ask pressure");
    }

    #[test]
    fn test_mild_bid_pressure() {
        // asymmetry = (600 - 400) / 1000 = 0.2
        let result = analyze_book(3.0, &[], 600.0, 400.0);
        assert_eq!(result.book_imbalance_label, "Mild bid pressure");
    }

    #[test]
    fn test_mild_ask_pressure() {
        // asymmetry = (400 - 600) / 1000 = -0.2
        let result = analyze_book(3.0, &[], 400.0, 600.0);
        assert_eq!(result.book_imbalance_label, "Mild ask pressure");
    }

    #[test]
    fn test_zero_depth() {
        let result = analyze_book(3.0, &[], 0.0, 0.0);
        assert_eq!(result.depth_asymmetry, 0.0);
        assert_eq!(result.book_imbalance_label, "Balanced");
    }

    #[test]
    fn test_spread_stats() {
        let spreads = vec![
            (1000, 2.0),
            (2000, 4.0),
            (3000, 6.0),
            (4000, 3.0),
            (5000, 5.0),
        ];
        let result = analyze_book(5.0, &spreads, 100.0, 100.0);
        assert_eq!(result.min_spread_pct, 2.0);
        assert_eq!(result.max_spread_pct, 6.0);
        assert!((result.avg_spread_pct - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_narrowing_trend() {
        // First half: wide spreads, second half: tight spreads
        let spreads: Vec<(i64, f64)> = (0..20)
            .map(|i| {
                let spread = if i < 10 { 10.0 } else { 5.0 };
                (i * 1000, spread)
            })
            .collect();
        let result = analyze_book(5.0, &spreads, 100.0, 100.0);
        assert_eq!(result.spread_trend, SpreadTrend::Narrowing);
    }

    #[test]
    fn test_widening_trend() {
        // First half: tight spreads, second half: wide spreads
        let spreads: Vec<(i64, f64)> = (0..20)
            .map(|i| {
                let spread = if i < 10 { 5.0 } else { 10.0 };
                (i * 1000, spread)
            })
            .collect();
        let result = analyze_book(10.0, &spreads, 100.0, 100.0);
        assert_eq!(result.spread_trend, SpreadTrend::Widening);
    }

    #[test]
    fn test_stable_trend() {
        let spreads: Vec<(i64, f64)> = (0..20)
            .map(|i| (i * 1000, 5.0))
            .collect();
        let result = analyze_book(5.0, &spreads, 100.0, 100.0);
        assert_eq!(result.spread_trend, SpreadTrend::Stable);
    }

    #[test]
    fn test_too_few_spreads_for_trend() {
        let spreads = vec![(1000, 5.0), (2000, 3.0)];
        let result = analyze_book(3.0, &spreads, 100.0, 100.0);
        // With fewer than 4 entries, should default to Stable
        assert_eq!(result.spread_trend, SpreadTrend::Stable);
    }

    #[test]
    fn test_spread_trend_display() {
        assert_eq!(format!("{}", SpreadTrend::Narrowing), "narrowing");
        assert_eq!(format!("{}", SpreadTrend::Stable), "stable");
        assert_eq!(format!("{}", SpreadTrend::Widening), "widening");
    }

    #[test]
    fn test_depth_trend_display() {
        assert_eq!(format!("{}", DepthTrend::Growing), "growing");
        assert_eq!(format!("{}", DepthTrend::Stable), "stable");
        assert_eq!(format!("{}", DepthTrend::Shrinking), "shrinking");
    }

    #[test]
    fn test_extended_with_history() {
        // Realistic spread values: 0.03 to 0.05 (3% to 5%)
        let historical_spreads: Vec<(i64, f64)> = (0..100)
            .map(|i| (i * 1000, 0.03 + (i as f64 % 5.0) * 0.005))
            .collect();
        let historical_depths: Vec<(i64, f64, f64)> = (0..100)
            .map(|i| (i * 1000, 200.0, 150.0))
            .collect();

        let result = analyze_book_extended(
            0.03,
            &historical_spreads[90..], // recent
            &historical_spreads,       // all
            200.0,
            150.0,
            &historical_depths,
        );
        assert_eq!(result.snapshot_count_24h, 100);
        assert!(result.p10_spread.is_some());
        assert!(result.p90_spread.is_some());
        assert!(result.avg_bid_depth_24h.is_some());
        assert!(result.depth_trend.is_some());
        assert_eq!(result.depth_trend.unwrap(), DepthTrend::Stable);
        // Verify median-based avg is within expected range (0.03-0.05)
        assert!(result.avg_spread_pct >= 0.03 && result.avg_spread_pct <= 0.06);
    }

    #[test]
    fn test_extended_growing_depth() {
        let depths: Vec<(i64, f64, f64)> = (0..20)
            .map(|i| {
                let d = if i < 10 { 100.0 } else { 250.0 };
                (i * 1000, d, d)
            })
            .collect();

        let result = analyze_book_extended(0.03, &[], &[(0, 0.03)], 250.0, 250.0, &depths);
        assert_eq!(result.depth_trend, Some(DepthTrend::Growing));
    }

    #[test]
    fn test_extended_empty_history() {
        let result = analyze_book_extended(0.03, &[], &[], 100.0, 100.0, &[]);
        assert_eq!(result.snapshot_count_24h, 0);
        assert!(result.p10_spread.is_none());
    }

    #[test]
    fn test_extended_filters_degenerate_books() {
        // Mix of healthy and degenerate snapshots
        let mut spreads = Vec::new();
        let mut depths = Vec::new();
        for i in 0..20 {
            // 15 healthy snapshots (5% spread, $200 depth)
            spreads.push((i * 1000, 0.05));
            depths.push((i * 1000, 100.0, 100.0));
        }
        // 5 degenerate: spread 40%, depth $5
        for i in 20..25 {
            spreads.push((i * 1000, 0.40));
            depths.push((i * 1000, 3.0, 2.0));
        }
        // 1 depth spike: $1M on one side
        depths.push((25000, 1_000_000.0, 100.0));
        spreads.push((25000, 0.04));

        let result = analyze_book_extended(0.05, &spreads[15..20], &spreads, 100.0, 100.0, &depths);

        // P10/P90 should only reflect healthy spreads (0.04-0.05), not the 0.40 outliers
        assert!(result.p10_spread.unwrap() < 0.06);
        assert!(result.p90_spread.unwrap() < 0.06);

        // Median depth should NOT be dominated by the $1M spike
        let median_bid = result.avg_bid_depth_24h.unwrap();
        assert!(median_bid < 10_000.0, "Median bid depth should not be dominated by spike, got {}", median_bid);
        // But should reflect the healthy $100 values
        assert!(median_bid >= 50.0, "Median bid depth should reflect healthy books, got {}", median_bid);
    }
}
