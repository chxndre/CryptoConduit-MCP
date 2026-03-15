// Spot price momentum calculations from a price time series.

/// Momentum analysis from spot price history.
#[derive(Debug, Clone)]
pub struct MomentumAnalysis {
    pub current_price: f64,
    pub move_1m_pct: Option<f64>,
    pub move_5m_pct: Option<f64>,
    pub move_15m_pct: Option<f64>,
    pub acceleration: Option<f64>,
    pub volatility_5m: Option<f64>,
    pub trend_label: String,
}

/// Calculate momentum from price series.
/// `prices` is a slice of (timestamp_ms, price) sorted ascending by time.
pub fn calculate_momentum(prices: &[(i64, f64)]) -> MomentumAnalysis {
    let empty = MomentumAnalysis {
        current_price: 0.0,
        move_1m_pct: None,
        move_5m_pct: None,
        move_15m_pct: None,
        acceleration: None,
        volatility_5m: None,
        trend_label: "Unknown".to_string(),
    };

    if prices.is_empty() {
        return empty;
    }

    let (now_ts, current_price) = prices[prices.len() - 1];

    let move_1m_pct = pct_change_at(prices, now_ts, 60_000, current_price);
    let move_5m_pct = pct_change_at(prices, now_ts, 300_000, current_price);
    let move_15m_pct = pct_change_at(prices, now_ts, 900_000, current_price);

    // Acceleration: compare rate-of-change over 5m vs 1m.
    // If 5m rate per minute < 1m rate, the move is accelerating (getting faster recently).
    let acceleration = match (move_1m_pct, move_5m_pct) {
        (Some(m1), Some(m5)) => {
            let rate_5m = m5 / 5.0; // average pct per minute over 5m
            let rate_1m = m1; // pct per minute over 1m
            Some(rate_1m - rate_5m) // positive = accelerating
        }
        _ => None,
    };

    let volatility_5m = compute_volatility_5m(prices, now_ts);

    let trend_label = label_trend(move_5m_pct);

    MomentumAnalysis {
        current_price,
        move_1m_pct,
        move_5m_pct,
        move_15m_pct,
        acceleration,
        volatility_5m,
        trend_label,
    }
}

/// Find the closest price to `target_ts - lookback_ms` and compute % change.
fn pct_change_at(prices: &[(i64, f64)], now_ts: i64, lookback_ms: i64, current: f64) -> Option<f64> {
    let target = now_ts - lookback_ms;
    // Allow up to 30s tolerance for finding a matching price point
    let tolerance = 30_000;

    let past_price = closest_price(prices, target, tolerance)?;
    if past_price <= 0.0 {
        return None;
    }
    Some(((current - past_price) / past_price) * 100.0)
}

/// Find the price closest to `target_ts` within `tolerance_ms`.
fn closest_price(prices: &[(i64, f64)], target_ts: i64, tolerance_ms: i64) -> Option<f64> {
    let mut best: Option<(i64, f64)> = None;
    for &(ts, price) in prices {
        let dist = (ts - target_ts).abs();
        if dist <= tolerance_ms {
            if best.is_none() || dist < best.unwrap().0 {
                best = Some((dist, price));
            }
        }
    }
    best.map(|(_, p)| p)
}

/// Compute standard deviation of per-minute returns over the last 5 minutes.
fn compute_volatility_5m(prices: &[(i64, f64)], now_ts: i64) -> Option<f64> {
    let window_start = now_ts - 300_000;

    // Collect prices within the 5m window
    let window_prices: Vec<(i64, f64)> = prices
        .iter()
        .filter(|(ts, _)| *ts >= window_start && *ts <= now_ts)
        .copied()
        .collect();

    if window_prices.len() < 3 {
        return None;
    }

    // Compute returns between consecutive points
    let returns: Vec<f64> = window_prices
        .windows(2)
        .filter_map(|w| {
            let prev = w[0].1;
            let curr = w[1].1;
            if prev > 0.0 {
                Some(((curr - prev) / prev) * 100.0)
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

/// Label the trend based on 5m move magnitude.
fn label_trend(move_5m_pct: Option<f64>) -> String {
    match move_5m_pct {
        Some(m) if m > 0.10 => "Strong Up".to_string(),
        Some(m) if m > 0.02 => "Up".to_string(),
        Some(m) if m >= -0.02 => "Flat".to_string(),
        Some(m) if m >= -0.10 => "Down".to_string(),
        Some(_) => "Strong Down".to_string(),
        None => "Unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_prices(start_ts: i64, interval_ms: i64, values: &[f64]) -> Vec<(i64, f64)> {
        values
            .iter()
            .enumerate()
            .map(|(i, &p)| (start_ts + i as i64 * interval_ms, p))
            .collect()
    }

    #[test]
    fn test_empty_prices() {
        let result = calculate_momentum(&[]);
        assert_eq!(result.current_price, 0.0);
        assert!(result.move_1m_pct.is_none());
        assert_eq!(result.trend_label, "Unknown");
    }

    #[test]
    fn test_single_price() {
        let prices = vec![(1000, 70000.0)];
        let result = calculate_momentum(&prices);
        assert_eq!(result.current_price, 70000.0);
        assert!(result.move_1m_pct.is_none());
        assert_eq!(result.trend_label, "Unknown");
    }

    #[test]
    fn test_upward_momentum() {
        // Prices every 10 seconds over 5+ minutes, trending up
        let now = 1_000_000_000i64;
        let mut prices = Vec::new();
        // 31 data points, 10s apart = 300s = 5 minutes
        for i in 0..=30 {
            let ts = now - 300_000 + i * 10_000;
            // Linear uptrend: starts at 70000, gains 10 per step
            let price = 70000.0 + i as f64 * 10.0;
            prices.push((ts, price));
        }
        let result = calculate_momentum(&prices);
        assert_eq!(result.current_price, 70300.0);
        assert!(result.move_5m_pct.is_some());
        let m5 = result.move_5m_pct.unwrap();
        assert!(m5 > 0.0, "Should show positive 5m move, got {m5}");
    }

    #[test]
    fn test_downward_momentum() {
        let now = 1_000_000_000i64;
        let mut prices = Vec::new();
        for i in 0..=30 {
            let ts = now - 300_000 + i * 10_000;
            let price = 70000.0 - i as f64 * 50.0;
            prices.push((ts, price));
        }
        let result = calculate_momentum(&prices);
        assert!(result.move_5m_pct.unwrap() < 0.0);
        assert!(
            result.trend_label == "Down" || result.trend_label == "Strong Down",
            "Expected Down or Strong Down, got {}",
            result.trend_label
        );
    }

    #[test]
    fn test_flat_market() {
        let now = 1_000_000_000i64;
        let mut prices = Vec::new();
        for i in 0..=30 {
            let ts = now - 300_000 + i * 10_000;
            prices.push((ts, 70000.0));
        }
        let result = calculate_momentum(&prices);
        assert_eq!(result.trend_label, "Flat");
        assert!((result.move_5m_pct.unwrap()).abs() < 0.001);
    }

    #[test]
    fn test_volatility_zero_for_flat() {
        let now = 1_000_000_000i64;
        let mut prices = Vec::new();
        for i in 0..=30 {
            let ts = now - 300_000 + i * 10_000;
            prices.push((ts, 70000.0));
        }
        let result = calculate_momentum(&prices);
        assert!(result.volatility_5m.is_some());
        assert!(result.volatility_5m.unwrap().abs() < 1e-10, "Flat prices should have zero volatility");
    }

    #[test]
    fn test_acceleration_positive() {
        // Move was slow in the first 4 minutes, then fast in the last minute
        let now = 1_000_000_000i64;
        let mut prices = Vec::new();
        // First 4 minutes: flat at 70000
        for i in 0..=24 {
            let ts = now - 300_000 + i * 10_000;
            prices.push((ts, 70000.0));
        }
        // Last minute: sharp rise
        for i in 25..=30 {
            let ts = now - 300_000 + i * 10_000;
            let price = 70000.0 + (i - 24) as f64 * 100.0;
            prices.push((ts, price));
        }
        let result = calculate_momentum(&prices);
        // The 1m move should be larger per-minute than the 5m average
        if let Some(acc) = result.acceleration {
            assert!(acc > 0.0, "Should show positive acceleration, got {acc}");
        }
    }

    #[test]
    fn test_trend_labels() {
        assert_eq!(label_trend(Some(0.15)), "Strong Up");
        assert_eq!(label_trend(Some(0.05)), "Up");
        assert_eq!(label_trend(Some(0.01)), "Flat");
        assert_eq!(label_trend(Some(-0.01)), "Flat");
        assert_eq!(label_trend(Some(-0.05)), "Down");
        assert_eq!(label_trend(Some(-0.15)), "Strong Down");
        assert_eq!(label_trend(None), "Unknown");
    }

    #[test]
    fn test_closest_price() {
        let prices = vec![(1000, 100.0), (2000, 200.0), (3000, 300.0)];
        assert_eq!(closest_price(&prices, 1500, 600), Some(100.0).or(Some(200.0)));
        // Exact match
        assert_eq!(closest_price(&prices, 2000, 100), Some(200.0));
        // Out of tolerance
        assert_eq!(closest_price(&prices, 5000, 100), None);
    }
}
