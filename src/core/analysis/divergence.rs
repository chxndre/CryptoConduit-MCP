// Spot vs implied probability divergence analysis.
// DEPRECATED: The naive linear model in this module has been replaced by the tiered
// fair_value module (empirical binning for 5m/15m, vol-scaled CDF for longer horizons).
// These functions are kept for reference and existing tests.

/// Calculate how the spot price relates to a strike level.
/// Returns the percentage distance: positive = above strike (favors YES/UP),
/// negative = below strike (favors NO/DOWN).
#[deprecated(note = "Use fair_value::vol_cdf_probability instead")]
pub fn spot_vs_strike_pct(spot_price: f64, strike_price: f64) -> f64 {
    if strike_price <= 0.0 {
        return 0.0;
    }
    ((spot_price - strike_price) / strike_price) * 100.0
}

/// Naive linear model for fair probability. DEPRECATED.
/// Replaced by fair_value::estimate_empirical (5m/15m) and fair_value::estimate_vol_cdf (longer).
#[deprecated(note = "Use fair_value module instead — this linear model is unreliable")]
pub fn spot_move_to_fair_prob(spot_move_pct: f64) -> f64 {
    let sensitivity = 5.0;
    let raw = 0.5 + spot_move_pct * sensitivity;
    raw.clamp(0.02, 0.98)
}

/// Calculate the divergence between market-implied probability and
/// spot-implied fair probability. DEPRECATED.
#[deprecated(note = "Use fair_value module instead")]
pub fn implied_divergence(market_implied_prob: f64, spot_move_pct: f64) -> f64 {
    #[allow(deprecated)]
    let fair = spot_move_to_fair_prob(spot_move_pct);
    fair - market_implied_prob
}

#[cfg(test)]
mod tests {
    #[allow(deprecated)]
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_spot_vs_strike() {
        let pct = spot_vs_strike_pct(72000.0, 71000.0);
        assert!((pct - 1.408).abs() < 0.01, "Should be ~1.41% above strike");

        let pct = spot_vs_strike_pct(70000.0, 71000.0);
        assert!(pct < 0.0, "Should be negative (below strike)");
    }

    #[test]
    #[allow(deprecated)]
    fn test_fair_prob_neutral() {
        let fair = spot_move_to_fair_prob(0.0);
        assert!((fair - 0.50).abs() < 1e-6, "Zero move should be 50/50");
    }

    #[test]
    #[allow(deprecated)]
    fn test_fair_prob_up_move() {
        let fair = spot_move_to_fair_prob(0.10);
        assert!(fair > 0.50, "Positive move should favor UP");
        assert!(fair < 1.0, "Should be clamped below 1.0");
    }

    #[test]
    #[allow(deprecated)]
    fn test_fair_prob_down_move() {
        let fair = spot_move_to_fair_prob(-0.10);
        assert!(fair < 0.50, "Negative move should favor DOWN");
        assert!(fair > 0.0, "Should be clamped above 0.0");
    }

    #[test]
    #[allow(deprecated)]
    fn test_divergence_underpriced() {
        let div = implied_divergence(0.55, 0.10);
        assert!(div > 0.0, "Positive divergence = underpriced");
    }

    #[test]
    #[allow(deprecated)]
    fn test_divergence_overpriced() {
        let div = implied_divergence(0.70, 0.02);
        assert!(div < 0.0, "Negative divergence = overpriced");
    }
}
