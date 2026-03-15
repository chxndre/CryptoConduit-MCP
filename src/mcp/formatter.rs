// Response formatting utilities for MCP tool output.

/// Generate a Polymarket event URL from a market slug.
pub fn polymarket_url(slug: &str) -> String {
    format!("https://polymarket.com/event/{slug}")
}

/// Format a price as percentage (0.65 → "65.0%").
pub fn fmt_pct(p: f64) -> String {
    format!("{:.1}%", p * 100.0)
}

/// Format USD amount.
pub fn fmt_usd(amount: f64) -> String {
    if amount.abs() >= 1000.0 {
        format!("${:.0}", amount)
    } else if amount.abs() >= 1.0 {
        format!("${:.2}", amount)
    } else {
        format!("${:.4}", amount)
    }
}

/// Format a Unix timestamp as a window time label like "6:05 PM ET".
pub fn fmt_window_time(ts: i64) -> String {
    use chrono::TimeZone;
    use chrono_tz::US::Eastern;
    let dt = Eastern.timestamp_opt(ts, 0);
    match dt {
        chrono::LocalResult::Single(t) => t.format("%-I:%M %p ET").to_string(),
        _ => "??:?? ET".to_string(),
    }
}

/// Format data freshness.
pub fn fmt_age(age_secs: f64) -> String {
    if age_secs < 1.0 {
        "< 1s".to_string()
    } else if age_secs < 60.0 {
        format!("{:.0}s", age_secs)
    } else {
        format!("{:.0}m {:.0}s", age_secs / 60.0, age_secs % 60.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fmt_pct() {
        assert_eq!(fmt_pct(0.0), "0.0%");
        assert_eq!(fmt_pct(0.5), "50.0%");
        assert_eq!(fmt_pct(1.0), "100.0%");
        assert_eq!(fmt_pct(0.156), "15.6%");
    }

    #[test]
    fn test_fmt_usd_large() {
        assert_eq!(fmt_usd(84000.0), "$84000");
        assert_eq!(fmt_usd(1000.0), "$1000");
    }

    #[test]
    fn test_fmt_usd_medium() {
        assert_eq!(fmt_usd(42.50), "$42.50");
        assert_eq!(fmt_usd(1.00), "$1.00");
    }

    #[test]
    fn test_fmt_usd_small() {
        assert_eq!(fmt_usd(0.0156), "$0.0156");
    }

    #[test]
    fn test_fmt_usd_negative() {
        assert_eq!(fmt_usd(-50.0), "$-50.00");
        assert_eq!(fmt_usd(-5000.0), "$-5000");
    }

    #[test]
    fn test_fmt_age_sub_second() {
        assert_eq!(fmt_age(0.5), "< 1s");
        assert_eq!(fmt_age(0.0), "< 1s");
    }

    #[test]
    fn test_fmt_age_seconds() {
        assert_eq!(fmt_age(5.0), "5s");
        assert_eq!(fmt_age(30.0), "30s");
    }

    #[test]
    fn test_fmt_age_minutes() {
        // 90.0 / 60.0 = 1.5 → rounds to "2m" with {:.0}, 90 % 60 = 30
        assert_eq!(fmt_age(90.0), "2m 30s");
        assert_eq!(fmt_age(120.0), "2m 0s");
    }

    #[test]
    fn test_polymarket_url() {
        let url = polymarket_url("test-slug");
        assert_eq!(url, "https://polymarket.com/event/test-slug");
    }
}
