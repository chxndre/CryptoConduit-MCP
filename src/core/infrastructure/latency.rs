// Extracted from market-scout src/infrastructure/latency.rs
// Simple sliding-window latency tracker with percentile calculations.

use tracing::info;

pub struct ApiLatencyTracker {
    samples: Vec<f64>,
    max_samples: usize,
}

impl ApiLatencyTracker {
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Vec::with_capacity(max_samples),
            max_samples,
        }
    }

    pub fn record(&mut self, latency_ms: f64) {
        if self.samples.len() >= self.max_samples {
            self.samples.remove(0);
        }
        self.samples.push(latency_ms);
    }

    fn percentile(&self, pct: f64) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let index = ((sorted.len() as f64) * pct) as usize;
        let index = index.min(sorted.len() - 1);
        Some(sorted[index])
    }

    pub fn report(&self) {
        if self.samples.is_empty() {
            return;
        }
        info!(
            samples = self.samples.len(),
            p50_ms = format!("{:.1}", self.percentile(0.50).unwrap_or(0.0)),
            p95_ms = format!("{:.1}", self.percentile(0.95).unwrap_or(0.0)),
            p99_ms = format!("{:.1}", self.percentile(0.99).unwrap_or(0.0)),
            "API latency stats"
        );
    }

    pub fn p50(&self) -> Option<f64> {
        self.percentile(0.50)
    }

    pub fn p95(&self) -> Option<f64> {
        self.percentile(0.95)
    }

    pub fn count(&self) -> usize {
        self.samples.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let tracker = ApiLatencyTracker::new(100);
        assert_eq!(tracker.p50(), None);
        assert_eq!(tracker.p95(), None);
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn test_single_sample() {
        let mut tracker = ApiLatencyTracker::new(100);
        tracker.record(42.0);
        assert_eq!(tracker.p50(), Some(42.0));
        assert_eq!(tracker.count(), 1);
    }

    #[test]
    fn test_percentiles() {
        let mut tracker = ApiLatencyTracker::new(100);
        for i in 1..=100 {
            tracker.record(i as f64);
        }
        // p50 of [1..100]: index = (100 * 0.5) = 50, sorted[50] = 51
        assert_eq!(tracker.p50(), Some(51.0));
        // p95 of [1..100]: index = (100 * 0.95) = 95, sorted[95] = 96
        assert_eq!(tracker.p95(), Some(96.0));
    }

    #[test]
    fn test_max_samples_eviction() {
        let mut tracker = ApiLatencyTracker::new(5);
        for i in 1..=10 {
            tracker.record(i as f64);
        }
        assert_eq!(tracker.count(), 5);
        // Should have samples 6-10
        assert_eq!(tracker.p50(), Some(8.0));
    }
}
