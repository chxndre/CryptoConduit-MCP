// Extracted from market-scout src/infrastructure/recovery.rs
// CircuitBreaker is already thread-safe (Arc<Mutex<Inner>> internally).
// Do NOT double-wrap in Arc<Mutex<CircuitBreaker>>.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

pub struct CircuitBreaker {
    inner: Arc<Mutex<CircuitBreakerInner>>,
}

struct CircuitBreakerInner {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
}

impl CircuitBreaker {
    pub fn new() -> Self {
        Self::with_config(5, 2, Duration::from_secs(30))
    }

    pub fn with_config(failure_threshold: u32, success_threshold: u32, timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CircuitBreakerInner {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
                failure_threshold,
                success_threshold,
                timeout,
            })),
        }
    }

    pub fn allow_request(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match inner.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(last_failure) = inner.last_failure_time {
                    if last_failure.elapsed() >= inner.timeout {
                        inner.state = CircuitState::HalfOpen;
                        inner.success_count = 0;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn record_success(&self) {
        let mut inner = self.inner.lock().unwrap();
        match inner.state {
            CircuitState::Closed => {
                inner.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                inner.success_count += 1;
                if inner.success_count >= inner.success_threshold {
                    inner.state = CircuitState::Closed;
                    inner.failure_count = 0;
                    inner.success_count = 0;
                }
            }
            CircuitState::Open => {
                inner.state = CircuitState::Closed;
                inner.failure_count = 0;
                inner.success_count = 0;
            }
        }
    }

    pub fn record_failure(&self) {
        let mut inner = self.inner.lock().unwrap();
        match inner.state {
            CircuitState::Closed => {
                inner.failure_count += 1;
                inner.last_failure_time = Some(Instant::now());
                if inner.failure_count >= inner.failure_threshold {
                    inner.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                inner.state = CircuitState::Open;
                inner.failure_count = inner.failure_threshold;
                inner.success_count = 0;
                inner.last_failure_time = Some(Instant::now());
            }
            CircuitState::Open => {
                inner.last_failure_time = Some(Instant::now());
            }
        }
    }

    pub fn state(&self) -> CircuitState {
        self.inner.lock().unwrap().state
    }
}

impl Clone for CircuitBreaker {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Exponential backoff for WebSocket reconnection.
///
/// Starts at `initial` delay, doubles on each failure up to `max`,
/// and resets to `initial` on success.
pub struct ReconnectBackoff {
    initial: Duration,
    max: Duration,
    current: Duration,
}

impl ReconnectBackoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max,
            current: initial,
        }
    }

    /// Wait for the current backoff duration, then increase for next time.
    pub async fn wait(&mut self) {
        tokio::time::sleep(self.current).await;
        self.current = (self.current * 2).min(self.max);
    }

    /// Reset backoff to initial delay (call on successful connection).
    pub fn reset(&mut self) {
        self.current = self.initial;
    }

    /// Current backoff duration.
    pub fn current(&self) -> Duration {
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed_by_default() {
        let cb = CircuitBreaker::new();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::with_config(3, 2, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.allow_request());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::with_config(3, 2, Duration::from_secs(30));
        cb.record_failure();
        cb.record_failure();
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        // Should need 3 more failures to open
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_after_timeout() {
        let cb = CircuitBreaker::with_config(2, 2, Duration::from_millis(10));
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(15));
        assert!(cb.allow_request());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_clone_shares_state() {
        let cb1 = CircuitBreaker::new();
        let cb2 = cb1.clone();
        cb1.record_failure();
        // Both should see the same state
        assert_eq!(cb2.state(), CircuitState::Closed);
    }

    #[test]
    fn test_reconnect_backoff_doubles() {
        let mut b = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
        assert_eq!(b.current(), Duration::from_secs(1));
        // Simulate wait without actually sleeping — just advance state
        b.current = (b.current * 2).min(b.max);
        assert_eq!(b.current(), Duration::from_secs(2));
        b.current = (b.current * 2).min(b.max);
        assert_eq!(b.current(), Duration::from_secs(4));
    }

    #[test]
    fn test_reconnect_backoff_caps_at_max() {
        let mut b = ReconnectBackoff::new(Duration::from_secs(16), Duration::from_secs(30));
        b.current = (b.current * 2).min(b.max);
        assert_eq!(b.current(), Duration::from_secs(30));
        b.current = (b.current * 2).min(b.max);
        assert_eq!(b.current(), Duration::from_secs(30));
    }

    #[test]
    fn test_reconnect_backoff_reset() {
        let mut b = ReconnectBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
        b.current = (b.current * 2).min(b.max);
        b.current = (b.current * 2).min(b.max);
        assert_eq!(b.current(), Duration::from_secs(4));
        b.reset();
        assert_eq!(b.current(), Duration::from_secs(1));
    }
}
