//! Advanced circuit breaker implementation for fault tolerance and latency management
//! 
//! This circuit breaker goes beyond traditional implementations by incorporating
//! latency-based decisions, adaptive thresholds, and ML-driven predictions.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::storage::ProcessorType;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests flow normally
    Closed,
    /// Circuit is open - requests are rejected
    Open,
    /// Circuit is half-open - testing if service recovered
    HalfOpen,
}

/// Advanced circuit breaker with multiple decision criteria
pub struct CircuitBreaker {
    /// Current state of the circuit
    state: AtomicU32, // 0: Closed, 1: Open, 2: HalfOpen
    
    /// Failure tracking
    consecutive_failures: AtomicU32,
    total_failures: AtomicU64,
    total_successes: AtomicU64,
    
    /// Latency tracking (in microseconds)
    recent_latencies: parking_lot::Mutex<Vec<u32>>,
    avg_latency_micros: AtomicU32,
    p99_latency_micros: AtomicU32,
    
    /// Timing
    last_failure_time: AtomicI64,
    last_state_change: AtomicI64,
    
    /// Configuration
    config: CircuitBreakerConfig,
    
    /// Statistics for ML features
    stats: CircuitBreakerStats,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Maximum consecutive failures before opening
    pub failure_threshold: u32,
    /// Minimum requests in evaluation window
    pub min_requests: u32,
    /// Failure rate threshold (0.0 to 1.0)
    pub failure_rate_threshold: f64,
    /// Maximum acceptable latency (microseconds)
    pub max_latency_micros: u32,
    /// P99 latency threshold (microseconds)
    pub p99_latency_threshold: u32,
    /// Time to wait before transitioning from Open to HalfOpen
    pub open_timeout: Duration,
    /// Time window for evaluation (seconds)
    pub evaluation_window: Duration,
    /// Maximum latency history size
    pub max_latency_history: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            min_requests: 10,
            failure_rate_threshold: 0.5, // 50%
            max_latency_micros: 5000,     // 5ms
            p99_latency_threshold: 10000, // 10ms
            open_timeout: Duration::from_secs(2),
            evaluation_window: Duration::from_secs(60),
            max_latency_history: 1000,
        }
    }
}

/// Advanced statistics for circuit breaker
#[derive(Default)]
struct CircuitBreakerStats {
    /// Request rate (requests per second)
    request_rate: AtomicU32,
    /// Last request count for rate calculation
    last_request_count: AtomicU64,
    /// Last rate calculation time
    last_rate_calc: AtomicI64,
    /// Time spent in each state (microseconds)
    time_closed: AtomicU64,
    time_open: AtomicU64,
    time_half_open: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicU32::new(CircuitState::Closed as u32),
            consecutive_failures: AtomicU32::new(0),
            total_failures: AtomicU64::new(0),
            total_successes: AtomicU64::new(0),
            recent_latencies: parking_lot::Mutex::new(Vec::with_capacity(config.max_latency_history)),
            avg_latency_micros: AtomicU32::new(0),
            p99_latency_micros: AtomicU32::new(0),
            last_failure_time: AtomicI64::new(0),
            last_state_change: AtomicI64::new(current_timestamp_micros()),
            config,
            stats: CircuitBreakerStats::default(),
        }
    }
    
    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
    
    /// Check if a request should be allowed through
    pub fn should_allow_request(&self) -> bool {
        let current_state = self.get_state();
        
        match current_state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if enough time has passed to try half-open
                let now = current_timestamp_micros();
                let last_change = self.last_state_change.load(Ordering::Relaxed);
                let elapsed = Duration::from_micros((now - last_change) as u64);
                
                if elapsed >= self.config.open_timeout {
                    // Try to transition to half-open
                    if self.try_transition_to_half_open() {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests to test service health
                true
            }
        }
    }
    
    /// Record a successful request
    pub fn record_success(&self, latency_micros: u32) {
        self.total_successes.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        
        // Update latency tracking
        self.update_latency_metrics(latency_micros);
        
        // Check if we should close the circuit from half-open
        let current_state = self.get_state();
        if current_state == CircuitState::HalfOpen {
            self.transition_to_closed();
        }
        
        self.update_request_rate();
    }
    
    /// Record a failed request
    pub fn record_failure(&self, latency_micros: Option<u32>) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        let consecutive = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        
        self.last_failure_time.store(current_timestamp_micros(), Ordering::Relaxed);
        
        // Update latency if available (timeouts might not have latency)
        if let Some(latency) = latency_micros {
            self.update_latency_metrics(latency);
        }
        
        // Decide whether to open the circuit
        if self.should_open_circuit(consecutive) {
            self.transition_to_open();
        }
        
        self.update_request_rate();
    }
    
    /// Get current circuit state
    pub fn get_state(&self) -> CircuitState {
        match self.state.load(Ordering::Relaxed) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Default fallback
        }
    }
    
    /// Get comprehensive health metrics
    pub fn get_health_metrics(&self) -> CircuitBreakerMetrics {
        let total_requests = self.total_successes.load(Ordering::Relaxed) + 
                           self.total_failures.load(Ordering::Relaxed);
        
        let failure_rate = if total_requests > 0 {
            self.total_failures.load(Ordering::Relaxed) as f64 / total_requests as f64
        } else {
            0.0
        };
        
        CircuitBreakerMetrics {
            state: self.get_state(),
            total_requests,
            total_failures: self.total_failures.load(Ordering::Relaxed),
            total_successes: self.total_successes.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            failure_rate,
            avg_latency_micros: self.avg_latency_micros.load(Ordering::Relaxed),
            p99_latency_micros: self.p99_latency_micros.load(Ordering::Relaxed),
            request_rate: self.stats.request_rate.load(Ordering::Relaxed),
        }
    }
    
    /// Reset circuit breaker statistics
    pub fn reset(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.total_failures.store(0, Ordering::Relaxed);
        self.total_successes.store(0, Ordering::Relaxed);
        self.avg_latency_micros.store(0, Ordering::Relaxed);
        self.p99_latency_micros.store(0, Ordering::Relaxed);
        
        {
            let mut latencies = self.recent_latencies.lock();
            latencies.clear();
        }
        
        self.transition_to_closed();
    }
    
    /// Advanced decision making: should we open the circuit?
    fn should_open_circuit(&self, consecutive_failures: u32) -> bool {
        // Multiple criteria for opening circuit
        
        // 1. Too many consecutive failures
        if consecutive_failures >= self.config.failure_threshold {
            return true;
        }
        
        // 2. High latency (service degraded)
        let avg_latency = self.avg_latency_micros.load(Ordering::Relaxed);
        if avg_latency > self.config.max_latency_micros {
            return true;
        }
        
        // 3. P99 latency too high (tail latency problem)
        let p99_latency = self.p99_latency_micros.load(Ordering::Relaxed);
        if p99_latency > self.config.p99_latency_threshold {
            return true;
        }
        
        // 4. Failure rate too high (if we have enough data)
        let total_requests = self.total_successes.load(Ordering::Relaxed) + 
                           self.total_failures.load(Ordering::Relaxed);
        
        if total_requests >= self.config.min_requests as u64 {
            let failure_rate = self.total_failures.load(Ordering::Relaxed) as f64 / total_requests as f64;
            if failure_rate > self.config.failure_rate_threshold {
                return true;
            }
        }
        
        false
    }
    
    /// Update latency metrics with new sample
    fn update_latency_metrics(&self, latency_micros: u32) {
        let mut latencies = self.recent_latencies.lock();
        
        // Add new latency
        latencies.push(latency_micros);
        
        // Keep only recent samples
        if latencies.len() > self.config.max_latency_history {
            latencies.remove(0);
        }
        
        // Calculate metrics
        if !latencies.is_empty() {
            // Average latency
            let sum: u64 = latencies.iter().map(|&l| l as u64).sum();
            let avg = (sum / latencies.len() as u64) as u32;
            self.avg_latency_micros.store(avg, Ordering::Relaxed);
            
            // P99 latency
            let mut sorted_latencies = latencies.clone();
            sorted_latencies.sort_unstable();
            let p99_index = ((sorted_latencies.len() as f64) * 0.99).ceil() as usize - 1;
            let p99_index = p99_index.min(sorted_latencies.len() - 1);
            let p99 = sorted_latencies[p99_index];
            self.p99_latency_micros.store(p99, Ordering::Relaxed);
        }
    }
    
    /// Update request rate calculation
    fn update_request_rate(&self) {
        let now = current_timestamp_micros();
        let last_calc = self.stats.last_rate_calc.load(Ordering::Relaxed);
        
        // Calculate rate every second
        if now - last_calc >= 1_000_000 { // 1 second in microseconds
            let current_total = self.total_successes.load(Ordering::Relaxed) + 
                              self.total_failures.load(Ordering::Relaxed);
            let last_total = self.stats.last_request_count.load(Ordering::Relaxed);
            
            let requests_in_period = current_total - last_total;
            let time_elapsed_secs = (now - last_calc) as f64 / 1_000_000.0;
            let rate = (requests_in_period as f64 / time_elapsed_secs) as u32;
            
            self.stats.request_rate.store(rate, Ordering::Relaxed);
            self.stats.last_request_count.store(current_total, Ordering::Relaxed);
            self.stats.last_rate_calc.store(now, Ordering::Relaxed);
        }
    }
    
    /// Transition to closed state
    fn transition_to_closed(&self) {
        self.state.store(CircuitState::Closed as u32, Ordering::Relaxed);
        self.last_state_change.store(current_timestamp_micros(), Ordering::Relaxed);
    }
    
    /// Transition to open state
    fn transition_to_open(&self) {
        self.state.store(CircuitState::Open as u32, Ordering::Relaxed);
        self.last_state_change.store(current_timestamp_micros(), Ordering::Relaxed);
    }
    
    /// Try to transition to half-open state
    fn try_transition_to_half_open(&self) -> bool {
        let expected = CircuitState::Open as u32;
        let new_state = CircuitState::HalfOpen as u32;
        
        if self.state.compare_exchange(expected, new_state, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            self.last_state_change.store(current_timestamp_micros(), Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

/// Circuit breaker metrics for monitoring and ML features
#[derive(Debug, Clone)]
pub struct CircuitBreakerMetrics {
    pub state: CircuitState,
    pub total_requests: u64,
    pub total_failures: u64,
    pub total_successes: u64,
    pub consecutive_failures: u32,
    pub failure_rate: f64,
    pub avg_latency_micros: u32,
    pub p99_latency_micros: u32,
    pub request_rate: u32,
}

impl CircuitBreakerMetrics {
    /// Get health score (0.0 = unhealthy, 1.0 = healthy)
    pub fn health_score(&self) -> f64 {
        if self.total_requests == 0 {
            return 1.0; // No data, assume healthy
        }
        
        let mut score = 1.0;
        
        // Penalize high failure rate
        score *= (1.0 - self.failure_rate).max(0.0);
        
        // Penalize high latency (normalize against 10ms threshold)
        let latency_penalty = (self.avg_latency_micros as f64 / 10000.0).min(1.0);
        score *= (1.0 - latency_penalty * 0.5).max(0.0);
        
        // Penalize open circuit
        if self.state == CircuitState::Open {
            score *= 0.1; // Severe penalty for open circuit
        } else if self.state == CircuitState::HalfOpen {
            score *= 0.5; // Moderate penalty for half-open
        }
        
        score.max(0.0).min(1.0)
    }
    
    /// Check if processor is recommended for use
    pub fn is_recommended(&self) -> bool {
        match self.state {
            CircuitState::Closed => self.health_score() > 0.7,
            CircuitState::HalfOpen => self.health_score() > 0.5,
            CircuitState::Open => false,
        }
    }
}

/// Dual circuit breaker for managing both processors
pub struct DualCircuitBreaker {
    /// Circuit breaker for default processor
    default_cb: CircuitBreaker,
    /// Circuit breaker for fallback processor
    fallback_cb: CircuitBreaker,
}

impl DualCircuitBreaker {
    /// Create new dual circuit breaker
    pub fn new(default_config: CircuitBreakerConfig, fallback_config: CircuitBreakerConfig) -> Self {
        Self {
            default_cb: CircuitBreaker::new(default_config),
            fallback_cb: CircuitBreaker::new(fallback_config),
        }
    }
    
    /// Create with default configurations
    pub fn with_defaults() -> Self {
        let default_config = CircuitBreakerConfig::default();
        let mut fallback_config = CircuitBreakerConfig::default();
        
        // Fallback processor has different thresholds (more tolerant)
        fallback_config.failure_threshold = 8;
        fallback_config.max_latency_micros = 15000; // 15ms
        fallback_config.p99_latency_threshold = 30000; // 30ms
        fallback_config.failure_rate_threshold = 0.7; // 70%
        
        Self::new(default_config, fallback_config)
    }
    
    /// Record result for specific processor
    pub fn record_result(&self, processor: ProcessorType, success: bool, latency_micros: u32) {
        let cb = match processor {
            ProcessorType::Default => &self.default_cb,
            ProcessorType::Fallback => &self.fallback_cb,
        };
        
        if success {
            cb.record_success(latency_micros);
        } else {
            cb.record_failure(Some(latency_micros));
        }
    }
    
    /// Check if processor should allow requests
    pub fn should_allow_request(&self, processor: ProcessorType) -> bool {
        let cb = match processor {
            ProcessorType::Default => &self.default_cb,
            ProcessorType::Fallback => &self.fallback_cb,
        };
        
        cb.should_allow_request()
    }
    
    /// Get metrics for both processors
    pub fn get_metrics(&self) -> (CircuitBreakerMetrics, CircuitBreakerMetrics) {
        (
            self.default_cb.get_health_metrics(),
            self.fallback_cb.get_health_metrics(),
        )
    }
    
    /// Get recommendation for which processor to use
    pub fn get_processor_recommendation(&self) -> ProcessorRecommendation {
        let (default_metrics, fallback_metrics) = self.get_metrics();
        
        let default_available = self.default_cb.should_allow_request();
        let fallback_available = self.fallback_cb.should_allow_request();
        
        match (default_available, fallback_available) {
            (true, true) => {
                // Both available - choose based on health score and latency
                let default_score = default_metrics.health_score();
                let fallback_score = fallback_metrics.health_score();
                
                // Factor in the cost difference (default has lower fees)
                let default_adjusted_score = default_score * 1.2; // Bonus for lower fees
                
                if default_adjusted_score >= fallback_score {
                    ProcessorRecommendation::UseDefault {
                        confidence: default_score,
                        expected_latency: default_metrics.avg_latency_micros,
                    }
                } else {
                    ProcessorRecommendation::UseFallback {
                        confidence: fallback_score,
                        expected_latency: fallback_metrics.avg_latency_micros,
                    }
                }
            }
            (true, false) => ProcessorRecommendation::UseDefault {
                confidence: default_metrics.health_score(),
                expected_latency: default_metrics.avg_latency_micros,
            },
            (false, true) => ProcessorRecommendation::UseFallback {
                confidence: fallback_metrics.health_score(),
                expected_latency: fallback_metrics.avg_latency_micros,
            },
            (false, false) => ProcessorRecommendation::Wait {
                retry_after_millis: 100,
            },
        }
    }
    
    /// Reset both circuit breakers
    pub fn reset(&self) {
        self.default_cb.reset();
        self.fallback_cb.reset();
    }
}

/// Processor recommendation from circuit breaker analysis
#[derive(Debug, Clone)]
pub enum ProcessorRecommendation {
    UseDefault {
        confidence: f64,
        expected_latency: u32,
    },
    UseFallback {
        confidence: f64,
        expected_latency: u32,
    },
    Wait {
        retry_after_millis: u64,
    },
}

/// Get current timestamp in microseconds
fn current_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_circuit_breaker_basic() {
        let cb = CircuitBreaker::with_defaults();
        
        // Should start closed
        assert_eq!(cb.get_state(), CircuitState::Closed);
        assert!(cb.should_allow_request());
        
        // Record successes
        cb.record_success(1000);
        cb.record_success(1500);
        assert_eq!(cb.get_state(), CircuitState::Closed);
        
        let metrics = cb.get_health_metrics();
        assert_eq!(metrics.total_successes, 2);
        assert_eq!(metrics.total_failures, 0);
    }
    
    #[test]
    fn test_circuit_breaker_failure_threshold() {
        let mut config = CircuitBreakerConfig::default();
        config.failure_threshold = 3;
        
        let cb = CircuitBreaker::new(config);
        
        // Record failures
        cb.record_failure(Some(5000));
        assert_eq!(cb.get_state(), CircuitState::Closed);
        
        cb.record_failure(Some(6000));
        assert_eq!(cb.get_state(), CircuitState::Closed);
        
        cb.record_failure(Some(7000));
        assert_eq!(cb.get_state(), CircuitState::Open);
        
        // Should not allow requests when open
        assert!(!cb.should_allow_request());
    }
    
    #[test]
    fn test_circuit_breaker_latency_threshold() {
        let mut config = CircuitBreakerConfig::default();
        config.max_latency_micros = 2000; // 2ms threshold
        
        let cb = CircuitBreaker::new(config);
        
        // Record high latency "successes"
        cb.record_success(3000); // 3ms - above threshold
        cb.record_success(3500); // 3.5ms
        cb.record_success(4000); // 4ms
        
        // Should open due to high latency
        cb.record_failure(Some(5000));
        assert_eq!(cb.get_state(), CircuitState::Open);
    }
    
    #[test]
    fn test_dual_circuit_breaker() {
        let dual_cb = DualCircuitBreaker::with_defaults();
        
        // Both should start available
        assert!(dual_cb.should_allow_request(ProcessorType::Default));
        assert!(dual_cb.should_allow_request(ProcessorType::Fallback));
        
        // Record some results
        dual_cb.record_result(ProcessorType::Default, true, 1000);
        dual_cb.record_result(ProcessorType::Fallback, true, 2000);
        
        let recommendation = dual_cb.get_processor_recommendation();
        match recommendation {
            ProcessorRecommendation::UseDefault { .. } => {
                // Expected - default should be preferred due to lower cost
            }
            _ => panic!("Expected UseDefault recommendation"),
        }
    }
    
    #[test]
    fn test_health_score_calculation() {
        let metrics = CircuitBreakerMetrics {
            state: CircuitState::Closed,
            total_requests: 100,
            total_failures: 10,
            total_successes: 90,
            consecutive_failures: 0,
            failure_rate: 0.1, // 10% failure rate
            avg_latency_micros: 2000, // 2ms
            p99_latency_micros: 5000, // 5ms
            request_rate: 50,
        };
        
        let score = metrics.health_score();
        assert!(score > 0.7); // Should be healthy
        assert!(metrics.is_recommended());
    }
    
    #[test]
    fn test_processor_recommendation_with_failures() {
        let dual_cb = DualCircuitBreaker::with_defaults();
        
        // Make default processor fail
        for _ in 0..10 {
            dual_cb.record_result(ProcessorType::Default, false, 10000);
        }
        
        // Keep fallback healthy
        for _ in 0..10 {
            dual_cb.record_result(ProcessorType::Fallback, true, 2000);
        }
        
        let recommendation = dual_cb.get_processor_recommendation();
        match recommendation {
            ProcessorRecommendation::UseFallback { .. } => {
                // Expected - should switch to fallback
            }
            _ => {
                let (default_metrics, fallback_metrics) = dual_cb.get_metrics();
                println!("Default: {:?}", default_metrics);
                println!("Fallback: {:?}", fallback_metrics);
                panic!("Expected UseFallback recommendation");
            }
        }
    }
}