use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ProcessorStats {
    pub success_count: u64,
    pub failure_count: u64,
    pub latency_avg: Duration,
    pub last_success: Option<Instant>,
    pub last_failure: Option<Instant>,
    pub circuit_breaker_state: CircuitBreakerState,
}

#[derive(Debug, Clone)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl ProcessorStats {
    pub fn new() -> Self {
        Self {
            success_count: 0,
            failure_count: 0,
            latency_avg: Duration::from_millis(0),
            last_success: None,
            last_failure: None,
            circuit_breaker_state: CircuitBreakerState::Closed,
        }
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.success_count + self.failure_count;
        if total == 0 {
            return 1.0;
        }
        self.success_count as f64 / total as f64
    }

    pub fn is_healthy(&self) -> bool {
        match self.circuit_breaker_state {
            CircuitBreakerState::Closed => self.success_rate() > 0.5,
            CircuitBreakerState::Open => false,
            CircuitBreakerState::HalfOpen => true,
        }
    }
}

pub struct SmartFallbackManager {
    processor_stats: Arc<RwLock<HashMap<String, ProcessorStats>>>,
    circuit_breaker_threshold: u64,
    circuit_breaker_timeout: Duration,
}

impl SmartFallbackManager {
    pub fn new(circuit_breaker_threshold: u64, circuit_breaker_timeout: Duration) -> Self {
        Self {
            processor_stats: Arc::new(RwLock::new(HashMap::new())),
            circuit_breaker_threshold,
            circuit_breaker_timeout,
        }
    }

    pub async fn record_success(&self, processor_id: &str, latency: Duration) {
        let mut stats_map = self.processor_stats.write().await;
        let stats = stats_map.entry(processor_id.to_string())
            .or_insert_with(ProcessorStats::new);

        stats.success_count += 1;
        stats.last_success = Some(Instant::now());
        
        let new_latency_ms = ((stats.latency_avg.as_millis() * 9 + latency.as_millis()) / 10) as u64;
        stats.latency_avg = Duration::from_millis(new_latency_ms);

        self.update_circuit_breaker(stats);
    }

    pub async fn record_failure(&self, processor_id: &str) {
        let mut stats_map = self.processor_stats.write().await;
        let stats = stats_map.entry(processor_id.to_string())
            .or_insert_with(ProcessorStats::new);

        stats.failure_count += 1;
        stats.last_failure = Some(Instant::now());

        self.update_circuit_breaker(stats);
    }

    fn update_circuit_breaker(&self, stats: &mut ProcessorStats) {
        match stats.circuit_breaker_state {
            CircuitBreakerState::Closed => {
                if stats.failure_count >= self.circuit_breaker_threshold {
                    stats.circuit_breaker_state = CircuitBreakerState::Open;
                }
            }
            CircuitBreakerState::Open => {
                if let Some(last_failure) = stats.last_failure {
                    if last_failure.elapsed() >= self.circuit_breaker_timeout {
                        stats.circuit_breaker_state = CircuitBreakerState::HalfOpen;
                    }
                }
            }
            CircuitBreakerState::HalfOpen => {
                if stats.success_count > 0 {
                    stats.circuit_breaker_state = CircuitBreakerState::Closed;
                    stats.failure_count = 0;
                }
            }
        }
    }

    pub async fn get_best_processor(&self, available_processors: &[String]) -> Option<String> {
        let stats_map = self.processor_stats.read().await;
        
        let mut best_processor: Option<String> = None;
        let mut best_score = 0.0f64;

        for processor_id in available_processors {
            if let Some(stats) = stats_map.get(processor_id) {
                if !stats.is_healthy() {
                    continue;
                }

                let score = self.calculate_processor_score(stats);
                if score > best_score {
                    best_score = score;
                    best_processor = Some(processor_id.clone());
                }
            } else {
                if best_processor.is_none() {
                    best_processor = Some(processor_id.clone());
                }
            }
        }

        best_processor
    }

    fn calculate_processor_score(&self, stats: &ProcessorStats) -> f64 {
        let success_rate = stats.success_rate();
        let latency_score = 1.0 / (1.0 + stats.latency_avg.as_millis() as f64 / 100.0);
        
        success_rate * 0.7 + latency_score * 0.3
    }

    pub async fn get_processor_stats(&self, processor_id: &str) -> Option<ProcessorStats> {
        let stats_map = self.processor_stats.read().await;
        stats_map.get(processor_id).cloned()
    }

    pub async fn is_processor_available(&self, processor_id: &str) -> bool {
        let stats_map = self.processor_stats.read().await;
        
        match stats_map.get(processor_id) {
            Some(stats) => stats.is_healthy(),
            None => true,
        }
    }
}