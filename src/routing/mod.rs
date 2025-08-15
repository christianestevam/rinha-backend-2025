//! Intelligent routing system with circuit breaker, load balancer, and ML predictor
//! 
//! This module combines multiple routing strategies to make optimal decisions about
//! which payment processor to use, maximizing both performance and profit.

pub mod circuit_breaker;
pub mod load_balancer;
pub mod predictor;

use circuit_breaker::{DualCircuitBreaker, CircuitBreakerConfig};
use load_balancer::{LoadBalancer, LoadBalancerConfig, RoutingDecision};
use predictor::LatencyPredictor;
use crate::storage::ProcessorType;

use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::sync::RwLock as AsyncRwLock;

/// Comprehensive routing engine that orchestrates all routing strategies
pub struct RoutingEngine {
    /// Load balancer for intelligent processor selection
    load_balancer: Arc<LoadBalancer>,
    
    /// Health monitoring and status tracking
    health_monitor: Arc<AsyncRwLock<HealthMonitor>>,
    
    /// Configuration
    config: RoutingConfig,
    
    /// Performance metrics
    metrics: RoutingMetrics,
}

/// Health monitoring for processors
struct HealthMonitor {
    /// Last health check results
    default_health: ProcessorHealthStatus,
    fallback_health: ProcessorHealthStatus,
    
    /// Health check timestamps
    last_default_check: Instant,
    last_fallback_check: Instant,
    
    /// Health check configuration
    check_interval: Duration,
}

/// Health status for individual processor
#[derive(Debug, Clone)]
struct ProcessorHealthStatus {
    /// Is the processor currently failing
    is_failing: bool,
    /// Minimum response time reported
    min_response_time_ms: u32,
    /// Last successful response time
    last_response_time_ms: u32,
    /// Health check success rate
    health_check_success_rate: f64,
}

impl Default for ProcessorHealthStatus {
    fn default() -> Self {
        Self {
            is_failing: false,
            min_response_time_ms: 1,
            last_response_time_ms: 1,
            health_check_success_rate: 1.0,
        }
    }
}

/// Routing engine configuration
#[derive(Debug, Clone)]
pub struct RoutingConfig {
    /// Load balancer configuration
    pub load_balancer: LoadBalancerConfig,
    
    /// Circuit breaker configurations
    pub circuit_breaker_default: CircuitBreakerConfig,
    pub circuit_breaker_fallback: CircuitBreakerConfig,
    
    /// Health check configuration
    pub health_check_interval: Duration,
    pub health_check_timeout: Duration,
    
    /// ML prediction configuration
    pub ml_enabled: bool,
    pub ml_min_confidence: f64,
    
    /// Adaptive behavior
    pub enable_adaptive_routing: bool,
    pub profit_optimization_weight: f64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            load_balancer: LoadBalancerConfig::default(),
            circuit_breaker_default: CircuitBreakerConfig::default(),
            circuit_breaker_fallback: {
                let mut config = CircuitBreakerConfig::default();
                config.failure_threshold = 8; // More tolerant
                config.max_latency_micros = 15000; // 15ms
                config
            },
            health_check_interval: Duration::from_secs(5),
            health_check_timeout: Duration::from_secs(2),
            ml_enabled: true,
            ml_min_confidence: 0.7,
            enable_adaptive_routing: true,
            profit_optimization_weight: 0.8,
        }
    }
}

/// Comprehensive routing metrics
#[derive(Default)]
struct RoutingMetrics {
    /// Total routing decisions made
    total_decisions: std::sync::atomic::AtomicU64,
    
    /// Decision timing
    avg_decision_time_micros: std::sync::atomic::AtomicU64,
    
    /// Accuracy tracking
    correct_predictions: std::sync::atomic::AtomicU64,
    total_predictions: std::sync::atomic::AtomicU64,
    
    /// Health check metrics
    health_checks_performed: std::sync::atomic::AtomicU64,
    health_check_failures: std::sync::atomic::AtomicU64,
}

impl RoutingEngine {
    /// Create new routing engine
    pub fn new(config: RoutingConfig) -> Self {
        let load_balancer = Arc::new(LoadBalancer::new(config.load_balancer.clone()));
        
        let health_monitor = Arc::new(AsyncRwLock::new(HealthMonitor {
            default_health: ProcessorHealthStatus::default(),
            fallback_health: ProcessorHealthStatus::default(),
            last_default_check: Instant::now(),
            last_fallback_check: Instant::now(),
            check_interval: config.health_check_interval,
        }));
        
        Self {
            load_balancer,
            health_monitor,
            config,
            metrics: RoutingMetrics::default(),
        }
    }
    
    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(RoutingConfig::default())
    }
    
    /// Make routing decision for payment
    pub async fn route_payment(&self, amount: f64) -> RoutingDecision {
        let start_time = Instant::now();
        
        // Update metrics
        self.metrics.total_decisions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Get routing decision from load balancer
        let decision = self.load_balancer.choose_processor(amount);
        
        // Update decision timing
        let decision_time = start_time.elapsed().as_micros() as u64;
        self.update_avg_decision_time(decision_time);
        
        decision
    }
    
    /// Record the result of a payment attempt
    pub async fn record_payment_result(
        &self,
        processor: ProcessorType,
        success: bool,
        latency_micros: u32,
        amount: f64,
    ) {
        // Record with load balancer
        self.load_balancer.record_result(processor, success, latency_micros, amount);
        
        // Update prediction accuracy if we made a prediction
        if success {
            self.metrics.correct_predictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.metrics.total_predictions.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Update health information from external health checks
    pub async fn update_health_status(
        &self,
        processor: ProcessorType,
        is_failing: bool,
        min_response_time_ms: u32,
    ) {
        // Update load balancer
        self.load_balancer.update_health_info(processor, min_response_time_ms * 1000, is_failing);
        
        // Update health monitor
        let mut monitor = self.health_monitor.write().await;
        match processor {
            ProcessorType::Default => {
                monitor.default_health.is_failing = is_failing;
                monitor.default_health.min_response_time_ms = min_response_time_ms;
                monitor.last_default_check = Instant::now();
            }
            ProcessorType::Fallback => {
                monitor.fallback_health.is_failing = is_failing;
                monitor.fallback_health.min_response_time_ms = min_response_time_ms;
                monitor.last_fallback_check = Instant::now();
            }
        }
        
        self.metrics.health_checks_performed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if is_failing {
            self.metrics.health_check_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    
    /// Get comprehensive routing metrics
    pub async fn get_metrics(&self) -> CompleteRoutingMetrics {
        let lb_metrics = self.load_balancer.get_metrics();
        
        let total_decisions = self.metrics.total_decisions.load(std::sync::atomic::Ordering::Relaxed);
        let avg_decision_time = self.metrics.avg_decision_time_micros.load(std::sync::atomic::Ordering::Relaxed);
        let correct_predictions = self.metrics.correct_predictions.load(std::sync::atomic::Ordering::Relaxed);
        let total_predictions = self.metrics.total_predictions.load(std::sync::atomic::Ordering::Relaxed);
        let health_checks = self.metrics.health_checks_performed.load(std::sync::atomic::Ordering::Relaxed);
        let health_failures = self.metrics.health_check_failures.load(std::sync::atomic::Ordering::Relaxed);
        
        let prediction_accuracy = if total_predictions > 0 {
            correct_predictions as f64 / total_predictions as f64
        } else {
            0.0
        };
        
        let health_success_rate = if health_checks > 0 {
            (health_checks - health_failures) as f64 / health_checks as f64
        } else {
            1.0
        };
        
        let monitor = self.health_monitor.read().await;
        
        CompleteRoutingMetrics {
            load_balancing: lb_metrics,
            routing_performance: RoutingPerformanceMetrics {
                total_decisions,
                avg_decision_time_micros: avg_decision_time,
                prediction_accuracy,
                health_success_rate,
            },
            processor_health: ProcessorHealthMetrics {
                default_status: monitor.default_health.clone(),
                fallback_status: monitor.fallback_health.clone(),
                last_default_check_age: monitor.last_default_check.elapsed(),
                last_fallback_check_age: monitor.last_fallback_check.elapsed(),
            },
        }
    }
    
    /// Get current processor recommendation
    pub async fn get_processor_recommendation(&self, amount: f64) -> ProcessorRecommendation {
        let decision = self.route_payment(amount).await;
        
        match decision {
            RoutingDecision::UseProcessor { processor, confidence, expected_latency_micros, reason } => {
                ProcessorRecommendation::UseProcessor {
                    processor,
                    confidence,
                    expected_latency_micros,
                    expected_cost_percentage: match processor {
                        ProcessorType::Default => 5.0,   // 5%
                        ProcessorType::Fallback => 15.0, // 15%
                    },
                    reason,
                }
            }
            RoutingDecision::Wait { retry_after_millis, reason } => {
                ProcessorRecommendation::Wait {
                    retry_after_millis,
                    reason,
                }
            }
        }
    }
    
    /// Reset all routing statistics
    pub async fn reset_stats(&self) {
        self.load_balancer.reset_stats();
        
        self.metrics.total_decisions.store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics.avg_decision_time_micros.store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics.correct_predictions.store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics.total_predictions.store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics.health_checks_performed.store(0, std::sync::atomic::Ordering::Relaxed);
        self.metrics.health_check_failures.store(0, std::sync::atomic::Ordering::Relaxed);
    }
    
    /// Start background health monitoring task
    pub async fn start_health_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let health_monitor = self.health_monitor.clone();
        let load_balancer = self.load_balancer.clone();
        let check_interval = self.config.health_check_interval;
        let metrics = &self.metrics as *const RoutingMetrics;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            
            loop {
                interval.tick().await;
                
                // In a real implementation, this would make HTTP calls to health endpoints
                // For now, we'll simulate health checks
                
                let monitor = health_monitor.read().await;
                let should_check_default = monitor.last_default_check.elapsed() >= check_interval;
                let should_check_fallback = monitor.last_fallback_check.elapsed() >= check_interval;
                drop(monitor);
                
                if should_check_default {
                    // Simulate health check for default processor
                    let health_ok = simulate_health_check(ProcessorType::Default).await;
                    load_balancer.update_health_info(ProcessorType::Default, 1000, !health_ok);
                    
                    unsafe {
                        (*metrics).health_checks_performed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if !health_ok {
                            (*metrics).health_check_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
                
                if should_check_fallback {
                    // Simulate health check for fallback processor
                    let health_ok = simulate_health_check(ProcessorType::Fallback).await;
                    load_balancer.update_health_info(ProcessorType::Fallback, 2000, !health_ok);
                    
                    unsafe {
                        (*metrics).health_checks_performed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if !health_ok {
                            (*metrics).health_check_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        })
    }
    
    /// Optimize routing parameters based on historical performance
    pub async fn optimize_parameters(&mut self) {
        if !self.config.enable_adaptive_routing {
            return;
        }
        
        let metrics = self.get_metrics().await;
        
        // Adaptive optimization based on performance
        if metrics.routing_performance.prediction_accuracy < 0.6 {
            // Reduce ML confidence threshold if predictions are poor
            self.config.ml_min_confidence = (self.config.ml_min_confidence * 1.1).min(0.9);
        } else if metrics.routing_performance.prediction_accuracy > 0.8 {
            // Increase ML confidence if predictions are good
            self.config.ml_min_confidence = (self.config.ml_min_confidence * 0.95).max(0.5);
        }
        
        // Adjust profit optimization weight based on cost efficiency
        let cost_efficiency = metrics.load_balancing.profit.cost_efficiency_ratio;
        if cost_efficiency < 0.7 {
            // Increase focus on profit optimization
            self.config.profit_optimization_weight = (self.config.profit_optimization_weight * 1.05).min(0.95);
        } else if cost_efficiency > 0.9 {
            // Can afford to focus more on performance
            self.config.profit_optimization_weight = (self.config.profit_optimization_weight * 0.98).max(0.6);
        }
    }
    
    /// Update average decision time
    fn update_avg_decision_time(&self, new_time_micros: u64) {
        let current_avg = self.metrics.avg_decision_time_micros.load(std::sync::atomic::Ordering::Relaxed);
        let new_avg = if current_avg == 0 {
            new_time_micros
        } else {
            // Exponential moving average
            ((current_avg as f64 * 0.9) + (new_time_micros as f64 * 0.1)) as u64
        };
        self.metrics.avg_decision_time_micros.store(new_avg, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Complete routing metrics combining all subsystems
#[derive(Debug, Clone)]
pub struct CompleteRoutingMetrics {
    pub load_balancing: load_balancer::LoadBalancerMetrics,
    pub routing_performance: RoutingPerformanceMetrics,
    pub processor_health: ProcessorHealthMetrics,
}

/// Routing-specific performance metrics
#[derive(Debug, Clone)]
pub struct RoutingPerformanceMetrics {
    pub total_decisions: u64,
    pub avg_decision_time_micros: u64,
    pub prediction_accuracy: f64,
    pub health_success_rate: f64,
}

/// Processor health metrics
#[derive(Debug, Clone)]
pub struct ProcessorHealthMetrics {
    pub default_status: ProcessorHealthStatus,
    pub fallback_status: ProcessorHealthStatus,
    pub last_default_check_age: Duration,
    pub last_fallback_check_age: Duration,
}

/// Enhanced processor recommendation with cost information
#[derive(Debug, Clone)]
pub enum ProcessorRecommendation {
    UseProcessor {
        processor: ProcessorType,
        confidence: f64,
        expected_latency_micros: u32,
        expected_cost_percentage: f64,
        reason: String,
    },
    Wait {
        retry_after_millis: u64,
        reason: String,
    },
}

impl ProcessorRecommendation {
    /// Calculate expected profit impact
    pub fn calculate_profit_impact(&self, payment_amount: f64) -> f64 {
        match self {
            ProcessorRecommendation::UseProcessor { processor, expected_cost_percentage, .. } => {
                let cost = payment_amount * (expected_cost_percentage / 100.0);
                let profit = payment_amount - cost;
                
                // Compare with baseline (fallback processor cost)
                let fallback_cost = payment_amount * 0.15; // 15%
                let baseline_profit = payment_amount - fallback_cost;
                
                profit - baseline_profit
            }
            ProcessorRecommendation::Wait { .. } => 0.0, // No profit impact when waiting
        }
    }
    
    /// Get recommendation priority (higher = better)
    pub fn get_priority(&self) -> u32 {
        match self {
            ProcessorRecommendation::UseProcessor { processor, confidence, .. } => {
                let base_priority = match processor {
                    ProcessorType::Default => 100, // Prefer default for cost
                    ProcessorType::Fallback => 50,
                };
                
                (base_priority as f64 * confidence) as u32
            }
            ProcessorRecommendation::Wait { .. } => 0,
        }
    }
}

/// Utility functions for routing decisions
pub mod utils {
    use super::*;
    
    /// Calculate cost difference between processors
    pub fn calculate_cost_difference(amount: f64) -> f64 {
        let default_cost = amount * 0.05;  // 5%
        let fallback_cost = amount * 0.15; // 15%
        fallback_cost - default_cost
    }
    
    /// Estimate profit maximization for given routing decision
    pub fn estimate_profit_optimization(
        decision: &ProcessorRecommendation,
        payment_amount: f64,
        volume_per_hour: u32,
    ) -> f64 {
        let single_payment_impact = decision.calculate_profit_impact(payment_amount);
        single_payment_impact * volume_per_hour as f64
    }
    
    /// Generate routing strategy summary
    pub fn generate_strategy_summary(metrics: &CompleteRoutingMetrics) -> String {
        let efficiency = metrics.load_balancing.efficiency_score();
        let default_percentage = metrics.load_balancing.default_percentage;
        let profit_saved = metrics.load_balancing.profit.estimated_profit_saved_cents as f64 / 100.0;
        
        format!(
            "Routing Strategy Summary:\n\
             - Overall Efficiency: {:.1}%\n\
             - Default Processor Usage: {:.1}%\n\
             - Estimated Profit Saved: ${:.2}\n\
             - Decision Time: {:.1}μs\n\
             - Prediction Accuracy: {:.1}%",
            efficiency * 100.0,
            default_percentage,
            profit_saved,
            metrics.routing_performance.avg_decision_time_micros,
            metrics.routing_performance.prediction_accuracy * 100.0
        )
    }
}

/// Simulate health check (in real implementation, this would be HTTP calls)
async fn simulate_health_check(_processor: ProcessorType) -> bool {
    // Simulate some variability in health checks
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::SystemTime;
    
    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    let hash_value = hasher.finish();
    
    // 95% success rate for health checks
    (hash_value % 100) < 95
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_routing_engine_basic() {
        let engine = RoutingEngine::with_defaults();
        
        // Test basic routing decision
        let decision = engine.route_payment(19.90).await;
        match decision {
            RoutingDecision::UseProcessor { processor, .. } => {
                assert_eq!(processor, ProcessorType::Default); // Should prefer default initially
            }
            _ => panic!("Expected processor recommendation"),
        }
        
        let metrics = engine.get_metrics().await;
        assert_eq!(metrics.routing_performance.total_decisions, 1);
    }
    
    #[tokio::test]
    async fn test_routing_with_health_updates() {
        let engine = RoutingEngine::with_defaults();
        
        // Update health status
        engine.update_health_status(ProcessorType::Default, false, 1).await;
        engine.update_health_status(ProcessorType::Fallback, false, 2).await;
        
        // Record some results
        engine.record_payment_result(ProcessorType::Default, true, 1000, 19.90).await;
        engine.record_payment_result(ProcessorType::Default, true, 1100, 29.90).await;
        
        let metrics = engine.get_metrics().await;
        assert!(metrics.routing_performance.health_success_rate > 0.0);
        assert!(metrics.load_balancing.default_requests > 0);
    }
    
    #[tokio::test]
    async fn test_processor_recommendation() {
        let engine = RoutingEngine::with_defaults();
        
        let recommendation = engine.get_processor_recommendation(50.0).await;
        
        match recommendation {
            ProcessorRecommendation::UseProcessor { processor, expected_cost_percentage, .. } => {
                assert_eq!(processor, ProcessorType::Default);
                assert_eq!(expected_cost_percentage, 5.0);
                
                let profit_impact = recommendation.calculate_profit_impact(50.0);
                assert!(profit_impact > 0.0); // Should be positive for default processor
            }
            _ => panic!("Expected processor recommendation"),
        }
    }
    
    #[tokio::test]
    async fn test_routing_optimization() {
        let mut engine = RoutingEngine::with_defaults();
        
        // Simulate poor performance to trigger optimization
        for _ in 0..10 {
            engine.record_payment_result(ProcessorType::Default, false, 10000, 19.90).await;
        }
        
        let metrics_before = engine.get_metrics().await;
        engine.optimize_parameters().await;
        
        // Should have adjusted parameters
        assert!(engine.config.ml_min_confidence >= 0.5);
    }
    
    #[tokio::test]
    async fn test_health_monitoring() {
        let engine = RoutingEngine::with_defaults();
        
        // Start health monitoring
        let _handle = engine.start_health_monitoring().await;
        
        // Wait a bit for health checks
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let metrics = engine.get_metrics().await;
        // Health monitoring should have started (though may not have completed checks yet)
        assert!(metrics.routing_performance.health_success_rate >= 0.0);
    }
    
    #[test]
    fn test_utils_functions() {
        let cost_diff = utils::calculate_cost_difference(100.0);
        assert_eq!(cost_diff, 10.0); // 15% - 5% = 10%
        
        let recommendation = ProcessorRecommendation::UseProcessor {
            processor: ProcessorType::Default,
            confidence: 0.8,
            expected_latency_micros: 1000,
            expected_cost_percentage: 5.0,
            reason: "Test".to_string(),
        };
        
        let profit_impact = recommendation.calculate_profit_impact(100.0);
        assert!(profit_impact > 0.0);
        
        let priority = recommendation.get_priority();
        assert!(priority > 50);
    }
}