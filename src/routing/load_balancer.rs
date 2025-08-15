//! Advanced load balancer with intelligent processor selection
//! 
//! This load balancer implements multiple strategies to maximize profit by:
//! - Prioritizing the default processor (lower fees)
//! - Using adaptive circuit breaking
//! - Implementing latency-based routing
//! - Learning from historical performance data

use crate::storage::ProcessorType;
use crate::utils::metrics::MetricsCollector;
use crate::client::http_client::ProcessorMetrics;
use super::circuit_breaker::{DualCircuitBreaker, CircuitBreakerMetrics};
use super::predictor::LatencyPredictor;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Intelligent load balancer with adaptive strategies
pub struct LoadBalancer {
    /// Circuit breaker for both processors
    circuit_breaker: Arc<DualCircuitBreaker>,
    
    /// ML-based latency predictor
    predictor: Arc<LatencyPredictor>,
    
    /// Configuration
    config: LoadBalancerConfig,
    
    /// Routing state
    state: RwLock<LoadBalancerState>,
    
    /// Performance metrics
    metrics: LoadBalancerMetrics,
}

/// Load balancer configuration
#[derive(Debug, Clone)]
pub struct LoadBalancerConfig {
    /// Default processor fee percentage (e.g., 0.05 for 5%)
    pub default_fee_rate: f64,
    
    /// Fallback processor fee percentage (e.g., 0.08 for 8%)
    pub fallback_fee_rate: f64,
    
    /// Maximum acceptable latency for default processor (ms)
    pub max_default_latency: u64,
    
    /// Latency threshold for switching to fallback (ms)
    pub latency_switch_threshold: u64,
    
    /// Minimum profit margin to use fallback
    pub min_profit_margin: f64,
    
    /// Adaptive algorithm parameters
    pub adaptive_config: AdaptiveConfig,
    
    /// Health check configuration
    pub health_config: HealthConfig,
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            default_fee_rate: 0.05,      // 5% fee for default
            fallback_fee_rate: 0.08,     // 8% fee for fallback
            max_default_latency: 500,    // 500ms max for default
            latency_switch_threshold: 200, // Switch at 200ms
            min_profit_margin: 0.02,     // 2% minimum margin
            adaptive_config: AdaptiveConfig::default(),
            health_config: HealthConfig::default(),
        }
    }
}

/// Adaptive algorithm configuration
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Learning rate for weight adjustments
    pub learning_rate: f64,
    
    /// Exploration vs exploitation ratio
    pub exploration_rate: f64,
    
    /// Window size for moving averages
    pub window_size: usize,
    
    /// Adjustment interval
    pub adjustment_interval: Duration,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            learning_rate: 0.1,
            exploration_rate: 0.05,
            window_size: 100,
            adjustment_interval: Duration::from_secs(30),
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Health check interval
    pub check_interval: Duration,
    
    /// Timeout for health checks
    pub check_timeout: Duration,
    
    /// Failure threshold for marking unhealthy
    pub failure_threshold: usize,
    
    /// Recovery threshold for marking healthy
    pub recovery_threshold: usize,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            check_timeout: Duration::from_millis(1000),
            failure_threshold: 3,
            recovery_threshold: 2,
        }
    }
}

/// Internal load balancer state
struct LoadBalancerState {
    /// Current routing weights (default, fallback)
    routing_weights: (f64, f64),
    
    /// Last adjustment timestamp
    last_adjustment: Instant,
    
    /// Performance history window
    performance_window: Vec<RoutingPerformance>,
    
    /// Current strategy being used
    current_strategy: RoutingStrategy,
}

/// Performance data for routing decisions
#[derive(Debug, Clone)]
struct RoutingPerformance {
    timestamp: Instant,
    processor: ProcessorType,
    latency_ms: u64,
    success: bool,
    profit_impact: f64,
}

/// Available routing strategies
#[derive(Debug, Clone, PartialEq)]
enum RoutingStrategy {
    /// Always prefer default (maximum profit)
    ProfitMaximizing,
    
    /// Balance between latency and profit
    LatencyAware,
    
    /// Adaptive based on current conditions
    Adaptive,
    
    /// Failover mode (fallback only)
    Failover,
}

/// Routing decision with reasoning
#[derive(Debug, Clone)]
pub struct RoutingDecision {
    /// Selected processor
    pub processor: ProcessorType,
    
    /// Confidence score (0.0 - 1.0)
    pub confidence: f64,
    
    /// Expected latency (ms)
    pub expected_latency: u64,
    
    /// Expected profit impact
    pub profit_impact: f64,
    
    /// Reasoning for the decision
    pub reasoning: String,
}

/// Load balancer performance metrics
#[derive(Default)]
struct LoadBalancerMetrics {
    /// Total routing decisions made
    total_decisions: AtomicU64,
    
    /// Decisions by processor type
    default_decisions: AtomicU64,
    fallback_decisions: AtomicU64,
    
    /// Strategy usage counters
    profit_strategy_count: AtomicU64,
    latency_strategy_count: AtomicU64,
    adaptive_strategy_count: AtomicU64,
    failover_strategy_count: AtomicU64,
    
    /// Performance tracking
    total_profit_impact: AtomicU64, // Stored as micros
    total_latency_saved: AtomicU64,
    
    /// Success rates
    default_success_count: AtomicU64,
    fallback_success_count: AtomicU64,
    
    /// Current performance scores
    current_default_score: AtomicU64, // Stored as percentage * 100
    current_fallback_score: AtomicU64,
}

impl LoadBalancer {
    /// Create new load balancer
    pub fn new(
        circuit_breaker: Arc<DualCircuitBreaker>,
        predictor: Arc<LatencyPredictor>,
        config: LoadBalancerConfig,
    ) -> Self {
        let state = LoadBalancerState {
            routing_weights: (0.8, 0.2), // Start with 80% default preference
            last_adjustment: Instant::now(),
            performance_window: Vec::with_capacity(config.adaptive_config.window_size),
            current_strategy: RoutingStrategy::ProfitMaximizing,
        };

        Self {
            circuit_breaker,
            predictor,
            config,
            state: RwLock::new(state),
            metrics: LoadBalancerMetrics::default(),
        }
    }

    /// Make routing decision based on current conditions
    pub async fn route_request(&self, amount: f64) -> RoutingDecision {
        let start_time = Instant::now();
        
        // Get current circuit breaker states
        let default_state = self.circuit_breaker.get_default_state().await;
        let fallback_state = self.circuit_breaker.get_fallback_state().await;
        
        // Determine available processors
        let available_processors = self.get_available_processors(default_state, fallback_state);
        
        // Make routing decision based on strategy
        let decision = self.make_routing_decision(amount, &available_processors).await;
        
        // Update metrics
        self.update_routing_metrics(&decision);
        
        decision
    }

    /// Record the outcome of a routing decision
    pub async fn record_outcome(
        &self,
        processor: ProcessorType,
        latency_ms: u64,
        success: bool,
        amount: f64,
    ) {
        let profit_impact = self.calculate_profit_impact(processor, amount, success);
        
        let performance = RoutingPerformance {
            timestamp: Instant::now(),
            processor,
            latency_ms,
            success,
            profit_impact,
        };
        
        // Update state with new performance data
        {
            let mut state = self.state.write();
            
            // Add to performance window
            state.performance_window.push(performance.clone());
            
            // Keep window size limited
            if state.performance_window.len() > self.config.adaptive_config.window_size {
                state.performance_window.remove(0);
            }
            
            // Check if we need to adjust strategy
            if state.last_adjustment.elapsed() >= self.config.adaptive_config.adjustment_interval {
                self.adjust_strategy(&mut state);
                state.last_adjustment = Instant::now();
            }
        }
        
        // Update metrics
        self.update_outcome_metrics(processor, success, profit_impact, latency_ms);
        
        // Feed data to predictor
        self.predictor.record_observation(processor, latency_ms, success).await;
    }

    /// Get current load balancer metrics
    pub fn get_metrics(&self) -> LoadBalancerMetricsSnapshot {
        let total_decisions = self.metrics.total_decisions.load(Ordering::Relaxed);
        let default_decisions = self.metrics.default_decisions.load(Ordering::Relaxed);
        let fallback_decisions = self.metrics.fallback_decisions.load(Ordering::Relaxed);
        
        let default_success = self.metrics.default_success_count.load(Ordering::Relaxed);
        let fallback_success = self.metrics.fallback_success_count.load(Ordering::Relaxed);
        
        LoadBalancerMetricsSnapshot {
            total_decisions,
            default_decisions,
            fallback_decisions,
            default_success_rate: if default_decisions > 0 {
                default_success as f64 / default_decisions as f64
            } else {
                0.0
            },
            fallback_success_rate: if fallback_decisions > 0 {
                fallback_success as f64 / fallback_decisions as f64
            } else {
                0.0
            },
            profit_maximization_rate: if total_decisions > 0 {
                default_decisions as f64 / total_decisions as f64
            } else {
                0.0
            },
            total_profit_impact: self.metrics.total_profit_impact.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            current_strategy: self.state.read().current_strategy.clone(),
        }
    }

    /// Get available processors based on circuit breaker states
    fn get_available_processors(
        &self,
        default_state: CircuitBreakerState,
        fallback_state: CircuitBreakerState,
    ) -> Vec<ProcessorType> {
        let mut available = Vec::new();
        
        if matches!(default_state, CircuitBreakerState::Closed | CircuitBreakerState::HalfOpen) {
            available.push(ProcessorType::Default);
        }
        
        if matches!(fallback_state, CircuitBreakerState::Closed | CircuitBreakerState::HalfOpen) {
            available.push(ProcessorType::Fallback);
        }
        
        available
    }

    /// Make the actual routing decision
    async fn make_routing_decision(
        &self,
        amount: f64,
        available_processors: &[ProcessorType],
    ) -> RoutingDecision {
        if available_processors.is_empty() {
            return RoutingDecision {
                processor: ProcessorType::Default, // Fallback to default
                confidence: 0.0,
                expected_latency: 5000, // High latency for unavailable service
                profit_impact: self.calculate_profit_impact(ProcessorType::Default, amount, false),
                reasoning: "No processors available - using default as last resort".to_string(),
            };
        }

        let state = self.state.read();
        
        match state.current_strategy {
            RoutingStrategy::ProfitMaximizing => {
                self.profit_maximizing_decision(amount, available_processors).await
            }
            RoutingStrategy::LatencyAware => {
                self.latency_aware_decision(amount, available_processors).await
            }
            RoutingStrategy::Adaptive => {
                self.adaptive_decision(amount, available_processors, &state).await
            }
            RoutingStrategy::Failover => {
                self.failover_decision(amount, available_processors).await
            }
        }
    }

    /// Profit-maximizing strategy (always prefer default if available)
    async fn profit_maximizing_decision(
        &self,
        amount: f64,
        available_processors: &[ProcessorType],
    ) -> RoutingDecision {
        if available_processors.contains(&ProcessorType::Default) {
            let expected_latency = self.predictor.predict_latency(ProcessorType::Default).await
                .unwrap_or(100); // Default to 100ms if no prediction
            
            RoutingDecision {
                processor: ProcessorType::Default,
                confidence: 0.9,
                expected_latency,
                profit_impact: self.calculate_profit_impact(ProcessorType::Default, amount, true),
                reasoning: "Profit maximizing: using default processor for lowest fees".to_string(),
            }
        } else {
            let expected_latency = self.predictor.predict_latency(ProcessorType::Fallback).await
                .unwrap_or(150);
            
            RoutingDecision {
                processor: ProcessorType::Fallback,
                confidence: 0.7,
                expected_latency,
                profit_impact: self.calculate_profit_impact(ProcessorType::Fallback, amount, true),
                reasoning: "Profit maximizing: default unavailable, using fallback".to_string(),
            }
        }
    }

    /// Latency-aware strategy (balance latency and profit)
    async fn latency_aware_decision(
        &self,
        amount: f64,
        available_processors: &[ProcessorType],
    ) -> RoutingDecision {
        let mut best_processor = available_processors[0];
        let mut best_score = f64::NEG_INFINITY;
        let mut best_latency = 1000u64;
        let mut reasoning = String::new();

        for &processor in available_processors {
            let predicted_latency = self.predictor.predict_latency(processor).await
                .unwrap_or(match processor {
                    ProcessorType::Default => 100,
                    ProcessorType::Fallback => 150,
                });

            let profit_impact = self.calculate_profit_impact(processor, amount, true);
            
            // Calculate composite score: profit impact - latency penalty
            let latency_penalty = predicted_latency as f64 / 1000.0; // Convert to seconds
            let score = profit_impact - latency_penalty;

            if score > best_score {
                best_score = score;
                best_processor = processor;
                best_latency = predicted_latency;
                reasoning = format!(
                    "Latency-aware: {} (latency: {}ms, profit: ${:.4})",
                    match processor {
                        ProcessorType::Default => "default",
                        ProcessorType::Fallback => "fallback",
                    },
                    predicted_latency,
                    profit_impact
                );
            }
        }

        RoutingDecision {
            processor: best_processor,
            confidence: 0.8,
            expected_latency: best_latency,
            profit_impact: self.calculate_profit_impact(best_processor, amount, true),
            reasoning,
        }
    }

    /// Adaptive strategy based on historical performance
    async fn adaptive_decision(
        &self,
        amount: f64,
        available_processors: &[ProcessorType],
        state: &LoadBalancerState,
    ) -> RoutingDecision {
        // Calculate processor scores based on recent performance
        let mut processor_scores = std::collections::HashMap::new();
        
        for &processor in available_processors {
            let score = self.calculate_adaptive_score(processor, state).await;
            processor_scores.insert(processor, score);
        }

        // Select processor with highest score
        let best_processor = processor_scores
            .iter()
            .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(processor, _)| *processor)
            .unwrap_or(available_processors[0]);

        let expected_latency = self.predictor.predict_latency(best_processor).await
            .unwrap_or(100);

        RoutingDecision {
            processor: best_processor,
            confidence: 0.85,
            expected_latency,
            profit_impact: self.calculate_profit_impact(best_processor, amount, true),
            reasoning: format!(
                "Adaptive: {} based on historical performance (score: {:.3})",
                match best_processor {
                    ProcessorType::Default => "default",
                    ProcessorType::Fallback => "fallback",
                },
                processor_scores.get(&best_processor).unwrap_or(&0.0)
            ),
        }
    }

    /// Failover strategy (use any available processor)
    async fn failover_decision(
        &self,
        amount: f64,
        available_processors: &[ProcessorType],
    ) -> RoutingDecision {
        // Prefer default if available, otherwise use fallback
        let processor = if available_processors.contains(&ProcessorType::Default) {
            ProcessorType::Default
        } else {
            ProcessorType::Fallback
        };

        let expected_latency = self.predictor.predict_latency(processor).await
            .unwrap_or(200);

        RoutingDecision {
            processor,
            confidence: 0.6,
            expected_latency,
            profit_impact: self.calculate_profit_impact(processor, amount, true),
            reasoning: "Failover mode: using available processor".to_string(),
        }
    }

    /// Calculate adaptive score for a processor
    async fn calculate_adaptive_score(&self, processor: ProcessorType, state: &LoadBalancerState) -> f64 {
        let recent_performance: Vec<_> = state.performance_window
            .iter()
            .filter(|p| p.processor == processor)
            .collect();

        if recent_performance.is_empty() {
            return match processor {
                ProcessorType::Default => 0.8, // Default bias
                ProcessorType::Fallback => 0.2,
            };
        }

        // Calculate success rate
        let success_rate = recent_performance.iter()
            .filter(|p| p.success)
            .count() as f64 / recent_performance.len() as f64;

        // Calculate average profit impact
        let avg_profit = recent_performance.iter()
            .map(|p| p.profit_impact)
            .sum::<f64>() / recent_performance.len() as f64;

        // Calculate average latency (lower is better)
        let avg_latency = recent_performance.iter()
            .map(|p| p.latency_ms as f64)
            .sum::<f64>() / recent_performance.len() as f64;

        // Composite score: success_rate * profit - latency_penalty
        let latency_penalty = (avg_latency / 1000.0).min(1.0); // Normalize to 0-1
        success_rate * avg_profit - latency_penalty
    }

    /// Calculate profit impact for a processor choice
    fn calculate_profit_impact(&self, processor: ProcessorType, amount: f64, success: bool) -> f64 {
        if !success {
            return -amount * 0.1; // Penalty for failed transactions
        }

        let fee_rate = match processor {
            ProcessorType::Default => self.config.default_fee_rate,
            ProcessorType::Fallback => self.config.fallback_fee_rate,
        };

        amount * (1.0 - fee_rate) // Profit after fees
    }

    /// Adjust routing strategy based on performance
    fn adjust_strategy(&self, state: &mut LoadBalancerState) {
        if state.performance_window.len() < 10 {
            return; // Not enough data
        }

        // Calculate recent success rates and profit
        let recent_window = &state.performance_window[state.performance_window.len().saturating_sub(20)..];
        
        let default_performance: Vec<_> = recent_window.iter()
            .filter(|p| p.processor == ProcessorType::Default)
            .collect();
        
        let fallback_performance: Vec<_> = recent_window.iter()
            .filter(|p| p.processor == ProcessorType::Fallback)
            .collect();

        // Calculate metrics
        let default_success_rate = if default_performance.is_empty() {
            0.0
        } else {
            default_performance.iter().filter(|p| p.success).count() as f64 / default_performance.len() as f64
        };

        let avg_default_latency = if default_performance.is_empty() {
            0.0
        } else {
            default_performance.iter().map(|p| p.latency_ms as f64).sum::<f64>() / default_performance.len() as f64
        };

        // Adjust strategy based on conditions
        state.current_strategy = if default_success_rate < 0.7 {
            RoutingStrategy::Failover
        } else if avg_default_latency > self.config.latency_switch_threshold as f64 {
            RoutingStrategy::LatencyAware
        } else if default_success_rate > 0.9 && avg_default_latency < 100.0 {
            RoutingStrategy::ProfitMaximizing
        } else {
            RoutingStrategy::Adaptive
        };
    }

    /// Update routing decision metrics
    fn update_routing_metrics(&self, decision: &RoutingDecision) {
        self.metrics.total_decisions.fetch_add(1, Ordering::Relaxed);
        
        match decision.processor {
            ProcessorType::Default => {
                self.metrics.default_decisions.fetch_add(1, Ordering::Relaxed);
            }
            ProcessorType::Fallback => {
                self.metrics.fallback_decisions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Update outcome metrics
    fn update_outcome_metrics(
        &self,
        processor: ProcessorType,
        success: bool,
        profit_impact: f64,
        latency_ms: u64,
    ) {
        if success {
            match processor {
                ProcessorType::Default => {
                    self.metrics.default_success_count.fetch_add(1, Ordering::Relaxed);
                }
                ProcessorType::Fallback => {
                    self.metrics.fallback_success_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Store profit impact as micros to avoid floating point atomics
        let profit_micros = (profit_impact * 1_000_000.0) as u64;
        self.metrics.total_profit_impact.fetch_add(profit_micros, Ordering::Relaxed);
    }
}

/// Snapshot of load balancer metrics
#[derive(Debug, Clone, Serialize)]
pub struct LoadBalancerMetricsSnapshot {
    pub total_decisions: u64,
    pub default_decisions: u64,
    pub fallback_decisions: u64,
    pub default_success_rate: f64,
    pub fallback_success_rate: f64,
    pub profit_maximization_rate: f64,
    pub total_profit_impact: f64,
    pub current_strategy: RoutingStrategy,
}

impl LoadBalancerMetricsSnapshot {
    /// Calculate profit efficiency (higher is better)
    pub fn profit_efficiency(&self) -> f64 {
        if self.total_decisions == 0 {
            return 0.0;
        }
        
        // Reward using default processor and high success rates
        let default_preference = self.default_decisions as f64 / self.total_decisions as f64;
        let avg_success_rate = (self.default_success_rate + self.fallback_success_rate) / 2.0;
        
        default_preference * avg_success_rate
    }
    
    /// Get performance recommendations
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if self.profit_maximization_rate < 0.8 {
            recommendations.push("Consider tuning circuit breaker to use default processor more often".to_string());
        }
        
        if self.default_success_rate < 0.9 && self.default_decisions > 100 {
            recommendations.push("Default processor showing high failure rate - investigate health".to_string());
        }
        
        if self.fallback_success_rate < 0.8 && self.fallback_decisions > 50 {
            recommendations.push("Fallback processor showing issues - monitor closely".to_string());
        }
        
        if self.total_profit_impact < 0.0 {
            recommendations.push("Negative profit impact detected - review routing strategy".to_string());
        }
        
        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::circuit_breaker::{CircuitBreakerConfig, DualCircuitBreaker};
    use crate::routing::predictor::LatencyPredictor;
    
    fn create_test_load_balancer() -> LoadBalancer {
        let circuit_breaker = Arc::new(DualCircuitBreaker::new(CircuitBreakerConfig::default()));
        let predictor = Arc::new(LatencyPredictor::new());
        let config = LoadBalancerConfig::default();
        
        LoadBalancer::new(circuit_breaker, predictor, config)
    }
    
    #[tokio::test]
    async fn test_profit_maximizing_strategy() {
        let lb = create_test_load_balancer();
        
        let decision = lb.route_request(100.0).await;
        
        // Should prefer default processor for maximum profit
        assert_eq!(decision.processor, ProcessorType::Default);
        assert!(decision.confidence > 0.8);
        assert!(decision.reasoning.contains("Profit maximizing"));
    }
    
    #[tokio::test]
    async fn test_routing_metrics() {
        let lb = create_test_load_balancer();
        
        // Make some routing decisions
        for _ in 0..10 {
            let decision = lb.route_request(100.0).await;
            lb.record_outcome(decision.processor, 50, true, 100.0).await;
        }
        
        let metrics = lb.get_metrics();
        assert_eq!(metrics.total_decisions, 10);
        assert!(metrics.default_success_rate > 0.0);
    }
    
    #[tokio::test]
    async fn test_adaptive_learning() {
        let lb = create_test_load_balancer();
        
        // Simulate poor performance from default processor
        for _ in 0..20 {
            lb.record_outcome(ProcessorType::Default, 1000, false, 100.0).await;
        }
        
        // Strategy should adapt
        let decision = lb.route_request(100.0).await;
        // Depending on circuit breaker state, might switch to fallback or adapt strategy
        assert!(decision.reasoning.contains("Failover") || decision.reasoning.contains("Adaptive"));
    }
    
    #[test]
    fn test_profit_calculation() {
        let lb = create_test_load_balancer();
        
        let default_profit = lb.calculate_profit_impact(ProcessorType::Default, 100.0, true);
        let fallback_profit = lb.calculate_profit_impact(ProcessorType::Fallback, 100.0, true);
        
        // Default should be more profitable
        assert!(default_profit > fallback_profit);
        
        // Failed transaction should be negative
        let failed_profit = lb.calculate_profit_impact(ProcessorType::Default, 100.0, false);
        assert!(failed_profit < 0.0);
    }
}