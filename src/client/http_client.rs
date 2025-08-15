//! High-performance HTTP client optimized for payment processor communication
//! 
//! This client is specifically optimized for the payment processor API patterns
//! with connection pooling, request batching, and intelligent retry logic.

use super::connection_pool::{ConnectionPool, PooledConnection};
use crate::storage::ProcessorType;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use uuid::Uuid;

/// Payment request payload for processors
#[derive(Debug, Serialize, Clone)]
pub struct PaymentRequest {
    #[serde(rename = "correlationId")]
    pub correlation_id: Uuid,
    pub amount: f64,
    #[serde(rename = "requestedAt")]
    pub requested_at: DateTime<Utc>,
}

/// Payment response from processors
#[derive(Debug, Deserialize)]
pub struct PaymentResponse {
    pub message: String,
}

/// Health check response from processors
#[derive(Debug, Deserialize)]
pub struct HealthResponse {
    pub failing: bool,
    #[serde(rename = "minResponseTime")]
    pub min_response_time: u32,
}

/// Payment processor endpoints
#[derive(Debug, Clone)]
pub struct ProcessorEndpoints {
    pub payments_url: String,
    pub health_url: String,
    pub admin_payments_summary_url: String,
}

impl ProcessorEndpoints {
    pub fn new(base_url: &str) -> Self {
        Self {
            payments_url: format!("{}/payments", base_url),
            health_url: format!("{}/payments/service-health", base_url),
            admin_payments_summary_url: format!("{}/admin/payments-summary", base_url),
        }
    }
}

/// HTTP client configuration
#[derive(Debug, Clone)]
pub struct HttpClientConfig {
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Maximum retries per request
    pub max_retries: u32,
    /// Retry delay base (exponential backoff)
    pub retry_delay_base: Duration,
    /// Connection pool size per processor
    pub connection_pool_size: usize,
    /// Keep-alive timeout
    pub keep_alive_timeout: Duration,
    /// Enable HTTP/2
    pub enable_http2: bool,
    /// User agent string
    pub user_agent: String,
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(500),
            request_timeout: Duration::from_millis(2000),
            max_retries: 2,
            retry_delay_base: Duration::from_millis(50),
            connection_pool_size: 20,
            keep_alive_timeout: Duration::from_secs(30),
            enable_http2: true,
            user_agent: "rinha-backend-2025/1.0".to_string(),
        }
    }
}

/// High-performance HTTP client for payment processors
pub struct HttpClient {
    /// Reqwest client instance
    client: reqwest::Client,
    
    /// Processor endpoints
    default_endpoints: ProcessorEndpoints,
    fallback_endpoints: ProcessorEndpoints,
    
    /// Connection pools
    connection_pools: ConnectionPools,
    
    /// Configuration
    config: HttpClientConfig,
    
    /// Performance metrics
    metrics: ClientMetrics,
}

/// Connection pools for both processors
struct ConnectionPools {
    default_pool: Arc<ConnectionPool>,
    fallback_pool: Arc<ConnectionPool>,
}

/// Client performance metrics
#[derive(Default)]
struct ClientMetrics {
    /// Request counts
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    retried_requests: AtomicU64,
    
    /// Timing metrics
    total_request_time_micros: AtomicU64,
    min_request_time_micros: AtomicU32,
    max_request_time_micros: AtomicU32,
    
    /// Per-processor metrics
    default_requests: AtomicU64,
    default_errors: AtomicU64,
    fallback_requests: AtomicU64,
    fallback_errors: AtomicU64,
    
    /// Health check metrics
    health_checks_performed: AtomicU64,
    health_check_failures: AtomicU64,
}

impl HttpClient {
    /// Create new HTTP client
    pub fn new(
        default_base_url: String,
        fallback_base_url: String,
        config: HttpClientConfig,
    ) -> Result<Self> {
        // Build reqwest client with optimizations
        let client = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .pool_idle_timeout(config.keep_alive_timeout)
            .pool_max_idle_per_host(config.connection_pool_size)
            .http2_prior_knowledge()
            .http2_keep_alive_interval(Some(Duration::from_secs(10)))
            .http2_keep_alive_timeout(Duration::from_secs(30))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .user_agent(&config.user_agent)
            .build()?;
        
        let default_endpoints = ProcessorEndpoints::new(&default_base_url);
        let fallback_endpoints = ProcessorEndpoints::new(&fallback_base_url);
        
        let connection_pools = ConnectionPools {
            default_pool: Arc::new(ConnectionPool::new(config.connection_pool_size)),
            fallback_pool: Arc::new(ConnectionPool::new(config.connection_pool_size)),
        };
        
        Ok(Self {
            client,
            default_endpoints,
            fallback_endpoints,
            connection_pools,
            config,
            metrics: ClientMetrics::default(),
        })
    }
    
    /// Create with default configuration
    pub fn with_defaults(default_url: String, fallback_url: String) -> Result<Self> {
        Self::new(default_url, fallback_url, HttpClientConfig::default())
    }
    
    /// Send payment request to specified processor
    pub async fn send_payment(
        &self,
        processor: ProcessorType,
        request: PaymentRequest,
    ) -> Result<PaymentResponse> {
        let start_time = Instant::now();
        
        // Get endpoints and connection pool
        let (endpoints, pool) = match processor {
            ProcessorType::Default => (&self.default_endpoints, &self.connection_pools.default_pool),
            ProcessorType::Fallback => (&self.fallback_endpoints, &self.connection_pools.fallback_pool),
        };
        
        // Update request metrics
        self.metrics.total_requests.fetch_add(1, Ordering::Relaxed);
        match processor {
            ProcessorType::Default => self.metrics.default_requests.fetch_add(1, Ordering::Relaxed),
            ProcessorType::Fallback => self.metrics.fallback_requests.fetch_add(1, Ordering::Relaxed),
        };
        
        // Acquire connection from pool
        let _connection = pool.acquire().await;
        
        // Perform request with retries
        let result = self.send_payment_with_retries(endpoints, request).await;
        
        // Update metrics
        let elapsed = start_time.elapsed();
        self.update_timing_metrics(elapsed);
        
        match &result {
            Ok(_) => {
                self.metrics.successful_requests.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.metrics.failed_requests.fetch_add(1, Ordering::Relaxed);
                match processor {
                    ProcessorType::Default => self.metrics.default_errors.fetch_add(1, Ordering::Relaxed),
                    ProcessorType::Fallback => self.metrics.fallback_errors.fetch_add(1, Ordering::Relaxed),
                };
            }
        }
        
        result
    }
    
    /// Check health of specified processor
    pub async fn check_health(&self, processor: ProcessorType) -> Result<HealthResponse> {
        let start_time = Instant::now();
        
        let endpoints = match processor {
            ProcessorType::Default => &self.default_endpoints,
            ProcessorType::Fallback => &self.fallback_endpoints,
        };
        
        self.metrics.health_checks_performed.fetch_add(1, Ordering::Relaxed);
        
        let result = self.client
            .get(&endpoints.health_url)
            .send()
            .await?
            .json::<HealthResponse>()
            .await;
        
        match &result {
            Ok(_) => {}
            Err(_) => {
                self.metrics.health_check_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        let elapsed = start_time.elapsed();
        self.update_timing_metrics(elapsed);
        
        result.map_err(|e| anyhow!("Health check failed: {}", e))
    }
    
    /// Batch health check for both processors
    pub async fn batch_health_check(&self) -> (Result<HealthResponse>, Result<HealthResponse>) {
        let (default_future, fallback_future) = tokio::join!(
            self.check_health(ProcessorType::Default),
            self.check_health(ProcessorType::Fallback)
        );
        
        (default_future, fallback_future)
    }
    
    /// Send multiple payment requests concurrently
    pub async fn send_batch_payments(
        &self,
        requests: Vec<(ProcessorType, PaymentRequest)>,
    ) -> Vec<Result<PaymentResponse>> {
        let futures = requests.into_iter().map(|(processor, request)| {
            self.send_payment(processor, request)
        });
        
        futures::future::join_all(futures).await
    }
    
    /// Get client performance metrics
    pub fn get_metrics(&self) -> HttpClientMetrics {
        let total_requests = self.metrics.total_requests.load(Ordering::Relaxed);
        let successful_requests = self.metrics.successful_requests.load(Ordering::Relaxed);
        let failed_requests = self.metrics.failed_requests.load(Ordering::Relaxed);
        let retried_requests = self.metrics.retried_requests.load(Ordering::Relaxed);
        
        let success_rate = if total_requests > 0 {
            successful_requests as f64 / total_requests as f64
        } else {
            0.0
        };
        
        let avg_request_time = if total_requests > 0 {
            self.metrics.total_request_time_micros.load(Ordering::Relaxed) / total_requests
        } else {
            0
        };
        
        let default_requests = self.metrics.default_requests.load(Ordering::Relaxed);
        let default_errors = self.metrics.default_errors.load(Ordering::Relaxed);
        let fallback_requests = self.metrics.fallback_requests.load(Ordering::Relaxed);
        let fallback_errors = self.metrics.fallback_errors.load(Ordering::Relaxed);
        
        let default_success_rate = if default_requests > 0 {
            (default_requests - default_errors) as f64 / default_requests as f64
        } else {
            0.0
        };
        
        let fallback_success_rate = if fallback_requests > 0 {
            (fallback_requests - fallback_errors) as f64 / fallback_requests as f64
        } else {
            0.0
        };
        
        let health_checks = self.metrics.health_checks_performed.load(Ordering::Relaxed);
        let health_failures = self.metrics.health_check_failures.load(Ordering::Relaxed);
        let health_success_rate = if health_checks > 0 {
            (health_checks - health_failures) as f64 / health_checks as f64
        } else {
            1.0
        };
        
        HttpClientMetrics {
            total_requests,
            successful_requests,
            failed_requests,
            retried_requests,
            success_rate,
            avg_request_time_micros: avg_request_time,
            min_request_time_micros: self.metrics.min_request_time_micros.load(Ordering::Relaxed),
            max_request_time_micros: self.metrics.max_request_time_micros.load(Ordering::Relaxed),
            default_processor: ProcessorMetrics {
                requests: default_requests,
                errors: default_errors,
                success_rate: default_success_rate,
            },
            fallback_processor: ProcessorMetrics {
                requests: fallback_requests,
                errors: fallback_errors,
                success_rate: fallback_success_rate,
            },
            health_checks_performed: health_checks,
            health_success_rate,
            connection_pool_utilization: self.get_connection_pool_utilization(),
        }
    }
    
    /// Reset client metrics
    pub fn reset_metrics(&self) {
        self.metrics.total_requests.store(0, Ordering::Relaxed);
        self.metrics.successful_requests.store(0, Ordering::Relaxed);
        self.metrics.failed_requests.store(0, Ordering::Relaxed);
        self.metrics.retried_requests.store(0, Ordering::Relaxed);
        self.metrics.total_request_time_micros.store(0, Ordering::Relaxed);
        self.metrics.min_request_time_micros.store(u32::MAX, Ordering::Relaxed);
        self.metrics.max_request_time_micros.store(0, Ordering::Relaxed);
        self.metrics.default_requests.store(0, Ordering::Relaxed);
        self.metrics.default_errors.store(0, Ordering::Relaxed);
        self.metrics.fallback_requests.store(0, Ordering::Relaxed);
        self.metrics.fallback_errors.store(0, Ordering::Relaxed);
        self.metrics.health_checks_performed.store(0, Ordering::Relaxed);
        self.metrics.health_check_failures.store(0, Ordering::Relaxed);
    }
    
    /// Send payment with retry logic
    async fn send_payment_with_retries(
        &self,
        endpoints: &ProcessorEndpoints,
        request: PaymentRequest,
    ) -> Result<PaymentResponse> {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            let result = self.client
                .post(&endpoints.payments_url)
                .json(&request)
                .send()
                .await;
            
            match result {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<PaymentResponse>().await {
                            Ok(payment_response) => {
                                if attempt > 0 {
                                    self.metrics.retried_requests.fetch_add(1, Ordering::Relaxed);
                                }
                                return Ok(payment_response);
                            }
                            Err(e) => {
                                last_error = Some(anyhow!("JSON parse error: {}", e));
                            }
                        }
                    } else {
                        last_error = Some(anyhow!("HTTP error: {}", response.status()));
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow!("Request error: {}", e));
                }
            }
            
            // Wait before retry (exponential backoff)
            if attempt < self.config.max_retries {
                let delay = self.config.retry_delay_base * (2_u32.pow(attempt));
                tokio::time::sleep(delay).await;
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow!("Unknown error after retries")))
    }
    
    /// Update timing metrics
    fn update_timing_metrics(&self, elapsed: Duration) {
        let elapsed_micros = elapsed.as_micros() as u64;
        let elapsed_micros_u32 = elapsed_micros.min(u32::MAX as u64) as u32;
        
        self.metrics.total_request_time_micros.fetch_add(elapsed_micros, Ordering::Relaxed);
        
        // Update min time
        let mut current_min = self.metrics.min_request_time_micros.load(Ordering::Relaxed);
        while elapsed_micros_u32 < current_min {
            match self.metrics.min_request_time_micros.compare_exchange_weak(
                current_min,
                elapsed_micros_u32,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }
        
        // Update max time
        let mut current_max = self.metrics.max_request_time_micros.load(Ordering::Relaxed);
        while elapsed_micros_u32 > current_max {
            match self.metrics.max_request_time_micros.compare_exchange_weak(
                current_max,
                elapsed_micros_u32,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }
    
    /// Get connection pool utilization
    fn get_connection_pool_utilization(&self) -> ConnectionPoolUtilization {
        ConnectionPoolUtilization {
            default_available: self.connection_pools.default_pool.available(),
            default_total: self.config.connection_pool_size,
            fallback_available: self.connection_pools.fallback_pool.available(),
            fallback_total: self.config.connection_pool_size,
        }
    }
}

/// HTTP client metrics
#[derive(Debug, Clone)]
pub struct HttpClientMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub retried_requests: u64,
    pub success_rate: f64,
    pub avg_request_time_micros: u64,
    pub min_request_time_micros: u32,
    pub max_request_time_micros: u32,
    pub default_processor: ProcessorMetrics,
    pub fallback_processor: ProcessorMetrics,
    pub health_checks_performed: u64,
    pub health_success_rate: f64,
    pub connection_pool_utilization: ConnectionPoolUtilization,
}

/// Per-processor metrics
#[derive(Debug, Clone)]
pub struct ProcessorMetrics {
    pub requests: u64,
    pub errors: u64,
    pub success_rate: f64,
}

/// Connection pool utilization metrics
#[derive(Debug, Clone)]
pub struct ConnectionPoolUtilization {
    pub default_available: usize,
    pub default_total: usize,
    pub fallback_available: usize,
    pub fallback_total: usize,
}

impl ConnectionPoolUtilization {
    /// Get utilization percentage for default processor
    pub fn default_utilization_percent(&self) -> f64 {
        if self.default_total == 0 {
            0.0
        } else {
            ((self.default_total - self.default_available) as f64 / self.default_total as f64) * 100.0
        }
    }
    
    /// Get utilization percentage for fallback processor
    pub fn fallback_utilization_percent(&self) -> f64 {
        if self.fallback_total == 0 {
            0.0
        } else {
            ((self.fallback_total - self.fallback_available) as f64 / self.fallback_total as f64) * 100.0
        }
    }
}

impl HttpClientMetrics {
    /// Calculate overall performance score (0.0 to 1.0)
    pub fn performance_score(&self) -> f64 {
        let success_component = self.success_rate * 0.4;
        let latency_component = if self.avg_request_time_micros > 0 {
            (1_000_000.0 / self.avg_request_time_micros as f64).min(1.0) * 0.3
        } else {
            0.0
        };
        let health_component = self.health_success_rate * 0.2;
        let retry_component = if self.total_requests > 0 {
            (1.0 - (self.retried_requests as f64 / self.total_requests as f64)).max(0.0) * 0.1
        } else {
            1.0
        };
        
        success_component + latency_component + health_component + retry_component
    }
    
    /// Get performance recommendations
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if self.success_rate < 0.95 {
            recommendations.push(format!(
                "Success rate is low ({:.1}%) - investigate processor issues",
                self.success_rate * 100.0
            ));
        }
        
        if self.avg_request_time_micros > 5000 {
            recommendations.push(format!(
                "Average request time is high ({}μs) - consider optimization",
                self.avg_request_time_micros
            ));
        }
        
        if self.retried_requests > self.total_requests / 10 {
            recommendations.push("High retry rate - check network stability".to_string());
        }
        
        let default_util = self.connection_pool_utilization.default_utilization_percent();
        let fallback_util = self.connection_pool_utilization.fallback_utilization_percent();
        
        if default_util > 80.0 {
            recommendations.push(format!(
                "Default processor connection pool highly utilized ({:.1}%)",
                default_util
            ));
        }
        
        if fallback_util > 80.0 {
            recommendations.push(format!(
                "Fallback processor connection pool highly utilized ({:.1}%)",
                fallback_util
            ));
        }
        
        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_processor_endpoints() {
        let endpoints = ProcessorEndpoints::new("http://localhost:8080");
        
        assert_eq!(endpoints.payments_url, "http://localhost:8080/payments");
        assert_eq!(endpoints.health_url, "http://localhost:8080/payments/service-health");
        assert_eq!(endpoints.admin_payments_summary_url, "http://localhost:8080/admin/payments-summary");
    }
    
    #[test]
    fn test_http_client_config() {
        let config = HttpClientConfig::default();
        
        assert_eq!(config.connect_timeout, Duration::from_millis(500));
        assert_eq!(config.request_timeout, Duration::from_millis(2000));
        assert_eq!(config.max_retries, 2);
        assert_eq!(config.connection_pool_size, 20);
    }
    
    #[test]
    fn test_connection_pool_utilization() {
        let utilization = ConnectionPoolUtilization {
            default_available: 5,
            default_total: 10,
            fallback_available: 8,
            fallback_total: 10,
        };
        
        assert_eq!(utilization.default_utilization_percent(), 50.0);
        assert_eq!(utilization.fallback_utilization_percent(), 20.0);
    }
    
    #[test]
    fn test_metrics_performance_score() {
        let metrics = HttpClientMetrics {
            total_requests: 100,
            successful_requests: 95,
            failed_requests: 5,
            retried_requests: 10,
            success_rate: 0.95,
            avg_request_time_micros: 1000,
            min_request_time_micros: 500,
            max_request_time_micros: 2000,
            default_processor: ProcessorMetrics {
                requests: 80,
                errors: 2,
                success_rate: 0.975,
            },
            fallback_processor: ProcessorMetrics {
                requests: 20,
                errors: 3,
                success_rate: 0.85,
            },
            health_checks_performed: 20,
            health_success_rate: 0.9,
            connection_pool_utilization: ConnectionPoolUtilization {
                default_available: 15,
                default_total: 20,
                fallback_available: 18,
                fallback_total: 20,
            },
        };
        
        let score = metrics.performance_score();
        assert!(score > 0.7); // Should be a good score
        
        let recommendations = metrics.get_recommendations();
        // Should have some recommendations due to high retry rate
        assert!(!recommendations.is_empty());
    }
}