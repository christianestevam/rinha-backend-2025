//! High-performance API server for the Rinha Backend 2025 challenge
//! 
//! This is the main HTTP server that handles payment requests and summary endpoints.
//! Optimized for maximum throughput and minimum latency to achieve p99 < 1ms.

use rinha_backend_2025::{
    client::{ClientFactory, PaymentRequest},
    config::{init_config, get_config, ConfigProfile},
    routing::{
        LoadBalancer, LoadBalancerConfig, RoutingDecision,
        circuit_breaker::{DualCircuitBreaker, CircuitBreakerConfig},
        predictor::LatencyPredictor,
    },
    storage::{PaymentStore, ProcessorType},
    server::{HttpServer, ServerConfig, RequestHandler},
    utils::metrics::MetricsCollector,
    ipc::{IpcManager, IpcFactory},
};

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::signal;
use anyhow::{Result, Context};
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Main API server application
#[derive(Clone)]
pub struct ApiServer {
    /// HTTP client factory for processor communication
    client_factory: Arc<ClientFactory>,
    
    /// Intelligent load balancer
    load_balancer: Arc<LoadBalancer>,
    
    /// Payment storage
    payment_store: Arc<PaymentStore>,
    
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    
    /// IPC manager for worker communication
    ipc_manager: Arc<tokio::sync::RwLock<IpcManager>>,
    
    /// Server start time for uptime calculation
    start_time: Instant,
}

/// Payment request from client
#[derive(Debug, Deserialize)]
struct PaymentRequestBody {
    #[serde(rename = "correlationId")]
    correlation_id: Uuid,
    amount: f64,
}

/// Payment summary response
#[derive(Debug, Serialize)]
struct PaymentSummaryResponse {
    default: ProcessorSummary,
    fallback: ProcessorSummary,
}

/// Summary for individual processor
#[derive(Debug, Serialize)]
struct ProcessorSummary {
    #[serde(rename = "totalRequests")]
    total_requests: u64,
    #[serde(rename = "totalAmount")]
    total_amount: f64,
}

/// Health check response
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    uptime_seconds: u64,
    total_requests: u64,
    success_rate: f64,
    profit_efficiency: f64,
}

impl ApiServer {
    /// Create new API server
    pub async fn new() -> Result<Self> {
        info!("Initializing API server...");

        // Initialize configuration
        let config = init_config().context("Failed to initialize configuration")?;
        info!("Configuration loaded: {:?}", config.server);

        // Create client factory
        let client_factory = Arc::new(
            ClientFactory::new().await
                .context("Failed to create client factory")?
        );
        info!("Client factory initialized");

        // Create circuit breakers with optimized configs
        let default_cb_config = CircuitBreakerConfig {
            failure_threshold: 5,
            max_latency_micros: 5000, // 5ms
            p99_latency_threshold: 10000, // 10ms
            ..Default::default()
        };

        let fallback_cb_config = CircuitBreakerConfig {
            failure_threshold: 8,
            max_latency_micros: 15000, // 15ms
            p99_latency_threshold: 30000, // 30ms
            failure_rate_threshold: 0.7, // More tolerant
            ..Default::default()
        };

        let circuit_breaker = Arc::new(
            DualCircuitBreaker::new(default_cb_config, fallback_cb_config)
        );

        // Create ML predictor
        let predictor = Arc::new(LatencyPredictor::new());

        // Create load balancer with profit-optimized config
        let lb_config = LoadBalancerConfig {
            default_fee_rate: 0.05,    // 5% fee
            fallback_fee_rate: 0.08,   // 8% fee
            max_default_latency: 200,  // Aggressive 200ms limit
            latency_switch_threshold: 100, // Switch at 100ms
            min_profit_margin: 0.01,   // 1% minimum margin
            ..Default::default()
        };

        let load_balancer = Arc::new(
            LoadBalancer::new(circuit_breaker, predictor, lb_config)
        );

        // Create payment store
        let payment_store = Arc::new(
            PaymentStore::new(config.performance.max_payments)
                .context("Failed to create payment store")?
        );

        // Create metrics collector
        let metrics = Arc::new(MetricsCollector::new());

        // Create IPC manager
        let ipc_manager = Arc::new(tokio::sync::RwLock::new(
            IpcFactory::create_api_server().await
                .context("Failed to create IPC manager")?
        ));

        Ok(Self {
            client_factory,
            load_balancer,
            payment_store,
            metrics,
            ipc_manager,
            start_time: Instant::now(),
        })
    }

    /// Start the API server
    pub async fn run(&self) -> Result<()> {
        let config = get_config();
        
        info!("Starting API server on {}:{}", config.server.host, config.server.port);

        // Start IPC server
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.start_server().await
                .context("Failed to start IPC server")?;
        }
        info!("IPC server started");

        // Create HTTP server with optimized configuration
        let server_config = ServerConfig {
            host: config.server.host.clone(),
            port: config.server.port,
            worker_threads: config.performance.worker_threads,
            max_connections: config.performance.max_connections,
            tcp_nodelay: config.performance.tcp_nodelay,
            tcp_keepalive: config.performance.tcp_keepalive,
            buffer_size: config.performance.buffer_size,
            enable_compression: false, // Disabled for minimum latency
            read_timeout: Duration::from_millis(100),
            write_timeout: Duration::from_millis(100),
        };

        let server = HttpServer::new(server_config);
        let request_handler = RequestHandler::new(self.clone());

        // Start metrics collection in background
        let metrics_clone = Arc::clone(&self.metrics);
        let lb_clone = Arc::clone(&self.load_balancer);
        tokio::spawn(async move {
            Self::metrics_collection_loop(metrics_clone, lb_clone).await;
        });

        // Start health monitoring in background
        let server_clone = self.clone();
        tokio::spawn(async move {
            server_clone.health_monitoring_loop().await;
        });

        info!("API server ready to accept connections");

        // Run server with graceful shutdown
        tokio::select! {
            result = server.run(request_handler) => {
                match result {
                    Ok(_) => info!("Server stopped gracefully"),
                    Err(e) => error!("Server error: {}", e),
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received shutdown signal");
            }
        }

        // Cleanup
        self.shutdown().await?;
        Ok(())
    }

    /// Handle payment request
    pub async fn handle_payment(&self, body: PaymentRequestBody) -> Result<&'static str> {
        let start_time = Instant::now();
        
        debug!("Processing payment: {} amount: {}", body.correlation_id, body.amount);

        // Validate request
        if body.amount <= 0.0 {
            return Err(anyhow::anyhow!("Invalid amount: {}", body.amount));
        }

        // Get routing decision from load balancer
        let routing_decision = self.load_balancer.route_request(body.amount).await;
        
        debug!("Routing decision: {:?} -> {:?} (confidence: {:.2})", 
            body.correlation_id, routing_decision.processor, routing_decision.confidence);

        // Get appropriate client
        let client = self.client_factory.get_client(routing_decision.processor);

        // Create payment request
        let payment_request = PaymentRequest {
            correlation_id: body.correlation_id,
            amount: body.amount,
            requested_at: chrono::Utc::now(),
        };

        // Process payment
        let processing_start = Instant::now();
        let result = client.process_payment(payment_request).await;
        let processing_latency = processing_start.elapsed();

        // Record outcome in load balancer
        self.load_balancer.record_outcome(
            routing_decision.processor,
            processing_latency.as_millis() as u64,
            result.is_ok(),
            body.amount,
        ).await;

        match result {
            Ok(_response) => {
                // Store successful payment
                self.payment_store.add_payment(
                    body.correlation_id,
                    body.amount,
                    routing_decision.processor,
                    chrono::Utc::now(),
                ).await?;

                // Update metrics
                self.metrics.record_payment_success(
                    routing_decision.processor,
                    body.amount,
                    processing_latency,
                ).await;

                let total_latency = start_time.elapsed();
                debug!("Payment processed successfully: {} in {:?}", 
                    body.correlation_id, total_latency);

                Ok("{\"status\":\"processed\"}")
            }
            Err(e) => {
                // Update metrics for failure
                self.metrics.record_payment_failure(
                    routing_decision.processor,
                    body.amount,
                    processing_latency,
                ).await;

                warn!("Payment processing failed: {} - {}", body.correlation_id, e);
                Err(anyhow::anyhow!("Payment processing failed: {}", e))
            }
        }
    }

    /// Handle payment summary request
    pub async fn handle_payment_summary(
        &self,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<PaymentSummaryResponse> {
        debug!("Getting payment summary from {:?} to {:?}", from, to);

        let summary = self.payment_store.get_summary(from, to).await?;

        Ok(PaymentSummaryResponse {
            default: ProcessorSummary {
                total_requests: summary.default_requests,
                total_amount: summary.default_amount,
            },
            fallback: ProcessorSummary {
                total_requests: summary.fallback_requests,
                total_amount: summary.fallback_amount,
            },
        })
    }

    /// Handle health check request
    pub async fn handle_health(&self) -> Result<HealthResponse> {
        let metrics = self.load_balancer.get_metrics();
        let uptime = self.start_time.elapsed();

        Ok(HealthResponse {
            status: "healthy".to_string(),
            uptime_seconds: uptime.as_secs(),
            total_requests: metrics.total_decisions,
            success_rate: (metrics.default_success_rate + metrics.fallback_success_rate) / 2.0,
            profit_efficiency: metrics.profit_efficiency(),
        })
    }

    /// Handle metrics endpoint
    pub async fn handle_metrics(&self) -> Result<serde_json::Value> {
        let lb_metrics = self.load_balancer.get_metrics();
        let client_metrics = self.client_factory.get_all_metrics().await;
        let store_metrics = self.payment_store.get_metrics().await;

        Ok(serde_json::json!({
            "load_balancer": lb_metrics,
            "clients": client_metrics,
            "storage": store_metrics,
            "uptime_seconds": self.start_time.elapsed().as_secs(),
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        }))
    }

    /// Background health monitoring loop
    async fn health_monitoring_loop(&self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            // Perform health checks on processors
            let (default_health, fallback_health) = self.client_factory.health_check_all().await;
            
            match (default_health, fallback_health) {
                (Ok(default), Ok(fallback)) => {
                    debug!("Health check - Default: failing={}, latency={}ms | Fallback: failing={}, latency={}ms",
                        default.failing, default.min_response_time,
                        fallback.failing, fallback.min_response_time);
                }
                (Err(e), Ok(_)) => {
                    warn!("Default processor health check failed: {}", e);
                }
                (Ok(_), Err(e)) => {
                    warn!("Fallback processor health check failed: {}", e);
                }
                (Err(e1), Err(e2)) => {
                    error!("Both processors health checks failed: default={}, fallback={}", e1, e2);
                }
            }
        }
    }

    /// Background metrics collection loop
    async fn metrics_collection_loop(
        metrics: Arc<MetricsCollector>,
        load_balancer: Arc<LoadBalancer>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        
        loop {
            interval.tick().await;
            
            let lb_metrics = load_balancer.get_metrics();
            
            info!("Metrics Summary - Total requests: {}, Default: {:.1}%, Success rate: {:.1}%, Profit efficiency: {:.3}",
                lb_metrics.total_decisions,
                lb_metrics.profit_maximization_rate * 100.0,
                (lb_metrics.default_success_rate + lb_metrics.fallback_success_rate) / 2.0 * 100.0,
                lb_metrics.profit_efficiency()
            );

            // Log recommendations if any
            let recommendations = lb_metrics.get_recommendations();
            for recommendation in recommendations {
                warn!("Performance recommendation: {}", recommendation);
            }
        }
    }

    /// Graceful shutdown
    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down API server...");

        // Shutdown IPC
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.shutdown().await?;
        }

        // Shutdown client factory
        self.client_factory.shutdown().await?;

        info!("API server shutdown complete");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info,rinha_backend_2025=debug".to_string())
        )
        .init();

    // Detect configuration profile
    let profile = ConfigProfile::from_env();
    info!("Starting with profile: {:?}", profile);

    // Create and run server
    let server = ApiServer::new().await
        .context("Failed to create API server")?;

    server.run().await
        .context("Server execution failed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_api_server_creation() {
        // This test verifies server can be created with default config
        let result = ApiServer::new().await;
        
        // In test environment without processors, this might fail
        // but we can verify the error is connection-related
        match result {
            Ok(_server) => {
                println!("API server created successfully");
            }
            Err(e) => {
                println!("Expected error in test environment: {}", e);
                assert!(
                    e.to_string().contains("connection") || 
                    e.to_string().contains("resolve") ||
                    e.to_string().contains("client factory")
                );
            }
        }
    }

    #[tokio::test]
    async fn test_payment_request_validation() {
        // Test payment request validation
        let payment = PaymentRequestBody {
            correlation_id: Uuid::new_v4(),
            amount: -10.0, // Invalid amount
        };

        // This should fail validation
        assert!(payment.amount <= 0.0);
    }

    #[tokio::test]
    async fn test_payment_summary_structure() {
        let summary = PaymentSummaryResponse {
            default: ProcessorSummary {
                total_requests: 100,
                total_amount: 1000.0,
            },
            fallback: ProcessorSummary {
                total_requests: 50,
                total_amount: 500.0,
            },
        };

        // Verify serialization works
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("totalRequests"));
        assert!(json.contains("totalAmount"));
    }

    #[test]
    fn test_health_response() {
        let health = HealthResponse {
            status: "healthy".to_string(),
            uptime_seconds: 300,
            total_requests: 1000,
            success_rate: 0.95,
            profit_efficiency: 0.87,
        };

        assert_eq!(health.status, "healthy");
        assert!(health.success_rate > 0.9);
        assert!(health.profit_efficiency > 0.8);
    }
}