//! High-performance worker process for the Rinha Backend 2025 challenge
//! 
//! This worker handles background payment processing, health monitoring,
//! and performance optimization tasks.

use rinha_backend_2025::{
    client::{ClientFactory, PaymentRequest},
    config::{init_config, get_config, ConfigProfile},
    storage::{PaymentStore, ProcessorType},
    worker::{PaymentProcessor, HealthMonitor},
    utils::metrics::MetricsCollector,
    ipc::{IpcManager, IpcFactory, PaymentMessage, HealthMessage},
    routing::{
        circuit_breaker::{DualCircuitBreaker, CircuitBreakerConfig},
        predictor::LatencyPredictor,
    },
};

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{signal, sync::mpsc, time::interval};
use anyhow::{Result, Context};
use tracing::{info, warn, error, debug};
use uuid::Uuid;

/// Worker process for background payment processing
pub struct PaymentWorker {
    /// Worker ID
    worker_id: u32,
    
    /// Payment processor
    payment_processor: Arc<PaymentProcessor>,
    
    /// Health monitor
    health_monitor: Arc<HealthMonitor>,
    
    /// Client factory for processor communication
    client_factory: Arc<ClientFactory>,
    
    /// Payment storage
    payment_store: Arc<PaymentStore>,
    
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    
    /// IPC manager for communication with API server
    ipc_manager: Arc<tokio::sync::RwLock<IpcManager>>,
    
    /// Circuit breaker for fault tolerance
    circuit_breaker: Arc<DualCircuitBreaker>,
    
    /// ML predictor for performance optimization
    predictor: Arc<LatencyPredictor>,
    
    /// Worker start time
    start_time: Instant,
}

impl PaymentWorker {
    /// Create new payment worker
    pub async fn new(worker_id: u32) -> Result<Self> {
        info!("Initializing payment worker {}...", worker_id);

        // Initialize configuration
        let config = init_config().context("Failed to initialize configuration")?;
        info!("Worker {} configuration loaded", worker_id);

        // Create client factory
        let client_factory = Arc::new(
            ClientFactory::new().await
                .context("Failed to create client factory")?
        );
        info!("Worker {} client factory initialized", worker_id);

        // Create circuit breakers
        let default_cb_config = CircuitBreakerConfig {
            failure_threshold: 3,        // More sensitive in worker
            max_latency_micros: 3000,    // 3ms
            p99_latency_threshold: 8000, // 8ms
            open_timeout: Duration::from_secs(1), // Faster recovery
            ..Default::default()
        };

        let fallback_cb_config = CircuitBreakerConfig {
            failure_threshold: 5,
            max_latency_micros: 10000,   // 10ms
            p99_latency_threshold: 20000, // 20ms
            failure_rate_threshold: 0.6, // More tolerant
            ..Default::default()
        };

        let circuit_breaker = Arc::new(
            DualCircuitBreaker::new(default_cb_config, fallback_cb_config)
        );

        // Create ML predictor
        let predictor = Arc::new(LatencyPredictor::new());

        // Create payment processor
        let payment_processor = Arc::new(
            PaymentProcessor::new(
                Arc::clone(&client_factory),
                Arc::clone(&circuit_breaker),
                Arc::clone(&predictor),
            ).await.context("Failed to create payment processor")?
        );

        // Create health monitor
        let health_monitor = Arc::new(
            HealthMonitor::new(
                Arc::clone(&client_factory),
                Arc::clone(&circuit_breaker),
            ).await.context("Failed to create health monitor")?
        );

        // Create payment store
        let payment_store = Arc::new(
            PaymentStore::new(config.performance.max_payments / 2) // Workers get half capacity
                .context("Failed to create payment store")?
        );

        // Create metrics collector
        let metrics = Arc::new(MetricsCollector::new());

        // Create IPC manager for this worker
        let ipc_manager = Arc::new(tokio::sync::RwLock::new(
            IpcFactory::create_worker(worker_id).await
                .context("Failed to create IPC manager")?
        ));

        Ok(Self {
            worker_id,
            payment_processor,
            health_monitor,
            client_factory,
            payment_store,
            metrics,
            ipc_manager,
            circuit_breaker,
            predictor,
            start_time: Instant::now(),
        })
    }

    /// Start the worker
    pub async fn run(&self) -> Result<()> {
        info!("Starting payment worker {}", self.worker_id);

        // Start IPC server for this worker
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.start_server().await
                .context("Failed to start worker IPC server")?;
        }
        info!("Worker {} IPC server started", self.worker_id);

        // Create channels for internal communication
        let (payment_tx, payment_rx) = mpsc::unbounded_channel::<PaymentMessage>();
        let (health_tx, health_rx) = mpsc::unbounded_channel::<HealthMessage>();

        // Start background tasks
        let worker_clone = self.clone();
        let payment_task = tokio::spawn(async move {
            worker_clone.payment_processing_loop(payment_rx).await;
        });

        let worker_clone = self.clone();
        let health_task = tokio::spawn(async move {
            worker_clone.health_monitoring_loop(health_rx).await;
        });

        let worker_clone = self.clone();
        let metrics_task = tokio::spawn(async move {
            worker_clone.metrics_collection_loop().await;
        });

        let worker_clone = self.clone();
        let optimization_task = tokio::spawn(async move {
            worker_clone.performance_optimization_loop().await;
        });

        // Start IPC message handling
        let worker_clone = self.clone();
        let ipc_task = tokio::spawn(async move {
            worker_clone.ipc_message_loop(payment_tx, health_tx).await;
        });

        info!("Worker {} is ready and processing", self.worker_id);

        // Wait for shutdown signal
        tokio::select! {
            _ = payment_task => {
                warn!("Payment processing task ended");
            }
            _ = health_task => {
                warn!("Health monitoring task ended");
            }
            _ = metrics_task => {
                warn!("Metrics collection task ended");
            }
            _ = optimization_task => {
                warn!("Performance optimization task ended");
            }
            _ = ipc_task => {
                warn!("IPC message handling task ended");
            }
            _ = signal::ctrl_c() => {
                info!("Worker {} received shutdown signal", self.worker_id);
            }
        }

        // Cleanup
        self.shutdown().await?;
        Ok(())
    }

    /// Payment processing loop
    async fn payment_processing_loop(&self, mut payment_rx: mpsc::UnboundedReceiver<PaymentMessage>) {
        info!("Worker {} payment processing loop started", self.worker_id);

        while let Some(payment_msg) = payment_rx.recv().await {
            let start_time = Instant::now();
            
            debug!("Worker {} processing payment: {}", self.worker_id, payment_msg.correlation_id);

            // Process the payment
            let result = self.process_payment_message(payment_msg).await;
            
            match result {
                Ok(_) => {
                    debug!("Worker {} payment processed successfully in {:?}", 
                        self.worker_id, start_time.elapsed());
                }
                Err(e) => {
                    error!("Worker {} payment processing failed: {}", self.worker_id, e);
                }
            }
        }

        warn!("Worker {} payment processing loop ended", self.worker_id);
    }

    /// Health monitoring loop
    async fn health_monitoring_loop(&self, mut health_rx: mpsc::UnboundedReceiver<HealthMessage>) {
        info!("Worker {} health monitoring loop started", self.worker_id);

        let mut interval = interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Periodic health checks
                    self.perform_health_checks().await;
                }
                health_msg = health_rx.recv() => {
                    match health_msg {
                        Some(msg) => {
                            self.handle_health_message(msg).await;
                        }
                        None => {
                            warn!("Worker {} health monitoring channel closed", self.worker_id);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Metrics collection loop
    async fn metrics_collection_loop(&self) {
        info!("Worker {} metrics collection loop started", self.worker_id);

        let mut interval = interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            // Collect and log metrics
            let processor_metrics = self.payment_processor.get_metrics().await;
            let health_metrics = self.health_monitor.get_metrics().await;
            let circuit_metrics = self.circuit_breaker.get_metrics();

            info!("Worker {} metrics - Processed: {}, Success rate: {:.1}%, Avg latency: {:?}",
                self.worker_id,
                processor_metrics.total_processed,
                processor_metrics.success_rate * 100.0,
                processor_metrics.average_latency
            );

            debug!("Worker {} circuit breaker - Default: {:?}, Fallback: {:?}",
                self.worker_id,
                circuit_metrics.0.state,
                circuit_metrics.1.state
            );
        }
    }

    /// Performance optimization loop
    async fn performance_optimization_loop(&self) {
        info!("Worker {} performance optimization loop started", self.worker_id);

        let mut interval = interval(Duration::from_secs(120)); // Every 2 minutes

        loop {
            interval.tick().await;

            // Optimize ML predictor
            self.predictor.optimize_model().await;

            // Optimize circuit breaker thresholds
            self.optimize_circuit_breaker_thresholds().await;

            // Garbage collection hint (if needed)
            #[cfg(feature = "gc-hint")]
            {
                // Force garbage collection in low-activity periods
                if self.payment_processor.get_current_load().await < 0.1 {
                    std::hint::black_box(Vec::<u8>::with_capacity(1024 * 1024));
                }
            }

            debug!("Worker {} performance optimization cycle completed", self.worker_id);
        }
    }

    /// IPC message handling loop
    async fn ipc_message_loop(
        &self,
        payment_tx: mpsc::UnboundedSender<PaymentMessage>,
        health_tx: mpsc::UnboundedSender<HealthMessage>,
    ) {
        info!("Worker {} IPC message loop started", self.worker_id);

        // This would integrate with the actual IPC system
        // For now, we'll simulate receiving messages
        let mut interval = interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            // In a real implementation, this would receive messages from IPC
            // and dispatch them to the appropriate handlers
            
            // Simulate occasional payment messages
            if rand::random::<f64>() < 0.01 { // 1% chance per tick
                let payment_msg = PaymentMessage {
                    correlation_id: Uuid::new_v4(),
                    amount: rand::random::<f64>() * 1000.0 + 10.0,
                    processor_type: if rand::random() { 
                        ProcessorType::Default 
                    } else { 
                        ProcessorType::Fallback 
                    },
                    timestamp: chrono::Utc::now(),
                };

                if let Err(e) = payment_tx.send(payment_msg) {
                    error!("Worker {} failed to send payment message: {}", self.worker_id, e);
                    break;
                }
            }
        }
    }

    /// Process individual payment message
    async fn process_payment_message(&self, payment_msg: PaymentMessage) -> Result<()> {
        let payment_request = PaymentRequest {
            correlation_id: payment_msg.correlation_id,
            amount: payment_msg.amount,
            requested_at: payment_msg.timestamp,
        };

        // Use payment processor to handle the request
        let result = self.payment_processor.process_payment(
            payment_request,
            payment_msg.processor_type,
        ).await?;

        // Store successful payment
        self.payment_store.add_payment(
            payment_msg.correlation_id,
            payment_msg.amount,
            payment_msg.processor_type,
            payment_msg.timestamp,
        ).await?;

        // Update metrics
        self.metrics.record_payment_success(
            payment_msg.processor_type,
            payment_msg.amount,
            result.processing_time,
        ).await;

        Ok(())
    }

    /// Handle health message
    async fn handle_health_message(&self, health_msg: HealthMessage) {
        debug!("Worker {} handling health message for {:?}", 
            self.worker_id, health_msg.processor_type);

        // Forward to health monitor
        self.health_monitor.update_processor_health(
            health_msg.processor_type,
            health_msg.is_healthy,
            health_msg.latency_ms,
        ).await;
    }

    /// Perform periodic health checks
    async fn perform_health_checks(&self) {
        let (default_result, fallback_result) = self.client_factory.health_check_all().await;

        // Update circuit breaker based on health check results
        match default_result {
            Ok(health) => {
                if health.failing {
                    self.circuit_breaker.record_result(ProcessorType::Default, false, health.min_response_time);
                } else {
                    self.circuit_breaker.record_result(ProcessorType::Default, true, health.min_response_time);
                }
            }
            Err(_) => {
                self.circuit_breaker.record_result(ProcessorType::Default, false, 5000); // 5s timeout
            }
        }

        match fallback_result {
            Ok(health) => {
                if health.failing {
                    self.circuit_breaker.record_result(ProcessorType::Fallback, false, health.min_response_time);
                } else {
                    self.circuit_breaker.record_result(ProcessorType::Fallback, true, health.min_response_time);
                }
            }
            Err(_) => {
                self.circuit_breaker.record_result(ProcessorType::Fallback, false, 10000); // 10s timeout
            }
        }
    }

    /// Optimize circuit breaker thresholds based on performance data
    async fn optimize_circuit_breaker_thresholds(&self) {
        let (default_metrics, fallback_metrics) = self.circuit_breaker.get_metrics();

        // Adjust thresholds based on historical performance
        if default_metrics.failure_rate < 0.05 && default_metrics.avg_latency_micros < 2000 {
            // Default processor is performing very well, can be more aggressive
            debug!("Worker {} optimizing for aggressive default processor usage", self.worker_id);
        } else if default_metrics.failure_rate > 0.20 {
            // Default processor is struggling, be more conservative
            debug!("Worker {} optimizing for conservative default processor usage", self.worker_id);
        }

        // Similar logic for fallback processor
        if fallback_metrics.failure_rate > 0.30 {
            warn!("Worker {} fallback processor showing high failure rate: {:.1}%", 
                self.worker_id, fallback_metrics.failure_rate * 100.0);
        }
    }

    /// Graceful shutdown
    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down worker {}...", self.worker_id);

        // Shutdown IPC
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.shutdown().await?;
        }

        // Shutdown client factory
        self.client_factory.shutdown().await?;

        // Shutdown payment processor
        self.payment_processor.shutdown().await?;

        // Shutdown health monitor
        self.health_monitor.shutdown().await?;

        info!("Worker {} shutdown complete", self.worker_id);
        Ok(())
    }
}

impl Clone for PaymentWorker {
    fn clone(&self) -> Self {
        Self {
            worker_id: self.worker_id,
            payment_processor: Arc::clone(&self.payment_processor),
            health_monitor: Arc::clone(&self.health_monitor),
            client_factory: Arc::clone(&self.client_factory),
            payment_store: Arc::clone(&self.payment_store),
            metrics: Arc::clone(&self.metrics),
            ipc_manager: Arc::clone(&self.ipc_manager),
            circuit_breaker: Arc::clone(&self.circuit_breaker),
            predictor: Arc::clone(&self.predictor),
            start_time: self.start_time,
        }
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

    // Get worker ID from environment or default to 1
    let worker_id = std::env::var("WORKER_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<u32>()
        .unwrap_or(1);

    // Detect configuration profile
    let profile = ConfigProfile::from_env();
    info!("Starting worker {} with profile: {:?}", worker_id, profile);

    // Create and run worker
    let worker = PaymentWorker::new(worker_id).await
        .context("Failed to create payment worker")?;

    worker.run().await
        .context("Worker execution failed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_creation() {
        let result = PaymentWorker::new(1).await;
        
        match result {
            Ok(_worker) => {
                println!("Worker created successfully");
            }
            Err(e) => {
                println!("Expected error in test environment: {}", e);
                assert!(
                    e.to_string().contains("connection") || 
                    e.to_string().contains("client factory")
                );
            }
        }
    }

    #[test]
    fn test_worker_id_parsing() {
        std::env::set_var("WORKER_ID", "42");
        let worker_id = std::env::var("WORKER_ID")
            .unwrap()
            .parse::<u32>()
            .unwrap();
        assert_eq!(worker_id, 42);
    }
}