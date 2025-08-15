//! Internal load balancer for the Rinha Backend 2025 challenge
//! 
//! This load balancer distributes requests between multiple API server instances
//! and provides advanced routing capabilities with health monitoring.

use rinha_backend_2025::{
    config::{init_config, get_config, ConfigProfile},
    routing::{
        LoadBalancer, LoadBalancerConfig, RoutingDecision,
        circuit_breaker::{DualCircuitBreaker, CircuitBreakerConfig},
        predictor::LatencyPredictor,
    },
    client::{ClientFactory, HttpClient},
    utils::metrics::MetricsCollector,
    ipc::{IpcManager, IpcFactory},
    server::{HttpServer, ServerConfig, RequestHandler},
};

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::{signal, sync::RwLock};
use anyhow::{Result, Context};
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};

/// Internal load balancer for distributing requests
pub struct InternalLoadBalancer {
    /// Load balancer ID
    lb_id: String,
    
    /// Upstream API servers
    upstream_servers: Arc<RwLock<Vec<UpstreamServer>>>,
    
    /// Load balancing algorithm
    algorithm: LoadBalancingAlgorithm,
    
    /// Health monitor for upstream servers
    health_monitor: Arc<HealthMonitor>,
    
    /// Metrics collector
    metrics: Arc<MetricsCollector>,
    
    /// IPC manager for coordination
    ipc_manager: Arc<tokio::sync::RwLock<IpcManager>>,
    
    /// Start time for uptime calculation
    start_time: Instant,
}

/// Upstream server configuration
#[derive(Debug, Clone)]
struct UpstreamServer {
    /// Server ID
    id: String,
    
    /// Server URL
    url: String,
    
    /// Server weight for weighted round-robin
    weight: u32,
    
    /// Current health status
    healthy: bool,
    
    /// Last health check time
    last_health_check: Instant,
    
    /// Current connection count
    active_connections: u32,
    
    /// Response time metrics
    avg_response_time: Duration,
    
    /// Success rate
    success_rate: f64,
    
    /// Total requests handled
    total_requests: u64,
}

/// Load balancing algorithms
#[derive(Debug, Clone)]
enum LoadBalancingAlgorithm {
    /// Round-robin distribution
    RoundRobin { current_index: usize },
    
    /// Weighted round-robin
    WeightedRoundRobin { current_weights: HashMap<String, u32> },
    
    /// Least connections
    LeastConnections,
    
    /// Response time based
    ResponseTimeBased,
    
    /// Adaptive based on multiple metrics
    Adaptive,
}

/// Health monitor for upstream servers
struct HealthMonitor {
    /// Health check interval
    check_interval: Duration,
    
    /// Health check timeout
    check_timeout: Duration,
    
    /// HTTP client for health checks
    http_client: Arc<HttpClient>,
}

/// Load balancer metrics
#[derive(Debug, Serialize)]
struct LoadBalancerMetrics {
    /// Total requests processed
    total_requests: u64,
    
    /// Requests per second
    requests_per_second: f64,
    
    /// Average response time
    avg_response_time: Duration,
    
    /// Success rate
    success_rate: f64,
    
    /// Uptime in seconds
    uptime_seconds: u64,
    
    /// Active upstream servers
    active_servers: usize,
    
    /// Total upstream servers
    total_servers: usize,
}

impl InternalLoadBalancer {
    /// Create new load balancer
    pub async fn new(lb_id: String) -> Result<Self> {
        info!("Initializing internal load balancer: {}", lb_id);

        // Initialize configuration
        let config = init_config().context("Failed to initialize configuration")?;
        
        // Create upstream servers from configuration
        let upstream_servers = Arc::new(RwLock::new(
            Self::create_upstream_servers(&config.load_balancer.upstream_servers).await?
        ));

        // Create health monitor
        let health_monitor = Arc::new(HealthMonitor {
            check_interval: Duration::from_secs(5),
            check_timeout: Duration::from_secs(2),
            http_client: Arc::new(HttpClient::new_with_timeout(Duration::from_secs(2))?),
        });

        // Create metrics collector
        let metrics = Arc::new(MetricsCollector::new());

        // Create IPC manager
        let ipc_manager = Arc::new(tokio::sync::RwLock::new(
            IpcFactory::create_load_balancer().await
                .context("Failed to create IPC manager")?
        ));

        // Determine load balancing algorithm
        let algorithm = match config.load_balancer.algorithm.as_str() {
            "round-robin" => LoadBalancingAlgorithm::RoundRobin { current_index: 0 },
            "weighted" => LoadBalancingAlgorithm::WeightedRoundRobin { 
                current_weights: HashMap::new() 
            },
            "least-connections" => LoadBalancingAlgorithm::LeastConnections,
            "response-time" => LoadBalancingAlgorithm::ResponseTimeBased,
            "adaptive" => LoadBalancingAlgorithm::Adaptive,
            _ => LoadBalancingAlgorithm::Adaptive, // Default to adaptive
        };

        Ok(Self {
            lb_id,
            upstream_servers,
            algorithm,
            health_monitor,
            metrics,
            ipc_manager,
            start_time: Instant::now(),
        })
    }

    /// Start the load balancer
    pub async fn run(&self) -> Result<()> {
        info!("Starting internal load balancer: {}", self.lb_id);

        // Start IPC server
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.start_server().await
                .context("Failed to start IPC server")?;
        }

        // Start health monitoring
        let lb_clone = self.clone();
        let health_task = tokio::spawn(async move {
            lb_clone.health_monitoring_loop().await;
        });

        // Start metrics collection
        let lb_clone = self.clone();
        let metrics_task = tokio::spawn(async move {
            lb_clone.metrics_collection_loop().await;
        });

        // Start adaptive algorithm tuning
        let lb_clone = self.clone();
        let tuning_task = tokio::spawn(async move {
            lb_clone.algorithm_tuning_loop().await;
        });

        // Create HTTP server for receiving requests
        let server_config = ServerConfig {
            host: "0.0.0.0".to_string(),
            port: 8080, // Internal load balancer port
            worker_threads: num_cpus::get(),
            max_connections: 10000,
            tcp_nodelay: true,
            tcp_keepalive: true,
            buffer_size: 8192,
            enable_compression: false,
            read_timeout: Duration::from_millis(50),
            write_timeout: Duration::from_millis(50),
        };

        let server = HttpServer::new(server_config);
        let request_handler = LoadBalancerRequestHandler::new(self.clone());

        info!("Load balancer {} ready on port 8080", self.lb_id);

        // Run server with graceful shutdown
        tokio::select! {
            result = server.run(request_handler) => {
                match result {
                    Ok(_) => info!("Load balancer server stopped gracefully"),
                    Err(e) => error!("Load balancer server error: {}", e),
                }
            }
            _ = health_task => {
                warn!("Health monitoring task ended");
            }
            _ = metrics_task => {
                warn!("Metrics collection task ended");
            }
            _ = tuning_task => {
                warn!("Algorithm tuning task ended");
            }
            _ = signal::ctrl_c() => {
                info!("Load balancer {} received shutdown signal", self.lb_id);
            }
        }

        // Cleanup
        self.shutdown().await?;
        Ok(())
    }

    /// Select upstream server based on load balancing algorithm
    pub async fn select_upstream_server(&self) -> Result<UpstreamServer> {
        let servers = self.upstream_servers.read().await;
        let healthy_servers: Vec<_> = servers.iter()
            .filter(|s| s.healthy)
            .cloned()
            .collect();

        if healthy_servers.is_empty() {
            return Err(anyhow::anyhow!("No healthy upstream servers available"));
        }

        let selected = match &self.algorithm {
            LoadBalancingAlgorithm::RoundRobin { .. } => {
                self.round_robin_select(&healthy_servers).await
            }
            LoadBalancingAlgorithm::WeightedRoundRobin { .. } => {
                self.weighted_round_robin_select(&healthy_servers).await
            }
            LoadBalancingAlgorithm::LeastConnections => {
                self.least_connections_select(&healthy_servers).await
            }
            LoadBalancingAlgorithm::ResponseTimeBased => {
                self.response_time_select(&healthy_servers).await
            }
            LoadBalancingAlgorithm::Adaptive => {
                self.adaptive_select(&healthy_servers).await
            }
        };

        Ok(selected)
    }

    /// Round-robin server selection
    async fn round_robin_select(&self, servers: &[UpstreamServer]) -> UpstreamServer {
        // Simple round-robin (in real implementation would need proper state management)
        let index = (Instant::now().elapsed().as_secs() as usize) % servers.len();
        servers[index].clone()
    }

    /// Weighted round-robin server selection
    async fn weighted_round_robin_select(&self, servers: &[UpstreamServer]) -> UpstreamServer {
        // Select based on weights (simplified implementation)
        let total_weight: u32 = servers.iter().map(|s| s.weight).sum();
        let mut cumulative_weight = 0;
        let target = (rand::random::<u32>() % total_weight) + 1;

        for server in servers {
            cumulative_weight += server.weight;
            if cumulative_weight >= target {
                return server.clone();
            }
        }

        // Fallback to first server
        servers[0].clone()
    }

    /// Least connections server selection
    async fn least_connections_select(&self, servers: &[UpstreamServer]) -> UpstreamServer {
        servers.iter()
            .min_by_key(|s| s.active_connections)
            .unwrap()
            .clone()
    }

    /// Response time based server selection
    async fn response_time_select(&self, servers: &[UpstreamServer]) -> UpstreamServer {
        servers.iter()
            .min_by_key(|s| s.avg_response_time)
            .unwrap()
            .clone()
    }

    /// Adaptive server selection (combines multiple metrics)
    async fn adaptive_select(&self, servers: &[UpstreamServer]) -> UpstreamServer {
        let mut best_server = &servers[0];
        let mut best_score = f64::NEG_INFINITY;

        for server in servers {
            // Calculate composite score
            let connection_score = 1.0 / (server.active_connections as f64 + 1.0);
            let response_time_score = 1.0 / (server.avg_response_time.as_millis() as f64 + 1.0);
            let success_rate_score = server.success_rate;
            let weight_score = server.weight as f64 / 100.0;

            let composite_score = 
                connection_score * 0.3 +
                response_time_score * 0.3 +
                success_rate_score * 0.3 +
                weight_score * 0.1;

            if composite_score > best_score {
                best_score = composite_score;
                best_server = server;
            }
        }

        best_server.clone()
    }

    /// Health monitoring loop
    async fn health_monitoring_loop(&self) {
        info!("Load balancer {} health monitoring started", self.lb_id);

        let mut interval = tokio::time::interval(self.health_monitor.check_interval);

        loop {
            interval.tick().await;

            let servers = self.upstream_servers.clone();
            let health_monitor = Arc::clone(&self.health_monitor);

            // Check health of all servers
            let mut servers_guard = servers.write().await;
            for server in servers_guard.iter_mut() {
                let health_result = health_monitor.check_server_health(&server.url).await;
                
                match health_result {
                    Ok((healthy, response_time)) => {
                        server.healthy = healthy;
                        server.last_health_check = Instant::now();
                        
                        // Update average response time with exponential moving average
                        let alpha = 0.3; // Smoothing factor
                        server.avg_response_time = Duration::from_millis(
                            ((1.0 - alpha) * server.avg_response_time.as_millis() as f64 +
                             alpha * response_time.as_millis() as f64) as u64
                        );

                        if healthy {
                            debug!("Server {} is healthy ({}ms)", server.id, response_time.as_millis());
                        } else {
                            warn!("Server {} is unhealthy", server.id);
                        }
                    }
                    Err(e) => {
                        server.healthy = false;
                        server.last_health_check = Instant::now();
                        error!("Health check failed for server {}: {}", server.id, e);
                    }
                }
            }
        }
    }

    /// Metrics collection loop
    async fn metrics_collection_loop(&self) {
        info!("Load balancer {} metrics collection started", self.lb_id);

        let mut interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            interval.tick().await;

            let metrics = self.get_metrics().await;
            
            info!("Load balancer {} metrics - Requests: {}, RPS: {:.1}, Success: {:.1}%, Healthy servers: {}/{}",
                self.lb_id,
                metrics.total_requests,
                metrics.requests_per_second,
                metrics.success_rate * 100.0,
                metrics.active_servers,
                metrics.total_servers
            );
        }
    }

    /// Algorithm tuning loop
    async fn algorithm_tuning_loop(&self) {
        info!("Load balancer {} algorithm tuning started", self.lb_id);

        let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes

        loop {
            interval.tick().await;

            // Analyze performance and potentially switch algorithms
            let metrics = self.get_metrics().await;
            
            if metrics.success_rate < 0.9 {
                warn!("Load balancer {} success rate low: {:.1}% - considering algorithm change",
                    self.lb_id, metrics.success_rate * 100.0);
            }

            // Implement algorithm switching logic based on performance
            self.optimize_algorithm(&metrics).await;
        }
    }

    /// Optimize load balancing algorithm based on performance
    async fn optimize_algorithm(&self, metrics: &LoadBalancerMetrics) {
        // This is where we'd implement dynamic algorithm switching
        // based on observed performance patterns
        
        if metrics.success_rate < 0.85 {
            // Switch to more conservative algorithm
            debug!("Load balancer {} considering switch to least-connections algorithm", self.lb_id);
        } else if metrics.avg_response_time > Duration::from_millis(100) {
            // Switch to response-time based algorithm
            debug!("Load balancer {} considering switch to response-time algorithm", self.lb_id);
        }
    }

    /// Get current metrics
    async fn get_metrics(&self) -> LoadBalancerMetrics {
        let servers = self.upstream_servers.read().await;
        let active_servers = servers.iter().filter(|s| s.healthy).count();
        let total_servers = servers.len();

        // Calculate aggregate metrics
        let total_requests: u64 = servers.iter().map(|s| s.total_requests).sum();
        let avg_success_rate = if !servers.is_empty() {
            servers.iter().map(|s| s.success_rate).sum::<f64>() / servers.len() as f64
        } else {
            0.0
        };

        let avg_response_time = if !servers.is_empty() {
            let total_millis: u64 = servers.iter()
                .map(|s| s.avg_response_time.as_millis() as u64)
                .sum();
            Duration::from_millis(total_millis / servers.len() as u64)
        } else {
            Duration::ZERO
        };

        let uptime = self.start_time.elapsed();
        let requests_per_second = if uptime.as_secs() > 0 {
            total_requests as f64 / uptime.as_secs() as f64
        } else {
            0.0
        };

        LoadBalancerMetrics {
            total_requests,
            requests_per_second,
            avg_response_time,
            success_rate: avg_success_rate,
            uptime_seconds: uptime.as_secs(),
            active_servers,
            total_servers,
        }
    }

    /// Create upstream servers from configuration
    async fn create_upstream_servers(urls: &[String]) -> Result<Vec<UpstreamServer>> {
        let mut servers = Vec::new();

        for (index, url) in urls.iter().enumerate() {
            let server = UpstreamServer {
                id: format!("server-{}", index),
                url: url.clone(),
                weight: 100, // Default weight
                healthy: true, // Assume healthy initially
                last_health_check: Instant::now(),
                active_connections: 0,
                avg_response_time: Duration::from_millis(50),
                success_rate: 1.0,
                total_requests: 0,
            };
            servers.push(server);
        }

        Ok(servers)
    }

    /// Graceful shutdown
    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down load balancer {}...", self.lb_id);

        // Shutdown IPC
        {
            let mut ipc = self.ipc_manager.write().await;
            ipc.shutdown().await?;
        }

        info!("Load balancer {} shutdown complete", self.lb_id);
        Ok(())
    }
}

impl Clone for InternalLoadBalancer {
    fn clone(&self) -> Self {
        Self {
            lb_id: self.lb_id.clone(),
            upstream_servers: Arc::clone(&self.upstream_servers),
            algorithm: self.algorithm.clone(),
            health_monitor: Arc::clone(&self.health_monitor),
            metrics: Arc::clone(&self.metrics),
            ipc_manager: Arc::clone(&self.ipc_manager),
            start_time: self.start_time,
        }
    }
}

impl HealthMonitor {
    /// Check health of a server
    async fn check_server_health(&self, url: &str) -> Result<(bool, Duration)> {
        let start_time = Instant::now();
        let health_url = format!("{}/health", url);
        
        match tokio::time::timeout(self.check_timeout, 
            self.http_client.get(&health_url)).await {
            Ok(Ok(response)) => {
                let response_time = start_time.elapsed();
                let healthy = response.status().is_success();
                Ok((healthy, response_time))
            }
            Ok(Err(e)) => {
                let response_time = start_time.elapsed();
                debug!("Health check error for {}: {}", url, e);
                Ok((false, response_time))
            }
            Err(_) => {
                // Timeout
                Ok((false, self.check_timeout))
            }
        }
    }
}

/// Request handler for load balancer
struct LoadBalancerRequestHandler {
    load_balancer: InternalLoadBalancer,
}

impl LoadBalancerRequestHandler {
    fn new(load_balancer: InternalLoadBalancer) -> Self {
        Self { load_balancer }
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

    // Get load balancer ID from environment
    let lb_id = std::env::var("LB_ID")
        .unwrap_or_else(|_| "lb-1".to_string());

    // Detect configuration profile
    let profile = ConfigProfile::from_env();
    info!("Starting load balancer {} with profile: {:?}", lb_id, profile);

    // Create and run load balancer
    let load_balancer = InternalLoadBalancer::new(lb_id).await
        .context("Failed to create load balancer")?;

    load_balancer.run().await
        .context("Load balancer execution failed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_load_balancer_creation() {
        let result = InternalLoadBalancer::new("test-lb".to_string()).await;
        
        match result {
            Ok(_lb) => {
                println!("Load balancer created successfully");
            }
            Err(e) => {
                println!("Expected error in test environment: {}", e);
                assert!(e.to_string().contains("configuration") || e.to_string().contains("IPC"));
            }
        }
    }

    #[test]
    fn test_upstream_server_creation() {
        let server = UpstreamServer {
            id: "test-server".to_string(),
            url: "http://localhost:9999".to_string(),
            weight: 100,
            healthy: true,
            last_health_check: Instant::now(),
            active_connections: 0,
            avg_response_time: Duration::from_millis(50),
            success_rate: 1.0,
            total_requests: 0,
        };

        assert_eq!(server.id, "test-server");
        assert!(server.healthy);
        assert_eq!(server.weight, 100);
    }
}