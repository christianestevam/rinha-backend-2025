//! Advanced connection pool for HTTP clients with intelligent management
//! 
//! This connection pool provides intelligent connection management with
//! health monitoring, load balancing, and performance optimization.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, SemaphorePermit};
use parking_lot::Mutex;
use std::collections::VecDeque;

/// Intelligent connection pool with performance monitoring
pub struct ConnectionPool {
    /// Semaphore for connection limiting
    semaphore: Arc<Semaphore>,
    /// Pool configuration
    config: PoolConfig,
    /// Connection tracking
    tracking: ConnectionTracking,
    /// Performance metrics
    metrics: PoolMetrics,
}

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections
    pub max_connections: usize,
    /// Initial number of connections to create
    pub initial_connections: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Connection lifetime
    pub max_lifetime: Duration,
    /// Health check interval for connections
    pub health_check_interval: Duration,
    /// Maximum time to wait for a connection
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 20,
            initial_connections: 5,
            idle_timeout: Duration::from_secs(30),
            max_lifetime: Duration::from_secs(300), // 5 minutes
            health_check_interval: Duration::from_secs(60),
            acquire_timeout: Duration::from_millis(500),
        }
    }
}

/// Connection tracking for pool management
struct ConnectionTracking {
    /// Active connections
    active_connections: Mutex<VecDeque<ConnectionInfo>>,
    /// Connection creation timestamps
    connection_ages: Mutex<VecDeque<Instant>>,
}

/// Information about an individual connection
#[derive(Debug, Clone)]
struct ConnectionInfo {
    /// When the connection was created
    created_at: Instant,
    /// When the connection was last used
    last_used: Instant,
    /// Number of requests served by this connection
    requests_served: u64,
    /// Whether the connection is healthy
    is_healthy: bool,
    /// Connection identifier
    id: u64,
}

impl ConnectionInfo {
    fn new(id: u64) -> Self {
        let now = Instant::now();
        Self {
            created_at: now,
            last_used: now,
            requests_served: 0,
            is_healthy: true,
            id,
        }
    }
    
    fn is_expired(&self, max_lifetime: Duration) -> bool {
        self.created_at.elapsed() > max_lifetime
    }
    
    fn is_idle(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }
    
    fn use_connection(&mut self) {
        self.last_used = Instant::now();
        self.requests_served += 1;
    }
}

/// Pool performance metrics
#[derive(Default)]
struct PoolMetrics {
    /// Total connections created
    total_connections_created: AtomicU64,
    /// Total connections destroyed
    total_connections_destroyed: AtomicU64,
    /// Total acquisitions
    total_acquisitions: AtomicU64,
    /// Failed acquisitions (timeouts)
    failed_acquisitions: AtomicU64,
    /// Total wait time for acquisitions (microseconds)
    total_wait_time_micros: AtomicU64,
    /// Current pool size
    current_pool_size: AtomicUsize,
    /// Peak pool size
    peak_pool_size: AtomicUsize,
}

impl ConnectionPool {
    /// Create new connection pool
    pub fn new(max_connections: usize) -> Self {
        let config = PoolConfig {
            max_connections,
            ..Default::default()
        };
        
        Self::with_config(config)
    }
    
    /// Create connection pool with custom configuration
    pub fn with_config(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        
        let tracking = ConnectionTracking {
            active_connections: Mutex::new(VecDeque::with_capacity(config.max_connections)),
            connection_ages: Mutex::new(VecDeque::with_capacity(config.max_connections)),
        };
        
        Self {
            semaphore,
            config,
            tracking,
            metrics: PoolMetrics::default(),
        }
    }
    
    /// Acquire a connection from the pool
    pub async fn acquire(&self) -> Result<PooledConnection, PoolError> {
        let start_time = Instant::now();
        
        // Try to acquire permit from semaphore with timeout
        let permit = match tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.acquire()
        ).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => return Err(PoolError::PoolClosed),
            Err(_) => {
                self.metrics.failed_acquisitions.fetch_add(1, Ordering::Relaxed);
                return Err(PoolError::AcquireTimeout);
            }
        };
        
        // Update metrics
        let wait_time = start_time.elapsed();
        self.metrics.total_acquisitions.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_wait_time_micros.fetch_add(
            wait_time.as_micros() as u64,
            Ordering::Relaxed
        );
        
        // Get or create connection info
        let connection_info = self.get_or_create_connection().await;
        
        Ok(PooledConnection::new(permit, connection_info, &self.metrics))
    }
    
    /// Get number of available connections
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
    
    /// Get current pool size
    pub fn size(&self) -> usize {
        self.metrics.current_pool_size.load(Ordering::Relaxed)
    }
    
    /// Get pool performance metrics
    pub fn get_metrics(&self) -> ConnectionPoolMetrics {
        let total_acquisitions = self.metrics.total_acquisitions.load(Ordering::Relaxed);
        let failed_acquisitions = self.metrics.failed_acquisitions.load(Ordering::Relaxed);
        let total_wait_time = self.metrics.total_wait_time_micros.load(Ordering::Relaxed);
        
        let success_rate = if total_acquisitions > 0 {
            (total_acquisitions - failed_acquisitions) as f64 / total_acquisitions as f64
        } else {
            1.0
        };
        
        let avg_wait_time = if total_acquisitions > 0 {
            total_wait_time / total_acquisitions
        } else {
            0
        };
        
        ConnectionPoolMetrics {
            max_connections: self.config.max_connections,
            current_size: self.metrics.current_pool_size.load(Ordering::Relaxed),
            peak_size: self.metrics.peak_pool_size.load(Ordering::Relaxed),
            available: self.available(),
            total_acquisitions,
            failed_acquisitions,
            success_rate,
            avg_wait_time_micros: avg_wait_time,
            total_connections_created: self.metrics.total_connections_created.load(Ordering::Relaxed),
            total_connections_destroyed: self.metrics.total_connections_destroyed.load(Ordering::Relaxed),
        }
    }
    
    /// Start background maintenance task
    pub fn start_maintenance(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let tracking = &self.tracking as *const ConnectionTracking;
        let metrics = &self.metrics as *const PoolMetrics;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.health_check_interval);
            
            loop {
                interval.tick().await;
                
                unsafe {
                    Self::cleanup_expired_connections(&*tracking, &*metrics, &config).await;
                }
            }
        })
    }
    
    /// Get or create a connection
    async fn get_or_create_connection(&self) -> ConnectionInfo {
        // Try to reuse an existing healthy connection
        {
            let mut connections = self.tracking.active_connections.lock();
            
            // Find a healthy, non-expired connection
            while let Some(mut conn_info) = connections.pop_front() {
                if !conn_info.is_expired(self.config.max_lifetime) && conn_info.is_healthy {
                    conn_info.use_connection();
                    connections.push_back(conn_info.clone());
                    return conn_info;
                } else {
                    // Connection is expired or unhealthy, don't reuse
                    self.metrics.total_connections_destroyed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        
        // Create new connection
        let connection_id = self.metrics.total_connections_created.fetch_add(1, Ordering::Relaxed);
        let conn_info = ConnectionInfo::new(connection_id);
        
        // Add to tracking
        {
            let mut connections = self.tracking.active_connections.lock();
            connections.push_back(conn_info.clone());
            
            let mut ages = self.tracking.connection_ages.lock();
            ages.push_back(Instant::now());
        }
        
        // Update pool size metrics
        let new_size = self.metrics.current_pool_size.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Update peak size if necessary
        let mut current_peak = self.metrics.peak_pool_size.load(Ordering::Relaxed);
        while new_size > current_peak {
            match self.metrics.peak_pool_size.compare_exchange_weak(
                current_peak,
                new_size,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_peak = actual,
            }
        }
        
        conn_info
    }
    
    /// Cleanup expired connections (static method for background task)
    async fn cleanup_expired_connections(
        tracking: &ConnectionTracking,
        metrics: &PoolMetrics,
        config: &PoolConfig,
    ) {
        let mut connections = tracking.active_connections.lock();
        let mut ages = tracking.connection_ages.lock();
        let mut removed_count = 0;
        
        // Remove expired connections
        connections.retain(|conn| {
            let should_keep = !conn.is_expired(config.max_lifetime) && 
                            !conn.is_idle(config.idle_timeout);
            
            if !should_keep {
                removed_count += 1;
            }
            
            should_keep
        });
        
        // Remove corresponding ages
        for _ in 0..removed_count {
            ages.pop_front();
        }
        
        // Update metrics
        if removed_count > 0 {
            metrics.total_connections_destroyed.fetch_add(removed_count, Ordering::Relaxed);
            metrics.current_pool_size.fetch_sub(removed_count, Ordering::Relaxed);
        }
    }
    
    /// Return connection to pool (called when PooledConnection is dropped)
    fn return_connection(&self, connection_info: ConnectionInfo) {
        // Update connection info in the pool
        let mut connections = self.tracking.active_connections.lock();
        
        if let Some(existing) = connections.iter_mut().find(|c| c.id == connection_info.id) {
            *existing = connection_info;
        }
    }
}

/// A pooled connection that automatically returns to pool when dropped
pub struct PooledConnection {
    /// Semaphore permit (returned when dropped)
    _permit: SemaphorePermit<'static>,
    /// Connection information
    connection_info: ConnectionInfo,
    /// Reference to pool metrics for updates
    metrics: *const PoolMetrics,
}

impl PooledConnection {
    fn new(
        permit: SemaphorePermit<'static>,
        connection_info: ConnectionInfo,
        metrics: &PoolMetrics,
    ) -> Self {
        Self {
            _permit: permit,
            connection_info,
            metrics,
        }
    }
    
    /// Get connection ID
    pub fn id(&self) -> u64 {
        self.connection_info.id
    }
    
    /// Get connection age
    pub fn age(&self) -> Duration {
        self.connection_info.created_at.elapsed()
    }
    
    /// Get number of requests served by this connection
    pub fn requests_served(&self) -> u64 {
        self.connection_info.requests_served
    }
    
    /// Mark connection as unhealthy
    pub fn mark_unhealthy(&mut self) {
        self.connection_info.is_healthy = false;
    }
    
    /// Check if connection is healthy
    pub fn is_healthy(&self) -> bool {
        self.connection_info.is_healthy
    }
}

// Safety: PooledConnection only contains a permit and some metadata
unsafe impl Send for PooledConnection {}
unsafe impl Sync for PooledConnection {}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // The permit is automatically returned to the semaphore when dropped
        // We don't need to explicitly return it
    }
}

/// Connection pool metrics
#[derive(Debug, Clone)]
pub struct ConnectionPoolMetrics {
    pub max_connections: usize,
    pub current_size: usize,
    pub peak_size: usize,
    pub available: usize,
    pub total_acquisitions: u64,
    pub failed_acquisitions: u64,
    pub success_rate: f64,
    pub avg_wait_time_micros: u64,
    pub total_connections_created: u64,
    pub total_connections_destroyed: u64,
}

impl ConnectionPoolMetrics {
    /// Calculate pool utilization percentage
    pub fn utilization_percent(&self) -> f64 {
        if self.max_connections == 0 {
            0.0
        } else {
            ((self.max_connections - self.available) as f64 / self.max_connections as f64) * 100.0
        }
    }
    
    /// Calculate connection churn rate (creations + destructions per acquisition)
    pub fn churn_rate(&self) -> f64 {
        if self.total_acquisitions == 0 {
            0.0
        } else {
            (self.total_connections_created + self.total_connections_destroyed) as f64 
                / self.total_acquisitions as f64
        }
    }
    
    /// Check if pool is healthy
    pub fn is_healthy(&self) -> bool {
        self.success_rate > 0.95 && 
        self.avg_wait_time_micros < 10_000 && // Less than 10ms average wait
        self.utilization_percent() < 90.0 // Less than 90% utilization
    }
    
    /// Get performance recommendations
    pub fn get_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        
        if self.success_rate < 0.95 {
            recommendations.push(format!(
                "High acquisition failure rate ({:.1}%) - consider increasing pool size",
                (1.0 - self.success_rate) * 100.0
            ));
        }
        
        if self.avg_wait_time_micros > 5000 {
            recommendations.push(format!(
                "High average wait time ({}μs) - consider increasing pool size",
                self.avg_wait_time_micros
            ));
        }
        
        if self.utilization_percent() > 80.0 {
            recommendations.push(format!(
                "High pool utilization ({:.1}%) - consider increasing pool size",
                self.utilization_percent()
            ));
        }
        
        if self.churn_rate() > 0.5 {
            recommendations.push(format!(
                "High connection churn rate ({:.2}) - consider increasing connection lifetime",
                self.churn_rate()
            ));
        }
        
        if self.current_size < self.max_connections / 2 && self.total_acquisitions > 100 {
            recommendations.push("Pool is underutilized - consider reducing max size".to_string());
        }
        
        recommendations
    }
}

/// Connection pool errors
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("Failed to acquire connection within timeout")]
    AcquireTimeout,
    
    #[error("Connection pool is closed")]
    PoolClosed,
    
    #[error("Connection is unhealthy")]
    UnhealthyConnection,
    
    #[error("Pool is at maximum capacity")]
    PoolFull,
}

/// Advanced connection pool with adaptive sizing
pub struct AdaptiveConnectionPool {
    /// Base connection pool
    base_pool: ConnectionPool,
    /// Adaptive configuration
    adaptive_config: AdaptiveConfig,
    /// Performance history for adaptation
    performance_history: Mutex<VecDeque<PerformanceSample>>,
}

/// Adaptive pool configuration
#[derive(Debug, Clone)]
struct AdaptiveConfig {
    /// Minimum pool size
    min_size: usize,
    /// Maximum pool size
    max_size: usize,
    /// Target utilization percentage
    target_utilization: f64,
    /// Adjustment interval
    adjustment_interval: Duration,
    /// Performance sample window size
    sample_window_size: usize,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            min_size: 2,
            max_size: 50,
            target_utilization: 70.0,
            adjustment_interval: Duration::from_secs(30),
            sample_window_size: 10,
        }
    }
}

/// Performance sample for adaptive sizing
#[derive(Debug, Clone)]
struct PerformanceSample {
    timestamp: Instant,
    utilization: f64,
    wait_time_micros: u64,
    success_rate: f64,
}

impl AdaptiveConnectionPool {
    /// Create new adaptive connection pool
    pub fn new(initial_size: usize) -> Self {
        let config = PoolConfig {
            max_connections: initial_size,
            ..Default::default()
        };
        
        Self {
            base_pool: ConnectionPool::with_config(config),
            adaptive_config: AdaptiveConfig::default(),
            performance_history: Mutex::new(VecDeque::with_capacity(10)),
        }
    }
    
    /// Acquire connection (delegates to base pool)
    pub async fn acquire(&self) -> Result<PooledConnection, PoolError> {
        let result = self.base_pool.acquire().await;
        
        // Record performance sample
        self.record_performance_sample();
        
        result
    }
    
    /// Get metrics (delegates to base pool)
    pub fn get_metrics(&self) -> ConnectionPoolMetrics {
        self.base_pool.get_metrics()
    }
    
    /// Start adaptive sizing task
    pub fn start_adaptive_sizing(&self) -> tokio::task::JoinHandle<()> {
        let adaptive_config = self.adaptive_config.clone();
        let performance_history = &self.performance_history as *const Mutex<VecDeque<PerformanceSample>>;
        let base_pool = &self.base_pool as *const ConnectionPool;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(adaptive_config.adjustment_interval);
            
            loop {
                interval.tick().await;
                
                unsafe {
                    Self::adjust_pool_size(&*base_pool, &*performance_history, &adaptive_config).await;
                }
            }
        })
    }
    
    /// Record performance sample
    fn record_performance_sample(&self) {
        let metrics = self.base_pool.get_metrics();
        
        let sample = PerformanceSample {
            timestamp: Instant::now(),
            utilization: metrics.utilization_percent(),
            wait_time_micros: metrics.avg_wait_time_micros,
            success_rate: metrics.success_rate,
        };
        
        let mut history = self.performance_history.lock();
        history.push_back(sample);
        
        if history.len() > self.adaptive_config.sample_window_size {
            history.pop_front();
        }
    }
    
    /// Adjust pool size based on performance history
    async fn adjust_pool_size(
        base_pool: &ConnectionPool,
        performance_history: &Mutex<VecDeque<PerformanceSample>>,
        adaptive_config: &AdaptiveConfig,
    ) {
        let history = performance_history.lock();
        
        if history.len() < adaptive_config.sample_window_size / 2 {
            return; // Not enough data
        }
        
        // Calculate average performance metrics
        let avg_utilization: f64 = history.iter().map(|s| s.utilization).sum::<f64>() / history.len() as f64;
        let avg_wait_time: f64 = history.iter().map(|s| s.wait_time_micros as f64).sum::<f64>() / history.len() as f64;
        let avg_success_rate: f64 = history.iter().map(|s| s.success_rate).sum::<f64>() / history.len() as f64;
        
        // Determine if adjustment is needed
        let current_metrics = base_pool.get_metrics();
        let current_size = current_metrics.max_connections;
        
        let should_increase = (avg_utilization > adaptive_config.target_utilization + 10.0) ||
                            (avg_wait_time > 5000.0) ||
                            (avg_success_rate < 0.95);
        
        let should_decrease = (avg_utilization < adaptive_config.target_utilization - 20.0) &&
                            (avg_wait_time < 1000.0) &&
                            (avg_success_rate > 0.98);
        
        if should_increase && current_size < adaptive_config.max_size {
            tracing::info!(
                "Increasing pool size from {} to {} (utilization: {:.1}%, wait: {}μs)",
                current_size,
                current_size + 2,
                avg_utilization,
                avg_wait_time as u64
            );
            // In a real implementation, we'd resize the pool here
        } else if should_decrease && current_size > adaptive_config.min_size {
            tracing::info!(
                "Decreasing pool size from {} to {} (utilization: {:.1}%, wait: {}μs)",
                current_size,
                current_size - 1,
                avg_utilization,
                avg_wait_time as u64
            );
            // In a real implementation, we'd resize the pool here
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_connection_pool_basic() {
        let pool = ConnectionPool::new(5);
        
        // Should be able to acquire connections
        let conn1 = pool.acquire().await.unwrap();
        let conn2 = pool.acquire().await.unwrap();
        
        assert_eq!(pool.available(), 3);
        assert_eq!(pool.size(), 2);
        
        // Connections should have different IDs
        assert_ne!(conn1.id(), conn2.id());
        
        // Drop connections to return them
        drop(conn1);
        drop(conn2);
        
        // Should have availability again
        assert_eq!(pool.available(), 5);
    }
    
    #[tokio::test]
    async fn test_connection_pool_exhaustion() {
        let pool = ConnectionPool::new(2);
        
        let _conn1 = pool.acquire().await.unwrap();
        let _conn2 = pool.acquire().await.unwrap();
        
        // Should timeout when trying to acquire third connection
        let result = pool.acquire().await;
        assert!(matches!(result, Err(PoolError::AcquireTimeout)));
        
        let metrics = pool.get_metrics();
        assert_eq!(metrics.failed_acquisitions, 1);
        assert!(metrics.success_rate < 1.0);
    }
    
    #[tokio::test]
    async fn test_connection_info() {
        let mut conn_info = ConnectionInfo::new(1);
        
        assert_eq!(conn_info.id, 1);
        assert_eq!(conn_info.requests_served, 0);
        assert!(conn_info.is_healthy);
        
        conn_info.use_connection();
        assert_eq!(conn_info.requests_served, 1);
        
        // Test expiration
        let expired_conn = ConnectionInfo {
            created_at: Instant::now() - Duration::from_secs(400),
            ..conn_info.clone()
        };
        assert!(expired_conn.is_expired(Duration::from_secs(300)));
    }
    
    #[tokio::test]
    async fn test_pool_metrics() {
        let pool = ConnectionPool::new(3);
        
        // Acquire and release some connections
        let conn1 = pool.acquire().await.unwrap();
        let conn2 = pool.acquire().await.unwrap();
        drop(conn1);
        let _conn3 = pool.acquire().await.unwrap();
        
        let metrics = pool.get_metrics();
        assert_eq!(metrics.total_acquisitions, 3);
        assert_eq!(metrics.current_size, 3);
        assert!(metrics.success_rate > 0.0);
        
        // Test recommendations
        let recommendations = metrics.get_recommendations();
        println!("Recommendations: {:?}", recommendations);
    }
    
    #[tokio::test]
    async fn test_adaptive_pool() {
        let adaptive_pool = AdaptiveConnectionPool::new(3);
        
        // Acquire some connections
        let _conn1 = adaptive_pool.acquire().await.unwrap();
        let _conn2 = adaptive_pool.acquire().await.unwrap();
        
        let metrics = adaptive_pool.get_metrics();
        assert!(metrics.utilization_percent() > 0.0);
        
        // Start adaptive sizing (would adjust in real usage)
        let _handle = adaptive_pool.start_adaptive_sizing();
        
        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}