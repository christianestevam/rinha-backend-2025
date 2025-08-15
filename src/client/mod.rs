//! High-performance HTTP client module for payment processor communication
//! 
//! This module provides optimized HTTP clients with connection pooling,
//! intelligent routing, and advanced performance features for the Rinha Backend challenge.

pub mod connection_pool;
pub mod http_client;

// Re-export main types for easier usage
pub use connection_pool::{
    ConnectionPool, 
    PooledConnection, 
    PoolConfig, 
    ConnectionPoolMetrics,
    PoolError,
    AdaptiveConnectionPool
};

pub use http_client::{
    HttpClient,
    PaymentRequest,
    PaymentResponse,
    HealthResponse,
    ProcessorEndpoints,
    HttpClientConfig,
    HttpClientMetrics,
    ProcessorMetrics,
    ConnectionPoolUtilization
};