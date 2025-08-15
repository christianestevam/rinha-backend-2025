//! Rinha Backend Ultra-High Performance Payment Gateway
//! 
//! This library provides a complete high-performance payment processing system
//! designed to achieve p99 latency < 1ms and 100k+ RPS throughput.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

// Core modules
pub mod client;
pub mod config;
pub mod ipc;
pub mod routing;
pub mod server;
pub mod storage;
pub mod utils;
pub mod worker;

// Re-exports for convenience
pub use config::{AppConfig, ConfigProfile};
pub use client::{HttpClient};
pub use storage::{PaymentStore};
pub use routing::load_balancer::LoadBalancer;
pub use routing::circuit_breaker::CircuitBreaker;
pub use server::{HttpServer};
pub use worker::{PaymentProcessor, HealthMonitor};
pub use ipc::{MessageHandler};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build information
pub const BUILD_INFO: &str = concat!(
    "version=", env!("CARGO_PKG_VERSION"),
    " target=", "x86_64",
);

/// Default configuration profile
pub const DEFAULT_PROFILE: ConfigProfile = ConfigProfile::Production;

/// Initialize the library with default configuration
pub async fn init() -> anyhow::Result<AppConfig> {
    config::load_config()
}

/// Initialize the library with custom configuration profile
pub async fn init_with_profile(profile: ConfigProfile) -> anyhow::Result<AppConfig> {
    std::env::set_var("RINHA_PROFILE", profile.as_str());
    config::load_config()
}

/// Health check function
pub async fn health_check() -> bool {
    // Basic health check - can be extended
    true
}

/// Performance optimizations
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// SIMD feature detection
#[cfg(feature = "simd")]
pub fn simd_available() -> bool {
    is_x86_feature_detected!("avx2") && is_x86_feature_detected!("sse4.2")
}

#[cfg(not(feature = "simd"))]
pub fn simd_available() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }

    #[tokio::test]
    async fn test_health_check() {
        assert!(health_check().await);
    }

    #[tokio::test]
    async fn test_init() {
        let config = init().await;
        assert!(config.is_ok());
    }
}