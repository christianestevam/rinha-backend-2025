//! Utility modules
//! Simplified version for compilation

pub mod atomic;
pub mod metrics;
pub mod simd;

// Re-exports
pub use metrics::MetricsCollector;
