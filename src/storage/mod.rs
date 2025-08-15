//! High-performance storage implementations for Rinha Backend 2025
//! 
//! This module provides optimized storage solutions including:
//! - Arena allocator for zero-fragmentation memory management
//! - Lock-free ring buffer for high-throughput data passing
//! - Payment store with segmented storage and atomic operations
//! 
//! All components are designed for maximum performance in concurrent scenarios.

pub mod arena;
pub mod ring_buffer;
pub mod payment_store;

// Re-export main types for convenience
pub use arena::{ArenaAllocator, with_thread_arena};
pub use ring_buffer::{RingBuffer, MPMCRingBuffer};
pub use payment_store::{
    Payment, PaymentStore, PaymentSummary, SummaryData, 
    ProcessorType, PaymentStoreMetrics
};

use chrono::{DateTime, Utc};
use std::sync::Arc;
use uuid::Uuid;

/// Main storage engine that coordinates all storage components
pub struct StorageEngine {
    /// Payment store for persistent data
    payment_store: Arc<PaymentStore>,
    /// Arena allocator for efficient memory management
    arena: Arc<ArenaAllocator>,
    /// Configuration
    config: StorageConfig,
}

/// Configuration for the storage engine
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Ring buffer size for recent payments
    pub ring_buffer_size: usize,
    /// Number of time-based segments
    pub num_segments: usize,
    /// Arena size in bytes
    pub arena_size: usize,
    /// Enable detailed metrics collection
    pub enable_metrics: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            ring_buffer_size: 32768,  // 32K recent payments
            num_segments: 24,         // 24 hour segments (1 hour each)
            arena_size: 16 * 1024 * 1024, // 16MB arena
            enable_metrics: true,
        }
    }
}

impl StorageEngine {
    /// Create a new storage engine with the given configuration
    pub fn new(config: StorageConfig) -> Self {
        let arena = Arc::new(ArenaAllocator::new());
        let payment_store = Arc::new(PaymentStore::new(
            config.ring_buffer_size,
            config.num_segments,
        ));
        
        Self {
            payment_store,
            arena,
            config,
        }
    }
    
    /// Create with default configuration
    pub fn with_default_config() -> Self {
        Self::new(StorageConfig::default())
    }
    
    /// Store a new payment
    pub async fn store_payment(
        &self,
        correlation_id: Uuid,
        amount: f64,
        processor: ProcessorType,
    ) -> Result<(), StorageError> {
        let payment = Payment::new(
            correlation_id,
            amount,
            Utc::now(),
            processor,
        );
        
        self.payment_store
            .insert(payment)
            .map_err(StorageError::InsertFailed)?;
        
        Ok(())
    }
    
    /// Get payment summary for a time range
    pub async fn get_summary(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<PaymentSummary, StorageError> {
        Ok(self.payment_store.get_summary(from, to))
    }
    
    /// Get current statistics (fast path)
    pub async fn get_current_stats(&self) -> Result<PaymentSummary, StorageError> {
        Ok(self.payment_store.get_stats())
    }
    
    /// Purge all stored data (for testing)
    pub async fn purge_all(&self) -> Result<(), StorageError> {
        self.payment_store.purge();
        unsafe {
            self.arena.reset();
        }
        Ok(())
    }
    
    /// Get performance metrics
    pub async fn get_metrics(&self) -> Result<StorageMetrics, StorageError> {
        if !self.config.enable_metrics {
            return Err(StorageError::MetricsDisabled);
        }
        
        let payment_metrics = self.payment_store.performance_metrics();
        let (arena_bytes, arena_allocs, arena_count) = self.arena.stats();
        
        Ok(StorageMetrics {
            total_payments: payment_metrics.total_payments,
            memory_usage_bytes: payment_metrics.memory_usage_bytes,
            arena_bytes_allocated: arena_bytes,
            arena_allocations: arena_allocs,
            arena_count,
            ring_buffer_utilization: payment_metrics.ring_buffer_utilization,
            segment_count: payment_metrics.segment_count,
        })
    }
    
    /// Get the underlying payment store (for advanced usage)
    pub fn payment_store(&self) -> &Arc<PaymentStore> {
        &self.payment_store
    }
    
    /// Get the arena allocator (for advanced usage)
    pub fn arena(&self) -> &Arc<ArenaAllocator> {
        &self.arena
    }
}

/// Storage-related errors
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Failed to insert payment: {0}")]
    InsertFailed(String),
    
    #[error("Invalid time range")]
    InvalidTimeRange,
    
    #[error("Metrics collection is disabled")]
    MetricsDisabled,
    
    #[error("Storage capacity exceeded")]
    CapacityExceeded,
    
    #[error("Internal storage error: {0}")]
    Internal(String),
}

/// Comprehensive storage metrics
#[derive(Debug, Clone)]
pub struct StorageMetrics {
    /// Total payments stored
    pub total_payments: u64,
    /// Total memory usage in bytes
    pub memory_usage_bytes: usize,
    /// Arena allocator bytes allocated
    pub arena_bytes_allocated: usize,
    /// Arena allocator number of allocations
    pub arena_allocations: usize,
    /// Number of arena chunks created
    pub arena_count: usize,
    /// Ring buffer utilization (0.0 to 1.0)
    pub ring_buffer_utilization: f64,
    /// Number of storage segments
    pub segment_count: usize,
}

impl StorageMetrics {
    /// Calculate memory efficiency (useful data vs overhead)
    pub fn memory_efficiency(&self) -> f64 {
        if self.memory_usage_bytes == 0 {
            return 0.0;
        }
        
        let useful_data_bytes = self.total_payments as usize * std::mem::size_of::<Payment>();
        useful_data_bytes as f64 / self.memory_usage_bytes as f64
    }
    
    /// Get average bytes per payment
    pub fn avg_bytes_per_payment(&self) -> f64 {
        if self.total_payments == 0 {
            return 0.0;
        }
        
        self.memory_usage_bytes as f64 / self.total_payments as f64
    }
}

/// Helper functions for storage operations
pub mod utils {
    use super::*;
    
    /// Convert amount from f64 to cents (u64) safely
    pub fn amount_to_cents(amount: f64) -> u64 {
        (amount * 100.0).round().max(0.0) as u64
    }
    
    /// Convert cents (u64) to amount (f64)
    pub fn cents_to_amount(cents: u64) -> f64 {
        cents as f64 / 100.0
    }
    
    /// Calculate fee for a given amount and processor
    pub fn calculate_fee(amount: f64, processor: ProcessorType) -> f64 {
        amount * processor.fee_rate()
    }
    
    /// Calculate net amount after fee
    pub fn calculate_net_amount(amount: f64, processor: ProcessorType) -> f64 {
        amount - calculate_fee(amount, processor)
    }
    
    /// Validate payment amount
    pub fn validate_amount(amount: f64) -> Result<(), &'static str> {
        if amount <= 0.0 {
            return Err("Amount must be positive");
        }
        
        if amount > 1_000_000.0 {
            return Err("Amount too large");
        }
        
        if amount.is_nan() || amount.is_infinite() {
            return Err("Amount must be a valid number");
        }
        
        Ok(())
    }
    
    /// Generate time ranges for efficient querying
    pub fn generate_time_ranges(
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        segment_duration_hours: i64,
    ) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
        let mut ranges = Vec::new();
        let mut current = from;
        let duration = chrono::Duration::hours(segment_duration_hours);
        
        while current < to {
            let end = std::cmp::min(current + duration, to);
            ranges.push((current, end));
            current = end;
        }
        
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;
    
    #[tokio::test]
    async fn test_storage_engine_basic() {
        let engine = StorageEngine::with_default_config();
        
        let correlation_id = Uuid::new_v4();
        let amount = 19.90;
        
        // Store a payment
        engine.store_payment(correlation_id, amount, ProcessorType::Default)
            .await
            .unwrap();
        
        // Get current stats
        let stats = engine.get_current_stats().await.unwrap();
        assert_eq!(stats.default.total_requests, 1);
        assert_eq!(stats.default.total_amount, amount);
    }
    
    #[tokio::test]
    async fn test_storage_engine_summary() {
        let engine = StorageEngine::with_default_config();
        let now = Utc::now();
        
        // Store multiple payments
        for i in 0..10 {
            engine.store_payment(
                Uuid::new_v4(),
                (i as f64) * 10.0,
                if i % 2 == 0 { ProcessorType::Default } else { ProcessorType::Fallback },
            ).await.unwrap();
        }
        
        // Get summary for a time range
        let from = now - chrono::Duration::hours(1);
        let to = now + chrono::Duration::hours(1);
        let summary = engine.get_summary(from, to).await.unwrap();
        
        assert_eq!(summary.default.total_requests, 5);
        assert_eq!(summary.fallback.total_requests, 5);
    }
    
    #[tokio::test]
    async fn test_storage_engine_metrics() {
        let engine = StorageEngine::with_default_config();
        
        // Store some payments
        for i in 0..100 {
            engine.store_payment(
                Uuid::new_v4(),
                (i as f64),
                ProcessorType::Default,
            ).await.unwrap();
        }
        
        let metrics = engine.get_metrics().await.unwrap();
        assert_eq!(metrics.total_payments, 100);
        assert!(metrics.memory_usage_bytes > 0);
        assert!(metrics.memory_efficiency() > 0.0);
    }
    
    #[tokio::test]
    async fn test_storage_utils() {
        use utils::*;
        
        // Test amount conversion
        assert_eq!(amount_to_cents(19.90), 1990);
        assert_eq!(cents_to_amount(1990), 19.90);
        
        // Test fee calculation
        assert_eq!(calculate_fee(100.0, ProcessorType::Default), 5.0);
        assert_eq!(calculate_fee(100.0, ProcessorType::Fallback), 15.0);
        
        // Test amount validation
        assert!(validate_amount(19.90).is_ok());
        assert!(validate_amount(-10.0).is_err());
        assert!(validate_amount(0.0).is_err());
        assert!(validate_amount(f64::NAN).is_err());
    }
}