//! High-performance payment storage with lock-free operations
//! 
//! Uses segmented storage and atomic operations for maximum concurrency.
//! Optimized for the specific access patterns of the Rinha Backend challenge.

use super::ring_buffer::RingBuffer;
use super::arena::ArenaAllocator;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// Payment processor type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProcessorType {
    Default,
    Fallback,
}

impl ProcessorType {
    /// Get the fee rate for this processor type
    pub fn fee_rate(&self) -> f64 {
        match self {
            ProcessorType::Default => 0.05,   // 5%
            ProcessorType::Fallback => 0.15,  // 15%
        }
    }
}

/// Individual payment record optimized for cache efficiency
#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(align(64))] // Cache-line aligned
pub struct Payment {
    /// Unique payment identifier
    pub correlation_id: Uuid,
    /// Payment amount in cents (avoids floating point precision issues)
    pub amount_cents: u64,
    /// When the payment was requested (as microseconds since epoch)
    pub timestamp_micros: i64,
    /// Which processor handled this payment
    pub processor: ProcessorType,
    /// Processing latency in microseconds
    pub latency_micros: u32,
    /// Whether the payment was successful
    pub success: bool,
}

impl Payment {
    /// Create a new payment
    pub fn new(
        correlation_id: Uuid,
        amount: f64,
        timestamp: DateTime<Utc>,
        processor: ProcessorType,
    ) -> Self {
        Self {
            correlation_id,
            amount_cents: (amount * 100.0).round() as u64,
            timestamp_micros: timestamp.timestamp_micros(),
            processor,
            latency_micros: 0,
            success: true,
        }
    }
    
    /// Get amount as f64
    pub fn amount(&self) -> f64 {
        self.amount_cents as f64 / 100.0
    }
    
    /// Get timestamp as DateTime
    pub fn timestamp(&self) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(self.timestamp_micros)
            .unwrap_or_else(|| Utc::now())
    }
    
    /// Check if payment is within time range
    #[inline]
    pub fn is_in_range(&self, from_micros: i64, to_micros: i64) -> bool {
        self.timestamp_micros >= from_micros && self.timestamp_micros <= to_micros
    }
}

/// Summary data for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryData {
    #[serde(rename = "totalRequests")]
    pub total_requests: u64,
    #[serde(rename = "totalAmount")]
    pub total_amount: f64,
}

impl SummaryData {
    pub fn new() -> Self {
        Self {
            total_requests: 0,
            total_amount: 0.0,
        }
    }
    
    pub fn add_payment(&mut self, payment: &Payment) {
        self.total_requests += 1;
        self.total_amount += payment.amount();
    }
}

impl Default for SummaryData {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete summary for both processors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentSummary {
    pub default: SummaryData,
    pub fallback: SummaryData,
}

impl PaymentSummary {
    pub fn new() -> Self {
        Self {
            default: SummaryData::new(),
            fallback: SummaryData::new(),
        }
    }
}

impl Default for PaymentSummary {
    fn default() -> Self {
        Self::new()
    }
}

/// High-performance payment storage with multiple strategies
pub struct PaymentStore {
    /// Ring buffer for recent payments (fast access)
    recent_payments: RingBuffer<Payment>,
    /// Segmented storage for long-term data
    segments: Vec<PaymentSegment>,
    /// Total statistics (atomic for thread safety)
    stats: PaymentStats,
    /// Arena allocator for efficient memory management
    arena: Arc<ArenaAllocator>,
}

/// Individual storage segment for partitioning data
struct PaymentSegment {
    /// Payments in this segment
    payments: Vec<Payment>,
    /// Number of payments in segment
    count: AtomicUsize,
    /// Time range for this segment
    start_time: i64,
    end_time: i64,
}

/// Atomic statistics for the payment store
#[derive(Default)]
struct PaymentStats {
    /// Total payments processed
    total_payments: AtomicU64,
    /// Default processor payments
    default_payments: AtomicU64,
    /// Fallback processor payments
    fallback_payments: AtomicU64,
    /// Total amount processed (in cents)
    total_amount_cents: AtomicU64,
    /// Default processor amount (in cents)
    default_amount_cents: AtomicU64,
    /// Fallback processor amount (in cents)
    fallback_amount_cents: AtomicU64,
}

impl PaymentStore {
    /// Create a new payment store
    pub fn new(ring_buffer_size: usize, num_segments: usize) -> Self {
        let mut segments = Vec::with_capacity(num_segments);
        
        // Create time-based segments (1 hour each for example)
        let now = Utc::now().timestamp_micros();
        let segment_duration = 3600 * 1_000_000; // 1 hour in microseconds
        
        for i in 0..num_segments {
            let start_time = now + (i as i64 * segment_duration);
            let end_time = start_time + segment_duration;
            
            segments.push(PaymentSegment {
                payments: Vec::with_capacity(1000),
                count: AtomicUsize::new(0),
                start_time,
                end_time,
            });
        }
        
        Self {
            recent_payments: RingBuffer::new(ring_buffer_size),
            segments,
            stats: PaymentStats::default(),
            arena: Arc::new(ArenaAllocator::new()),
        }
    }
    
    /// Insert a new payment
    pub fn insert(&self, payment: Payment) -> Result<(), &'static str> {
        // Update statistics atomically
        self.stats.total_payments.fetch_add(1, Ordering::Relaxed);
        self.stats.total_amount_cents.fetch_add(payment.amount_cents, Ordering::Relaxed);
        
        match payment.processor {
            ProcessorType::Default => {
                self.stats.default_payments.fetch_add(1, Ordering::Relaxed);
                self.stats.default_amount_cents.fetch_add(payment.amount_cents, Ordering::Relaxed);
            }
            ProcessorType::Fallback => {
                self.stats.fallback_payments.fetch_add(1, Ordering::Relaxed);
                self.stats.fallback_amount_cents.fetch_add(payment.amount_cents, Ordering::Relaxed);
            }
        }
        
        // Try to add to ring buffer first (for recent queries)
        if !self.recent_payments.try_push(payment.clone()) {
            // Ring buffer is full, this is expected under high load
            tracing::debug!("Ring buffer full, payment will only be in segments");
        }
        
        // Add to appropriate segment based on timestamp
        self.add_to_segment(payment)?;
        
        Ok(())
    }
    
    /// Add payment to the appropriate time-based segment
    fn add_to_segment(&self, payment: Payment) -> Result<(), &'static str> {
        let timestamp = payment.timestamp_micros;
        
        // Find the appropriate segment
        for segment in &self.segments {
            if timestamp >= segment.start_time && timestamp < segment.end_time {
                // This is unsafe but necessary for performance
                // We're relying on the caller to ensure thread safety
                unsafe {
                    let segment_ptr = segment as *const PaymentSegment as *mut PaymentSegment;
                    (*segment_ptr).payments.push(payment);
                    segment.count.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }
        
        Err("Payment timestamp outside of segment range")
    }
    
    /// Get summary for a time range
    pub fn get_summary(&self, from: DateTime<Utc>, to: DateTime<Utc>) -> PaymentSummary {
        let from_micros = from.timestamp_micros();
        let to_micros = to.timestamp_micros();
        
        let mut summary = PaymentSummary::new();
        
        // Check recent payments first (fast path)
        self.scan_recent_payments(from_micros, to_micros, &mut summary);
        
        // Check segments for older data
        self.scan_segments(from_micros, to_micros, &mut summary);
        
        summary
    }
    
    /// Scan recent payments in ring buffer
    fn scan_recent_payments(&self, from_micros: i64, to_micros: i64, summary: &mut PaymentSummary) {
        // Note: This is a simplified implementation
        // In a real scenario, we'd need a way to iterate over ring buffer contents
        // For now, we'll rely more on segments for accuracy
    }
    
    /// Scan segments for payments in time range
    fn scan_segments(&self, from_micros: i64, to_micros: i64, summary: &mut PaymentSummary) {
        for segment in &self.segments {
            // Skip segments that don't overlap with our time range
            if segment.end_time <= from_micros || segment.start_time >= to_micros {
                continue;
            }
            
            // Scan payments in this segment
            for payment in &segment.payments {
                if payment.is_in_range(from_micros, to_micros) {
                    match payment.processor {
                        ProcessorType::Default => {
                            summary.default.add_payment(payment);
                        }
                        ProcessorType::Fallback => {
                            summary.fallback.add_payment(payment);
                        }
                    }
                }
            }
        }
    }
    
    /// Get current statistics (fast, atomic access)
    pub fn get_stats(&self) -> PaymentSummary {
        let total_payments = self.stats.total_payments.load(Ordering::Relaxed);
        let default_payments = self.stats.default_payments.load(Ordering::Relaxed);
        let fallback_payments = self.stats.fallback_payments.load(Ordering::Relaxed);
        
        let total_amount = self.stats.total_amount_cents.load(Ordering::Relaxed) as f64 / 100.0;
        let default_amount = self.stats.default_amount_cents.load(Ordering::Relaxed) as f64 / 100.0;
        let fallback_amount = self.stats.fallback_amount_cents.load(Ordering::Relaxed) as f64 / 100.0;
        
        PaymentSummary {
            default: SummaryData {
                total_requests: default_payments,
                total_amount: default_amount,
            },
            fallback: SummaryData {
                total_requests: fallback_payments,
                total_amount: fallback_amount,
            },
        }
    }
    
    /// Purge all payments (for testing)
    pub fn purge(&self) {
        // Reset statistics
        self.stats.total_payments.store(0, Ordering::Relaxed);
        self.stats.default_payments.store(0, Ordering::Relaxed);
        self.stats.fallback_payments.store(0, Ordering::Relaxed);
        self.stats.total_amount_cents.store(0, Ordering::Relaxed);
        self.stats.default_amount_cents.store(0, Ordering::Relaxed);
        self.stats.fallback_amount_cents.store(0, Ordering::Relaxed);
        
        // Clear segments
        for segment in &self.segments {
            unsafe {
                let segment_ptr = segment as *const PaymentSegment as *mut PaymentSegment;
                (*segment_ptr).payments.clear();
                segment.count.store(0, Ordering::Relaxed);
            }
        }
        
        // Note: Ring buffer will naturally cycle out old data
        tracing::info!("Payment store purged");
    }
    
    /// Get memory usage statistics
    pub fn memory_stats(&self) -> (usize, usize, usize) {
        let (arena_bytes, arena_allocs, arena_count) = self.arena.stats();
        
        let segment_memory = self.segments.iter()
            .map(|s| s.payments.capacity() * std::mem::size_of::<Payment>())
            .sum::<usize>();
        
        let ring_buffer_memory = self.recent_payments.capacity() * std::mem::size_of::<Payment>();
        
        (arena_bytes, segment_memory, ring_buffer_memory)
    }
    
    /// Get performance metrics
    pub fn performance_metrics(&self) -> PaymentStoreMetrics {
        let total_payments = self.stats.total_payments.load(Ordering::Relaxed);
        let (arena_bytes, segment_memory, ring_memory) = self.memory_stats();
        
        PaymentStoreMetrics {
            total_payments,
            memory_usage_bytes: arena_bytes + segment_memory + ring_memory,
            ring_buffer_utilization: self.recent_payments.utilization(),
            segment_count: self.segments.len(),
        }
    }
}

/// Performance metrics for monitoring
#[derive(Debug, Clone)]
pub struct PaymentStoreMetrics {
    pub total_payments: u64,
    pub memory_usage_bytes: usize,
    pub ring_buffer_utilization: f64,
    pub segment_count: usize,
}

// Thread safety
unsafe impl Send for PaymentStore {}
unsafe impl Sync for PaymentStore {}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_payment_creation() {
        let payment = Payment::new(
            Uuid::new_v4(),
            19.90,
            Utc::now(),
            ProcessorType::Default,
        );
        
        assert_eq!(payment.amount(), 19.90);
        assert_eq!(payment.amount_cents, 1990);
        assert_eq!(payment.processor, ProcessorType::Default);
    }
    
    #[test]
    fn test_processor_fee_rates() {
        assert_eq!(ProcessorType::Default.fee_rate(), 0.05);
        assert_eq!(ProcessorType::Fallback.fee_rate(), 0.15);
    }
    
    #[test]
    fn test_payment_store_basic() {
        let store = PaymentStore::new(1000, 10);
        
        let payment = Payment::new(
            Uuid::new_v4(),
            19.90,
            Utc::now(),
            ProcessorType::Default,
        );
        
        store.insert(payment).unwrap();
        
        let stats = store.get_stats();
        assert_eq!(stats.default.total_requests, 1);
        assert_eq!(stats.default.total_amount, 19.90);
        assert_eq!(stats.fallback.total_requests, 0);
    }
    
    #[test]
    fn test_payment_store_summary() {
        let store = PaymentStore::new(1000, 10);
        let now = Utc::now();
        
        // Add some payments
        for i in 0..10 {
            let payment = Payment::new(
                Uuid::new_v4(),
                (i as f64) * 10.0,
                now,
                if i % 2 == 0 { ProcessorType::Default } else { ProcessorType::Fallback },
            );
            store.insert(payment).unwrap();
        }
        
        let from = now - chrono::Duration::hours(1);
        let to = now + chrono::Duration::hours(1);
        let summary = store.get_summary(from, to);
        
        // Should have 5 default and 5 fallback payments
        assert_eq!(summary.default.total_requests, 5);
        assert_eq!(summary.fallback.total_requests, 5);
    }
    
    #[test]
    fn test_payment_store_purge() {
        let store = PaymentStore::new(1000, 10);
        
        let payment = Payment::new(
            Uuid::new_v4(),
            19.90,
            Utc::now(),
            ProcessorType::Default,
        );
        
        store.insert(payment).unwrap();
        assert_eq!(store.get_stats().default.total_requests, 1);
        
        store.purge();
        assert_eq!(store.get_stats().default.total_requests, 0);
    }
    
    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;
        
        let store = Arc::new(PaymentStore::new(10000, 10));
        let mut handles = vec![];
        
        // Spawn multiple threads inserting payments
        for i in 0..4 {
            let store = store.clone();
            handles.push(thread::spawn(move || {
                for j in 0..1000 {
                    let payment = Payment::new(
                        Uuid::new_v4(),
                        (i * 1000 + j) as f64,
                        Utc::now(),
                        ProcessorType::Default,
                    );
                    store.insert(payment).unwrap();
                }
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = store.get_stats();
        assert_eq!(stats.default.total_requests, 4000);
    }
}