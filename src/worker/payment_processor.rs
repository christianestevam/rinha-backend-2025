//! Payment processor worker implementation

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use crate::storage::{ProcessorType, Payment};

/// High-performance payment processor
#[derive(Debug)]
pub struct PaymentProcessor {
    processor_type: ProcessorType,
    processed_count: AtomicU64,
    total_amount: AtomicU64,
}

impl PaymentProcessor {
    /// Create new payment processor
    pub fn new(processor_type: ProcessorType) -> Self {
        Self {
            processor_type,
            processed_count: AtomicU64::new(0),
            total_amount: AtomicU64::new(0),
        }
    }

    /// Process a payment
    pub async fn process_payment(&self, payment: Payment) -> Result<ProcessedPayment> {
        // Simulate processing
        self.processed_count.fetch_add(1, Ordering::Relaxed);
        self.total_amount.fetch_add(payment.amount_cents, Ordering::Relaxed);

        Ok(ProcessedPayment {
            payment,
            processor_type: self.processor_type,
            processing_time_micros: 1000, // 1ms
            success: true,
        })
    }

    /// Get processing statistics
    pub fn get_stats(&self) -> ProcessorStats {
        ProcessorStats {
            processor_type: self.processor_type,
            processed_count: self.processed_count.load(Ordering::Relaxed),
            total_amount_cents: self.total_amount.load(Ordering::Relaxed),
        }
    }
}

/// Processed payment result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedPayment {
    pub payment: Payment,
    pub processor_type: ProcessorType,
    pub processing_time_micros: u32,
    pub success: bool,
}

/// Processor statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessorStats {
    pub processor_type: ProcessorType,
    pub processed_count: u64,
    pub total_amount_cents: u64,
}
