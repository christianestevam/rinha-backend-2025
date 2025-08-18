use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicMetrics {
    submitted: AtomicU64,
    processed: AtomicU64,
    failed: AtomicU64,
}

impl AtomicMetrics {
    pub fn new() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        }
    }

    pub fn increment_submitted(&self) {
        self.submitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_processed(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_submitted(&self) -> u64 {
        self.submitted.load(Ordering::Relaxed)
    }

    pub fn get_processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn get_failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}