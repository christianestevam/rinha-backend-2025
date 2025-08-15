//! Atomic utilities for high-performance operations

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic f64 implementation using u64 backing
#[derive(Debug)]
pub struct AtomicF64 {
    bits: AtomicU64,
}

impl AtomicF64 {
    /// Create new atomic f64
    pub fn new(value: f64) -> Self {
        Self {
            bits: AtomicU64::new(value.to_bits()),
        }
    }

    /// Load value
    pub fn load(&self, ordering: Ordering) -> f64 {
        f64::from_bits(self.bits.load(ordering))
    }

    /// Store value
    pub fn store(&self, value: f64, ordering: Ordering) {
        self.bits.store(value.to_bits(), ordering)
    }

    /// Fetch and add
    pub fn fetch_add(&self, value: f64, ordering: Ordering) -> f64 {
        let mut current = self.load(ordering);
        loop {
            let new = current + value;
            match self.bits.compare_exchange_weak(
                current.to_bits(),
                new.to_bits(),
                ordering,
                Ordering::Relaxed,
            ) {
                Ok(_) => return current,
                Err(actual) => current = f64::from_bits(actual),
            }
        }
    }
}

impl Default for AtomicF64 {
    fn default() -> Self {
        Self::new(0.0)
    }
}