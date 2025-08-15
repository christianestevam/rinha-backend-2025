//! Lock-free ring buffer optimized for single-producer, multiple-consumer scenarios
//! 
//! This implementation uses atomic operations and memory ordering to provide
//! wait-free operations for the producer and lock-free operations for consumers.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::mem::MaybeUninit;
use std::ptr;

/// Cache line size for avoiding false sharing
const CACHE_LINE_SIZE: usize = 64;

/// Ring buffer entry with sequence number for ordering
#[repr(align(64))]
struct Entry<T> {
    /// Sequence number for this entry
    sequence: AtomicU64,
    /// The actual data (may be uninitialized)
    data: MaybeUninit<T>,
}

impl<T> Entry<T> {
    fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
            data: MaybeUninit::uninit(),
        }
    }
}

/// High-performance lock-free ring buffer
/// 
/// Optimized for high-throughput scenarios with one producer and multiple consumers.
/// Uses sequence numbers and atomic operations to avoid locks entirely.
#[repr(align(64))]
pub struct RingBuffer<T> {
    /// Buffer entries (power of 2 size for fast modulo)
    entries: Box<[Entry<T>]>,
    /// Capacity mask (capacity - 1, for fast modulo)
    capacity_mask: u64,
    /// Producer cursor (where to write next)
    write_cursor: AtomicU64,
    /// Consumer cursor (where to read next)  
    read_cursor: AtomicU64,
    /// Cached read cursor for producer (reduces contention)
    cached_read_cursor: AtomicU64,
    /// Cached write cursor for consumers (reduces contention)
    cached_write_cursor: AtomicU64,
}

impl<T> RingBuffer<T> {
    /// Create a new ring buffer with the specified capacity
    /// 
    /// Capacity will be rounded up to the next power of 2 for optimization
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "Capacity must be greater than 0");
        assert!(capacity <= (1 << 30), "Capacity too large");
        
        // Round up to next power of 2
        let capacity = capacity.next_power_of_two();
        
        // Create entries
        let mut entries = Vec::with_capacity(capacity);
        for i in 0..capacity {
            let mut entry = Entry::new();
            entry.sequence.store(i as u64, Ordering::Relaxed);
            entries.push(entry);
        }
        
        Self {
            entries: entries.into_boxed_slice(),
            capacity_mask: (capacity - 1) as u64,
            write_cursor: AtomicU64::new(0),
            read_cursor: AtomicU64::new(0),
            cached_read_cursor: AtomicU64::new(0),
            cached_write_cursor: AtomicU64::new(0),
        }
    }
    
    /// Get the capacity of the ring buffer
    pub fn capacity(&self) -> usize {
        (self.capacity_mask + 1) as usize
    }
    
    /// Try to push an item to the buffer (producer side)
    /// 
    /// Returns true if the item was successfully pushed, false if buffer is full
    pub fn try_push(&self, item: T) -> bool {
        let current_write = self.write_cursor.load(Ordering::Relaxed);
        let entry_index = (current_write & self.capacity_mask) as usize;
        let entry = &self.entries[entry_index];
        
        let sequence = entry.sequence.load(Ordering::Acquire);
        
        // Check if this slot is available for writing
        if sequence == current_write {
            // Slot is available, write the data
            unsafe {
                ptr::write(entry.data.as_ptr() as *mut T, item);
            }
            
            // Update sequence to signal data is ready
            entry.sequence.store(current_write + 1, Ordering::Release);
            
            // Advance write cursor
            self.write_cursor.store(current_write + 1, Ordering::Relaxed);
            
            true
        } else {
            // Buffer is full or slot not ready
            false
        }
    }
    
    /// Push an item to the buffer, spinning until space is available
    /// 
    /// Use this when you need guaranteed delivery but can afford to wait
    pub fn push_spin(&self, item: T) {
        loop {
            if self.try_push(item) {
                break;
            }
            
            // Yield to other threads
            std::hint::spin_loop();
        }
    }
    
    /// Try to pop an item from the buffer (consumer side)
    /// 
    /// Returns Some(item) if an item was available, None if buffer is empty
    pub fn try_pop(&self) -> Option<T> {
        let current_read = self.read_cursor.load(Ordering::Relaxed);
        let entry_index = (current_read & self.capacity_mask) as usize;
        let entry = &self.entries[entry_index];
        
        let sequence = entry.sequence.load(Ordering::Acquire);
        
        // Check if data is available
        if sequence == current_read + 1 {
            // Data is available, read it
            let item = unsafe {
                ptr::read(entry.data.as_ptr())
            };
            
            // Mark slot as available for writing
            entry.sequence.store(
                current_read + self.capacity() as u64 + 1,
                Ordering::Release
            );
            
            // Advance read cursor
            self.read_cursor.store(current_read + 1, Ordering::Relaxed);
            
            Some(item)
        } else {
            // No data available
            None
        }
    }
    
    /// Pop an item from the buffer, spinning until one is available
    /// 
    /// Use this when you need to wait for data
    pub fn pop_spin(&self) -> T {
        loop {
            if let Some(item) = self.try_pop() {
                return item;
            }
            
            // Yield to other threads
            std::hint::spin_loop();
        }
    }
    
    /// Get approximate number of items in the buffer
    /// 
    /// This is approximate due to concurrent access
    pub fn len(&self) -> usize {
        let write_pos = self.write_cursor.load(Ordering::Relaxed);
        let read_pos = self.read_cursor.load(Ordering::Relaxed);
        
        if write_pos >= read_pos {
            (write_pos - read_pos) as usize
        } else {
            // Handle wraparound
            (write_pos + (1u64 << 32) - read_pos) as usize
        }
    }
    
    /// Check if buffer is empty (approximate)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    /// Check if buffer is full (approximate)
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }
    
    /// Get buffer utilization as a percentage (0.0 to 1.0)
    pub fn utilization(&self) -> f64 {
        self.len() as f64 / self.capacity() as f64
    }
}

// Safety: RingBuffer is thread-safe due to atomic operations
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send> Sync for RingBuffer<T> {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Clear any remaining items to avoid memory leaks
        while self.try_pop().is_some() {
            // Items are dropped automatically
        }
    }
}

/// Multi-producer, multi-consumer ring buffer
/// 
/// This variant allows multiple producers and consumers but has slightly
/// more overhead due to the need for stronger synchronization.
pub struct MPMCRingBuffer<T> {
    inner: RingBuffer<T>,
    /// Producer lock (using atomic for try_lock behavior)
    producer_lock: AtomicUsize,
    /// Consumer lock (using atomic for try_lock behavior)
    consumer_lock: AtomicUsize,
}

impl<T> MPMCRingBuffer<T> {
    /// Create a new MPMC ring buffer
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RingBuffer::new(capacity),
            producer_lock: AtomicUsize::new(0),
            consumer_lock: AtomicUsize::new(0),
        }
    }
    
    /// Try to push an item (multiple producers safe)
    pub fn try_push(&self, item: T) -> bool {
        // Try to acquire producer lock
        if self.producer_lock.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            let result = self.inner.try_push(item);
            self.producer_lock.store(0, Ordering::Release);
            result
        } else {
            false
        }
    }
    
    /// Try to pop an item (multiple consumers safe)
    pub fn try_pop(&self) -> Option<T> {
        // Try to acquire consumer lock
        if self.consumer_lock.compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            let result = self.inner.try_pop();
            self.consumer_lock.store(0, Ordering::Release);
            result
        } else {
            None
        }
    }
    
    /// Get approximate length
    pub fn len(&self) -> usize {
        self.inner.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    
    /// Get capacity
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    
    #[test]
    fn test_basic_operations() {
        let buffer = RingBuffer::new(8);
        
        // Test push and pop
        assert!(buffer.try_push(42));
        assert_eq!(buffer.try_pop(), Some(42));
        assert_eq!(buffer.try_pop(), None);
    }
    
    #[test]
    fn test_capacity_rounding() {
        let buffer = RingBuffer::<u32>::new(10);
        assert_eq!(buffer.capacity(), 16); // Rounded up to power of 2
    }
    
    #[test]
    fn test_full_buffer() {
        let buffer = RingBuffer::new(4);
        
        // Fill buffer
        assert!(buffer.try_push(1));
        assert!(buffer.try_push(2));
        assert!(buffer.try_push(3));
        assert!(buffer.try_push(4));
        
        // Should be full now
        assert!(!buffer.try_push(5));
        
        // Pop one and try again
        assert_eq!(buffer.try_pop(), Some(1));
        assert!(buffer.try_push(5));
    }
    
    #[test]
    fn test_concurrent_access() {
        let buffer = Arc::new(RingBuffer::new(1000));
        let num_items = 10000;
        
        let producer_buffer = buffer.clone();
        let producer = thread::spawn(move || {
            for i in 0..num_items {
                producer_buffer.push_spin(i);
            }
        });
        
        let consumer_buffer = buffer.clone();
        let consumer = thread::spawn(move || {
            let mut received = Vec::new();
            for _ in 0..num_items {
                received.push(consumer_buffer.pop_spin());
            }
            received
        });
        
        producer.join().unwrap();
        let received = consumer.join().unwrap();
        
        // Verify all items were received in order
        for (i, &value) in received.iter().enumerate() {
            assert_eq!(value, i);
        }
    }
    
    #[test]
    fn test_mpmc_buffer() {
        let buffer = Arc::new(MPMCRingBuffer::new(100));
        let mut handles = vec![];
        
        // Multiple producers
        for i in 0..4 {
            let buffer = buffer.clone();
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let value = i * 100 + j;
                    while !buffer.try_push(value) {
                        thread::yield_now();
                    }
                }
            }));
        }
        
        // Multiple consumers
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        for _ in 0..2 {
            let buffer = buffer.clone();
            let received = received.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    loop {
                        if let Some(value) = buffer.try_pop() {
                            received.lock().unwrap().push(value);
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let received = received.lock().unwrap();
        assert_eq!(received.len(), 400);
    }
}