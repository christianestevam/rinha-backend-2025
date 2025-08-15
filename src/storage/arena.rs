//! Arena memory allocator for zero-fragmentation, high-performance allocation
//! 
//! This allocator pre-allocates large chunks of memory and hands out portions
//! sequentially, eliminating fragmentation and providing O(1) allocation.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Size of each arena chunk (4MB)
const ARENA_SIZE: usize = 4 * 1024 * 1024;

/// Cache-line size for alignment optimization
const CACHE_LINE_SIZE: usize = 64;

/// Individual arena chunk
#[repr(align(64))] // Cache-line aligned
struct Arena {
    /// Start of the memory chunk
    start: *mut u8,
    /// Current position in the chunk
    current: AtomicPtr<u8>,
    /// End of the memory chunk
    end: *mut u8,
    /// Next arena in the chain
    next: AtomicPtr<Arena>,
}

impl Arena {
    /// Create a new arena with the specified size
    fn new(size: usize) -> Option<Box<Arena>> {
        let layout = Layout::from_size_align(size, CACHE_LINE_SIZE).ok()?;
        
        unsafe {
            let ptr = std::alloc::alloc(layout);
            if ptr.is_null() {
                return None;
            }
            
            let arena = Box::new(Arena {
                start: ptr,
                current: AtomicPtr::new(ptr),
                end: ptr.add(size),
                next: AtomicPtr::new(std::ptr::null_mut()),
            });
            
            Some(arena)
        }
    }
    
    /// Try to allocate from this arena
    fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        let size = layout.size();
        let align = layout.align();
        
        loop {
            let current = self.current.load(Ordering::Relaxed);
            
            // Align the current pointer
            let aligned = align_up(current as usize, align);
            let new_current = aligned + size;
            
            // Check if we have enough space
            if new_current > self.end as usize {
                return None;
            }
            
            // Try to update the current pointer atomically
            if self.current.compare_exchange_weak(
                current,
                new_current as *mut u8,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ).is_ok() {
                return Some(NonNull::new(aligned as *mut u8).unwrap());
            }
            
            // Another thread updated, try again
            std::hint::spin_loop();
        }
    }
    
    /// Reset this arena (dangerous - only use when no allocations are in use)
    unsafe fn reset(&self) {
        self.current.store(self.start, Ordering::Relaxed);
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align_unchecked(
                self.end as usize - self.start as usize,
                CACHE_LINE_SIZE,
            );
            std::alloc::dealloc(self.start, layout);
        }
    }
}

/// High-performance arena allocator
pub struct ArenaAllocator {
    /// Current arena being used for allocations
    current: AtomicPtr<Arena>,
    /// Statistics
    stats: ArenaStats,
}

#[derive(Default)]
struct ArenaStats {
    /// Total bytes allocated
    bytes_allocated: AtomicUsize,
    /// Number of allocations
    allocations: AtomicUsize,
    /// Number of arenas created
    arenas_created: AtomicUsize,
}

impl ArenaAllocator {
    /// Create a new arena allocator
    pub fn new() -> Self {
        let initial_arena = Arena::new(ARENA_SIZE)
            .expect("Failed to create initial arena");
        
        Self {
            current: AtomicPtr::new(Box::into_raw(initial_arena)),
            stats: ArenaStats::default(),
        }
    }
    
    /// Allocate memory with the given layout
    pub fn allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        // Try to allocate from current arena
        let current_arena = self.current.load(Ordering::Acquire);
        if !current_arena.is_null() {
            unsafe {
                if let Some(ptr) = (*current_arena).allocate(layout) {
                    self.stats.bytes_allocated.fetch_add(layout.size(), Ordering::Relaxed);
                    self.stats.allocations.fetch_add(1, Ordering::Relaxed);
                    return Some(ptr);
                }
            }
        }
        
        // Current arena is full, create a new one
        self.grow_and_allocate(layout)
    }
    
    /// Create a new arena and allocate from it
    fn grow_and_allocate(&self, layout: Layout) -> Option<NonNull<u8>> {
        // Calculate arena size (at least double the requested size, minimum ARENA_SIZE)
        let new_arena_size = std::cmp::max(ARENA_SIZE, layout.size() * 2);
        
        let new_arena = Arena::new(new_arena_size)?;
        let new_arena_ptr = Box::into_raw(new_arena);
        
        unsafe {
            // Set up the chain
            let old_arena = self.current.swap(new_arena_ptr, Ordering::AcqRel);
            if !old_arena.is_null() {
                (*new_arena_ptr).next.store(old_arena, Ordering::Relaxed);
            }
            
            // Allocate from the new arena
            let result = (*new_arena_ptr).allocate(layout);
            if result.is_some() {
                self.stats.bytes_allocated.fetch_add(layout.size(), Ordering::Relaxed);
                self.stats.allocations.fetch_add(1, Ordering::Relaxed);
                self.stats.arenas_created.fetch_add(1, Ordering::Relaxed);
            }
            
            result
        }
    }
    
    /// Get allocation statistics
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.stats.bytes_allocated.load(Ordering::Relaxed),
            self.stats.allocations.load(Ordering::Relaxed),
            self.stats.arenas_created.load(Ordering::Relaxed),
        )
    }
    
    /// Reset all arenas (DANGEROUS - only use when no allocations are active)
    pub unsafe fn reset(&self) {
        let mut current = self.current.load(Ordering::Acquire);
        
        while !current.is_null() {
            (*current).reset();
            current = (*current).next.load(Ordering::Relaxed);
        }
        
        // Reset statistics
        self.stats.bytes_allocated.store(0, Ordering::Relaxed);
        self.stats.allocations.store(0, Ordering::Relaxed);
    }
}

impl Default for ArenaAllocator {
    fn default() -> Self {
        Self::new()
    }
}

// Thread-local arena for even faster allocations
thread_local! {
    static THREAD_ARENA: Cell<Option<ArenaAllocator>> = const { Cell::new(None) };
}

/// Get or create thread-local arena
pub fn with_thread_arena<F, R>(f: F) -> R
where
    F: FnOnce(&ArenaAllocator) -> R,
{
    THREAD_ARENA.with(|arena| {
        if arena.get().is_none() {
            arena.set(Some(ArenaAllocator::new()));
        }
        
        // SAFETY: We just ensured the arena exists
        unsafe {
            let arena_ref = arena.as_ptr().as_ref().unwrap().as_ref().unwrap();
            f(arena_ref)
        }
    })
}

/// Align a value up to the nearest multiple of align
#[inline(always)]
fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

// Global allocator implementation (optional - can be used to replace system allocator)
unsafe impl GlobalAlloc for ArenaAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        self.allocate(layout)
            .map(|ptr| ptr.as_ptr())
            .unwrap_or(std::ptr::null_mut())
    }
    
    unsafe fn dealloc(&self, _ptr: *mut u8, _layout: Layout) {
        // Arena allocator doesn't deallocate individual allocations
        // Memory is freed when the entire arena is dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_allocation() {
        let arena = ArenaAllocator::new();
        
        let layout = Layout::new::<u64>();
        let ptr1 = arena.allocate(layout).unwrap();
        let ptr2 = arena.allocate(layout).unwrap();
        
        // Pointers should be different
        assert_ne!(ptr1.as_ptr(), ptr2.as_ptr());
        
        // Should be properly aligned
        assert_eq!(ptr1.as_ptr() as usize % layout.align(), 0);
        assert_eq!(ptr2.as_ptr() as usize % layout.align(), 0);
    }
    
    #[test]
    fn test_large_allocation() {
        let arena = ArenaAllocator::new();
        
        // Allocate something larger than default arena size
        let large_size = ARENA_SIZE * 2;
        let layout = Layout::from_size_align(large_size, 8).unwrap();
        let ptr = arena.allocate(layout).unwrap();
        
        assert_eq!(ptr.as_ptr() as usize % layout.align(), 0);
    }
    
    #[test]
    fn test_stats() {
        let arena = ArenaAllocator::new();
        
        let layout = Layout::new::<u64>();
        arena.allocate(layout).unwrap();
        arena.allocate(layout).unwrap();
        
        let (bytes, allocs, arenas) = arena.stats();
        assert_eq!(bytes, 16); // 2 * 8 bytes
        assert_eq!(allocs, 2);
        assert_eq!(arenas, 0); // Still using initial arena
    }
    
    #[test]
    fn test_thread_local_arena() {
        with_thread_arena(|arena| {
            let layout = Layout::new::<u32>();
            let ptr = arena.allocate(layout).unwrap();
            assert!(!ptr.as_ptr().is_null());
        });
    }
}