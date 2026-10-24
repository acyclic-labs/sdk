//! The shared footprint for native reference-counted allocations.

#[repr(C)]
struct Allocation<T> {
    _strong: std::sync::atomic::AtomicUsize,
    _weak: std::sync::atomic::AtomicUsize,
    _value: T,
}

/// Logical footprint of one `Arc<T>` allocation, including its counters and
/// alignment padding. Native accounting uses this before calling the allocator.
/// Allocator regressions verify the supported standard-library layout directly.
#[must_use]
pub const fn arc_allocation_bytes<T>() -> usize {
    std::mem::size_of::<Allocation<T>>()
}
