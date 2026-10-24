//! Thread-local allocation measurements and independent differential runs.
//!
//! A library never installs a global allocator: two in one binary fail to
//! link, and a dependency would silently replace the allocator of every
//! binary that links it. A test, bench or fuzz binary that measures declares
//! [`counting_allocator!`] once at its crate root.
pub mod differential;
mod fixtures;
pub use fixtures::source_path;
pub mod module_cases;
/// Compiler observations made on the build host for every module path case.
/// Target tests compare their actual source traversal with this independent proof.
pub const MODULE_COMPILER_REACHED: [bool; module_cases::CASES.len()] =
    include!(concat!(env!("OUT_DIR"), "/module-compiler.rs"));

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    sync::atomic::{AtomicBool, Ordering},
};
/// Successful allocations and net live storage within a measurement scope.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AllocationInfo {
    pub count_total: u64,
    pub count_current: i64,
    pub count_max: u64,
    pub bytes_total: u64,
    pub bytes_current: i64,
    pub bytes_max: u64,
}
impl std::ops::AddAssign for AllocationInfo {
    fn add_assign(&mut self, other: Self) {
        self.count_total += other.count_total;
        self.count_current += other.count_current;
        self.count_max += other.count_max;
        self.bytes_total += other.bytes_total;
        self.bytes_current += other.bytes_current;
        self.bytes_max += other.bytes_max;
    }
}
thread_local! {
    static COUNTER: Cell<Option<AllocationInfo>> = const { Cell::new(None) };
}
struct Scope(Option<AllocationInfo>);
impl Drop for Scope {
    fn drop(&mut self) {
        if let Some(parent) = &mut self.0 {
            let child = COUNTER.get().unwrap_or_default();
            let peak = |live: i64, child: u64| (live + child as i64).max(0) as u64;
            let count_max = parent
                .count_max
                .max(peak(parent.count_current, child.count_max));
            let bytes_max = parent
                .bytes_max
                .max(peak(parent.bytes_current, child.bytes_max));
            *parent += child;
            parent.count_max = count_max;
            parent.bytes_max = bytes_max;
        }
        COUNTER.set(self.0);
    }
}
/// Measure this thread, including nested scopes; restore on unwind without allocating.
///
/// # Panics
///
/// If this binary does not install [`CountingAllocator`], which would report
/// every closure as allocation-free.
pub fn measure(run: impl FnOnce()) -> AllocationInfo {
    assert!(
        INSTALLED.load(Ordering::Relaxed),
        "acyclic_test_support::measure needs `acyclic_test_support::counting_allocator!();` at this binary's crate root"
    );
    let scope = Scope(COUNTER.replace(Some(AllocationInfo::default())));
    run();
    let result = COUNTER.get().unwrap_or_default();
    drop(scope);
    result
}
/// Compare independently initialized implementations, including their complete
/// result/error/trace, and measure each without allocating measurement state.
pub fn differential<T: Eq + std::fmt::Debug>(
    baseline: impl FnOnce() -> T,
    candidate: impl FnOnce() -> T,
) -> (T, [AllocationInfo; 2]) {
    let (mut before, mut after) = (None, None);
    let baseline_work = measure(|| before = Some(baseline()));
    let candidate_work = measure(|| after = Some(candidate()));
    assert_eq!(
        before, after,
        "production behavior differs from independent baseline"
    );
    (
        after.expect("candidate completed"),
        [baseline_work, candidate_work],
    )
}

// Set by the first allocation through a CountingAllocator. Read before the
// store so steady-state allocations only share the cache line.
static INSTALLED: AtomicBool = AtomicBool::new(false);

fn record(pointer: *mut u8, size: usize, allocate: bool) -> *mut u8 {
    if !INSTALLED.load(Ordering::Relaxed) {
        INSTALLED.store(true, Ordering::Relaxed);
    }
    if !pointer.is_null()
        && let Some(mut info) = COUNTER.get()
    {
        let delta = if allocate { 1 } else { -1 };
        info.count_current += delta;
        info.bytes_current += delta * size as i64;
        if allocate {
            info.count_total += 1;
            info.bytes_total += size as u64;
            info.count_max = info.count_max.max(info.count_current.max(0) as u64);
            info.bytes_max = info.bytes_max.max(info.bytes_current.max(0) as u64);
        }
        COUNTER.set(Some(info));
    }
    pointer
}
/// Forwards to `A` and counts this thread's allocations inside [`measure`].
pub struct CountingAllocator<A>(A);

impl CountingAllocator<System> {
    /// The counting allocator over the system allocator.
    #[must_use]
    pub const fn system() -> Self {
        Self(System)
    }
}

/// Installs [`CountingAllocator`] as this binary's global allocator. Invoke it
/// once at the crate root of each test, bench or fuzz binary that measures.
#[macro_export]
macro_rules! counting_allocator {
    () => {
        #[global_allocator]
        static ACYCLIC_TEST_SUPPORT_ALLOCATOR: $crate::CountingAllocator<::std::alloc::System> =
            $crate::CountingAllocator::system();
    };
}
// SAFETY: each operation forwards its unchanged inputs to the same allocator;
// thread-local bookkeeping neither reads nor modifies the allocated storage.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller supplies a valid layout.
        unsafe { record(self.0.alloc(layout), layout.size(), true) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the backing allocator preserves the zeroing contract.
        unsafe { record(self.0.alloc_zeroed(layout), layout.size(), true) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: pointer/layout belong to this allocator; size is caller-validated.
        let next = unsafe { self.0.realloc(pointer, layout, size) };
        record(record(next, size, true), layout.size(), false)
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        record(pointer, layout.size(), false);
        // SAFETY: the caller supplies the allocation's original pointer/layout.
        unsafe { self.0.dealloc(pointer, layout) };
    }
}
#[cfg(test)]
counting_allocator!();
#[cfg(test)]
mod tests;
