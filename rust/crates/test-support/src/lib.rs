//! Thread-local allocation measurements and independent differential runs.
pub mod differential;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
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
pub fn measure(run: impl FnOnce()) -> AllocationInfo {
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

fn record(pointer: *mut u8, size: usize, allocate: bool) -> *mut u8 {
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
struct CountingAllocator<A>(A);
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
#[global_allocator]
static GLOBAL: CountingAllocator<System> = CountingAllocator(System);
#[cfg(test)]
mod tests;
