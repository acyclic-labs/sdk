use super::*;
use proptest::prelude::*;
use std::cell::Cell;

thread_local! {
    static CALLS: Cell<[usize; 4]> = const { Cell::new([0; 4]) };
    static FAIL: Cell<bool> = const { Cell::new(false) };
}

fn called(index: usize) {
    CALLS.with(|calls| {
        let mut value = calls.get();
        value[index] += 1;
        calls.set(value);
    });
}

struct Spy;
// SAFETY: Spy forwards each successful operation to System with identical inputs.
unsafe impl GlobalAlloc for Spy {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        called(0);
        if FAIL.with(Cell::get) {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        called(1);
        if FAIL.with(Cell::get) {
            return std::ptr::null_mut();
        }
        // SAFETY: the caller supplies a valid allocation layout.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        called(2);
        if FAIL.with(Cell::get) {
            return std::ptr::null_mut();
        }
        // SAFETY: the pointer/layout belong to System and size satisfies realloc.
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        called(3);
        // SAFETY: the pointer/layout belong to System and remain live.
        unsafe { System.dealloc(pointer, layout) };
    }
}

// The former implementation omitted both fast paths. Keep the real trait
// defaults as the negative control, rather than inventing measurement overhead.
struct MissingFastPaths(CountingAllocator<Spy>);
// SAFETY: allocation provenance and layout are forwarded unchanged.
unsafe impl GlobalAlloc for MissingFastPaths {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's valid allocation layout.
        unsafe { self.0.alloc(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forward the allocation's original pointer and layout.
        unsafe { self.0.dealloc(pointer, layout) };
    }
}

fn exercise(
    allocator: &impl GlobalAlloc,
    before: usize,
    after: usize,
    alignment: usize,
) -> (crate::AllocationInfo, [usize; 4]) {
    CALLS.with(|calls| calls.set([0; 4]));
    let layout = Layout::from_size_align(before, alignment).unwrap();
    let next = Layout::from_size_align(after, alignment).unwrap();
    let info = crate::measure(|| {
        // SAFETY: all sizes are positive, layouts are valid, and each allocation
        // is read within its initialized extent and released exactly once.
        unsafe {
            let pointer = allocator.alloc_zeroed(layout);
            assert!(!pointer.is_null());
            assert_eq!((pointer as usize) % alignment, 0);
            assert!(std::slice::from_raw_parts(pointer, before)
                .iter()
                .all(|byte| *byte == 0));
            std::ptr::write_bytes(pointer, 0x5a, before);
            let replacement = allocator.realloc(pointer, layout, after);
            assert!(!replacement.is_null());
            assert_eq!((replacement as usize) % alignment, 0);
            assert!(std::slice::from_raw_parts(replacement, before.min(after))
                .iter()
                .all(|byte| *byte == 0x5a));
            allocator.dealloc(replacement, next);
        }
    });
    (info, CALLS.with(Cell::get))
}

proptest! {
    // Delegation preserves requested bytes/alignment and the retained prefix;
    // successful accounting is identical to the old allocate-copy-free path.
    #[test]
    fn native_fast_paths_preserve_bytes_alignment_and_all_counts(
        before in 1usize..4097, after in 1usize..8193, power in 0u32..7,
    ) {
        let (actual, calls) = exercise(&CountingAllocator(Spy), before, after, 1usize << power);
        let (former, broken_calls) = exercise(&MissingFastPaths(CountingAllocator(Spy)), before, after, 1usize << power);
        prop_assert_eq!(actual, former);
        prop_assert_eq!(actual.count_total, 2);
        prop_assert_eq!(actual.count_current, 0);
        prop_assert_eq!(actual.count_max, 2);
        prop_assert_eq!(actual.bytes_total, (before + after) as u64);
        prop_assert_eq!(actual.bytes_current, 0);
        prop_assert_eq!(actual.bytes_max, (before + after) as u64);
        let native = |calls| calls == [0, 1, 1, 1];
        prop_assert!(native(calls));
        prop_assert!(!native(broken_calls), "old trait defaults escaped delegation checks");
        prop_assert_eq!(broken_calls, [2, 0, 0, 2]);
    }
}

#[test]
fn failed_native_calls_do_not_invent_allocations_or_release_the_original() {
    let allocator = CountingAllocator(Spy);
    let layout = Layout::from_size_align(32, 8).unwrap();
    // SAFETY: this positive valid layout is allocated and released by the same
    // allocator. A failed realloc leaves the original pointer live.
    unsafe {
        let pointer = allocator.alloc_zeroed(layout);
        assert!(!pointer.is_null());
        FAIL.with(|fail| fail.set(true));
        let info = crate::measure(|| {
            assert!(allocator.alloc(layout).is_null());
            assert!(allocator.alloc_zeroed(layout).is_null());
            assert!(allocator.realloc(pointer, layout, 64).is_null());
        });
        FAIL.with(|fail| fail.set(false));
        assert_eq!(info, crate::AllocationInfo::default());
        assert!(std::slice::from_raw_parts(pointer, 32)
            .iter()
            .all(|byte| *byte == 0));
        allocator.dealloc(pointer, layout);
    }
}
