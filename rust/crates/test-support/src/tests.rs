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
            assert!(
                std::slice::from_raw_parts(pointer, before)
                    .iter()
                    .all(|byte| *byte == 0)
            );
            std::ptr::write_bytes(pointer, 0x5a, before);
            let replacement = allocator.realloc(pointer, layout, after);
            assert!(!replacement.is_null());
            assert_eq!((replacement as usize) % alignment, 0);
            assert!(
                std::slice::from_raw_parts(replacement, before.min(after))
                    .iter()
                    .all(|byte| *byte == 0x5a)
            );
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
        assert!(
            std::slice::from_raw_parts(pointer, 32)
                .iter()
                .all(|byte| *byte == 0)
        );
        allocator.dealloc(pointer, layout);
    }
}

#[test]
fn scopes_restore_after_unwind_and_isolate_threads() {
    use std::hint::black_box;
    let other = std::thread::spawn(|| measure(|| drop(black_box(Box::new([0u8; 17])))));
    let allocation = measure(|| drop(black_box(Box::new([0u8; 9]))));
    assert_eq!(allocation.bytes_total, 9);
    assert_eq!(allocation.count_total, 1);
    assert_eq!(other.join().unwrap().bytes_total, 17);
    assert!(std::panic::catch_unwind(|| measure(|| panic!("scope unwind"))).is_err());
    assert_eq!(measure(|| {}), AllocationInfo::default());
    assert_eq!(COUNTER.get(), None);
}

#[test]
fn nested_scopes_accumulate_and_restore_the_parent() {
    use std::hint::black_box;
    let mut child = AllocationInfo::default();
    let parent = measure(|| {
        drop(black_box(Box::new([0u8; 7])));
        child = measure(|| drop(black_box(Box::new([0u8; 11]))));
        drop(black_box(Box::new([0u8; 13])));
    });
    assert_eq!(child.count_total, 1);
    assert_eq!(child.bytes_total, 11);
    assert_eq!(parent.count_total, 3);
    assert_eq!(parent.bytes_total, 31);
    assert_eq!(parent.count_current, 0);
    assert_eq!(parent.bytes_current, 0);
    assert_eq!(parent.count_max, 1);
    assert_eq!(parent.bytes_max, 13);
    assert_eq!(COUNTER.get(), None);
}

#[test]
fn summing_nonoverlapping_peaks_fails_the_lifetime_oracle() {
    use std::hint::black_box;
    let broken = measure(|| {
        drop(black_box(Box::new([0u8; 7])));
        let previous = COUNTER.replace(Some(AllocationInfo::default()));
        drop(black_box(Box::new([0u8; 11])));
        let child = COUNTER.replace(previous).unwrap();
        // The former Scope::drop: add peaks, despite no overlapping lifetimes.
        let mut parent = COUNTER.get().unwrap();
        parent += child;
        COUNTER.set(Some(parent));
        drop(black_box(Box::new([0u8; 13])));
    });
    assert_eq!((broken.count_max, broken.bytes_max), (2, 18));
    let matches_lifetimes = |info: AllocationInfo| (info.count_max, info.bytes_max) == (1, 13);
    assert!(!matches_lifetimes(broken));
}

fn nested(depth: u8, run: &mut dyn FnMut()) {
    if depth == 0 {
        run();
    } else {
        measure(|| nested(depth - 1, run));
    }
}

proptest! {
    // Every successful raw allocation contributes its size exactly once;
    // net storage and peaks equal sums and prefix maxima of the lifetime log.
    #[test]
    fn accounting_matches_complete_allocation_lifetimes(
        commands in proptest::collection::vec((0u8..4, 0usize..8, 1usize..257, 0u8..4), 0..33),
    ) {
        let allocator = CountingAllocator(System);
        let mut slots = [(std::ptr::null_mut::<u8>(), 0usize); 8];
        let mut log = Vec::with_capacity(commands.len() * 2);
        let measured = measure(|| {
            for &(kind, slot, size, depth) in &commands {
                nested(depth, &mut || {
                    let (pointer, previous) = slots[slot];
                    let layout = Layout::from_size_align(previous.max(1), 8).unwrap();
                    // SAFETY: occupied slots contain a live System allocation with
                    // the recorded layout; each replacement retains its new size.
                    unsafe {
                        match kind {
                            0 | 1 if pointer.is_null() => {
                                let layout = Layout::from_size_align(size, 8).unwrap();
                                let pointer = if kind == 0 { allocator.alloc(layout) } else { allocator.alloc_zeroed(layout) };
                                assert!(!pointer.is_null());
                                slots[slot] = (pointer, size);
                                log.push((1i64, size as i64));
                            }
                            2 if !pointer.is_null() => {
                                let next = allocator.realloc(pointer, layout, size);
                                assert!(!next.is_null());
                                slots[slot] = (next, size);
                                log.push((1, size as i64));
                                log.push((-1, -(previous as i64)));
                            }
                            3 if !pointer.is_null() => {
                                allocator.dealloc(pointer, layout);
                                slots[slot] = (std::ptr::null_mut(), 0);
                                log.push((-1, -(previous as i64)));
                            }
                            _ => {}
                        }
                    }
                });
            }
        });
        // Release survivors outside the measurement, including after a failing
        // assertion; the property itself owns every live allocation.
        for (pointer, size) in slots {
            if !pointer.is_null() {
                // SAFETY: survivors retain their allocation's original layout.
                unsafe { allocator.dealloc(pointer, Layout::from_size_align(size, 8).unwrap()) };
            }
        }
        let mut live = (0i64, 0i64);
        let mut peaks = (0i64, 0i64);
        for &(count, bytes) in &log {
            live.0 += count;
            live.1 += bytes;
            peaks.0 = peaks.0.max(live.0);
            peaks.1 = peaks.1.max(live.1);
        }
        prop_assert_eq!(measured.count_total, log.iter().filter(|(count, _)| *count > 0).count() as u64);
        prop_assert_eq!(measured.bytes_total, log.iter().filter(|(count, _)| *count > 0).map(|(_, bytes)| *bytes as u64).sum::<u64>());
        prop_assert_eq!((measured.count_current, measured.bytes_current), live);
        prop_assert_eq!((measured.count_max, measured.bytes_max), (peaks.0 as u64, peaks.1 as u64));
    }
}

#[test]
fn differential_rejects_changed_bytes_errors_and_observer_order() {
    let observe = || (b"bytes".to_vec(), Err::<(), _>(7u8), vec![1u8, 2]);
    for fault in 0..3 {
        let rejected = std::panic::catch_unwind(|| {
            differential(observe, || {
                let mut result = observe();
                match fault {
                    0 => result.0.push(0),
                    1 => result.1 = Ok(()),
                    2 => result.2.reverse(),
                    _ => unreachable!("bounded fault"),
                }
                result
            })
        });
        assert!(
            rejected.is_err(),
            "differential oracle accepted corruption {fault}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn differential_keeps_complete_results_and_symmetric_measurements(bytes in prop::collection::vec(any::<u8>(), 0..65), error in any::<u8>()) {
        let observe = || (bytes.clone(), Err::<(), _>(error), vec![1u8, 2]);
        let (result, [before, after]) = differential(observe, observe);
        prop_assert_eq!(result, (bytes, Err(error), vec![1u8, 2]));
        prop_assert_eq!(before, after);
    }
}
