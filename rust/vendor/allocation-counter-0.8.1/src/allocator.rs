use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;

pub const MAX_DEPTH: usize = 64;

pub struct AllocationInfoStack {
    pub depth: u32,
    pub elements: [crate::AllocationInfo; MAX_DEPTH],
}

thread_local! {
    pub static ALLOCATIONS: RefCell<AllocationInfoStack> = RefCell::new(AllocationInfoStack {
        depth: 0,
        elements: [crate::AllocationInfo::default(); MAX_DEPTH],
    });
}
thread_local! {
    pub static DO_COUNT: RefCell<u32> = RefCell::new(0);
}

struct CountingAllocator<A>(A);

fn allocated(l: Layout) {
    DO_COUNT.with(|b| {
        if *b.borrow() == 0 {
            ALLOCATIONS.with(|info_stack| {
                let mut info_stack = info_stack.borrow_mut();
                let depth = info_stack.depth;
                let info = &mut info_stack.elements[depth as usize];

                info.count_total += 1;
                info.count_current += 1;
                if info.count_current > 0 {
                    info.count_max = info.count_max.max(info.count_current as u64);
                }
                info.bytes_total += l.size() as u64;
                info.bytes_current += l.size() as i64;
                if info.bytes_current > 0 {
                    info.bytes_max = info.bytes_max.max(info.bytes_current as u64);
                }
            });
        }
    });
}

fn deallocated(l: Layout) {
    DO_COUNT.with(|b| {
        if *b.borrow() == 0 {
            ALLOCATIONS.with(|info_stack| {
                let mut info_stack = info_stack.borrow_mut();
                let depth = info_stack.depth;
                let info = &mut info_stack.elements[depth as usize];
                info.count_current -= 1;
                info.bytes_current -= l.size() as i64;
            });
        }
    });
}

// SAFETY: every operation delegates to the same backing allocator with the
// caller's layout; counting never reads or changes the allocated storage.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        // SAFETY: the caller supplies the backing allocator's required layout.
        let pointer = unsafe { self.0.alloc(l) };
        if !pointer.is_null() {
            allocated(l);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        // SAFETY: preserve both the caller's layout and its zeroing contract.
        let pointer = unsafe { self.0.alloc_zeroed(l) };
        if !pointer.is_null() {
            allocated(l);
        }
        pointer
    }

    unsafe fn dealloc(&self, ptr: *mut u8, l: Layout) {
        deallocated(l);
        // SAFETY: ptr and l have the backing allocator's allocation provenance.
        unsafe { self.0.dealloc(ptr, l) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: GlobalAlloc::realloc requires a valid new size at this alignment.
        let next = unsafe { Layout::from_size_align_unchecked(new_size, l.align()) };
        // SAFETY: forward the original allocation and caller's new size unchanged.
        let replacement = unsafe { self.0.realloc(ptr, l, new_size) };
        if !replacement.is_null() {
            allocated(next);
            deallocated(l);
        }
        replacement
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator<System> = CountingAllocator(System);

#[cfg(test)]
#[path = "fast_paths_tests.rs"]
mod tests;
