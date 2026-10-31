//! The QuickJS C allocator retaining the original native allocation owner.
#![allow(
    unsafe_code,
    reason = "QuickJS requires an allocator FFI contract; each block carries its original allocation custody"
)]

use super::{Error, Result, engine::Runtime, lock};
use acyclic_fs::{
    NativeAllocation, NativeMaterializationWork, NativeTreeMaterializationError, WorkCounters,
};
use rquickjs_sys as qjs;
use std::{
    alloc::{self, Layout},
    mem, ptr,
    sync::{Arc, Mutex},
};

/// The runtime retains this owner until after QuickJS and its allocator holder
/// are destroyed. Allocator failures cannot be caught and acknowledged by JS.
pub(super) struct AllocatorState {
    first: Mutex<Option<NativeTreeMaterializationError>>,
    work: NativeMaterializationWork,
    _custody: NativeAllocation,
}

impl AllocatorState {
    fn record(&self, cause: NativeTreeMaterializationError) {
        let mut first = lock(&self.first);
        if first.is_none() {
            *first = Some(cause);
        }
    }

    pub(super) fn take_failure(&self) -> Option<Error> {
        lock(&self.first).take().map(Error::from)
    }

    pub(super) fn claim(&self, bytes: u64, operations: u64) -> Result<NativeAllocation> {
        self.work
            .claim(bytes, operations, WorkCounters::UNCHARGED)
            .map_err(Into::into)
    }

    pub(super) fn work(&self) -> &NativeMaterializationWork {
        &self.work
    }
}

struct NativeAllocator<'a> {
    state: &'a AllocatorState,
}

struct Header {
    custody: NativeAllocation,
    payload: usize,
}

const ALIGN: usize = if mem::align_of::<Header>() > mem::align_of::<u64>() {
    mem::align_of::<Header>()
} else {
    mem::align_of::<u64>()
};
const HEADER: usize = mem::size_of::<Header>().next_multiple_of(ALIGN);

fn layout(payload: usize) -> std::result::Result<Layout, NativeTreeMaterializationError> {
    let bytes = HEADER
        .checked_add(payload)
        .ok_or(acyclic_fs::WorkError::Overflow)?;
    Layout::from_size_align(bytes, ALIGN).map_err(|_| NativeTreeMaterializationError::InvalidLayout)
}

pub(super) fn runtime(work: &NativeMaterializationWork) -> Result<(Runtime, Arc<AllocatorState>)> {
    let metadata = acyclic_native_runtime::arc_allocation_bytes::<AllocatorState>();
    let custody = work.claim(metadata as u64, 1, WorkCounters::UNCHARGED)?;
    let state = Arc::new(AllocatorState {
        first: Mutex::new(None),
        work: work.clone(),
        _custody: custody,
    });
    Runtime::with_allocator(Arc::clone(&state)).map(|runtime| (runtime, state))
}

impl NativeAllocator<'_> {
    fn protected<T>(&mut self, fallback: T, action: impl FnOnce(&mut Self) -> T) -> T {
        // A Rust failure, including a producer waker, must never unwind across
        // QuickJS's C frames or become a JS-catchable acknowledgement.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action(self))) {
            Ok(value) => value,
            Err(_) => {
                self.state
                    .record(NativeTreeMaterializationError::InvalidLayout);
                fallback
            }
        }
    }
    fn failed(&self, cause: NativeTreeMaterializationError) -> *mut u8 {
        self.state.record(cause);
        ptr::null_mut()
    }

    fn allocate(&mut self, payload: usize, zeroed: bool) -> *mut u8 {
        if payload == 0 {
            return ptr::null_mut();
        }
        let layout = match layout(payload) {
            Ok(layout) => layout,
            Err(error) => return self.failed(error),
        };
        let custody = match self
            .state
            .work
            .claim(layout.size() as u64, 1, WorkCounters::UNCHARGED)
        {
            Ok(custody) => custody,
            Err(error) => return self.failed(error),
        };
        // SAFETY: layout is nonzero and valid. The returned block owns exactly
        // one Header followed by payload bytes aligned for QuickJS's u64 cells.
        let backing = unsafe {
            if zeroed {
                alloc::alloc_zeroed(layout)
            } else {
                alloc::alloc(layout)
            }
        };
        if backing.is_null() {
            return self.failed(std::io::Error::from(std::io::ErrorKind::OutOfMemory).into());
        }
        // SAFETY: backing has space/alignment for Header; it is initialized once
        // and moved only with its allocation, then read once by dealloc.
        unsafe {
            backing.cast::<Header>().write(Header { custody, payload });
            backing.add(HEADER)
        }
    }
}

// SAFETY: all successful callbacks return at least the requested payload, with
// u64 alignment. The preceding header retains its exact original reservation.
// realloc leaves the old block/charge intact on refusal; free destroys backing
// before releasing credit. QuickJS serializes callbacks on its runtime lock.
impl NativeAllocator<'_> {
    fn alloc(&mut self, size: usize) -> *mut u8 {
        self.allocate(size, false)
    }

    fn calloc(&mut self, count: usize, size: usize) -> *mut u8 {
        match count.checked_mul(size) {
            Some(bytes) => self.allocate(bytes, true),
            None => self.failed(acyclic_fs::WorkError::Overflow.into()),
        }
    }

    unsafe fn dealloc(&mut self, pointer: *mut u8) {
        if pointer.is_null() {
            return;
        }
        // SAFETY: caller supplies a live pointer from this allocator. The header
        // layout was checked on creation/realloc and has one live custody owner.
        let backing = unsafe { pointer.sub(HEADER) };
        let Header {
            mut custody,
            payload,
        } = unsafe { backing.cast::<Header>().read() };
        // SAFETY: this exact header was created only after checking this layout.
        let layout = unsafe { Layout::from_size_align_unchecked(HEADER + payload, ALIGN) };
        // SAFETY: backing and layout are the original successful allocation.
        unsafe { alloc::dealloc(backing, layout) };
        if let Err(error) = custody.release(layout.size() as u64) {
            self.state.record(error);
        }
    }

    unsafe fn realloc(&mut self, pointer: *mut u8, payload: usize) -> *mut u8 {
        if pointer.is_null() {
            return self.alloc(payload);
        }
        if payload == 0 {
            // SAFETY: caller supplied the same original live block.
            unsafe { self.dealloc(pointer) };
            return ptr::null_mut();
        }
        // SAFETY: caller gives exclusive allocation access for this callback.
        let backing = unsafe { pointer.sub(HEADER) };
        let header = unsafe { &mut *backing.cast::<Header>() };
        if payload == header.payload {
            return pointer;
        }
        let previous = match layout(header.payload) {
            Ok(layout) => layout,
            Err(error) => return self.failed(error),
        };
        let next = match layout(payload) {
            Ok(layout) => layout,
            Err(error) => return self.failed(error),
        };
        let growth = next.size().saturating_sub(previous.size());
        let mut claim = match self
            .state
            .work
            .claim(growth as u64, 1, WorkCounters::UNCHARGED)
        {
            Ok(claim) => claim,
            Err(error) => return self.failed(error),
        };
        // realloc may move the original block. Withhold only that possible copy
        // and report zero when the allocator actually keeps the same address.
        let copying = match self.state.work.reserve_work(WorkCounters {
            bytes_copied: previous.size().min(next.size()) as u64,
            ..WorkCounters::UNCHARGED
        }) {
            Ok(copying) => copying,
            Err(error) => return self.failed(error),
        };
        if let Err(error) = header.custody.absorb(&mut claim) {
            return self.failed(error);
        }
        // SAFETY: backing/layout identify the original allocation; next.size()
        // is nonzero, has unchanged alignment, and its growth was admitted.
        let replacement = unsafe { alloc::realloc(backing, previous, next.size()) };
        if replacement.is_null() {
            // SAFETY: failed realloc leaves the old header and payload live.
            let header = unsafe { &mut *backing.cast::<Header>() };
            if let Err(error) = header.custody.release(growth as u64) {
                self.state.record(error);
            }
            return self.failed(std::io::Error::from(std::io::ErrorKind::OutOfMemory).into());
        }
        // SAFETY: successful realloc moves the single initialized Header with
        // the block; its custody is not duplicated or read from the old address.
        let header = unsafe { &mut *replacement.cast::<Header>() };
        header.payload = payload;
        if next.size() < previous.size()
            && let Err(error) = header
                .custody
                .release((previous.size() - next.size()) as u64)
        {
            self.state.record(error);
        }
        if let Err(error) = copying.record_completed(WorkCounters {
            bytes_copied: if replacement == backing {
                0
            } else {
                previous.size().min(next.size()) as u64
            },
            ..WorkCounters::UNCHARGED
        }) {
            self.state.record(error);
        }
        // SAFETY: HEADER is the fixed aligned prefix of this live block.
        unsafe { replacement.add(HEADER) }
    }

    unsafe fn usable_size(pointer: *mut u8) -> usize {
        if pointer.is_null() {
            return 0;
        }
        // SAFETY: the allocator contract supplies one of its live blocks.
        unsafe { (*pointer.sub(HEADER).cast::<Header>()).payload }
    }
}

pub(super) const FUNCTIONS: qjs::JSMallocFunctions = qjs::JSMallocFunctions {
    js_malloc: Some(malloc),
    js_calloc: Some(calloc),
    js_realloc: Some(realloc),
    js_free: Some(free),
    js_malloc_usable_size: Some(usable_size),
};

unsafe fn allocator<'a>(opaque: *mut qjs::c_void) -> NativeAllocator<'a> {
    // SAFETY: RuntimeInner retains the exact Arc through JS_FreeRuntime.
    NativeAllocator {
        state: unsafe { &*opaque.cast::<AllocatorState>() },
    }
}
unsafe extern "C" fn malloc(opaque: *mut qjs::c_void, size: qjs::size_t) -> *mut qjs::c_void {
    let mut allocator = unsafe { allocator(opaque) };
    match usize::try_from(size) {
        Ok(size) => allocator
            .protected(ptr::null_mut(), |allocator| allocator.alloc(size))
            .cast(),
        Err(_) => allocator
            .failed(acyclic_fs::WorkError::Overflow.into())
            .cast(),
    }
}
unsafe extern "C" fn calloc(
    opaque: *mut qjs::c_void,
    count: qjs::size_t,
    size: qjs::size_t,
) -> *mut qjs::c_void {
    let mut allocator = unsafe { allocator(opaque) };
    match (usize::try_from(count), usize::try_from(size)) {
        (Ok(count), Ok(size)) => allocator
            .protected(ptr::null_mut(), |allocator| allocator.calloc(count, size))
            .cast(),
        _ => allocator
            .failed(acyclic_fs::WorkError::Overflow.into())
            .cast(),
    }
}
unsafe extern "C" fn realloc(
    opaque: *mut qjs::c_void,
    pointer: *mut qjs::c_void,
    size: qjs::size_t,
) -> *mut qjs::c_void {
    let mut allocator = unsafe { allocator(opaque) };
    match usize::try_from(size) {
        Ok(size) => allocator
            .protected(ptr::null_mut(), |allocator| unsafe {
                allocator.realloc(pointer.cast(), size)
            })
            .cast(),
        Err(_) => allocator
            .failed(acyclic_fs::WorkError::Overflow.into())
            .cast(),
    }
}
unsafe extern "C" fn free(opaque: *mut qjs::c_void, pointer: *mut qjs::c_void) {
    unsafe { allocator(opaque) }
        .protected((), |allocator| unsafe { allocator.dealloc(pointer.cast()) });
}
unsafe extern "C" fn usable_size(pointer: *const qjs::c_void) -> qjs::size_t {
    (unsafe { NativeAllocator::usable_size(pointer.cast_mut().cast()) }) as _
}

#[cfg(test)]
mod tests {
    use super::super::engine::Context;
    use super::*;
    use acyclic_fs::{CancellationToken, WorkBudget};

    #[test]
    fn actual_engine_heap_retires_original_credit_and_refuses_exhausted_owner() -> Result<()> {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let (runtime, state) = runtime(&work)?;
        let context = Context::new(&runtime, None)?;
        context.with(|ctx| {
            let value = ctx.eval(c"Array.from({length: 1024}, (_, n) => n).length")?;
            assert_eq!(value.text()?.as_str()?, "1024");
            Ok(())
        })?;
        assert!(work.live_allocation_bytes() > baseline);
        assert!(state.take_failure().is_none());
        drop(context);
        drop(runtime);
        assert!(work.live_allocation_bytes() > baseline);
        drop(state);
        assert_eq!(work.live_allocation_bytes(), baseline);

        // Negative control: the same real runtime cannot be admitted when its
        // original owner has no allocation operations left. No runtime/realm
        // is returned and the original typed work error survives the C callback.
        let exhausted = NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 5,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(
            super::runtime(&exhausted),
            Err(Error::NativeWork { .. })
        ));
        Ok(())
    }
}
