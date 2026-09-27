//! The stack every `ProjFS` callback runs on.
//!
//! Every driver polls a callback's source operation to completion in place,
//! so a callback needs the stack its kind of operation uses, which the
//! `stack_budget` tests bound. FUSE and macOS NFS callbacks arrive on threads
//! their drivers create with stacks of fixed size, far larger than any
//! budget. `ProjFS` alone calls a provider back on threads with the
//! executable's default stack reserve: 1 MiB for most programs, and whatever
//! an embedding host chose otherwise, which the provider can neither see nor
//! change. Overflowing that stack kills the provider, and every access to the
//! projection then fails as unavailable.
//!
//! [`run`] therefore switches the calling thread onto a fiber whose stack the
//! provider reserves, runs the callback there, and switches back. The callback
//! stays on its thread, so thread-local state, parking, and blocking behave as
//! they would in place, and no other thread is woken to serve it.

#![allow(unsafe_code)]

use std::cell::RefCell;
use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use windows::Win32::Foundation::E_UNEXPECTED;
use windows::Win32::System::Threading::{
    ConvertFiberToThread, ConvertThreadToFiberEx, CreateFiberEx, DeleteFiber, SwitchToFiber,
};
use windows::Win32::System::WindowsProgramming::FIBER_FLAG_FLOAT_SWITCH;

/// The stack reserve of every provider fiber, as large as the stacks the
/// Acyclic service runs its own threads on. Only address space is reserved;
/// a page is committed when a callback first touches it.
pub(super) const PROVIDER_STACK_BYTES: usize = 32 << 20;

/// One thread's provider fiber and the slots it exchanges with its caller.
struct ProviderFiber {
    fiber: *mut c_void,
    /// Owned, from `Box::into_raw`; the fiber holds it as its parameter.
    exchange: *const Exchange,
}

/// What a switch onto the provider fiber carries each way.
#[derive(Default)]
struct Exchange {
    /// The fiber to switch back to once the job has run.
    caller: std::cell::Cell<*mut c_void>,
    /// A `*mut &mut dyn FnMut()` on the caller's stack, set for one switch.
    job: std::cell::Cell<*mut c_void>,
}

impl Drop for ProviderFiber {
    fn drop(&mut self) {
        // SAFETY: the fiber was created by this thread and is not running:
        // every switch onto it returns before its owner is dropped. Once it
        // is deleted, nothing refers to its exchange.
        unsafe {
            DeleteFiber(self.fiber);
            drop(Box::from_raw(self.exchange.cast_mut()));
        }
    }
}

thread_local! {
    /// Created on a thread's first callback and reused for every later one.
    static PROVIDER_FIBER: RefCell<Option<ProviderFiber>> = const { RefCell::new(None) };
}

/// The provider fiber's body: run each job it is handed, then return to the
/// fiber that handed it.
unsafe extern "system" fn serve(exchange: *mut c_void) {
    // SAFETY: the owning `ProviderFiber` keeps the exchange boxed and alive
    // for as long as this fiber exists.
    let exchange = unsafe { &*exchange.cast::<Exchange>() };
    loop {
        let job = exchange.job.replace(std::ptr::null_mut());
        if !job.is_null() {
            // SAFETY: `run` points `job` at a live `&mut dyn FnMut()` and
            // stays suspended, with it borrowed, until this switch back.
            unsafe { (*job.cast::<&mut dyn FnMut()>())() };
        }
        // SAFETY: `caller` is the live fiber `run` converted its thread into.
        unsafe { SwitchToFiber(exchange.caller.get()) };
    }
}

/// This thread's provider fiber and its exchange, created on first use.
fn provider_fiber() -> windows::core::Result<(*mut c_void, *const Exchange)> {
    PROVIDER_FIBER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(fiber) = slot.as_ref() {
            return Ok((fiber.fiber, fiber.exchange));
        }
        let exchange = Box::into_raw(Box::<Exchange>::default()).cast_const();
        // SAFETY: `serve` matches the fiber start routine contract and its
        // parameter, the exchange, outlives the fiber (see `Drop`).
        let fiber = unsafe {
            CreateFiberEx(
                0,
                PROVIDER_STACK_BYTES,
                FIBER_FLAG_FLOAT_SWITCH,
                Some(serve),
                Some(exchange.cast::<c_void>()),
            )
        };
        if fiber.is_null() {
            let error = windows::core::Error::from_thread();
            // SAFETY: no fiber was created, so the exchange is unshared.
            drop(unsafe { Box::from_raw(exchange.cast_mut()) });
            return Err(error);
        }
        *slot = Some(ProviderFiber { fiber, exchange });
        Ok((fiber, exchange))
    })
}

/// Runs `callback` on this thread's provider fiber and returns its result.
///
/// A panic in `callback` resumes on the caller's stack. The thread is left
/// exactly as it was found: not a fiber, and on its own stack.
pub(super) fn run<T>(callback: impl FnOnce() -> T) -> windows::core::Result<T> {
    let (fiber, exchange) = provider_fiber()?;
    let mut callback = Some(callback);
    let mut outcome = None;
    let mut job = || {
        if let Some(callback) = callback.take() {
            outcome = Some(std::panic::catch_unwind(AssertUnwindSafe(callback)));
        }
    };
    let mut job: &mut dyn FnMut() = &mut job;
    // SAFETY: converting a thread that is not yet a fiber has no
    // precondition; a thread that already is one fails without change.
    let caller = unsafe { ConvertThreadToFiberEx(None, FIBER_FLAG_FLOAT_SWITCH) };
    if caller.is_null() {
        return Err(windows::core::Error::from_thread());
    }
    // SAFETY: the exchange lives as long as this thread's provider fiber.
    let exchange = unsafe { &*exchange };
    exchange.caller.set(caller);
    exchange.job.set((&raw mut job).cast::<c_void>());
    // SAFETY: the provider fiber is not running anywhere: it runs only
    // between this switch and its switch back to `caller`, on this thread.
    unsafe { SwitchToFiber(fiber) };
    // SAFETY: this thread was converted above and runs its original fiber
    // again; converting back frees only that conversion.
    unsafe { ConvertFiberToThread() }?;
    match outcome {
        Some(Ok(value)) => Ok(value),
        Some(Err(panic)) => std::panic::resume_unwind(panic),
        None => Err(E_UNEXPECTED.into()),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::{PROVIDER_STACK_BYTES, run};
    use windows::Win32::System::Threading::{GetCurrentThreadStackLimits, IsThreadAFiber};

    /// The part of the current stack still below the caller's frame.
    fn remaining_stack() -> usize {
        let (mut low, mut high) = (0, 0);
        // SAFETY: both outputs are writable locals.
        unsafe { GetCurrentThreadStackLimits(&raw mut low, &raw mut high) };
        let here = std::hint::black_box(&raw const low) as usize;
        here - low
    }

    /// Uses `frames` 64 KiB stack frames at once.
    #[inline(never)]
    fn deep(frames: u8) -> u8 {
        let frame = std::hint::black_box([frames; 64 * 1024]);
        if frames == 0 {
            frame[0]
        } else {
            deep(frames - 1).wrapping_add(frame[1])
        }
    }

    /// Runs `test` on a thread whose own stack is far smaller than a
    /// callback may need, as a `ProjFS` thread's is.
    fn on_small_thread<T: Send + 'static>(test: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(test)
            .expect("spawn a small-stack thread")
            .join()
            .expect("small-stack thread completes")
    }

    #[test]
    fn a_callback_gets_the_provider_stack_whatever_its_thread_has() {
        let (outside, inside, deepest) = on_small_thread(|| {
            let outside = remaining_stack();
            let inside = run(remaining_stack).expect("run on the provider stack");
            // Four MiB of frames: more than a ProjFS thread's whole stack.
            let deepest = run(|| deep(64)).expect("run deep on the provider stack");
            (outside, inside, deepest)
        });
        assert!(outside < 256 * 1024);
        assert!(
            inside > PROVIDER_STACK_BYTES - 64 * 1024,
            "callback had {inside} bytes of stack"
        );
        assert_eq!(deepest, (0..=64_u8).fold(0, u8::wrapping_add));
    }

    #[test]
    fn a_callback_leaves_its_thread_as_it_found_it() {
        let (before, after, again) = on_small_thread(|| {
            // SAFETY: a plain query of this thread's state.
            let before = unsafe { IsThreadAFiber() }.as_bool();
            let first = run(|| 1).expect("first run");
            // SAFETY: as above.
            let after = unsafe { IsThreadAFiber() }.as_bool();
            let second = run(|| 2).expect("second run reuses the fiber");
            (before, after, first + second)
        });
        assert!(!before);
        assert!(!after);
        assert_eq!(again, 3);
    }

    #[test]
    fn a_panicking_callback_unwinds_to_its_caller() {
        let (panicked, after) = on_small_thread(|| {
            let panicked = std::panic::catch_unwind(|| run(|| panic!("callback failed")));
            (panicked.is_err(), run(|| 7).expect("run after a panic"))
        });
        assert!(panicked);
        assert_eq!(after, 7);
    }
}
