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
//!
//! A native callback carries its full original invocation and resource allowance
//! into this engine. Its Rust exchange has a strong allocation guard admitted
//! before construction; native resource creation and mandatory teardown are
//! counted at their actual calls. Existing cached fibers are borrowed. A fiber
//! first created by this invocation is retired after switch-back, so no TLS
//! allocation or meter alias escapes the physical callback.
//! The SDK entry point verifies its retained authentic source guard using the
//! existing callback runtime before entering this engine; allocation accounting
//! here is not authorization and cannot replace that verification.
//! OS stack reservation/commit and pure fiber scheduling are not estimated.

#![allow(
    unsafe_code,
    reason = "switches the callback thread onto a provider-reserved fiber stack"
)]

use std::cell::RefCell;
use std::ffi::c_void;
use std::panic::AssertUnwindSafe;
use windows::Win32::Foundation::E_UNEXPECTED;
use windows::Win32::System::Threading::{
    ConvertFiberToThread, ConvertThreadToFiberEx, CreateFiberEx, DeleteFiber, SwitchToFiber,
};
use windows::Win32::System::WindowsProgramming::FIBER_FLAG_FLOAT_SWITCH;

/// Copyable custody of the exact control failure, retained by the driver even
/// after the SDK callback can report only its distinct HRESULT.
#[derive(Clone, Copy, Debug)]
pub(super) enum ProviderFailure {
    Work(crate::WorkError),
    Cancelled,
    OriginalWorkPoisoned,
    Native(windows::core::HRESULT),
}

impl ProviderFailure {
    pub(super) fn into_mount_error(self) -> super::NativeMountError {
        match self {
            Self::Work(error) => super::NativeMountError::Work(error),
            Self::Cancelled => super::NativeMountError::Demand(crate::DemandError::Cancelled),
            Self::OriginalWorkPoisoned => super::NativeMountError::Materialization(crate::NativeTreeMaterializationError::OriginalWorkPoisoned),
            Self::Native(code) => super::NativeMountError::Io(windows::core::Error::from_hresult(code).into()),
        }
    }

    pub(super) fn hresult(self) -> windows::core::HRESULT {
        match self {
            Self::Native(code) => code,
            Self::Work(_) | Self::OriginalWorkPoisoned => windows::core::HRESULT(0x8007_045d_u32.cast_signed()),
            Self::Cancelled => windows::core::HRESULT(0x8007_03ee_u32.cast_signed()),
        }
    }
}

fn materialization_failure(error: crate::NativeTreeMaterializationError) -> ProviderFailure {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => ProviderFailure::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => ProviderFailure::Cancelled,
        crate::NativeTreeMaterializationError::OriginalWorkPoisoned => ProviderFailure::OriginalWorkPoisoned,
        _ => unreachable!("native control admission has no source or allocator operation"),
    }
}

/// Serializes only admission and attempt bookkeeping. The native resource
/// call never owns the original engine's mutex or a queued-I/O waiter.
fn native_write<T>(
    meter: Option<&crate::NativeMaterializationWork>,
    operation: impl FnOnce() -> windows::core::Result<T>,
) -> Result<(windows::core::Result<T>, Result<(), ProviderFailure>), ProviderFailure> {
    if let Some(meter) = meter {
        let delta = crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED };
        meter.admit(delta).map_err(materialization_failure)?;
        let recorded = meter.record_admitted(delta).map_err(materialization_failure);
        Ok((operation(), recorded))
    } else {
        Ok((operation(), Ok(())))
    }
}

struct OwnedExchange {
    allocation: crate::NativeAllocation,
    delete: crate::materializer::NativeCleanupWrite,
}

/// The stack reserve of every provider fiber, as large as the stacks the
/// Acyclic service runs its own threads on. Only address space is reserved;
/// a page is committed when a callback first touches it.
pub(super) const PROVIDER_STACK_BYTES: usize = 32 << 20;

/// One thread's provider fiber and the slots it exchanges with its caller.
struct ProviderFiber {
    fiber: *mut c_void,
    /// Owned, from `Box::into_raw`; the fiber holds it as its parameter.
    exchange: *const Exchange,
    owned: Option<OwnedExchange>,
}

/// What a switch onto the provider fiber carries each way.
#[derive(Default)]
struct Exchange {
    /// The fiber to switch back to once the job has run.
    caller: std::cell::Cell<*mut c_void>,
    /// A `*mut &mut dyn FnMut()` on the caller's stack, set for one switch.
    job: std::cell::Cell<*mut c_void>,
}

impl ProviderFiber {
    fn finish(&mut self) -> Result<(), ProviderFailure> {
        if self.fiber.is_null() {
            return Ok(());
        }
        let delete = self.owned.take();
        let operation = || unsafe {
            // SAFETY: every switch returned; this fiber is no longer running.
            DeleteFiber(self.fiber);
            drop(Box::from_raw(self.exchange.cast_mut()));
        };
        let recorded = match delete {
            Some(owned) => {
                let (_, recorded) = owned.delete.attempt_recorded(operation);
                drop(owned.allocation);
                recorded.map_err(materialization_failure)
            }
            None => { operation(); Ok(()) }
        };
        self.fiber = std::ptr::null_mut();
        self.exchange = std::ptr::null();
        recorded
    }
}

impl Drop for ProviderFiber {
    fn drop(&mut self) {
        // Measured retirement calls finish explicitly so its typed failure is
        // observed; ordinary cached fibers have no invocation claim.
        let _ = self.finish();
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

/// This same thread's fiber. Only a new stop-owned exchange carries a claim.
fn provider_fiber(
    meter: Option<&crate::NativeMaterializationWork>,
) -> Result<(*mut c_void, *const Exchange, bool), ProviderFailure> {
    PROVIDER_FIBER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if let Some(fiber) = slot.as_ref() {
            return Ok((fiber.fiber, fiber.exchange, false));
        }
        let bytes = std::mem::size_of::<Exchange>() as u64;
        let owned = if let Some(meter) = meter {
            let delete = meter.reserve_cleanup_write().map_err(materialization_failure)?;
            meter.admit(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED })
                .map_err(materialization_failure)?;
            let allocation = meter.claim(bytes).map_err(materialization_failure)?;
            Some(OwnedExchange { allocation, delete })
        } else {
            None
        };
        // Admission precedes the real Rust allocation, never a predicted
        // allocation for a callback that reuses its ordinary cached fiber.
        let exchange = Box::into_raw(Box::<Exchange>::default()).cast_const();
        let created = native_write(meter, || {
            // SAFETY: the existing start routine and exchange lifetime contract
            // are unchanged; the new fiber does not run until switched to.
            let fiber = unsafe { CreateFiberEx(0, PROVIDER_STACK_BYTES, FIBER_FLAG_FLOAT_SWITCH,
                Some(serve), Some(exchange.cast::<c_void>())) };
            if fiber.is_null() { Err(windows::core::Error::from_thread()) } else { Ok(fiber) }
        });
        let (fiber, recorded) = match created {
            Ok((Ok(fiber), recorded)) => (fiber, recorded),
            failed => {
                // SAFETY: no fiber was created, so no one shares this box.
                unsafe { drop(Box::from_raw(exchange.cast_mut())) };
                drop(owned);
                return Err(match failed {
                    Err(error) | Ok((Err(_), Err(error))) => error,
                    Ok((Err(error), Ok(()))) => ProviderFailure::Native(error.code()),
                    Ok((Ok(_), _)) => unreachable!("successful create was handled"),
                });
            }
        };
        let mut created = ProviderFiber { fiber, exchange, owned };
        if let Err(error) = recorded {
            created.finish()?;
            return Err(error);
        }
        let scoped = created.owned.is_some();
        *slot = Some(created);
        Ok((fiber, exchange, scoped))
    })
}

struct ScopedFiber(bool);

impl ScopedFiber {
    fn finish(&mut self) -> Result<(), ProviderFailure> {
        if !std::mem::replace(&mut self.0, false) {
            return Ok(());
        }
        PROVIDER_FIBER.with(|slot| {
            let mut fiber = slot.borrow_mut().take().expect("this callback owns the scoped fiber");
            fiber.finish()
        })
    }
}

impl Drop for ScopedFiber {
    fn drop(&mut self) { let _ = self.finish(); }
}

struct ThreadConversion {
    cleanup: Option<crate::materializer::NativeCleanupWrite>,
    caller: *mut c_void,
    active: bool,
}

impl ThreadConversion {
    fn start(meter: Option<&crate::NativeMaterializationWork>) -> Result<Self, ProviderFailure> {
        let cleanup = meter.map(|meter| meter.reserve_cleanup_write()
            .map_err(materialization_failure)).transpose()?;
        let converted = native_write(meter, || {
            // SAFETY: an ordinary thread converts once; an already-fiber thread
            // is rejected by the same native API without changing its state.
            let caller = unsafe { ConvertThreadToFiberEx(None, FIBER_FLAG_FLOAT_SWITCH) };
            if caller.is_null() { Err(windows::core::Error::from_thread()) } else { Ok(caller) }
        });
        match converted {
            Ok((Ok(caller), recorded)) => {
                let mut conversion = Self { cleanup, caller, active: true };
                if let Err(error) = recorded {
                    conversion.finish()?;
                    return Err(error);
                }
                Ok(conversion)
            }
            failed => {
                drop(cleanup);
                Err(match failed {
                    Err(error) | Ok((Err(_), Err(error))) => error,
                    Ok((Err(error), Ok(()))) => ProviderFailure::Native(error.code()),
                    Ok((Ok(_), _)) => unreachable!("successful conversion was handled"),
                })
            }
        }
    }

    fn finish(&mut self) -> Result<(), ProviderFailure> {
        if !std::mem::replace(&mut self.active, false) {
            return Ok(());
        }
        let operation = || unsafe {
            // SAFETY: this thread runs its original converted fiber again.
            ConvertFiberToThread()
        };
        let (converted, recorded) = match self.cleanup.take() {
            Some(cleanup) => cleanup.attempt_recorded(operation),
            None => (operation(), Ok(())),
        };
        let recorded = recorded.map_err(materialization_failure);
        recorded?;
        converted.map_err(|error| ProviderFailure::Native(error.code()))
    }
}

impl Drop for ThreadConversion {
    fn drop(&mut self) { let _ = self.finish(); }
}

/// Runs on the existing provider-fiber engine, borrowing ordinary cached
/// controls or retiring newly admitted stop-owned controls after switch-back.
/// The SDK caller must complete source authorization before this call: the
/// first uncached operation below allocates the actual exchange box.
pub(super) fn run_with_meter<T>(
    meter: Option<&crate::NativeMaterializationWork>, callback: impl FnOnce() -> T,
) -> Result<T, ProviderFailure> {
    let (fiber, exchange, scoped) = provider_fiber(meter)?;
    let mut fiber_scope = ScopedFiber(scoped);
    let result: Result<_, ProviderFailure> = (|| {
        let mut conversion = ThreadConversion::start(meter)?;
        let mut callback = Some(callback);
        let mut outcome = None;
        let mut job = || {
            if let Some(callback) = callback.take() {
                outcome = Some(std::panic::catch_unwind(AssertUnwindSafe(callback)));
            }
        };
        let mut job: &mut dyn FnMut() = &mut job;
        // SAFETY: the original live exchange points at this caller's borrowed
        // job only while that caller is suspended on the provider fiber.
        let exchange = unsafe { &*exchange };
        exchange.caller.set(conversion.caller);
        exchange.job.set((&raw mut job).cast::<c_void>());
        unsafe { SwitchToFiber(fiber) };
        conversion.finish()?;
        Ok(outcome)
    })();
    fiber_scope.finish()?;
    match result? {
        Some(Ok(value)) => Ok(value),
        Some(Err(panic)) => std::panic::resume_unwind(panic),
        None => Err(ProviderFailure::Native(E_UNEXPECTED)),
    }
}

/// Ordinary callbacks use the same engine without a stop invocation loan.
/// Their preexisting source admissions remain independent of a later drain.
pub(super) fn run<T>(callback: impl FnOnce() -> T) -> windows::core::Result<T> {
    run_with_meter(None, callback).map_err(|error| match error {
        ProviderFailure::Native(code) => windows::core::Error::from_hresult(code),
        _ => unreachable!("ordinary callbacks have no finite control admission"),
    })
}

#[cfg(test)]
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
    pub(super) fn on_small_thread<T: Send + 'static>(test: impl FnOnce() -> T + Send + 'static) -> T {
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

    #[test]
    fn measured_exchange_denial_never_runs_the_callback_or_leaves_a_tls_claim() {
        on_small_thread(|| {
            let payload = std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64;
            let budget = crate::WorkBudget { peak_allocation_bytes: payload, ..crate::WorkBudget::UNBOUNDED };
            let work = crate::NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                .expect("the original accounting state is admitted");
            let error = super::run_with_meter(Some(&work), || -> () {
                panic!("denial must not fall back to ordinary callback execution")
            }).expect_err("the actual exchange box is not admitted");
            assert!(matches!(error, super::ProviderFailure::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            })));
            assert_eq!(work.receipt().allocation_operations, 1, "only the actual accounting state was allocated");
            assert_eq!(work.receipt().backend_write_operations, 0, "no fiber SDK creation entered");
            assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), payload);
            assert_eq!(work.native_read_meter().lock().budget, budget, "unused mandatory cleanup admission was cancelled");
            super::PROVIDER_FIBER.with(|slot| assert!(slot.borrow().is_none()));
        });
    }

    #[test]
    fn measured_new_fiber_releases_actual_controls_and_records_all_four_resource_calls() {
        on_small_thread(|| {
            let payload = std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64;
            let exchange = std::mem::size_of::<super::Exchange>() as u64;
            let budget = crate::WorkBudget {
                backend_write_operations: 4,
                allocation_operations: 2,
                peak_allocation_bytes: payload + exchange,
                ..crate::WorkBudget::UNBOUNDED
            };
            let work = crate::NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                .expect("the original accounting state is admitted");
            let result = super::run_with_meter(Some(&work), || {
                // SAFETY: plain query of the currently running real fiber.
                assert!(unsafe { IsThreadAFiber() }.as_bool());
                42
            }).expect("four exact native control operations admitted");
            assert_eq!(result, 42);
            assert_eq!(work.receipt().backend_write_operations, 4);
            assert_eq!(work.receipt().allocation_operations, 2);
            assert_eq!(work.receipt().peak_allocation_bytes, payload + exchange);
            assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), payload);
            assert_eq!(work.native_read_meter().lock().budget, budget);
            // SAFETY: the actual conversion cleanup has returned this thread.
            assert!(!unsafe { IsThreadAFiber() }.as_bool());
            super::PROVIDER_FIBER.with(|slot| assert!(slot.borrow().is_none()));
        });
    }

    #[test]
    fn measured_conversion_denial_still_performs_the_reserved_real_fiber_delete() {
        on_small_thread(|| {
            let budget = crate::WorkBudget { backend_write_operations: 3, ..crate::WorkBudget::UNBOUNDED };
            let work = crate::NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                .expect("the original accounting state is admitted");
            let error = super::run_with_meter(Some(&work), || {
                panic!("unadmitted conversion cannot enter the callback")
            }).expect_err("mandatory cleanup leaves too little allowance to convert");
            assert!(matches!(error, super::ProviderFailure::Work(crate::WorkError::BudgetExceeded {
                counter: "backend_write_operations", ..
            })));
            assert_eq!(work.receipt().backend_write_operations, 2, "real create and mandatory delete, no conversion");
            assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
            assert_eq!(work.native_read_meter().lock().budget, budget);
            // SAFETY: conversion was never entered.
            assert!(!unsafe { IsThreadAFiber() }.as_bool());
            super::PROVIDER_FIBER.with(|slot| assert!(slot.borrow().is_none()));
        });
    }

    #[test]
    fn measured_callback_cancellation_cannot_skip_native_control_cleanup() {
        on_small_thread(|| {
            let cancellation = crate::CancellationToken::new();
            let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
                .expect("the original accounting state is admitted");
            super::run_with_meter(Some(&work), || cancellation.cancel())
                .expect("the admitted callback returns after mandatory cleanup");
            assert!(matches!(work.admit(crate::WorkCounters::UNCHARGED),
                Err(crate::NativeTreeMaterializationError::Cancelled)), "the original token remains active");
            assert_eq!(work.receipt().backend_write_operations, 4, "both actual cleanup calls survived cancellation");
            assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), crate::NativeMaterializationWork::accounting_state_bytes());
            // SAFETY: conversion cleanup still ran.
            assert!(!unsafe { IsThreadAFiber() }.as_bool());
            super::PROVIDER_FIBER.with(|slot| assert!(slot.borrow().is_none()));
        });
    }

    #[test]
    fn measured_callback_borrows_an_ordinary_cached_fiber_without_claiming_it() {
        on_small_thread(|| {
            run(|| 1).expect("preexisting ordinary fiber");
            let payload = std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64;
            let budget = crate::WorkBudget {
                backend_write_operations: 2,
                allocation_operations: 1,
                peak_allocation_bytes: payload,
                ..crate::WorkBudget::UNBOUNDED
            };
            let work = crate::NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
                .expect("the original accounting state is admitted");
            super::run_with_meter(Some(&work), || 2)
                .expect("existing fiber is borrowed");
            assert_eq!(work.receipt().backend_write_operations, 2, "actual thread conversion and cleanup only");
            assert_eq!(work.receipt().allocation_operations, 1);
            assert_eq!(work.receipt().peak_allocation_bytes, payload);
            assert_eq!(work.native_read_meter().lock().live_allocation_bytes(), payload);
            super::PROVIDER_FIBER.with(|slot| {
                assert!(slot.borrow().as_ref().expect("original cached fiber").owned.is_none());
            });
            run(|| 3).expect("ordinary cached fiber remains usable without a retained stop meter");
        });
    }
}

#[cfg(test)]
mod resource_scope_tests {
    #[test]
    fn provider_exchange_keeps_the_exact_resource_allocation_ceiling() {
        super::tests::on_small_thread(|| {
            let original = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
            ).expect("fixture original work");
            let resource = original.native_volume_work(crate::WorkBudget {
                allocation_operations: 0, ..crate::WorkBudget::UNBOUNDED
            }).expect("resource metadata belongs to the original");
            let full = resource.clone();
            let before = original.receipt();
            let failure = super::run_with_meter(Some(&full), || panic!("denied exchange cannot enter callback"))
                .expect_err("actual exchange must use the resource view");
            assert!(matches!(failure, super::ProviderFailure::Work(crate::WorkError::BudgetExceeded {
                counter: "allocation_operations", ..
            })));
            assert_eq!(original.receipt(), before, "denied exchange enters neither allocator nor SDK");
            super::PROVIDER_FIBER.with(|slot| assert!(slot.borrow().is_none()));
        });
    }
}
