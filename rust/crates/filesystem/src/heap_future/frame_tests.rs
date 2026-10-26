use super::{AdmittedFrame, FrameContext, in_heap_admitted};
use crate::kernel::allocation::{AllocationLedger, KernelAccounting};
use crate::{CancellationToken, FsError, WorkBudget, WorkCounters};
use futures::FutureExt;

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn erasing_a_funded_future_preserves_its_address_and_original_charge() {
    fn assert_send<T: Send>(_: &T) {}
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let future = super::native_future(&work, || async {
        let payload = [0x71_u8; 64];
        std::future::pending::<()>().await;
        payload
    })
    .unwrap();
    let address = future.as_ref().get_ref() as *const _ as *const ();
    let bytes = std::mem::size_of_val(future.as_ref().get_ref()) as u64;
    assert_eq!(work.live_allocation_bytes(), baseline + bytes);
    let admitted = work.receipt();
    let future = future.erase_send();
    assert_send(&future);
    assert_eq!(future.as_ref().get_ref() as *const _ as *const (), address);
    assert_eq!(
        std::mem::size_of_val(future.as_ref().get_ref()) as u64,
        bytes
    );
    assert_eq!(
        work.receipt(),
        admitted,
        "erasure must not allocate or copy again"
    );
    assert_eq!(work.live_allocation_bytes(), baseline + bytes);
    drop(future);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[test]
fn borrowed_phase_wrapper_retires_storage_after_its_body_on_every_exit() {
    for exit in 0..4 {
        let cancellation = CancellationToken::new();
        let destroyed = std::cell::Cell::new(false);
        let mut context = OrderedContext {
            accounting: KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
            destroyed: &destroyed,
        };
        let marker = FutureDestroyed(&destroyed);
        let mut operation = Box::pin(async {
            super::in_borrowed_frame!(&mut context, |context| async move {
                let _marker = marker;
                context.accounting.check()?;
                if exit < 2 {
                    std::future::pending::<()>().await;
                }
                if exit == 3 {
                    Err(FsError::InvalidDiff)
                } else {
                    Ok(())
                }
            })
        });
        if exit != 0 {
            let result = operation.as_mut().now_or_never();
            assert_eq!(result.is_some(), exit >= 2);
            if let Some(result) = result {
                assert_eq!(result.is_ok(), exit == 2);
            }
        }
        drop(operation);
        assert!(destroyed.get());
        assert_eq!(
            context
                .accounting
                .standalone_parts()
                .unwrap()
                .0
                .live_bytes(),
            0
        );
    }
}

fn cancel_frame(accounting: &mut KernelAccounting<'_>, poll: bool) {
    let mut frame = AdmittedFrame::new(accounting);
    let (mut future, _) = in_heap_admitted(
        &mut frame,
        |frame, bytes| frame.admit(bytes),
        |frame| async move {
            frame
                .accounting()
                .charge_items(1)
                .unwrap_or_else(|_| unreachable!("fixture work fits"));
            std::future::pending::<()>().await;
            Ok::<(), FsError>(())
        },
    )
    .unwrap_or_else(|_| unreachable!("fixture frame fits"));
    if poll {
        assert!(future.as_mut().now_or_never().is_none());
    }
    drop(future);
}

#[test]
fn frame_cancellation_retires_only_its_own_borrowed_claim_before_and_after_poll() {
    for poll in [false, true] {
        let cancellation = CancellationToken::new();
        let mut ledger = AllocationLedger::default();
        let mut work = WorkCounters::UNCHARGED;
        ledger
            .claim_bytes(17, 1, &mut work, WorkBudget::UNBOUNDED)
            .unwrap_or_else(|_| unreachable!("sibling claim fits"));
        cancel_frame(
            &mut KernelAccounting::borrowed_standalone(
                &mut ledger,
                &mut work,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ),
            poll,
        );
        assert_eq!(ledger.live_bytes(), 17, "frame retained after cancellation");
        assert_eq!(work.allocation_operations, 2);
        assert_eq!(work.items_examined, u64::from(poll));
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn frame_cancellation_retains_original_native_custody() {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .unwrap_or_else(|_| unreachable!("fixture owner fits"));
    let sibling = original
        .claim(17, 1, WorkCounters::UNCHARGED)
        .unwrap_or_else(|_| unreachable!("sibling fits"));
    let baseline = original.live_allocation_bytes();
    for poll in [false, true] {
        cancel_frame(
            &mut KernelAccounting::native(&original, &cancellation),
            poll,
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
    drop(sibling);
}

#[tokio::test]
async fn frame_completion_and_error_release_after_destroying_the_box() {
    for success in [false, true] {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut frame = AdmittedFrame::new(&mut accounting);
        let (mut future, bytes) = in_heap_admitted(
            &mut frame,
            |frame, bytes| frame.admit(bytes),
            |frame| async move {
                frame.accounting().check()?;
                if success {
                    Ok(())
                } else {
                    Err(FsError::InvalidDiff)
                }
            },
        )
        .unwrap_or_else(|_| unreachable!("fixture frame fits"));
        let result = future.as_mut().await;
        drop(future);
        assert_eq!(
            frame
                .accounting()
                .standalone_parts()
                .unwrap_or_else(|| unreachable!("standalone fixture"))
                .0
                .live_bytes(),
            bytes
        );
        assert_eq!(frame.finish(result).is_ok(), success);
        assert_eq!(
            accounting
                .standalone_parts()
                .unwrap_or_else(|| unreachable!("standalone fixture"))
                .0
                .live_bytes(),
            0
        );
    }
}

#[test]
#[should_panic(expected = "frame retained after cancellation")]
fn old_manual_frame_retirement_leaks_a_borrowed_claim_on_cancellation() {
    let cancellation = CancellationToken::new();
    let mut ledger = AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    {
        let mut accounting = KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancellation,
        );
        let mut allocation = None;
        let (future, _) = in_heap_admitted(
            &mut accounting,
            |accounting, bytes| {
                allocation = Some(accounting.claim_owned(bytes, u64::from(bytes != 0))?);
                Ok::<(), FsError>(())
            },
            |accounting| async move {
                std::future::pending::<()>().await;
                accounting.check()
            },
        )
        .unwrap_or_else(|_| unreachable!("fixture frame fits"));
        drop(future);
        drop(allocation);
    }
    assert_eq!(ledger.live_bytes(), 0, "frame retained after cancellation");
}

struct FutureDestroyed<'a>(&'a std::cell::Cell<bool>);
impl Drop for FutureDestroyed<'_> {
    fn drop(&mut self) {
        self.0.set(true);
    }
}
struct OrderedContext<'a> {
    accounting: KernelAccounting<'a>,
    destroyed: &'a std::cell::Cell<bool>,
}
impl FrameContext for OrderedContext<'_> {
    fn claim_frame(
        &mut self,
        bytes: u64,
    ) -> Result<crate::kernel::allocation::KernelOwnedAllocation, FsError> {
        self.accounting.claim_frame(bytes)
    }
    fn retire_frame(
        &mut self,
        allocation: crate::kernel::allocation::KernelOwnedAllocation,
        bytes: u64,
    ) -> Result<(), FsError> {
        assert!(
            self.destroyed.get(),
            "storage retired before the future was destroyed"
        );
        self.accounting.retire_frame(allocation, bytes)
    }
}
#[test]
fn frame_owner_outlives_its_future_on_every_exit() {
    for exit in 0..4 {
        let cancellation = CancellationToken::new();
        let destroyed = std::cell::Cell::new(false);
        let mut context = OrderedContext {
            accounting: KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
            destroyed: &destroyed,
        };
        let mut frame = AdmittedFrame::new(&mut context);
        let marker = FutureDestroyed(&destroyed);
        let (mut future, _) = in_heap_admitted(
            &mut frame,
            |frame, bytes| frame.admit(bytes),
            |frame| async move {
                let _marker = marker;
                frame.accounting().accounting.check()?;
                if exit < 2 {
                    std::future::pending::<()>().await;
                }
                if exit == 3 {
                    Err(FsError::InvalidDiff)
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_or_else(|_| unreachable!("fixture frame fits"));
        let result = if exit == 0 {
            None
        } else {
            future.as_mut().now_or_never()
        };
        drop(future);
        match result {
            Some(result) => assert_eq!(frame.finish(result).is_ok(), exit == 2),
            None => drop(frame),
        }
        assert!(destroyed.get());
        assert_eq!(
            context
                .accounting
                .standalone_parts()
                .unwrap_or_else(|| unreachable!("standalone fixture"))
                .0
                .live_bytes(),
            0
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeDropProbe<'a> {
    original: crate::NativeMaterializationWork,
    live_at_drop: &'a std::sync::atomic::AtomicU64,
    ready: bool,
}
#[cfg(not(target_arch = "wasm32"))]
impl std::future::Future for NativeDropProbe<'_> {
    type Output = Result<(), FsError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.ready {
            std::task::Poll::Ready(Ok(()))
        } else {
            std::task::Poll::Pending
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeDropProbe<'_> {
    fn drop(&mut self) {
        self.live_at_drop.store(
            self.original.live_allocation_bytes(),
            std::sync::atomic::Ordering::Release,
        );
    }
}
#[test]
#[cfg(not(target_arch = "wasm32"))]
fn native_owned_frame_destroys_the_future_before_retiring_its_storage() {
    for exit in 0..3 {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| unreachable!("fixture owner fits"));
        let live_at_drop = std::sync::atomic::AtomicU64::new(0);
        let baseline = original.live_allocation_bytes();
        let mut future = super::native_future(&original, || NativeDropProbe {
            original: original.clone(),
            live_at_drop: &live_at_drop,
            ready: exit == 2,
        })
        .unwrap_or_else(|_| unreachable!("fixture frame fits"));
        let admitted = original.live_allocation_bytes();
        assert!(admitted > baseline);
        if exit != 0 {
            assert_eq!((&mut future).now_or_never().is_some(), exit == 2);
        }
        drop(future);
        assert_eq!(
            live_at_drop.load(std::sync::atomic::Ordering::Acquire),
            admitted,
            "native frame retired before its future"
        );
        assert_eq!(original.live_allocation_bytes(), baseline);
    }
}
#[test]
#[cfg(not(target_arch = "wasm32"))]
#[should_panic(expected = "native frame retired before its future")]
fn retiring_native_storage_before_the_box_is_detected() {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .unwrap_or_else(|_| unreachable!("fixture owner fits"));
    let live_at_drop = std::sync::atomic::AtomicU64::new(0);
    let mut allocation = None;
    let (future, _) = in_heap_admitted(
        &mut allocation,
        |allocation, bytes| {
            **allocation = Some(original.claim(bytes, 1, WorkCounters::UNCHARGED)?);
            Ok::<(), FsError>(())
        },
        |_| NativeDropProbe {
            original: original.clone(),
            live_at_drop: &live_at_drop,
            ready: false,
        },
    )
    .unwrap_or_else(|_| unreachable!("fixture frame fits"));
    let admitted = original.live_allocation_bytes();
    // Tuple fields drop in order: this deliberately reverses production custody.
    let mut reversed = (allocation, future);
    assert!(reversed.1.as_mut().now_or_never().is_none());
    drop(reversed);
    assert_eq!(
        live_at_drop.load(std::sync::atomic::Ordering::Acquire),
        admitted,
        "native frame retired before its future"
    );
}
