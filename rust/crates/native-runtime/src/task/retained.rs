//! Event-driven ownership for futures whose cancellation is frame destruction.
use crate::{
    InlineCondvar, InlineMutex, NativeReadMeter, ReadAllocation, ReadAllocationReservation,
    arc_allocation_bytes,
};
use std::{
    future::Future,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

struct Frame {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    runtime: Option<tokio::runtime::Handle>,
    #[cfg(feature = "test-support")]
    active: Option<crate::testing::Active>,
    // The physical frame is destroyed before its original backing credit.
    _allocation: ReadAllocation,
}
struct State {
    frame: Option<Frame>,
    failure: Option<Box<dyn std::any::Any + Send>>,
    retirement_failure: Option<Box<dyn std::any::Any + Send>>,
    scheduling_failure: Option<io::Error>,
}
struct Submission {
    pending: Option<crate::workers::NativeJob>,
    waiter: Option<Arc<crate::workers::CapacityWaiter>>,
}
struct Inner {
    state: InlineMutex<State>,
    owner: InlineMutex<Option<std::thread::ThreadId>>,
    retired: InlineCondvar,
    notified: AtomicBool,
    driving: AtomicBool,
    stopping: AtomicBool,
    stopped: AtomicBool,
    submission_handoff: super::submission::SubmissionHandoff,
    submission: InlineMutex<Submission>,
    waiter: Arc<crate::workers::CapacityWaiter>,
    _allocation: ReadAllocation,
}

/// Owns one future without submitting a task or creating an executor. The
/// initial and external event polls run on the calling thread. Self-wakes hand
/// the next poll to the existing shared native worker service;
/// pending futures retain only their frame, original credit and event waker.
/// Polls must be nonblocking. Stopping from the future's own callback requests
/// retirement but returns `WouldBlock` until that callback has returned.
///
/// `stop` observes destruction of this frame, including a concurrent poll's
/// retained state. Effects detached by the future need their own completion
/// owner; frame retirement cannot certify their physical completion.
/// The owner and prepaid admission-node credit remain funded through their
/// last aliases. An accepted queued poll or an externally retained event waker
/// can hold that control credit after `stop` returns; a queued poll releases
/// its alias when the shared worker receives it. Neither can poll the retired
/// frame after stop, and neither permits its credit to be reused early.
pub struct OwnedFuture {
    inner: Arc<Inner>,
}

/// Admits the frame, owner and reusable admission node before the first poll, on the supplied original
/// ledger. Native I/O services must already be initialized. No per-owner
/// runtime, timer, thread or replacement allowance is created.
pub fn retain_future<F>(future: F, meter: &Arc<dyn NativeReadMeter>) -> io::Result<OwnedFuture>
where
    F: Future<Output = ()> + Send + 'static,
{
    meter.check()?;
    crate::workers::workers(&crate::workers::NATIVE_WORKERS, true)?;
    let owner_bytes = arc_allocation_bytes::<Inner>();
    let frame_bytes = std::mem::size_of::<F>();
    let owner = ReadAllocationReservation::new(meter, owner_bytes as u64)?;
    let frame = if frame_bytes == 0 {
        None
    } else {
        Some(ReadAllocationReservation::new(meter, frame_bytes as u64)?)
    };
    let waiter = crate::workers::prepay_capacity_waiter(meter)?;
    let frame_allocation = match frame {
        Some(frame) => frame.allocate_exact(frame_bytes)?,
        None => ReadAllocation::empty(meter),
    };
    let frame = Frame {
        future: Box::pin(future),
        runtime: tokio::runtime::Handle::try_current().ok(),
        #[cfg(feature = "test-support")]
        active: crate::testing::accept_retained_future(),
        _allocation: frame_allocation,
    };
    let owner = owner.allocate_exact(owner_bytes)?;
    let inner = Arc::new(Inner {
        state: InlineMutex::new(State {
            frame: Some(frame),
            failure: None,
            retirement_failure: None,
            scheduling_failure: None,
        }),
        owner: InlineMutex::new(None),
        retired: InlineCondvar::new(),
        notified: AtomicBool::new(false),
        driving: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        submission_handoff: super::submission::SubmissionHandoff::default(),
        submission: InlineMutex::new(Submission {
            pending: None,
            waiter: None,
        }),
        waiter: crate::workers::capacity_waiter(Some(waiter)),
        _allocation: owner,
    });
    Inner::drive(&inner);
    Ok(OwnedFuture { inner })
}

impl OwnedFuture {
    /// Cancels future polling and waits for its frame to be destroyed. Waiting
    /// uses the existing inline lock/condition primitive; it never waits for a
    /// pending future to become ready or relies on an executor making progress.
    /// This retirement path needs no new admission after cancellation.
    /// A polling or destruction panic is retained by the owner and reported
    /// as `Other`, without allocating a new error wrapper during cleanup.
    pub fn stop(&self) -> io::Result<()> {
        self.inner.stopping.store(true, Ordering::Release);
        if self
            .inner
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some_and(|owner| owner == std::thread::current().id())
        {
            self.inner.notified.store(true, Ordering::Release);
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let (pending, waiter) = {
            let mut submission = self
                .inner
                .submission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (submission.pending.take(), submission.waiter.take())
        };
        drop(pending);
        if let Ok(pool) = crate::workers::workers(&crate::workers::NATIVE_WORKERS, true) {
            crate::workers::remove_capacity_waiter(pool, waiter);
        }
        drop(
            self.inner
                .waiter
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .waker
                .take(),
        );
        let frame = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frame
            .take();
        if let Some(frame) = frame {
            self.inner.retire(frame);
        }
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !self.inner.stopped.load(Ordering::Acquire) {
            state = self
                .inner
                .retired
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if state.failure.is_some()
            || state.retirement_failure.is_some()
            || state.scheduling_failure.is_some()
        {
            Err(io::ErrorKind::Other.into())
        } else {
            Ok(())
        }
    }
    /// True only after the owned future frame and its payload credit retired.
    pub fn is_stopped(&self) -> bool {
        self.inner.stopped.load(Ordering::Acquire)
    }
}
impl Drop for OwnedFuture {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl Inner {
    fn drive(this: &Arc<Self>) {
        if this.stopping.load(Ordering::Acquire) || this.stopped.load(Ordering::Acquire) {
            return;
        }
        this.notified.store(true, Ordering::Release);
        if this.driving.swap(true, Ordering::AcqRel) {
            this.submit_pending();
            return;
        }
        Self::poll_and_handoff(this);
    }
    fn poll_and_handoff(this: &Arc<Self>) {
        this.notified.store(false, Ordering::Release);
        this.poll_once();
        this.driving.store(false, Ordering::Release);
        // The handoff checks only an actual wake. A wake racing the driver
        // release either takes ownership itself or is consumed here.
        if !this.notified.load(Ordering::Acquire) || this.driving.swap(true, Ordering::AcqRel) {
            return;
        }
        if this.stopping.load(Ordering::Acquire) || this.stopped.load(Ordering::Acquire) {
            this.driving.store(false, Ordering::Release);
            return;
        }
        {
            let mut submission = this
                .submission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if this.stopping.load(Ordering::Acquire) || this.stopped.load(Ordering::Acquire) {
                this.driving.store(false, Ordering::Release);
                return;
            }
            submission.pending = Some(crate::workers::NativeJob::Deferred(this.clone()));
        }
        this.submit_pending();
    }
    fn submit_pending(self: &Arc<Self>) {
        if !self.submission_handoff.request() {
            return;
        }
        loop {
            self.submission_handoff.begin();
            let mut submission = self
                .submission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if submission.pending.is_some() && !self.stopping.load(Ordering::Acquire) {
                // Keep one original prepaid node across repeated handoffs.
                submission.waiter.get_or_insert_with(|| self.waiter.clone());
                let wake = Waker::from(self.clone());
                let Submission { pending, waiter } = &mut *submission;
                let result = crate::workers::poll_submission(
                    &crate::workers::NATIVE_WORKERS,
                    pending,
                    waiter,
                    &Context::from_waker(&wake),
                );
                let retired_waker = if result.is_ready() {
                    self.waiter
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .waker
                        .take()
                } else {
                    None
                };
                if let Poll::Ready(Err(error)) = result {
                    pending.take();
                    drop(submission);
                    self.state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .scheduling_failure = Some(error);
                    self.stopping.store(true, Ordering::Release);
                    self.poll_once();
                    self.driving.store(false, Ordering::Release);
                } else {
                    drop(submission);
                }
                drop(retired_waker);
            } else {
                drop(submission);
            }
            if self.submission_handoff.release() {
                return;
            }
        }
    }
    fn poll_once(self: &Arc<Self>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(frame) = state.frame.as_mut() else {
            return;
        };
        self.set_owner(Some(std::thread::current().id()));
        let result = if self.stopping.load(Ordering::Acquire) {
            Ok(Poll::Ready(()))
        } else {
            let wake = Waker::from(Arc::clone(self));
            catch_unwind(AssertUnwindSafe(|| {
                let Frame {
                    future,
                    runtime,
                    #[cfg(feature = "test-support")]
                    active,
                    ..
                } = frame;
                let _runtime = runtime.as_ref().map(tokio::runtime::Handle::enter);
                let poll = || future.as_mut().poll(&mut Context::from_waker(&wake));
                #[cfg(feature = "test-support")]
                {
                    crate::testing::poll_retained_future(active.as_ref(), poll)
                }
                #[cfg(not(feature = "test-support"))]
                {
                    let mut poll = poll;
                    poll()
                }
            }))
        };
        if matches!(result, Ok(Poll::Pending)) && !self.stopping.load(Ordering::Acquire) {
            self.set_owner(None);
            return;
        }
        if let Err(payload) = result {
            state.failure = Some(payload);
        }
        let frame = state.frame.take();
        drop(state);
        if let Some(frame) = frame {
            self.retire(frame);
        }
    }
    fn retire(&self, frame: Frame) {
        self.set_owner(Some(std::thread::current().id()));
        let result = catch_unwind(AssertUnwindSafe(|| drop(frame)));
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Polling and frame destruction can each fail once. Preserve both
        // original payloads without allocating or destroying user data under
        // the control lock or in a concurrent stop's completion handoff.
        if let Err(payload) = result {
            state.retirement_failure = Some(payload);
        }
        self.stopped.store(true, Ordering::Release);
        self.set_owner(None);
        drop(state);
        self.retired.notify_all();
    }
    fn set_owner(&self, owner: Option<std::thread::ThreadId>) {
        *self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = owner;
    }
}
impl crate::workers::DeferredPoll for Inner {
    fn poll_deferred(self: Arc<Self>) {
        Self::poll_and_handoff(&self);
    }
}
impl Wake for Inner {
    fn wake(self: Arc<Self>) {
        Self::drive(&self);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        Self::drive(self);
    }
}
