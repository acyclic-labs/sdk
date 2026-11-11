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
    #[cfg(feature = "test-support")]
    active: Option<crate::testing::Active>,
    // The physical frame is destroyed before its original backing credit.
    _allocation: ReadAllocation,
}
struct State {
    frame: Option<Frame>,
    failure: Option<Box<dyn std::any::Any + Send>>,
}
struct Inner {
    state: InlineMutex<State>,
    owner: InlineMutex<Option<std::thread::ThreadId>>,
    retired: InlineCondvar,
    notified: AtomicBool,
    driving: AtomicBool,
    stopping: AtomicBool,
    stopped: AtomicBool,
    _allocation: ReadAllocation,
}

/// Owns one future without submitting a task or creating an executor. The
/// initial poll and subsequent wake-driven polls run on the calling thread;
/// pending futures retain only their frame, original credit and event waker.
/// Polls must be nonblocking. Stopping from the future's own callback requests
/// retirement but returns `WouldBlock` until that callback has returned.
///
/// `stop` observes destruction of this frame, including a concurrent poll's
/// retained state. Effects detached by the future need their own completion
/// owner; frame retirement cannot certify their physical completion.
pub struct OwnedFuture {
    inner: Arc<Inner>,
}

/// Admits both allocations before the first poll, on the supplied original
/// ledger. No runtime, timer, thread or replacement allowance is created.
pub fn retain_future<F>(future: F, meter: &Arc<dyn NativeReadMeter>) -> io::Result<OwnedFuture>
where
    F: Future<Output = ()> + Send + 'static,
{
    meter.check()?;
    let owner_bytes = arc_allocation_bytes::<Inner>();
    let frame_bytes = std::mem::size_of::<F>();
    let owner = ReadAllocationReservation::new(meter, owner_bytes as u64)?;
    let frame = if frame_bytes == 0 {
        None
    } else {
        Some(ReadAllocationReservation::new(meter, frame_bytes as u64)?)
    };
    let frame_allocation = match frame {
        Some(frame) => frame.allocate_exact(frame_bytes)?,
        None => ReadAllocation::empty(meter),
    };
    let frame = Frame {
        future: Box::pin(future),
        #[cfg(feature = "test-support")]
        active: crate::testing::accept_retained_future(),
        _allocation: frame_allocation,
    };
    let owner = owner.allocate_exact(owner_bytes)?;
    let inner = Arc::new(Inner {
        state: InlineMutex::new(State {
            frame: Some(frame),
            failure: None,
        }),
        owner: InlineMutex::new(None),
        retired: InlineCondvar::new(),
        notified: AtomicBool::new(false),
        driving: AtomicBool::new(false),
        stopping: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
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
        if state.failure.is_some() {
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
        this.notified.store(true, Ordering::Release);
        if this.driving.swap(true, Ordering::AcqRel) {
            return;
        }
        loop {
            this.notified.store(false, Ordering::Release);
            this.poll_once();
            this.driving.store(false, Ordering::Release);
            // The handoff checks only an actual wake. A wake racing the driver
            // release either takes ownership itself or is consumed here.
            if !this.notified.load(Ordering::Acquire) || this.driving.swap(true, Ordering::AcqRel) {
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
                    #[cfg(feature = "test-support")]
                    active,
                    ..
                } = frame;
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
        let discarded = match result {
            Err(payload) if state.failure.is_none() => {
                state.failure = Some(payload);
                None
            }
            Err(payload) => Some(payload),
            Ok(()) => None,
        };
        self.stopped.store(true, Ordering::Release);
        self.set_owner(None);
        drop(state);
        self.retired.notify_all();
        drop(discarded);
    }
    fn set_owner(&self, owner: Option<std::thread::ThreadId>) {
        *self
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = owner;
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
