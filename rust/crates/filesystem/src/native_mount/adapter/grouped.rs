struct GroupedQueue {
    requests: std::collections::VecDeque<GroupedRequest>,
    draining: bool,
}

/// One change waiting for group commit, and where its own result goes.
struct GroupedRequest {
    change: GroupedChange,
    admission: Arc<GroupedAdmission>,
    reply: tokio::sync::oneshot::Sender<Result<GroupedOutcome, MountSourceError>>,
    /// The requester's origin, which its change takes whichever caller or
    /// drain applies the group.
    origin: ViewOrigin,
    /// Whether the change was recorded when it was requested, as a pending
    /// creation is (see [`PendingCreate`]).
    recorded: bool,
}

/// Whether a queued change was claimed for application or abandoned by its
/// caller first. Exactly one of the two ever happens.
struct GroupedAdmission(AtomicU8);

const GROUPED_QUEUED: u8 = 0;
const GROUPED_CLAIMED: u8 = 1;
const GROUPED_ABANDONED: u8 = 2;

impl GroupedAdmission {
    fn claim(&self) -> bool {
        self.0
            .compare_exchange(
                GROUPED_QUEUED,
                GROUPED_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

/// A future polled in place by its caller that, if the caller drops it
/// unfinished, continues on its own task: no caller's deadline can stop it
/// midway.
struct RunToCompletion {
    future: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    runtime: tokio::runtime::Handle,
}

impl RunToCompletion {
    fn new(future: impl std::future::Future<Output = ()> + Send + 'static) -> Self {
        Self {
            future: Some(Box::pin(future)),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl std::future::Future for RunToCompletion {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let Some(future) = self.future.as_mut() else {
            return std::task::Poll::Ready(());
        };
        let polled = future.as_mut().poll(context);
        if polled.is_ready() {
            self.future = None;
        }
        polled
    }
}

impl Drop for RunToCompletion {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            drop(self.runtime.spawn(future));
        }
    }
}

/// Abandons a still-queued change when its caller stops waiting, so a
/// change whose caller saw it fail can never apply later. A change already
/// claimed is being applied and completes regardless.
struct AbandonUnlessClaimed(Arc<GroupedAdmission>);

impl Drop for AbandonUnlessClaimed {
    fn drop(&mut self) {
        let _ = self.0.0.compare_exchange(
            GROUPED_QUEUED,
            GROUPED_ABANDONED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}
