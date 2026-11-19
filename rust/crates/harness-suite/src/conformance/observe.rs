use acyclic_stream::testing::{RequestClass, RequestLedger};
use acyclic_stream::*;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Deterministic faults at a public provider call boundary.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fault {
    Before,
    After,
    Refuse,
}

enum FaultAt {
    Index(usize),
    Matching(fn(&CommitRequest) -> bool),
}

pub(crate) struct Observed {
    pub inner: ProviderOwner<super::Stream>,
    pub requests: RequestLedger,
    // A history-read sentinel, independent of the aggregate request budget.
    pub history_reads: AtomicUsize,
    counter_only: bool,
    calls: AtomicUsize,
    commit_count: AtomicUsize,
    capture: Option<Mutex<Capture>>,
    /// Counts captured commits, so a vector waits on a commit event.
    captured: tokio::sync::watch::Sender<usize>,
    fault: Mutex<Option<(FaultAt, Fault)>>,
    crash: Mutex<Option<(usize, Fault)>>,
}

#[derive(Default)]
struct Capture {
    commits: Vec<CommitRequest>,
    follows: Vec<(StreamPath, u64)>,
}

impl Observed {
    pub fn new(inner: ProviderOwner<super::Stream>) -> Self {
        Self {
            inner,
            requests: RequestLedger::new(),
            history_reads: AtomicUsize::new(0),
            counter_only: false,
            calls: AtomicUsize::new(0),
            commit_count: AtomicUsize::new(0),
            capture: None,
            captured: tokio::sync::watch::Sender::new(0),
            fault: Mutex::new(None),
            crash: Mutex::new(None),
        }
    }

    /// Only the finite exact-retry vectors retain requests. Counter and scale
    /// workloads use `new`, with no history allocation, lock or request clone.
    pub fn capturing(inner: ProviderOwner<super::Stream>) -> Self {
        Self {
            capture: Some(Mutex::default()),
            ..Self::new(inner)
        }
    }

    /// Count dispatches without retaining requests or consulting fault state.
    pub fn counters(inner: ProviderOwner<super::Stream>) -> Self {
        Self {
            counter_only: true,
            ..Self::new(inner)
        }
    }

    pub fn arm(&self, offset: usize, fault: Fault) {
        let next = self.commit_count() + offset;
        *self.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some((FaultAt::Index(next), fault));
    }

    pub fn arm_matching(&self, matches: fn(&CommitRequest) -> bool, fault: Fault) {
        *self.fault.lock().unwrap_or_else(|e| e.into_inner()) =
            Some((FaultAt::Matching(matches), fault));
    }

    pub fn commit_count(&self) -> usize {
        self.commit_count.load(Ordering::SeqCst)
    }

    pub fn take_commits(&self) -> Vec<CommitRequest> {
        self.take_capture(|capture| std::mem::take(&mut capture.commits))
            .unwrap_or_default()
    }

    /// The first captured commit `select` accepts, awaited as commits arrive;
    /// captured history stays in place.
    pub async fn committed<T>(
        &self,
        select: impl Fn(&CommitRequest) -> Option<T>,
    ) -> Result<T, StreamError> {
        // Subscribe before scanning, so no commit lands unseen in between.
        let mut captured = self.captured.subscribe();
        loop {
            if let Some(found) = self
                .take_capture(|capture| capture.commits.iter().find_map(&select))
                .ok_or(StreamError::InvalidArgument)?
            {
                return Ok(found);
            }
            captured
                .changed()
                .await
                .map_err(|_| StreamError::Unavailable)?;
        }
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn take_follows(&self) -> Vec<(StreamPath, u64)> {
        self.take_capture(|capture| std::mem::take(&mut capture.follows))
            .unwrap_or_default()
    }

    fn capture(&self, record: impl FnOnce(&mut Capture)) {
        if let Some(capture) = &self.capture {
            record(&mut capture.lock().unwrap_or_else(|error| error.into_inner()));
        }
    }

    fn take_capture<T>(&self, take: impl FnOnce(&mut Capture) -> T) -> Option<T> {
        let capture = self.capture.as_ref()?;
        Some(take(
            &mut capture.lock().unwrap_or_else(|error| error.into_inner()),
        ))
    }

    /// Lose this provider session at the selected request. After that request,
    /// no further operation reaches storage until the caller opens a new session.
    pub fn crash_at(&self, at: usize, fault: Fault) {
        *self.crash.lock().unwrap_or_else(|e| e.into_inner()) = Some((at, fault));
    }

    /// Allow a crash-after request to finish once, then refuse every later call.
    fn crash_fault(&self) -> Option<Fault> {
        let mut crash = self.crash.lock().unwrap_or_else(|e| e.into_inner());
        let at = self.calls.fetch_add(1, Ordering::SeqCst);
        let (step, fault) = crash.as_mut()?;
        (*step <= at).then(|| std::mem::replace(fault, Fault::Before))
    }

    async fn io<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, StreamError>> + ProviderTask,
    ) -> Result<T, StreamError> {
        if self.counter_only {
            return operation.await;
        }
        let fault = self.crash_fault();
        if matches!(fault, Some(Fault::Before)) {
            return Err(StreamError::Unavailable);
        }
        let result = operation.await;
        if matches!(fault, Some(Fault::After)) {
            return Err(StreamError::Unavailable);
        }
        result
    }

    async fn commit_inner(
        &self,
        command: acyclic_stream::WriteRequest,
    ) -> Result<acyclic_stream::WriteOutcome, StreamError> {
        let request = match &command {
            acyclic_stream::WriteRequest::Commit(request)
            | acyclic_stream::WriteRequest::CommitAccepted(request) => request,
            _ => return Err(StreamError::InvalidArgument),
        };
        match self.commit_fault(request) {
            Some(Fault::Before) => Err(StreamError::Unavailable),
            Some(Fault::Refuse) => Err(StreamError::AccessDenied),
            Some(Fault::After) => {
                self.inner.write(command).await?;
                Err(StreamError::Unavailable)
            }
            None => self.inner.write(command).await,
        }
    }

    /// Captures one commit and selects the fault armed for it, at the same
    /// boundary on the portable and native paths.
    fn commit_fault(&self, request: &CommitRequest) -> Option<Fault> {
        let index = if let Some(capture) = &self.capture {
            let mut capture = capture.lock().unwrap_or_else(|error| error.into_inner());
            // Keep captured order and fault ordinals at the same boundary even
            // when two callers submit concurrently. Taking history never resets
            // the original operation count.
            let index = self.commit_count.fetch_add(1, Ordering::SeqCst);
            capture.commits.push(request.clone());
            self.captured.send_modify(|count| *count += 1);
            index
        } else {
            self.commit_count.fetch_add(1, Ordering::SeqCst)
        };
        let mut fault = self.fault.lock().unwrap_or_else(|e| e.into_inner());
        if fault.as_ref().is_some_and(|(at, _)| match at {
            FaultAt::Index(at) => *at == index,
            FaultAt::Matching(matches) => matches(request),
        }) {
            fault.take().map(|(_, fault)| fault)
        } else {
            None
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for Observed {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.requests.record(RequestClass::Metadata);
        self.io(self.inner.inspect_idempotency(key)).await
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.requests.record(RequestClass::of_heads(&request));
        self.io(self.inner.heads(request)).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        if self.counter_only {
            self.requests.record(RequestClass::Data);
            return self.inner.write(request);
        }
        match request {
            acyclic_stream::WriteRequest::Append(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.requests.record(RequestClass::Data);
                        self.io(self.inner.append(request)).await
                    },
                    acyclic_stream::WriteOutcome::Append,
                ))
            }
            acyclic_stream::WriteRequest::Fork(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    self.requests.record(RequestClass::Data);
                    self.io(self.inner.fork(request)).await
                },
                acyclic_stream::WriteOutcome::Fork,
            )),
            command @ (acyclic_stream::WriteRequest::Commit(_)
            | acyclic_stream::WriteRequest::CommitAccepted(_)) => Box::pin(async move {
                self.requests.record(RequestClass::Data);
                self.io(self.commit_inner(command)).await
            }),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.requests.record(RequestClass::Data);
        if !self.counter_only {
            self.history_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.io(self.inner.read(request)).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.capture(|capture| capture.follows.push((request.path.clone(), request.from)));
        self.requests.record(RequestClass::Data);
        if !self.counter_only {
            self.history_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.io(self.inner.follow(request)).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.requests.record(RequestClass::Data);
        if !self.counter_only {
            self.history_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.io(self.inner.read_commit(id)).await
    }
}

/// Native calls reach the same provider as portable ones, with the same
/// request ledger, call-ordinal crashes and commit faults. A native consumer
/// therefore never meets a refusing default in place of the real provider, and
/// the explicit trait makes every native method a required forward.
#[cfg(not(target_arch = "wasm32"))]
impl Observed {
    /// Records one native call: its request class, and a history read.
    fn count(&self, call: Native) {
        self.requests.record(call.class);
        if call.history && !self.counter_only {
            self.history_reads.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The one native forward: it counts the call before the inner call is
    /// made, then applies the armed crash and commit faults. Every native
    /// method goes through it, so a forwarded call cannot skip the ledger.
    fn native<'a, T: Send + 'a, E: From<StreamError> + Send + 'a>(
        &'a self,
        call: Native,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        commit: Option<CommitRequest>,
        forward: impl FnOnce(
            Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> NativeProviderFuture<'a, T, E>,
    ) -> NativeProviderFuture<'a, T, E> {
        self.count(call);
        let operation = forward(Arc::clone(&meter));
        self.faulted(&meter, commit, operation)
    }

    fn faulted<'a, T: Send + 'a, E: From<StreamError> + Send + 'a>(
        &'a self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        commit: Option<CommitRequest>,
        operation: NativeProviderFuture<'a, T, E>,
    ) -> NativeProviderFuture<'a, T, E> {
        if self.counter_only {
            return operation;
        }
        NativeProviderFuture::new(meter, async move {
            let crash = self.crash_fault();
            if matches!(crash, Some(Fault::Before)) {
                return Ok(Err(StreamError::Unavailable.into()));
            }
            let result = match commit
                .as_ref()
                .and_then(|request| self.commit_fault(request))
            {
                Some(Fault::Before) => return Ok(Err(StreamError::Unavailable.into())),
                Some(Fault::Refuse) => return Ok(Err(StreamError::AccessDenied.into())),
                Some(Fault::After) => match operation.await? {
                    Ok(_) => Ok(Err(StreamError::Unavailable.into())),
                    Err(error) => Ok(Err(error)),
                },
                None => operation.await,
            };
            if matches!(crash, Some(Fault::After)) {
                return Ok(Err(StreamError::Unavailable.into()));
            }
            result
        })
    }
}

/// What a native call records: its request class, and whether it reads history.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
struct Native {
    class: RequestClass,
    history: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl Native {
    const DATA: Self = Self::of(RequestClass::Data);
    const HISTORY: Self = Self {
        class: RequestClass::Data,
        history: true,
    };
    const fn of(class: RequestClass) -> Self {
        Self {
            class,
            history: false,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::ExplicitNativeStreamProvider for Observed {
    fn native_durability_barrier(
        &self,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, ()> {
        self.native(Native::DATA, meter, None, |meter| {
            self.inner.native_durability_barrier(meter)
        })
    }
    fn prepare_native_durability_barrier(
        &self,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        // Counted before preparation, so a refused preparation still counts,
        // as it does at the provider.
        self.count(Native::DATA);
        let operation = self
            .inner
            .prepare_native_durability_barrier(Arc::clone(&meter))?;
        Ok(self.faulted(&meter, None, operation))
    }
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        self.native(Native::of(RequestClass::Metadata), meter, None, |meter| {
            self.inner.native_inspect_idempotency(key, meter)
        })
    }
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        self.native(Native::of(RequestClass::Head), meter, None, |meter| {
            self.inner.native_tail(path, meter)
        })
    }
    fn native_heads(
        &self,
        request: HeadsRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Heads> {
        let call = Native::of(RequestClass::of_heads(&request));
        self.native(call, meter, None, |meter| {
            self.inner.native_heads(request, meter)
        })
    }
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        self.native(Native::HISTORY, meter, None, |meter| {
            self.inner.native_read(request, meter)
        })
    }
    fn native_follow(
        &self,
        request: FollowRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        self.capture(|capture| capture.follows.push((request.path.clone(), request.from)));
        self.native(Native::HISTORY, meter, None, |meter| {
            self.inner.native_follow(request, meter)
        })
    }
    fn native_write(
        &self,
        request: WriteRequest,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, WriteOutcome> {
        let commit = match &request {
            WriteRequest::Commit(commit) | WriteRequest::CommitAccepted(commit)
                if !self.counter_only =>
            {
                Some(commit.clone())
            }
            _ => None,
        };
        self.native(Native::DATA, meter, commit, |meter| {
            self.inner.native_write(request, meter)
        })
    }
    fn native_commit_guarded(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: NativeCommitGuard,
    ) -> NativeProviderFuture<'_, CommitOutcome, NativeCommitError> {
        let commit = (!self.counter_only).then(|| request.clone());
        self.native(Native::DATA, meter, commit, |meter| {
            self.inner
                .native_commit_guarded(request, deadline_unix_millis, meter, before)
        })
    }
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        capacity: PreparedRecordCapacity,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        // A prepared ingress has not published, so an armed commit fault
        // refuses it before preparation, as it would refuse the commit.
        let commit = (!self.counter_only).then(|| request.clone());
        self.native(Native::DATA, meter, commit, |meter| {
            self.inner.native_prepare_commit(request, capacity, meter)
        })
    }
}

impl acyclic_harness_core::filesystem::attestation::AttestedProvider for Observed {}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    fn measured_commit(
        provider: &dyn StreamProvider,
        request: CommitRequest,
    ) -> crate::conformance::Result<(CommitOutcome, acyclic_test_support::AllocationInfo)> {
        let mut result = None;
        let allocation = acyclic_test_support::measure(|| {
            result = Some(futures::executor::block_on(provider.commit(request)));
        });
        Ok((
            result.ok_or("commit observation lost its result")??,
            allocation,
        ))
    }

    #[test]
    fn counter_observation_has_fixed_allocation_cost_with_request_capture_negative_control()
    -> crate::conformance::Result {
        let mut previous = None;
        for records in [1, 1_024] {
            let memory = ProviderOwner::new(MemoryStream::default());
            let path = StreamPath::new("observation")?;
            let request = CommitRequest {
                deadline_unix_millis: None,
                effects: vec![acyclic_stream::CommitEffect::hard(
                    vec![CommitCondition::Absent { path: path.clone() }],
                    vec![CommitMutation::Append {
                        path,
                        records: vec![AppendRecord::from("value"); records],
                    }],
                )],
                idempotency_key: IdempotencyKey::unique(),
            };
            // All comparisons replay one actual committed request against the
            // same provider and unchanged state, excluding fixture preparation.
            let committed = futures::executor::block_on(memory.commit(request.clone()))?;
            assert!(matches!(committed, CommitOutcome::Committed(_)));
            let counters = Observed::new(memory.clone());
            let captured = Observed::capturing(memory.clone());
            let (direct, direct_cost) = measured_commit(&*memory, request.clone())?;
            let (counted, counted_cost) = measured_commit(&counters, request.clone())?;
            let (recorded, recorded_cost) = measured_commit(&captured, request.clone())?;
            assert_eq!(direct, committed);
            assert_eq!(counted, committed);
            assert_eq!(recorded, committed);
            let overhead = (
                counted_cost.count_total - direct_cost.count_total,
                counted_cost.bytes_total - direct_cost.bytes_total,
            );
            if let Some(previous) = previous {
                assert_eq!(
                    overhead, previous,
                    "observer cost grew with request records"
                );
            }
            previous = Some(overhead);
            assert!(recorded_cost.count_total > counted_cost.count_total);
            assert!(recorded_cost.bytes_total > counted_cost.bytes_total);
            eprintln!(
                "records={records} direct={direct_cost:?} counter={counted_cost:?} capture={recorded_cost:?}"
            );
            // Taking evidence transfers its original allocation and cannot
            // change the absolute ordinal used by an armed fault.
            let history = captured.take_commits();
            assert_eq!(history, [request.clone()]);
            assert_eq!(captured.commit_count(), 1);
            captured.arm(0, Fault::Before);
            assert!(matches!(
                futures::executor::block_on(captured.commit(request.clone())),
                Err(StreamError::Unavailable)
            ));
            let retried = captured.take_commits();
            assert_eq!(retried, history);
            assert_eq!(captured.commit_count(), 2);
            // The refusal did not replace or remove the actual original effect.
            assert_eq!(
                futures::executor::block_on(memory.commit(request))?,
                committed
            );
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "observe/tests.rs"]
mod counter_tests;
