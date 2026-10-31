use acyclic_stream::testing::{RequestClass, RequestLedger};
use acyclic_stream::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Deterministic faults at a public provider call boundary.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Fault {
    Before,
    After,
    Refuse,
}

pub(crate) struct Observed {
    pub inner: Arc<dyn StreamProvider>,
    pub requests: RequestLedger,
    // A history-read sentinel, independent of the aggregate request budget.
    pub history_reads: AtomicUsize,
    calls: AtomicUsize,
    commit_count: AtomicUsize,
    capture: Option<Mutex<Capture>>,
    fault: Mutex<Option<(usize, Fault)>>,
    crash: Mutex<Option<(usize, Fault)>>,
}

#[derive(Default)]
struct Capture {
    commits: Vec<CommitRequest>,
    follows: Vec<(StreamPath, u64)>,
    appends: Vec<AppendRequest>,
}

impl Observed {
    pub fn new(inner: Arc<dyn StreamProvider>) -> Self {
        Self {
            inner,
            requests: RequestLedger::new(),
            history_reads: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            commit_count: AtomicUsize::new(0),
            capture: None,
            fault: Mutex::new(None),
            crash: Mutex::new(None),
        }
    }

    /// Only the finite exact-retry vectors retain requests. Counter and scale
    /// workloads use `new`, with no history allocation, lock or request clone.
    pub fn capturing(inner: Arc<dyn StreamProvider>) -> Self {
        Self {
            capture: Some(Mutex::default()),
            ..Self::new(inner)
        }
    }

    pub fn arm(&self, offset: usize, fault: Fault) {
        let next = self.commit_count() + offset;
        *self.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some((next, fault));
    }

    pub fn commit_count(&self) -> usize {
        self.commit_count.load(Ordering::SeqCst)
    }

    pub fn take_commits(&self) -> Vec<CommitRequest> {
        self.take_capture(|capture| std::mem::take(&mut capture.commits))
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn take_follows(&self) -> Vec<(StreamPath, u64)> {
        self.take_capture(|capture| std::mem::take(&mut capture.follows))
    }

    pub fn take_appends(&self) -> Vec<AppendRequest> {
        self.take_capture(|capture| std::mem::take(&mut capture.appends))
    }

    fn capture(&self, record: impl FnOnce(&mut Capture)) {
        if let Some(capture) = &self.capture {
            record(&mut capture.lock().unwrap_or_else(|error| error.into_inner()));
        }
    }

    fn take_capture<T>(&self, take: impl FnOnce(&mut Capture) -> T) -> T {
        let capture = self.capture.as_ref().expect("request capture was selected");
        take(&mut capture.lock().unwrap_or_else(|error| error.into_inner()))
    }

    pub fn crash_at(&self, at: usize, fault: Fault) {
        *self.crash.lock().unwrap_or_else(|e| e.into_inner()) = Some((at, fault));
    }

    async fn io<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, StreamError>> + ProviderTask,
    ) -> Result<T, StreamError> {
        let at = self.calls.fetch_add(1, Ordering::SeqCst);
        let fault = {
            let mut crash = self.crash.lock().unwrap_or_else(|e| e.into_inner());
            if crash.as_ref().is_some_and(|(step, _)| *step == at) {
                crash.take().map(|(_, fault)| fault)
            } else {
                None
            }
        };
        if matches!(fault, Some(Fault::Before)) {
            return Err(StreamError::Unavailable);
        }
        let result = operation.await;
        if matches!(fault, Some(Fault::After)) {
            return Err(StreamError::Unavailable);
        }
        result
    }

    async fn commit_inner(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let index = if let Some(capture) = &self.capture {
            let mut capture = capture.lock().unwrap_or_else(|error| error.into_inner());
            // Keep captured order and fault ordinals at the same boundary even
            // when two callers submit concurrently. Taking history never resets
            // the original operation count.
            let index = self.commit_count.fetch_add(1, Ordering::SeqCst);
            capture.commits.push(request.clone());
            index
        } else {
            self.commit_count.fetch_add(1, Ordering::SeqCst)
        };
        let fault = {
            let mut fault = self.fault.lock().unwrap_or_else(|e| e.into_inner());
            if fault.as_ref().is_some_and(|(at, _)| *at == index) {
                fault.take().map(|(_, fault)| fault)
            } else {
                None
            }
        };
        match fault {
            Some(Fault::Before) => Err(StreamError::Unavailable),
            Some(Fault::Refuse) => Err(StreamError::AccessDenied),
            Some(Fault::After) => {
                self.inner.commit(request).await?;
                Err(StreamError::Unavailable)
            }
            None => self.inner.commit(request).await,
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
        self.requests
            .record(if request.paths.is_empty() && request.children.is_some() {
                RequestClass::List
            } else {
                RequestClass::Head
            });
        self.io(self.inner.heads(request)).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> acyclic_stream::BoxProviderFuture<
        '_,
        std::result::Result<acyclic_stream::WriteOutcome, acyclic_stream::StreamError>,
    > {
        match request {
            acyclic_stream::WriteRequest::Append(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.capture(|capture| capture.appends.push(request.clone()));
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
            acyclic_stream::WriteRequest::Commit(request) => {
                Box::pin(futures::TryFutureExt::map_ok(
                    async move {
                        self.requests.record(RequestClass::Data);
                        self.io(self.commit_inner(request)).await
                    },
                    acyclic_stream::WriteOutcome::Commit,
                ))
            }
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.requests.record(RequestClass::Data);
        self.history_reads.fetch_add(1, Ordering::SeqCst);
        self.io(self.inner.read(request)).await
    }
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.capture(|capture| capture.follows.push((request.path.clone(), request.from)));
        self.requests.record(RequestClass::Data);
        self.history_reads.fetch_add(1, Ordering::SeqCst);
        self.io(self.inner.follow(request)).await
    }

    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.requests.record(RequestClass::Data);
        self.history_reads.fetch_add(1, Ordering::SeqCst);
        self.io(self.inner.read_commit(id)).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for Observed {}

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
            let memory = Arc::new(MemoryStream::default());
            let path = StreamPath::new("observation")?;
            let request = CommitRequest {
                deadline_unix_millis: None,
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutations: vec![CommitMutation::Append {
                    path,
                    records: vec![AppendRecord::from("value"); records],
                }],
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
