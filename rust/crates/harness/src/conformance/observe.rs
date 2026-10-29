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
    commits: Mutex<Vec<CommitRequest>>,
    follows: Mutex<Vec<(StreamPath, u64)>>,
    appends: Mutex<Vec<AppendRequest>>,
    fault: Mutex<Option<(usize, Fault)>>,
    crash: Mutex<Option<(usize, Fault)>>,
}

impl Observed {
    pub fn new(inner: Arc<dyn StreamProvider>) -> Self {
        Self {
            inner,
            requests: RequestLedger::new(),
            history_reads: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            commits: Mutex::new(Vec::new()),
            follows: Mutex::new(Vec::new()),
            appends: Mutex::new(Vec::new()),
            fault: Mutex::new(None),
            crash: Mutex::new(None),
        }
    }

    pub fn arm(&self, offset: usize, fault: Fault) {
        let next = self.commits.lock().unwrap_or_else(|e| e.into_inner()).len() + offset;
        *self.fault.lock().unwrap_or_else(|e| e.into_inner()) = Some((next, fault));
    }

    pub fn commits(&self) -> Vec<CommitRequest> {
        self.commits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn follows(&self) -> Vec<(StreamPath, u64)> {
        self.follows
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn appends(&self) -> Vec<AppendRequest> {
        self.appends
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
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
        let index = {
            let mut commits = self.commits.lock().unwrap_or_else(|e| e.into_inner());
            let index = commits.len();
            commits.push(request.clone());
            index
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
                        self.appends
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .push(request.clone());
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
        self.follows
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push((request.path.clone(), request.from));
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

impl acyclic_harness_core::filesystem::attestation::AttestedProvider for Observed {}
