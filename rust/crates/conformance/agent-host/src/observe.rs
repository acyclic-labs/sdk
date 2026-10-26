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
    pub reads: AtomicUsize,
    pub writes: AtomicUsize,
    calls: AtomicUsize,
    commits: Mutex<Vec<CommitRequest>>,
    fault: Mutex<Option<(usize, Fault)>>,
    crash: Mutex<Option<(usize, Fault)>>,
}

impl Observed {
    pub fn new(inner: Arc<dyn StreamProvider>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            commits: Mutex::new(Vec::new()),
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

    fn read_call(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }
    fn write_call(&self) {
        self.writes.fetch_add(1, Ordering::SeqCst);
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for Observed {
    fn operation_claims(&self) -> &OperationClaims {
        self.inner.operation_claims()
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.read_call();
        self.io(self.inner.inspect_idempotency(key)).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.read_call();
        self.io(self.inner.tail(path)).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.read_call();
        self.io(self.inner.bounds(path)).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.write_call();
        self.io(self.inner.append(request)).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.write_call();
        self.io(self.inner.fork(request)).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.read_call();
        self.io(self.inner.read(request)).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.read_call();
        self.io(self.inner.follow(path, from, filter)).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.read_call();
        self.io(self.inner.children_page(request)).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.write_call();
        self.io(self.commit_inner(request)).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.read_call();
        self.io(self.inner.read_commit(id)).await
    }
}
