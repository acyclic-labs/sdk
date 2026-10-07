//! Shared Stream fault fixture; all successful operations use the real provider.
#![allow(dead_code)]
use acyclic_stream::{AppendOutcome, MemoryStream, StreamError, StreamProvider};
use bytes::Bytes;
const COORDINATOR_PATH: &str = "harness/v2/coordinator/events";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ExecutionFaultMode {
    Before = 1,
    AfterVisible = 2,
    AfterHidden = 3,
}
#[derive(Default)]
pub struct LostSessionAck<P = MemoryStream> {
    pub inner: P,
    pub lose_ack: std::sync::atomic::AtomicBool,
    pub hide_receipt: std::sync::atomic::AtomicBool,
    pub location_fault: std::sync::atomic::AtomicU8,
    pub hide_location_read: std::sync::atomic::AtomicBool,
    pub forbid_writes: std::sync::atomic::AtomicBool,
    pub observation_reads: std::sync::atomic::AtomicUsize,
    pub observation_maximum: std::sync::atomic::AtomicU32,
    pub observation_writes: std::sync::atomic::AtomicUsize,
    pub execution_race: std::sync::atomic::AtomicBool,
    pub commit_lose_ack: std::sync::atomic::AtomicBool,
    pub execution_fault: std::sync::atomic::AtomicU8,
    pub execution_tail: std::sync::atomic::AtomicU64,
    pub execution_faults: std::sync::atomic::AtomicUsize,
}
impl<P> LostSessionAck<P> {
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            lose_ack: Default::default(),
            hide_receipt: Default::default(),
            location_fault: Default::default(),
            hide_location_read: Default::default(),
            forbid_writes: Default::default(),
            observation_reads: Default::default(),
            observation_maximum: Default::default(),
            observation_writes: Default::default(),
            execution_race: Default::default(),
            commit_lose_ack: Default::default(),
            execution_fault: Default::default(),
            execution_tail: Default::default(),
            execution_faults: Default::default(),
        }
    }

    pub fn arm_execution(&self, tail: u64, mode: ExecutionFaultMode) {
        self.execution_tail
            .store(tail, std::sync::atomic::Ordering::SeqCst);
        self.execution_fault
            .store(mode as u8, std::sync::atomic::Ordering::SeqCst);
    }
}
#[async_trait::async_trait]
impl<P: StreamProvider> StreamProvider for LostSessionAck<P> {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        if self
            .hide_receipt
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        if self.forbid_writes.load(std::sync::atomic::Ordering::SeqCst) {
            self.observation_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        if request.path.as_str() == COORDINATOR_PATH
            && self
                .location_fault
                .compare_exchange(
                    4,
                    0,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_ok()
        {
            return Err(StreamError::Unavailable);
        }
        let location = request
            .path
            .as_str()
            .starts_with("harness/v2/coordinator/intent-locations/");
        let fault = if location {
            self.location_fault
                .swap(0, std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(StreamError::Unavailable);
        }
        let outcome = self.inner.append(request).await?;
        if fault >= 2 {
            if fault == 3 {
                self.hide_location_read
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            return Err(StreamError::Unavailable);
        }
        if matches!(outcome, AppendOutcome::Committed(_))
            && self
                .lose_ack
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        Ok(outcome)
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        if self.forbid_writes.load(std::sync::atomic::Ordering::SeqCst) {
            self.observation_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        self.inner.fork(request).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        if self.forbid_writes.load(std::sync::atomic::Ordering::SeqCst) {
            self.observation_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.observation_maximum
                .fetch_max(request.limit, std::sync::atomic::Ordering::SeqCst);
        }
        if request
            .path
            .as_str()
            .starts_with("harness/v2/coordinator/intent-locations/")
            && self
                .hide_location_read
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, StreamError> {
        self.inner.children_page(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        if self.forbid_writes.load(std::sync::atomic::Ordering::SeqCst) {
            self.observation_writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(StreamError::Unavailable);
        }
        if self
            .execution_race
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            let path = request
                .conditions
                .iter()
                .find_map(|condition| match condition {
                    acyclic_stream::CommitCondition::Absent { path }
                        if path.as_str().starts_with("harness/v2/execution/") =>
                    {
                        Some((path.clone(), 0))
                    }
                    acyclic_stream::CommitCondition::Tail { path, expected }
                        if path.as_str().starts_with("harness/v2/execution/") =>
                    {
                        Some((path.clone(), *expected))
                    }
                    _ => None,
                })
                .ok_or(StreamError::Unavailable)?;
            self.inner
                .append(acyclic_stream::AppendRequest {
                    path: path.0,
                    records: vec![Bytes::from_static(b"dispatch won")],
                    if_tail: Some(path.1),
                    idempotency_key: None,
                })
                .await?;
        }
        let selected = request.conditions.iter().any(|condition| {
            matches!(condition,
            acyclic_stream::CommitCondition::Tail { path, expected }
                if path.as_str().starts_with("harness/v2/execution/") &&
                *expected == self.execution_tail.load(std::sync::atomic::Ordering::SeqCst))
        });
        let fault = if selected {
            self.execution_fault
                .swap(0, std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if fault != 0 {
            self.execution_faults
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if fault == ExecutionFaultMode::Before as u8 {
            return Err(StreamError::Unavailable);
        }
        let outcome = self.inner.commit(request).await?;
        if fault == ExecutionFaultMode::AfterHidden as u8 {
            self.hide_receipt
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if fault != 0 {
            return Err(StreamError::Unavailable);
        }
        if self
            .commit_lose_ack
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        Ok(outcome)
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
    async fn commit_before(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline_unix_millis: u64,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        self.inner
            .commit_before(request, deadline_unix_millis)
            .await
    }
}
