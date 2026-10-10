//! Shared Stream fault fixture; all successful operations use the real provider.
#![allow(
    dead_code,
    reason = "shared fault fixtures expose controls used by different test modules"
)]
use acyclic_stream::{AppendOutcome, MemoryStream, StreamError, StreamProvider};
use bytes::Bytes;
const COORDINATOR_PATH: &str = "harness/v3/coordinator/events";
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
    pub append_receipt_fault: std::sync::atomic::AtomicU8,
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
    pub history_read_fault: std::sync::atomic::AtomicU8,
    pub message_head_race: std::sync::Mutex<Option<acyclic_stream::CommitRequest>>,
    pub aggregate_commit_fault: std::sync::atomic::AtomicU8,
    pub aggregate_commits: std::sync::atomic::AtomicUsize,
    pub hide_aggregate_read: std::sync::atomic::AtomicBool,
}
impl<P> LostSessionAck<P> {
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            lose_ack: Default::default(),
            append_receipt_fault: Default::default(),
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
            history_read_fault: Default::default(),
            message_head_race: Default::default(),
            aggregate_commit_fault: Default::default(),
            aggregate_commits: Default::default(),
            hide_aggregate_read: Default::default(),
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
            .starts_with("harness/v3/coordinator/intent-locations/");
        let fault = if location {
            self.location_fault
                .swap(0, std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if fault == 1 {
            return Err(StreamError::Unavailable);
        }
        let mut outcome = self.inner.append(request).await?;
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
        if let AppendOutcome::Committed(receipt) = &mut outcome {
            match self
                .append_receipt_fault
                .swap(0, std::sync::atomic::Ordering::SeqCst)
            {
                1 => receipt.start = receipt.end,
                2 => receipt.end = receipt.start,
                3 => receipt.tail = receipt.start,
                _ => {}
            }
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
            .starts_with("harness/v3/coordinator/intent-locations/")
            && self
                .hide_location_read
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        if request
            .path
            .as_str()
            .starts_with("harness/v2/aggregate-operations/")
            && self
                .hide_aggregate_read
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        let head_race = if request
            .path
            .as_str()
            .starts_with("harness/v2/conversation-heads/")
        {
            self.message_head_race
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take()
        } else {
            None
        };
        if let Some(commit) = head_race {
            self.inner.commit(commit).await?;
        }
        let fault = self
            .history_read_fault
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        if fault == 0 {
            return self.inner.read(request).await;
        }
        use futures::TryStreamExt as _;
        let mut records = self
            .inner
            .read(request)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        match fault {
            1 => {
                if let Some(record) = records.first_mut() {
                    record.sequence += 1;
                }
            }
            2 => {
                if let Some(record) = records.last().cloned() {
                    records.push(record);
                }
            }
            3 => records.clear(),
            4 => {
                if let Some(record) = records.first_mut() {
                    record.value = Bytes::from_static(b"corrupt");
                }
            }
            _ => {}
        }
        Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))))
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
        let aggregate = request.mutations.iter().any(|mutation| {
            matches!(mutation,
            acyclic_stream::CommitMutation::Append { path, .. }
            if path.as_str().starts_with("harness/v2/aggregate-operations/"))
        });
        let aggregate_fault = if aggregate {
            self.aggregate_commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.aggregate_commit_fault
                .swap(0, std::sync::atomic::Ordering::SeqCst)
        } else {
            0
        };
        if aggregate_fault == 1 {
            return Err(StreamError::Unavailable);
        }
        let mut outcome = self.inner.commit(request).await?;
        if aggregate_fault == 3 {
            self.hide_aggregate_read
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if matches!(aggregate_fault, 2 | 3) {
            return Err(StreamError::Unavailable);
        }
        corrupt_aggregate_receipt(&mut outcome, aggregate_fault);
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

fn corrupt_aggregate_receipt(outcome: &mut acyclic_stream::CommitOutcome, aggregate_fault: u8) {
    if let acyclic_stream::CommitOutcome::Committed(envelope) = outcome {
        if aggregate_fault == 4 {
            envelope.mutations.retain(|mutation| {
                !matches!(mutation,
                    acyclic_stream::CommittedMutation::Append(append)
                    if append.path.as_str().starts_with("harness/v2/aggregate-operations/"))
            });
        } else if aggregate_fault == 5 {
            for mutation in &mut envelope.mutations {
                if let acyclic_stream::CommittedMutation::Append(append) = mutation
                    && append
                        .path
                        .as_str()
                        .starts_with("harness/v2/aggregate-operations/")
                    && let Some(record) = append.records.first_mut()
                {
                    record.value = Bytes::from_static(b"contradictory receipt");
                }
            }
        }
    }
}
