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
pub struct StreamIo {
    pub reads: std::sync::atomic::AtomicU64,
    pub writes: std::sync::atomic::AtomicU64,
    pub records: std::sync::atomic::AtomicU64,
    pub write_bytes: std::sync::atomic::AtomicU64,
    pub inspections: std::sync::atomic::AtomicU64,
    pub commit_reads: std::sync::atomic::AtomicU64,
    pub receipt_bytes: std::sync::atomic::AtomicU64,
    pub other_calls: std::sync::atomic::AtomicU64,
}
#[derive(Debug)]
pub struct StreamIoSnapshot {
    pub reads: u64,
    pub tails: usize,
    pub read_bytes: u64,
    pub writes: u64,
    pub records: u64,
    pub write_bytes: u64,
    pub inspections: u64,
    pub commit_reads: u64,
    pub receipt_bytes: u64,
    pub other_calls: u64,
}
#[derive(Default)]
pub struct LostSessionAck<P = MemoryStream> {
    pub inner: P,
    pub io: StreamIo,
    pub root_records: std::sync::Mutex<std::collections::BTreeMap<(String, u64), u64>>,
    pub lose_ack: std::sync::atomic::AtomicBool,
    pub append_receipt_fault: std::sync::atomic::AtomicU8,
    pub hide_receipt: std::sync::atomic::AtomicBool,
    pub location_fault: std::sync::atomic::AtomicU8,
    pub hide_location_read: std::sync::atomic::AtomicBool,
    pub forbid_writes: std::sync::atomic::AtomicBool,
    pub observation_reads: std::sync::atomic::AtomicUsize,
    pub observation_bytes: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub observation_tails: std::sync::atomic::AtomicUsize,
    pub observation_maximum: std::sync::atomic::AtomicU32,
    pub observation_writes: std::sync::atomic::AtomicUsize,
    pub execution_race: std::sync::atomic::AtomicBool,
    pub commit_lose_ack: std::sync::atomic::AtomicBool,
    pub execution_fault: std::sync::atomic::AtomicU8,
    pub execution_tail: std::sync::atomic::AtomicU64,
    pub execution_faults: std::sync::atomic::AtomicUsize,
    pub hide_effect_node_read: std::sync::atomic::AtomicBool,
    pub hide_effect_transition_position: std::sync::atomic::AtomicU64,
    pub history_read_fault: std::sync::atomic::AtomicU8,
    pub message_head_race: std::sync::Mutex<Option<acyclic_stream::CommitRequest>>,
    pub aggregate_commit_fault: std::sync::atomic::AtomicU8,
    pub aggregate_commits: std::sync::atomic::AtomicUsize,
    pub stale_projection_head_tail: std::sync::atomic::AtomicU64,
    pub hide_aggregate_read: std::sync::atomic::AtomicBool,
}
impl<P> LostSessionAck<P> {
    pub fn reset_io(&self) {
        use std::sync::atomic::Ordering;
        for counter in [
            &self.io.reads,
            &self.io.writes,
            &self.io.records,
            &self.io.write_bytes,
            &self.io.inspections,
            &self.io.commit_reads,
            &self.io.receipt_bytes,
            &self.io.other_calls,
        ] {
            counter.store(0, Ordering::SeqCst);
        }
        self.observation_bytes.store(0, Ordering::SeqCst);
        self.observation_tails.store(0, Ordering::SeqCst);
    }
    pub fn io_snapshot(&self) -> StreamIoSnapshot {
        use std::sync::atomic::Ordering;
        StreamIoSnapshot {
            reads: self.io.reads.load(Ordering::SeqCst),
            tails: self.observation_tails.load(Ordering::SeqCst),
            read_bytes: self.observation_bytes.load(Ordering::SeqCst),
            writes: self.io.writes.load(Ordering::SeqCst),
            records: self.io.records.load(Ordering::SeqCst),
            write_bytes: self.io.write_bytes.load(Ordering::SeqCst),
            inspections: self.io.inspections.load(Ordering::SeqCst),
            commit_reads: self.io.commit_reads.load(Ordering::SeqCst),
            receipt_bytes: self.io.receipt_bytes.load(Ordering::SeqCst),
            other_calls: self.io.other_calls.load(Ordering::SeqCst),
        }
    }
    fn measure_commit_request(&self, request: &acyclic_stream::CommitRequest) {
        self.io
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        for mutation in &request.mutations {
            if let acyclic_stream::CommitMutation::Append { records, .. } = mutation {
                self.measure_write(records);
            }
        }
    }
    fn measure_committed(
        &self,
        outcome: &acyclic_stream::CommitOutcome,
    ) -> std::result::Result<(), StreamError> {
        if let acyclic_stream::CommitOutcome::Committed(envelope) = outcome {
            self.measure_received(envelope);
            let mut retained = self
                .root_records
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            for mutation in &envelope.mutations {
                if let acyclic_stream::CommittedMutation::Append(append) = mutation
                    && append.path.as_str().contains("/effect-heads/")
                {
                    for record in &append.records {
                        retained
                            .entry((append.path.as_str().to_owned(), record.sequence))
                            .or_insert(record.value.len() as u64);
                    }
                }
            }
        }

        Ok(())
    }
    fn measure_received(&self, envelope: &acyclic_stream::CommittedEnvelope) {
        let bytes = envelope
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                acyclic_stream::CommittedMutation::Append(append) => Some(
                    append
                        .records
                        .iter()
                        .map(|record| record.value.len() as u64)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum();
        self.io
            .receipt_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    }
    fn measure_write(&self, records: &[Bytes]) {
        use std::sync::atomic::Ordering;
        self.io
            .records
            .fetch_add(records.len() as u64, Ordering::SeqCst);
        self.io.write_bytes.fetch_add(
            records.iter().map(|record| record.len() as u64).sum(),
            Ordering::SeqCst,
        );
    }

    pub fn new(inner: P) -> Self {
        Self {
            inner,
            io: Default::default(),
            root_records: Default::default(),
            lose_ack: Default::default(),
            append_receipt_fault: Default::default(),
            hide_receipt: Default::default(),
            location_fault: Default::default(),
            hide_location_read: Default::default(),
            forbid_writes: Default::default(),
            observation_reads: Default::default(),
            observation_bytes: Default::default(),
            observation_tails: Default::default(),
            observation_maximum: Default::default(),
            observation_writes: Default::default(),
            execution_race: Default::default(),
            commit_lose_ack: Default::default(),
            execution_fault: Default::default(),
            execution_tail: Default::default(),
            execution_faults: Default::default(),
            hide_effect_node_read: Default::default(),
            hide_effect_transition_position: Default::default(),
            history_read_fault: Default::default(),
            message_head_race: Default::default(),
            aggregate_commit_fault: Default::default(),
            aggregate_commits: Default::default(),
            stale_projection_head_tail: Default::default(),
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
        self.io
            .inspections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .hide_receipt
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(StreamError::Unavailable);
        }
        let observation = self.inner.inspect_idempotency(key).await?;
        if let Some(observation) = &observation
            && let acyclic_stream::IdempotencyOutcome::Commit(
                acyclic_stream::CommitOutcome::Committed(envelope),
            ) = &observation.outcome
        {
            self.measure_received(envelope);
        }
        Ok(observation)
    }
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<u64, StreamError> {
        self.observation_tails
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if path
            .as_str()
            .starts_with("harness/v3/projection-checkpoints/")
        {
            let stale = self
                .stale_projection_head_tail
                .swap(0, std::sync::atomic::Ordering::SeqCst);
            if stale != 0 {
                return Ok(stale - 1);
            }
        }
        self.inner.tail(path).await
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        self.io
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.measure_write(&request.records);
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
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        self.io
            .reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
        if (request.path.as_str().contains("/effect-heads/")
            && request.path.as_str().contains("/nodes/")
            && self
                .hide_effect_node_read
                .load(std::sync::atomic::Ordering::SeqCst))
            || (request.path.as_str().contains("/effect-transitions/")
                && self
                    .hide_effect_transition_position
                    .load(std::sync::atomic::Ordering::SeqCst)
                    == request.from.saturating_add(1))
        {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let fault = self
            .history_read_fault
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        use futures::TryStreamExt as _;
        if fault == 0 {
            let delivered = self.observation_bytes.clone();
            return Ok(Box::pin(self.inner.read(request).await?.inspect_ok(
                move |record| {
                    delivered.fetch_add(
                        record.value.len() as u64,
                        std::sync::atomic::Ordering::SeqCst,
                    );
                },
            )));
        }
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
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, StreamError> {
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children_page(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        self.measure_commit_request(&request);
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
        self.measure_committed(&outcome)?;
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
        self.io
            .commit_reads
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let envelope = self.inner.read_commit(id).await?;
        self.measure_received(&envelope);
        Ok(envelope)
    }
    async fn commit_before(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline_unix_millis: u64,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
