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
    pub native_only: std::sync::atomic::AtomicBool,
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
    #[cfg(feature = "filesystem")]
    pub history_cancel_after_tail: std::sync::Mutex<Option<acyclic_fs::CancellationToken>>,
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
            native_only: Default::default(),
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
            #[cfg(feature = "filesystem")]
            history_cancel_after_tail: Default::default(),
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary inspection on native-only fixture");
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary tail on native-only fixture");
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
        let result = self.inner.tail(path).await;
        #[cfg(feature = "filesystem")]
        if let Some(cancellation) = self.history_cancel_after_tail
            .lock().map_err(|_| StreamError::Unavailable)?.take()
        {
            cancellation.cancel();
        }
        result
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary bounds on native-only fixture");
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary append on native-only fixture");
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary fork on native-only fixture");
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary read on native-only fixture");
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
            5 => {
                if let Some(record) = records.first_mut() {
                    record.value = Bytes::from(vec![0; acyclic_stream::MAX_RECORD_BYTES + 1]);
                }
            }
            6 => {
                return Ok(Box::pin(futures::stream::iter(
                    records.into_iter().take(1).map(Ok)
                        .chain(std::iter::once(Err(StreamError::Unavailable))),
                )));
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary follow on native-only fixture");
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary children on native-only fixture");
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, StreamError> {
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary children page on native-only fixture");
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children_page(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary commit on native-only fixture");
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary envelope read on native-only fixture");
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
        assert!(!self.native_only.load(std::sync::atomic::Ordering::SeqCst), "ordinary timed commit on native-only fixture");
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner
            .commit_before(request, deadline_unix_millis)
            .await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_inspect_idempotency(
        &self, key: acyclic_stream::IdempotencyKey,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, Option<acyclic_stream::IdempotencyObservation>> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.io.inspections.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.hide_receipt.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            let result = self.inner.native_inspect_idempotency(key, original).await?;
            if let Ok(output) = &result
                && let Some(observation) = output.value()
                && let acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(envelope),
                ) = &observation.outcome
            { self.measure_received(envelope); }
            Ok(result)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_tail(
        &self, path: acyclic_stream::StreamPath,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, u64> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.observation_tails.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if path.as_str().starts_with("harness/v3/projection-checkpoints/") {
                let stale = self.stale_projection_head_tail.swap(0, std::sync::atomic::Ordering::SeqCst);
                if stale != 0 {
                    return Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                        stale - 1, acyclic_native_runtime::ReadAllocation::empty(&original),
                    )));
                }
            }
            let result = self.inner.native_tail(path, original).await;
            if let Some(cancellation) = self.history_cancel_after_tail.lock()
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?.take()
            { cancellation.cancel(); }
            result
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_bounds(
        &self, path: acyclic_stream::StreamPath,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::StreamBounds> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.io.other_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.native_bounds(path, original).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_read(
        &self, request: acyclic_stream::ReadRequest,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::NativeRecordStream> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            use std::sync::atomic::Ordering;
            self.io.reads.fetch_add(1, Ordering::SeqCst);
            if self.forbid_writes.load(Ordering::SeqCst) {
                self.observation_reads.fetch_add(1, Ordering::SeqCst);
                self.observation_maximum.fetch_max(request.limit, Ordering::SeqCst);
            }
            if (request.path.as_str().starts_with("harness/v3/coordinator/intent-locations/")
                && self.hide_location_read.swap(false, Ordering::SeqCst))
                || (request.path.as_str().starts_with("harness/v2/aggregate-operations/")
                    && self.hide_aggregate_read.swap(false, Ordering::SeqCst))
            { return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original))); }
            let head_race = if request.path.as_str().starts_with("harness/v2/conversation-heads/") {
                self.message_head_race.lock()
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?.take()
            } else { None };
            if let Some(commit) = head_race {
                // This is the separate adversarial writer, not harness IO.
                if let Err(error) = self.inner.commit(commit).await {
                    return Ok(Err(native_fixture_failure(error, &original)));
                }
            }
            let hidden = (request.path.as_str().contains("/effect-heads/")
                && request.path.as_str().contains("/nodes/")
                && self.hide_effect_node_read.load(Ordering::SeqCst))
                || (request.path.as_str().contains("/effect-transitions/")
                    && self.hide_effect_transition_position.load(Ordering::SeqCst)
                        == request.from.saturating_add(1));
            let fault = if hidden { 3 } else { self.history_read_fault.swap(0, Ordering::SeqCst) };
            let source = match self.inner.native_read(request, original.clone()).await? {
                Ok(source) => source,
                Err(error) => return Ok(Err(error)),
            };
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &original, std::mem::size_of::<NativeFaultRecordCursor>(),
            )?;
            let cursor = NativeFaultRecordCursor {
                source, meter: original, delivered: self.observation_bytes.clone(),
                fault, index: 0, previous: None, finished: false,
            };
            Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                Box::pin(cursor) as acyclic_stream::NativeRecordStream, allocation,
            )))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_append(
        &self, request: acyclic_stream::AppendRequest,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, AppendOutcome> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            use std::sync::atomic::Ordering;
            self.io.writes.fetch_add(1, Ordering::SeqCst);
            self.measure_write(&request.records);
            if self.forbid_writes.load(Ordering::SeqCst) {
                self.observation_writes.fetch_add(1, Ordering::SeqCst);
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            if request.path.as_str() == COORDINATOR_PATH
                && self.location_fault.compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok()
            { return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original))); }
            let location = request.path.as_str().starts_with("harness/v3/coordinator/intent-locations/");
            let fault = if location { self.location_fault.swap(0, Ordering::SeqCst) } else { 0 };
            if fault == 1 { return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original))); }
            let mut output = match self.inner.native_append(request, original.clone()).await? {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if fault >= 2 {
                if fault == 3 { self.hide_location_read.store(true, Ordering::SeqCst); }
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            if matches!(output.value(), AppendOutcome::Committed(_))
                && self.lose_ack.swap(false, Ordering::SeqCst)
            { return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original))); }
            if let AppendOutcome::Committed(receipt) = output.value_mut() {
                match self.append_receipt_fault.swap(0, Ordering::SeqCst) {
                    1 => receipt.start = receipt.end,
                    2 => receipt.end = receipt.start,
                    3 => receipt.tail = receipt.start,
                    _ => {}
                }
            }
            Ok(Ok(output))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_commit(
        &self, request: acyclic_stream::CommitRequest,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::CommitOutcome> {
        self.native_commit_with_deadline(request, None, meter)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_commit_before(
        &self, request: acyclic_stream::CommitRequest, deadline_unix_millis: u64,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::CommitOutcome> {
        self.native_commit_with_deadline(request, Some(deadline_unix_millis), meter)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_read_commit(
        &self, id: acyclic_stream::CommitId,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::CommittedEnvelope> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.io.commit_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let result = self.inner.native_read_commit(id, original).await?;
            if let Ok(output) = &result { self.measure_received(output.value()); }
            Ok(result)
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeFaultRecordCursor {
    source: acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>,
    meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    delivered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    fault: u8,
    index: u64,
    previous: Option<acyclic_stream::Record>,
    finished: bool,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeFixtureBytes {
    bytes: Bytes,
    _producer: acyclic_stream::NativeStreamOutput<()>,
    _owner: acyclic_native_runtime::ReadAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl AsRef<[u8]> for NativeFixtureBytes {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_fixture_failure(
    source: StreamError,
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> acyclic_stream::NativeStreamFailure {
    acyclic_stream::NativeStreamFailure::new(
        source, acyclic_native_runtime::ReadAllocation::empty(meter),
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_fixture_record_owner(
    output: acyclic_stream::NativeObservedRecord,
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<acyclic_stream::NativeObservedRecord> {
    let (record, allocation) = output.into_parts();
    if allocation.bytes() == 0 {
        return Ok(acyclic_stream::NativeStreamOutput::new(record, allocation));
    }
    let output = acyclic_stream::NativeStreamOutput::new(record, allocation);
    let owner = acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<NativeFixtureBytes>())?;
    meter.allocate_refcount_header()?;
    let (mut record, allocation) = output.into_parts();
    record.value = Bytes::from_owner(NativeFixtureBytes {
        bytes: record.value,
        _producer: acyclic_stream::NativeStreamOutput::new((), allocation),
        _owner: owner,
    });
    Ok(acyclic_stream::NativeStreamOutput::new(record, acyclic_native_runtime::ReadAllocation::empty(meter)))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl futures::Stream for NativeFaultRecordCursor {
    type Item = std::io::Result<std::result::Result<acyclic_stream::NativeObservedRecord, acyclic_stream::NativeStreamFailure>>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.finished || this.fault == 3 { return Poll::Ready(None); }
        if this.fault == 6 && this.index != 0 {
            this.finished = true;
            return Poll::Ready(Some(Ok(Err(native_fixture_failure(StreamError::Unavailable, &this.meter)))));
        }
        let mut output = match this.source.value_mut().as_mut().poll_next(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(Ok(output)))) => output,
            Poll::Ready(Some(error)) => return Poll::Ready(Some(error)),
            Poll::Ready(None) => {
                this.finished = true;
                return Poll::Ready(if this.fault == 2 {
                    this.previous.take().map(|record| Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                        record, acyclic_native_runtime::ReadAllocation::empty(&this.meter),
                    ))))
                } else { None });
            }
        };
        if this.index == 0 {
            match this.fault {
                1 => output.value_mut().sequence += 1,
                4 => output.value_mut().value = Bytes::from_static(b"corrupt"),
                5 => {
                    let bytes = acyclic_stream::MAX_RECORD_BYTES + 1;
                    let allocation = match acyclic_native_runtime::ReadAllocation::new(&this.meter, bytes) {
                        Ok(allocation) => allocation,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    if let Err(error) = this.meter.allocate_refcount_header() {
                        return Poll::Ready(Some(Err(error)));
                    }
                    let mut record = output.value().clone();
                    record.value = Bytes::from(vec![0; bytes]);
                    drop(output);
                    output = acyclic_stream::NativeStreamOutput::new(record, allocation);
                }
                _ => {}
            }
        }
        if this.fault == 2 {
            output = match native_fixture_record_owner(output, &this.meter) {
                Ok(output) => output,
                Err(error) => return Poll::Ready(Some(Err(error))),
            };
            this.previous = Some(output.value().clone());
        }
        this.index += 1;
        this.delivered.fetch_add(output.value().value.len() as u64, std::sync::atomic::Ordering::SeqCst);
        Poll::Ready(Some(Ok(Ok(output))))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl<P: StreamProvider> LostSessionAck<P> {
    fn native_commit_with_deadline(
        &self, request: acyclic_stream::CommitRequest, deadline: Option<u64>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::CommitOutcome> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            use std::sync::atomic::Ordering;
            self.measure_commit_request(&request);
            if self.forbid_writes.load(Ordering::SeqCst) {
                self.observation_writes.fetch_add(1, Ordering::SeqCst);
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            if self.execution_race.swap(false, Ordering::SeqCst) {
                let path = request.conditions.iter().find_map(|condition| match condition {
                    acyclic_stream::CommitCondition::Absent { path }
                        if path.as_str().starts_with("harness/v2/execution/") => Some((path.clone(), 0)),
                    acyclic_stream::CommitCondition::Tail { path, expected }
                        if path.as_str().starts_with("harness/v2/execution/") => Some((path.clone(), *expected)),
                    _ => None,
                });
                let Some((path, tail)) = path else {
                    return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
                };
                // Separate competing writer: it never borrows the harness meter.
                if let Err(error) = self.inner.append(acyclic_stream::AppendRequest {
                    path, records: vec![Bytes::from_static(b"dispatch won")], if_tail: Some(tail), idempotency_key: None,
                }).await { return Ok(Err(native_fixture_failure(error, &original))); }
            }
            let selected = request.conditions.iter().any(|condition| matches!(condition,
                acyclic_stream::CommitCondition::Tail { path, expected }
                    if path.as_str().starts_with("harness/v2/execution/")
                        && *expected == self.execution_tail.load(Ordering::SeqCst)));
            let fault = if selected { self.execution_fault.swap(0, Ordering::SeqCst) } else { 0 };
            if fault != 0 { self.execution_faults.fetch_add(1, Ordering::SeqCst); }
            if fault == ExecutionFaultMode::Before as u8 {
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            let aggregate = request.mutations.iter().any(|mutation| matches!(mutation,
                acyclic_stream::CommitMutation::Append { path, .. }
                    if path.as_str().starts_with("harness/v2/aggregate-operations/")));
            let aggregate_fault = if aggregate {
                self.aggregate_commits.fetch_add(1, Ordering::SeqCst);
                self.aggregate_commit_fault.swap(0, Ordering::SeqCst)
            } else { 0 };
            if aggregate_fault == 1 {
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            let result = match deadline {
                Some(deadline) => self.inner.native_commit_before(request, deadline, original.clone()).await?,
                None => self.inner.native_commit(request, original.clone()).await?,
            };
            let mut output = match result {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if let Err(error) = self.measure_committed(output.value()) {
                return Ok(Err(native_fixture_failure(error, &original)));
            }
            if aggregate_fault == 3 { self.hide_aggregate_read.store(true, Ordering::SeqCst); }
            if matches!(aggregate_fault, 2 | 3) {
                return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original)));
            }
            corrupt_aggregate_receipt(output.value_mut(), aggregate_fault);
            if fault == ExecutionFaultMode::AfterHidden as u8 { self.hide_receipt.store(true, Ordering::SeqCst); }
            if fault != 0
                || self.commit_lose_ack.swap(false, Ordering::SeqCst)
                || self.lose_ack.swap(false, Ordering::SeqCst)
            { return Ok(Err(native_fixture_failure(StreamError::Unavailable, &original))); }
            Ok(Ok(output))
        })
    }
}

/// Ordinary-capable negative control: it deliberately inherits every real
/// SDK native capability default, rather than simulating unsupported results.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Default)]
pub struct NoNative {
    inner: MemoryStream,
    pub ordinary_calls: std::sync::atomic::AtomicU64,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[async_trait::async_trait]
impl StreamProvider for NoNative {
    async fn inspect_idempotency(&self, key: acyclic_stream::IdempotencyKey)
        -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: acyclic_stream::StreamPath) -> std::result::Result<u64, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: acyclic_stream::StreamPath)
        -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.bounds(path).await
    }
    async fn append(&self, request: acyclic_stream::AppendRequest) -> std::result::Result<AppendOutcome, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.append(request).await
    }
    async fn fork(&self, request: acyclic_stream::ForkRequest)
        -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.fork(request).await
    }
    async fn read(&self, request: acyclic_stream::ReadRequest)
        -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read(request).await
    }
    async fn follow(&self, path: acyclic_stream::StreamPath, from: u64)
        -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.follow(path, from).await
    }
    async fn children(&self, request: acyclic_stream::ChildrenRequest)
        -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children(request).await
    }
    async fn commit(&self, request: acyclic_stream::CommitRequest)
        -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.commit(request).await
    }
    async fn read_commit(&self, id: acyclic_stream::CommitId)
        -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.ordinary_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.read_commit(id).await
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
