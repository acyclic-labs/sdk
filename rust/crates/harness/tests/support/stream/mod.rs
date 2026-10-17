//! Shared Stream fault fixture; all successful operations use the real provider.
#![allow(
    dead_code,
    reason = "shared fault fixtures expose controls used by different test modules"
)]
use acyclic_stream::{AppendOutcome, MemoryStream, OwnedBytes, StreamError, StreamProvider};
mod controls;
mod hints;
use controls::COORDINATOR_PATH;
pub use controls::{ExecutionFaultMode, LostSessionAck};
mod append;
mod commit;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use native::NativeFaultRecordCursor;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> StreamProvider for LostSessionAck<P> {
    async fn durable_receipt(
        &self,
        path: acyclic_stream::StreamPath,
        through: u64,
    ) -> std::result::Result<Option<acyclic_stream::DurableReceipt>, acyclic_stream::StreamError> {
        self.inner.durable_receipt(path, through).await
    }
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary inspection on native-only fixture"
        );
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
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary tail on native-only fixture"
        );
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
        if let Some(cancellation) = self
            .history_cancel_after_tail
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .take()
        {
            cancellation.cancel();
        }
        result
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> std::result::Result<acyclic_stream::StreamBounds, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary bounds on native-only fixture"
        );
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary append on native-only fixture"
        );
        let fault = self.before_append(&request)?;
        let mut outcome = self.inner.append(request).await?;
        self.after_append(&mut outcome, fault)?;
        Ok(outcome)
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary fork on native-only fixture"
        );
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
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary read on native-only fixture"
        );
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
            7..=9 => return Ok(hints::records(records, fault)),
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
                    record.value = OwnedBytes::from_static(b"corrupt");
                }
            }
            5 => {
                if let Some(record) = records.first_mut() {
                    record.value = OwnedBytes::from(vec![0; acyclic_stream::MAX_RECORD_BYTES + 1]);
                }
            }
            6 => {
                return Ok(Box::pin(futures::stream::iter(
                    records
                        .into_iter()
                        .take(1)
                        .map(Ok)
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
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary follow on native-only fixture"
        );
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> std::result::Result<acyclic_stream::ChildStream, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary children on native-only fixture"
        );
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children(request).await
    }
    async fn children_page(
        &self,
        request: acyclic_stream::ChildrenPageRequest,
    ) -> std::result::Result<acyclic_stream::ChildrenPage, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary children page on native-only fixture"
        );
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.children_page(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> std::result::Result<acyclic_stream::CommitOutcome, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary commit on native-only fixture"
        );
        let (fault, aggregate_fault) = self.before_commit(&request).await?;
        let mut outcome = self.inner.commit(request).await?;
        self.after_commit(&mut outcome, fault, aggregate_fault)?;
        Ok(outcome)
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary envelope read on native-only fixture"
        );
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
        assert!(
            !self.native_only.load(std::sync::atomic::Ordering::SeqCst),
            "ordinary timed commit on native-only fixture"
        );
        self.io
            .other_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner
            .commit_before(request, deadline_unix_millis)
            .await
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, Option<acyclic_stream::IdempotencyObservation>>
    {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.io
                .inspections
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self
                .hide_receipt
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Ok(Err(StreamError::Unavailable));
            }
            let result = self.inner.native_inspect_idempotency(key, original).await?;
            if let Ok(output) = &result
                && let Some(observation) = output.value()
                && let acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(envelope),
                ) = &observation.outcome
            {
                self.measure_received(envelope);
            }
            Ok(result)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_tail(
        &self,
        path: acyclic_stream::StreamPath,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, u64> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
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
                    return Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                        stale - 1,
                        acyclic_native_runtime::ReadAllocation::empty(&original),
                    )));
                }
            }
            let result = self.inner.native_tail(path, original).await;
            if let Some(cancellation) = self
                .history_cancel_after_tail
                .lock()
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?
                .take()
            {
                cancellation.cancel();
            }
            result
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_bounds(
        &self,
        path: acyclic_stream::StreamPath,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::StreamBounds> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            self.io
                .other_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.native_bounds(path, original).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_read(
        &self,
        request: acyclic_stream::ReadRequest,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::NativeRecordStream> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            use std::sync::atomic::Ordering;
            self.io.reads.fetch_add(1, Ordering::SeqCst);
            if self.forbid_writes.load(Ordering::SeqCst) {
                self.observation_reads.fetch_add(1, Ordering::SeqCst);
                self.observation_maximum
                    .fetch_max(request.limit, Ordering::SeqCst);
            }
            if (request
                .path
                .as_str()
                .starts_with("harness/v3/coordinator/intent-locations/")
                && self.hide_location_read.swap(false, Ordering::SeqCst))
                || (request
                    .path
                    .as_str()
                    .starts_with("harness/v2/aggregate-operations/")
                    && self.hide_aggregate_read.swap(false, Ordering::SeqCst))
            {
                return Ok(Err(StreamError::Unavailable));
            }
            let head_race = if request
                .path
                .as_str()
                .starts_with("harness/v2/conversation-heads/")
            {
                self.message_head_race
                    .lock()
                    .map_err(|_| std::io::Error::from(std::io::ErrorKind::Other))?
                    .take()
            } else {
                None
            };
            if let Some(commit) = head_race
                && let Err(error) = self.inner.commit(commit).await
            {
                return Ok(Err(error));
            }
            let hidden = (request.path.as_str().contains("/effect-heads/")
                && request.path.as_str().contains("/nodes/")
                && self.hide_effect_node_read.load(Ordering::SeqCst))
                || (request.path.as_str().contains("/effect-transitions/")
                    && self.hide_effect_transition_position.load(Ordering::SeqCst)
                        == request.from.saturating_add(1));
            let fault = if hidden {
                3
            } else {
                self.history_read_fault.swap(0, Ordering::SeqCst)
            };
            let source = match self.inner.native_read(request, original.clone()).await? {
                Ok(source) => source,
                Err(error) => return Ok(Err(error)),
            };
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &original,
                std::mem::size_of::<NativeFaultRecordCursor>(),
            )?;
            let cursor = NativeFaultRecordCursor {
                source,
                meter: original,
                delivered: self.observation_bytes.clone(),
                fault,
                index: 0,
                previous: None,
            };
            Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                Box::pin(cursor) as acyclic_stream::NativeRecordStream,
                allocation,
            )))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_append(
        &self,
        request: acyclic_stream::AppendRequest,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, AppendOutcome> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            let fault = match self.before_append(&request) {
                Ok(fault) => fault,
                Err(error) => return Ok(Err(error)),
            };
            let mut output = match self.inner.native_append(request, original).await? {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if let Err(error) = self.after_append(output.value_mut(), fault) {
                return Ok(Err(error));
            }
            Ok(Ok(output))
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_commit(
        &self,
        request: acyclic_stream::CommitRequest,
        deadline: Option<u64>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> acyclic_stream::NativeProviderFuture<'_, acyclic_stream::CommitOutcome> {
        let original = meter.clone();
        acyclic_stream::NativeProviderFuture::new(&meter, async move {
            let (fault, aggregate_fault) = match self.before_commit(&request).await {
                Ok(faults) => faults,
                Err(error) => return Ok(Err(error)),
            };
            let result = self
                .inner
                .native_commit(request, deadline, original.clone())
                .await?;
            let mut output = match result {
                Ok(output) => output,
                Err(error) => return Ok(Err(error)),
            };
            if let Err(error) = self.after_commit(output.value_mut(), fault, aggregate_fault) {
                return Ok(Err(error));
            }
            Ok(Ok(output))
        })
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
                    record.value = OwnedBytes::from_static(b"contradictory receipt");
                }
            }
        }
    }
}
