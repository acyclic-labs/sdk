//! A constant-size authenticated cut into immutable execution-index pages.
use super::*;
use persistent_index::NodeRef;

pub(super) struct IndexPath<'a> {
    path: acyclic_stream::StreamPath,
    _hold: Option<crate::filesystem::fork_preparer_io::DecodeReservation<'a>>,
}

impl std::ops::Deref for IndexPath<'_> {
    type Target = acyclic_stream::StreamPath;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

#[derive(Clone, Copy)]
pub(super) struct IndexState {
    pub(super) operation: OperationId,
    pub(super) turn: OperationId,
    pub(super) volume: [u8; 32],
    pub(super) tail: u64,
    pub(super) nodes_tail: u64,
    pub(super) roots: [Option<NodeRef>; 5],
    pub(super) pending_dispatches: u64,
    pub(super) anchor: [u8; 32],
    pub(super) latest_response: Option<u32>,
    pub(super) model_dispatched: bool,
}

#[derive(Clone, Copy)]
pub(super) struct Recovery {
    pub(super) state: IndexState,
    pub(super) head_tail: u64,
    pub(super) indexed: bool,
}

impl ExecutionSummary {
    pub(super) fn install_checkpoint(&mut self, state: IndexState, head_tail: u64) {
        // The signature and atomic anchor authenticate this prefix even when
        // later raw records exist. Validate that tail against the recovered
        // retry/dispatch roots instead of replaying the authenticated prefix.
        self.tail = state.tail;
        self.pending_dispatches = state.pending_dispatches;
        self.latest_response = state.latest_response;
        self.model_dispatched = state.model_dispatched;
        self.partial = true;
        self.recovery = Some(Recovery {
            state,
            head_tail,
            indexed: true,
        });
    }
}

#[derive(Clone, Copy)]
struct Checkpoint {
    state: IndexState,
    proof: [u8; 64],
}

mod codec;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn index_path<'a>(
        &self,
        operation: OperationId,
        class: &str,
        io: Option<&'a PreparationIo>,
    ) -> Result<IndexPath<'a>> {
        let name = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!("harness/v1/execution-index/{operation}/{class}"),
                work,
            )
        })?;
        let scratch_hold = io
            .map(|io| io.guard_owned(name.capacity() as u64))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let path = acyclic_stream::StreamPath::new_native(&name, &work.runtime_meter())
                .map_err(|error| Error::from(work.runtime_error(error)))??;
            return Ok(IndexPath { path, _hold: None });
        }
        let hold = io
            .map(|io| io.reserve_decode(name.len() as u64))
            .transpose()?;
        if let Some(io) = io {
            io.allocation_attempt(name.len() as u64)?;
            io.charge(acyclic_fs::WorkCounters {
                bytes_copied: name.len() as u64,
                ..Default::default()
            })?;
        }
        let path = acyclic_stream::StreamPath::new(&name)?;
        drop(name);
        drop(scratch_hold);
        Ok(IndexPath { path, _hold: hold })
    }

    pub(super) fn index_volume_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &self.volume,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })
    }

    fn verify_checkpoint(
        &self,
        operation: OperationId,
        checkpoint: &Checkpoint,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let state = checkpoint.state;
        if state.operation != operation
            || state.turn != self.turn_operation(operation)
            || state.volume != self.index_volume_digest(io)?
            || state.tail == 0
            || state.roots[4].is_none()
            || state
                .roots
                .iter()
                .flatten()
                .any(|root| root.sequence >= state.nodes_tail)
        {
            return Err(missing(io));
        }
        crate::executor::with_history_io(io, |work| {
            let digest = crate::model::hash_bytes(&codec::state_bytes(&checkpoint.state, io)?, io)?;
            self.verifier
                .verify_execution_checkpoint(digest, &checkpoint.proof, work)
        })
    }

    #[cfg(test)]
    pub(super) async fn execution_tail(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<u64> {
        let hold;
        let path = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(operation, work)
        })?;
        hold = io
            .map(|io| io.guard_owned(path.as_str().len() as u64))
            .transpose()?;
        let mut history = io.map(PreparationIo::history_io);
        let result = crate::store::operations::tail_optional_with_work(
            &self.stream,
            &path,
            &mut history
                .as_mut()
                .map(|io| io as &mut dyn crate::store::HistoryIo),
        )
        .await
        .map(|tail| tail.unwrap_or(0));
        drop(path);
        drop(hold);
        result
    }

    pub(super) async fn authenticated_metadata(
        &self,
        operation: OperationId,
        sequence: u64,
        retry: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
        page_root: &mut Option<Option<IndexState>>,
    ) -> Result<bool> {
        let cache = self.verified.lock().await;
        if cache.authenticates(operation, sequence, retry, event, io)? {
            return Ok(true);
        }
        let cached = cache
            .summary()
            .recovery
            .filter(|recovery| {
                recovery.indexed
                    && recovery.state.operation == operation
                    && sequence <= recovery.state.tail
            })
            .map(|recovery| recovery.state);
        drop(cache);
        let state = if let Some(state) = cached {
            state
        } else {
            if page_root.is_none() {
                *page_root = Some(
                    self.load_index_checkpoint(operation, io)
                        .await?
                        .map(|value| value.0),
                );
            }
            let Some(Some(state)) = *page_root else {
                return Ok(false);
            };
            state
        };
        self.index_authenticates(state, sequence, retry, event, io)
            .await
    }

    /// The root and its anchor must have been published in the same commit.
    pub(super) async fn load_index_checkpoint(
        &self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(IndexState, u64)>> {
        self.require_operation(operation, io)?;
        crate::executor::with_history_io(io, |work| {
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.scope,
                &self.volume,
                VolumeOperation::Read,
                self.maximum_payload_bytes,
                work,
            )
        })?;
        let path = self.index_path(operation, "head", io)?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            journal_reads = 1_u64, "execution checkpoint tail requested");
        let tail =
            crate::store::operations::tail_optional_with_work(&self.stream, &path, &mut work)
                .await?
                .unwrap_or(0);
        let Some(sequence) = tail.checked_sub(1) else {
            return Ok(None);
        };
        let _record_hold;
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            journal_reads = 1_u64, "execution checkpoint record requested");
        let record = crate::store::operations::one_record(&self.stream, &path, sequence, &mut work)
            .await?
            .ok_or_else(|| missing(io))?;
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            journal_records = 1_u64, journal_read_bytes = record.value.len() as u64,
            "execution checkpoint record returned");
        _record_hold = io
            .map(|io| {
                io.guard_owned(crate::store::operations::sdk_record_retained_bytes(
                    &record, &work,
                ))
            })
            .transpose()?;
        let checkpoint = codec::decode(&record.value, io)?;
        let state = checkpoint.state;
        self.verify_checkpoint(operation, &checkpoint, io)?;
        self.verify_checkpoint_anchor(operation, &state, record.commit_id, io)
            .await?;
        Ok(Some((state, tail)))
    }

    async fn verify_checkpoint_anchor(
        &self,
        operation: OperationId,
        state: &IndexState,
        commit: acyclic_stream::CommitId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let _anchor_path_hold;
        let path = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(operation, work)
        })?;
        _anchor_path_hold = io
            .map(|io| io.guard_owned(path.as_str().len() as u64))
            .transpose()?;
        let _anchor_hold;
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            journal_reads = 1_u64, "execution checkpoint anchor requested");
        let anchor =
            crate::store::operations::one_record(&self.stream, &path, state.tail - 1, &mut work)
                .await?
                .ok_or_else(|| missing(io))?;
        #[cfg(feature = "test-support")]
        tracing::trace!(target: "acyclic_harness::journal_work",
            journal_records = 1_u64, journal_read_bytes = anchor.value.len() as u64,
            "execution checkpoint anchor returned");
        _anchor_hold = io
            .map(|io| {
                io.guard_owned(crate::store::operations::sdk_record_retained_bytes(
                    &anchor, &work,
                ))
            })
            .transpose()?;
        if anchor.commit_id != commit
            || crate::model::hash_bytes(&anchor.value, io)? != state.anchor
        {
            return Err(missing(io));
        }
        Ok(())
    }

    pub(super) fn encode_index_checkpoint(
        &self,
        state: IndexState,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let bytes = codec::state_bytes(&state, io)?;
        let digest = crate::model::hash_bytes(&bytes, io)?;
        let proof = crate::executor::with_history_io(io, |work| {
            self.verifier.execution_checkpoint_proof(digest, work)
        })?;
        codec::encode_state(&bytes, &proof, io)
    }
}

fn missing(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "execution index checkpoint is not authenticated at its journal cut",
    )
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
