//! One owner-fenced commit publishes the journal cut and its authenticated root.
use super::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommittedMutation, OwnedBytes, StreamPath,
};
use checkpoint::{IndexState, Recovery};

mod pages;

async fn condition<P: StreamProvider>(
    provider: &Arc<P>,
    path: &StreamPath,
    expected: u64,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<CommitCondition> {
    let exists = if expected == 0 {
        crate::store::operations::tail_optional_with_work(provider, path, work)
            .await?
            .is_some()
    } else {
        true
    };
    Ok(if exists {
        CommitCondition::Tail {
            path: path.clone(),
            expected,
        }
    } else {
        CommitCondition::Absent { path: path.clone() }
    })
}

fn add_records(
    request: &mut (
        acyclic_stream::CommitRequest,
        crate::wire_codec::CodecQuota<'_, '_>,
    ),
    path: &StreamPath,
    input: &[OwnedBytes],
) -> Result<()> {
    request
        .1
        .reserve(request.0.mutations.len() + 1, &mut request.0.mutations)?;
    let mut records = Vec::new();
    request.1.reserve(input.len(), &mut records)?;
    records.extend(input.iter().cloned().map(Into::into));
    request.0.mutations.push(CommitMutation::Append {
        path: path.clone(),
        records,
    });
    Ok(())
}

fn confirms(
    envelope: &acyclic_stream::CommittedEnvelope,
    path: &StreamPath,
    start: u64,
    records: &[OwnedBytes],
) -> bool {
    let Some(CommittedMutation::Append(append)) = envelope.mutations.iter().find(
        |mutation| matches!(mutation, CommittedMutation::Append(append) if append.path == *path),
    ) else {
        return false;
    };
    append.start == start
        && start.checked_add(records.len() as u64) == Some(append.end)
        && append.tail == append.end
        && append.records.len() == records.len()
        && append
            .records
            .iter()
            .zip(records)
            .enumerate()
            .all(|(index, (record, value))| {
                record.sequence == start + index as u64
                    && record.value == *value
                    && record.headers.keys().is_empty()
                    && record.headers.attributes().is_empty()
                    && record.commit_id == envelope.commit_id
            })
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn publish_indexed_observation(
        &self,
        summary: &mut ExecutionSummary,
        retry: [u8; 32],
        event: &ExecutionEvent,
        bytes: &OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Recovery>> {
        let operation = summary.operation.ok_or_else(|| invalid(io))?;
        let pages = self.index_path(operation, "pages", io)?;
        let head = self.index_path(operation, "head", io)?;
        let execution_hold;
        let execution = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(operation, work)
        })?;
        execution_hold = io
            .map(|io| io.guard_owned(execution.as_str().len() as u64))
            .transpose()?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let base =
            crate::store::operations::tail_optional_with_work(&self.stream, &pages, &mut work)
                .await?
                .unwrap_or(0);
        let mut writer =
            persistent_index::Writer::new(&self.stream, &pages, base, io, summary.scratch.take())?;
        writer.set_cache(std::mem::take(&mut summary.pages), io)?;
        let mut roots = self
            .stage_summary_index(&mut writer, summary, retry, event, io)
            .await?;
        roots[4] = summary.pending_steps.stage(&mut writer, roots[4]).await?;
        summary.pages = writer.prepare_cache()?;
        let state = IndexState {
            operation,
            turn: self.turn_operation(operation),
            volume: self.index_volume_digest(io)?,
            tail: summary.tail,
            nodes_tail: writer.tail()?,
            roots,
            pending_dispatches: summary.pending_dispatches,
            anchor: crate::model::hash_bytes(bytes, io)?,
            latest_response: summary.latest_response,
            model_dispatched: summary.model_dispatched,
        };
        let checkpoint_hold;
        let checkpoint = self.encode_index_checkpoint(state, io)?;
        checkpoint_hold = io
            .map(|io| io.guard_bytes(&checkpoint))
            .transpose()?
            .flatten();
        let head_tail = summary.recovery.map_or(0, |recovery| recovery.head_tail);
        let next_head_tail = head_tail.checked_add(1).ok_or_else(|| invalid(io))?;
        let head_condition = condition(&self.stream, &head, head_tail, &mut work).await?;
        let execution_condition =
            condition(&self.stream, &execution, summary.tail - 1, &mut work).await?;
        let owner = match self.owner.as_ref() {
            Some((owner, _)) => Some(
                owner
                    .execution_condition(operation, event, &mut work)
                    .await?,
            ),
            None => None,
        };
        let digest = publication_digest(&checkpoint, retry, &owner, io)?;
        let cut = pages::Cut {
            // Recovery belongs to the admitted task, just as the original
            // owner-fenced journal append did. The execution ID names storage.
            operation: self
                .owner
                .as_ref()
                .map_or(operation, |(owner, _)| owner.operation_id()),
            execution: &execution,
            head: &head,
            pages: &pages,
            execution_condition,
            head_condition,
            owner,
            execution_tail: summary.tail - 1,
            head_tail,
            base,
            digest,
        };
        let committed = cut
            .publish(
                &self.stream,
                writer.records(),
                bytes,
                &checkpoint,
                &mut work,
            )
            .await?;
        let result = if committed {
            summary.scratch = Some(writer.take_scratch());
            Some(Recovery {
                state,
                head_tail: next_head_tail,
                indexed: true,
            })
        } else {
            None
        };
        drop(checkpoint);
        drop(checkpoint_hold);
        drop(execution);
        drop(execution_hold);
        Ok(result)
    }
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "indexed execution publication is invalid",
    )
}

fn publication_digest(
    checkpoint: &OwnedBytes,
    retry: [u8; 32],
    owner: &Option<CommitCondition>,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    let owner = match owner {
        Some(CommitCondition::Tail { path, expected }) => Some((path.as_str(), *expected)),
        None => None,
        _ => return Err(invalid(io)),
    };
    let checkpoint = crate::model::hash_bytes(checkpoint, io)?;
    let domain = b"harness/v1/indexed-execution\0";
    let length = crate::core::owned_sum([
        domain.len() as u64,
        64,
        1,
        owner.map_or(0, |(path, _)| 16 + path.len() as u64),
    ])?;
    if let Some(io) = io {
        io.charge(acyclic_fs::WorkCounters {
            bytes_hashed: length,
            ..Default::default()
        })?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(&checkpoint);
    hasher.update(&retry);
    hasher.update(&[u8::from(owner.is_some())]);
    if let Some((path, expected)) = owner {
        hasher.update(&(path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());
        hasher.update(&expected.to_le_bytes());
    }
    Ok(*hasher.finalize().as_bytes())
}
