//! One owner-fenced commit publishes the journal cut and its authenticated root.
use super::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, OwnedBytes, StreamPath,
};
use checkpoint::{IndexState, Recovery};

mod pages;

async fn condition<P: StreamProvider + ?Sized>(
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

/// The receipt assigns exactly `records` to `path`, starting at `start`.
fn confirms(
    receipt: &acyclic_stream::CommitReceipt,
    path: &StreamPath,
    start: u64,
    records: &[OwnedBytes],
) -> bool {
    receipt.appends.iter().any(|append| {
        append.path == *path
            && append.start == start
            && start.checked_add(records.len() as u64) == Some(append.tail)
    })
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + ?Sized,
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
        let digest = publication_digest(&checkpoint, retry, io)?;
        let cut = pages::Cut {
            operation,
            execution: &execution,
            head: &head,
            pages: &pages,
            execution_condition,
            head_condition,
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
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    let checkpoint = crate::model::hash_bytes(checkpoint, io)?;
    let domain = b"harness/v1/indexed-execution\0";
    let length = crate::core::owned_sum([domain.len() as u64, 64, 1])?;
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
    // The former owner-presence flag; kept so existing digests still match.
    hasher.update(&[0]);
    Ok(*hasher.finalize().as_bytes())
}
