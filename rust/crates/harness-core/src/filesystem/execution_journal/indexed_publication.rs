//! One owner-fenced commit publishes the journal cut and its authenticated root.
use super::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, OwnedBytes, StreamPath, StreamProviderExt as _,
};
use checkpoint::{IndexState, Recovery};

mod pages;
pub(in crate::filesystem) use pages::Contribution;
pub(super) use pages::contribution_identity;

async fn condition<P: crate::store::JournalProvider + ?Sized>(
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

fn add_records<R: Clone + Into<acyclic_stream::AppendRecord>>(
    request: &mut acyclic_stream::CommitRequest,
    custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    path: &StreamPath,
    input: &[R],
) -> Result<()> {
    custody.reserve(
        request.effects[0].mutations.len() + 1,
        &mut request.effects[0].mutations,
    )?;
    let mut records = Vec::new();
    custody.reserve(input.len(), &mut records)?;
    records.extend(input.iter().cloned().map(Into::into));
    request.effects[0].mutations.push(CommitMutation::Append {
        path: path.clone(),
        records,
    });
    Ok(())
}

/// The receipt assigns exactly `records` to `path`, starting at `start`.
fn confirms<R>(
    append: &acyclic_stream::AppendRange,
    path: &StreamPath,
    start: u64,
    records: &[R],
) -> bool {
    append.path == *path
        && append.start == start
        && start.checked_add(records.len() as u64) == Some(append.tail)
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn publish_contributed_observation(
        &self,
        summary: &mut ExecutionSummary,
        retry: [u8; 32],
        event: &ExecutionEvent,
        inputs: (Option<&ModelEvent>, Option<&super::supervision::Inputs<'_>>),
        bytes: &OwnedBytes,
        contributions: &[Contribution<'_>],
        io: Option<&PreparationIo>,
    ) -> Result<Option<(Recovery, acyclic_stream::CommitId, u64)>> {
        let operation = summary.operation.ok_or_else(|| invalid(io))?;
        let (model, inputs) = inputs;
        let source_key = super::model_calls::observation_key(event, model, io)?;
        let _source_key_hold = source_key
            .as_ref()
            .map(|key| io.map(|io| io.guard_bytes(key)).transpose())
            .transpose()?;
        let pages = self.index_path(operation, "pages", io)?;
        let head = self.index_path(operation, "head", io)?;
        let execution_hold;
        let execution = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(operation, work)
        })?;
        execution_hold = io
            .filter(|io| io.original_native_work().is_none())
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
            latest_response_output: summary.latest_response_output,
            latest_response_completed: summary.latest_response_completed,
            model_dispatched: summary.model_dispatched,
        };
        let checkpoint_hold;
        let checkpoint = self.encode_index_checkpoint(state, io).await?;
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
        // This is a projection of the accepted source, never an independently
        // submitted lifecycle write. A rejected/replayed cut has exactly the
        // source journal's outcome and original commit identity.
        let lifecycle = self
            .agent_events
            .as_ref()
            .filter(|(_, _, _, start)| *start && matches!(event, ExecutionEvent::Started { .. }));
        let lifecycle = lifecycle.map(|(agent, run, input, _)| crate::supervision::Event {
            version: 1,
            agent: agent.clone(),
            run: Some(*run),
            source: crate::supervision::Position {
                stream: execution.clone(),
                sequence: summary.tail - 1,
            },
            event: crate::supervision::Lifecycle::Started { input: *input },
        });
        let lifecycle_path = lifecycle
            .as_ref()
            .map(|event| crate::supervision::streams::path(&event.agent))
            .transpose()?;
        let lifecycle_bytes = lifecycle
            .as_ref()
            .map(|event| match io {
                Some(io) => io.encode(event, acyclic_stream::MAX_RECORD_BYTES as u64, false),
                None => serde_json::to_vec(event)
                    .map(Into::into)
                    .map_err(|_| invalid(io)),
            })
            .transpose()?;
        let lifecycle_hold = lifecycle_bytes
            .as_ref()
            .map(|bytes| io.map(|io| io.guard_bytes(bytes)).transpose())
            .transpose()?;
        let lifecycle_record = lifecycle_bytes
            .as_ref()
            .map(|bytes| -> Result<_> {
                Ok(acyclic_stream::AppendRecord {
                    value: bytes.clone(),
                    headers: supervision::indexed_headers(
                        [
                            super::super::bounded_io::clone_bytes(
                                crate::supervision::streams::CURRENT_RUN_KEY,
                                io,
                            )?,
                            super::super::bounded_io::clone_bytes(crate::supervision::RUN_KEY, io)?,
                        ],
                        io,
                    )?,
                })
            })
            .transpose()?;
        let source = crate::supervision::Position {
            stream: execution.clone(),
            sequence: summary.tail - 1,
        };
        let projection = self.supervision_projection(&source, inputs, io).await?;
        let has_projection = projection.is_some();
        let mut derived = projection.map(|projection| projection.effect);
        if matches!(
            event,
            ExecutionEvent::ModelStarted { .. } | ExecutionEvent::ToolStarted { .. }
        ) && (self.actor_admission.is_some() || self.agent_events.is_some())
        {
            // One authority cut observes actor pause and every ancestor's archive.
            // These exact paths also fence the original dispatch commit, so
            // archive delivery/cascade timing cannot admit new paid work.
            let mut paths = Vec::new();
            if let Some(path) = &self.actor_admission {
                crate::executor::stock_vec_push(&mut paths, path.clone(), io)?;
            }
            let closed_start = paths.len();
            if let Some((agent, ..)) = &self.agent_events {
                for ancestor in agent.ancestors() {
                    let mut text = crate::executor::stock_string_copy(ancestor, io)?;
                    crate::executor::stock_string_push(
                        &mut text,
                        "/closed",
                        u64::MAX,
                        "archive path overflows",
                        io,
                    )?;
                    let text_hold = io
                        .map(|io| io.guard_owned(text.capacity() as u64))
                        .transpose()?;
                    let closed = crate::distributed::stream_path(&text, &mut work)?;
                    drop(text);
                    drop(text_hold);
                    crate::executor::stock_vec_push(&mut paths, closed, io)?;
                }
            }
            let _paths_hold = io
                .map(|io| {
                    io.guard_owned((paths.capacity() * std::mem::size_of::<StreamPath>()) as u64)
                })
                .transpose()?;
            let request = acyclic_stream::HeadsRequest {
                limit: u32::try_from(paths.len()).map_err(|_| invalid(io))?,
                paths: acyclic_stream::HeadSet::from_vec(paths),
                retained: Default::default(),
                durable_through: None,
                projection: acyclic_stream::HeadProjection::History,
                children: None,
                at_least: Default::default(),
            };
            let observed =
                crate::store::operations::heads_with_work(&self.stream, &request, &mut work)
                    .await?;
            for (closed, head) in request.paths[closed_start..]
                .iter()
                .zip(&observed.heads[closed_start..])
            {
                if let Some(record) = head.as_ref().and_then(|head| head.last.as_ref()) {
                    return Err(Error::ExecutionControlled(crate::supervision::Control {
                        source: crate::supervision::Position {
                            stream: closed.clone(),
                            sequence: record.sequence,
                        },
                        action: crate::supervision::Action::Stop,
                        signal: None,
                    }));
                }
            }
            let effect = derived
                .get_or_insert_with(|| acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new()));
            for path in &request.paths[closed_start..] {
                crate::executor::stock_vec_push(
                    &mut effect.conditions,
                    CommitCondition::Absent { path: path.clone() },
                    io,
                )?;
            }
            if let Some(path) = &self.actor_admission {
                let head = observed
                    .heads
                    .first()
                    .and_then(Option::as_ref)
                    .ok_or_else(|| invalid(io))?;
                let record = head.last.as_ref().ok_or_else(|| invalid(io))?;
                let admission = acyclic_actors::local::AdmissionState::from_record(&record.value)
                    .map_err(|_| invalid(io))?;
                if admission.paused {
                    return Err(Error::ExecutionControlled(crate::supervision::Control {
                        source: crate::supervision::Position {
                            stream: path.clone(),
                            sequence: record.sequence,
                        },
                        action: crate::supervision::Action::Pause,
                        signal: None,
                    }));
                }
                crate::executor::stock_vec_push(
                    &mut effect.conditions,
                    CommitCondition::Head {
                        path: path.clone(),
                        expected: head.tail,
                        commit_id: record.commit_id,
                    },
                    io,
                )?;
            }
        }
        if !has_projection
            && matches!(
                event,
                ExecutionEvent::ModelStarted { .. } | ExecutionEvent::ToolStarted { .. }
            )
            && let Some((agent, run, _, _)) = &self.agent_events
        {
            let defaults = crate::supervision::Policy::default();
            let policy = self
                .run_admission
                .as_ref()
                .map(|source| &source.value)
                .unwrap_or(&defaults);
            let admission = crate::supervision::streams::Queries::new(&*self.stream, agent, *run)
                .admission(policy)
                .await?;
            if let Some(control) = admission.control {
                return Err(Error::ExecutionControlled(control));
            }
            let effect = derived
                .get_or_insert_with(|| acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new()));
            crate::executor::stock_vec_push(&mut effect.conditions, admission.condition, io)?;
        }
        if let Some((path, record)) = lifecycle_path.as_ref().zip(lifecycle_record.as_ref()) {
            let effect = derived
                .get_or_insert_with(|| acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new()));
            if let Some(CommitMutation::Append { records, .. }) = effect.mutations.iter_mut().find(
                |mutation| matches!(mutation, CommitMutation::Append { path: target, .. } if target == path)
            ) {
                // A run's original admission can itself exhaust a zero
                // reservation. Started precedes its signals in one append;
                // there are never two mutations for the same stream path.
                crate::executor::stock_vec_prepare_push(records, io)?;
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        bytes_copied: (records.len() as u64)
                            .checked_mul(std::mem::size_of::<acyclic_stream::AppendRecord>() as u64)
                            .ok_or_else(|| invalid(Some(io)))?,
                        ..Default::default()
                    })?;
                }
                records.insert(0, record.clone().into());
            } else {
                let mut records = Vec::new();
                crate::executor::stock_vec_push(&mut records, record.clone().into(), io)?;
                crate::executor::stock_vec_push(
                    &mut effect.mutations,
                    CommitMutation::Append {
                        path: path.clone(),
                        records,
                    },
                    io,
                )?;
            }
        }
        if let Some(effect) = &mut derived {
            effect.mutations.sort_by(|left, right| match (left, right) {
                (
                    CommitMutation::Append { path: left, .. },
                    CommitMutation::Append { path: right, .. },
                ) => left.cmp(right),
                _ => std::cmp::Ordering::Equal,
            });
        }
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
            contributions,
            derived: derived.as_ref(),
            source_key: source_key.as_ref(),
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
        let result = if let Some((commit_id, committed_at_micros)) = committed {
            summary.scratch = Some(writer.take_scratch());
            Some((
                Recovery {
                    state,
                    head_tail: next_head_tail,
                    indexed: true,
                },
                commit_id,
                committed_at_micros,
            ))
        } else {
            None
        };
        drop(lifecycle_bytes);
        drop(lifecycle_hold);
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
