//! Atomic generation retention for the agent's conversation cuts.
use crate::{
    agent::{
        AgentPath, AgentRecord as _, AgentState, ContextAction, ContextCut, ConversationEntry,
        HistoryCheckpoint, SavedVolume,
    },
    conversation::VolumeClass,
};
use acyclic_actors::guest::GuestError;
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, FilesystemCommit, ForkSpec, Fs, IdempotencyKey,
    RestoreSpec, StreamAuthorityStore, WorkBudget, WorkspaceName, distributed::AuthorityProvider,
};
use acyclic_stream::{CommitCondition, CommitMutation, ReadRequest, StreamProvider};
use futures::TryStreamExt as _;

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

/// Read exactly one indexed checkpoint through the authoritative stream owner.
pub async fn read<P: StreamProvider + ?Sized>(
    stream: &P,
    agent: &AgentPath,
    at: u64,
) -> crate::Result<HistoryCheckpoint> {
    let mut records = stream
        .read(ReadRequest {
            key: None,
            path: agent
                .history()
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            from: at,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let record = records
        .try_next()
        .await?
        .ok_or_else(|| crate::Error::Invalid("history cut is absent".into()))?;
    let checkpoint = HistoryCheckpoint::decode(&record.value)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    if record.sequence != at || at.checked_add(1) != Some(checkpoint.conversation) {
        return Err(crate::Error::Invalid(
            "history descriptor differs from its indexed cut".into(),
        ));
    }
    Ok(checkpoint)
}

pub(super) async fn capture<P, O>(
    fs: &Fs<StreamAuthorityStore<P>, O>,
    parent: &AgentState,
    next: &AgentState,
    commit: &mut FilesystemCommit,
) -> Result<(), GuestError>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let path = parent.path.conversation().map_err(guest)?;
    let entries = commit
        .request()
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            CommitMutation::Append { path: at, records } if *at == path => Some(records),
            _ => None,
        })
        .ok_or_else(|| guest("history contribution has no conversation"))?;
    let entries = entries
        .iter()
        .map(|record| ConversationEntry::decode(&record.value).map_err(guest))
        .collect::<Result<Vec<_>, _>>()?;
    let count = u64::try_from(entries.len()).map_err(guest)?;
    if parent.conversation_tail.checked_add(count) != Some(next.conversation_tail) {
        return Err(guest("history contribution differs from conversation cut"));
    }
    // A file publication names one conversation cut. Other settled entries
    // in that publication have no distinct file cut, so accepting such a
    // contribution would silently attach the wrong generation to a turn.
    if entries.len() != 1
        && entries.iter().any(|entry| {
            entry.outcome.is_completed()
                && matches!(entry.input.context(), Some(ContextAction::Restore { mode, .. }) if mode.files())
        })
    {
        return Err(guest(
            "file restoration must publish its own conversation cut",
        ));
    }
    let cancel = CancellationToken::new();
    let restore = match entries.as_slice() {
        [entry] if entry.outcome.is_completed() => match entry.input.context() {
            Some(ContextAction::Restore { at, mode, expected }) => {
                if *at >= parent.conversation_tail {
                    return Err(guest("restore cut is not a prior settled turn"));
                }
                if mode.files() {
                    let saved = read(fs.stream_provider().as_ref(), &parent.path, *at)
                        .await
                        .map_err(guest)?;
                    Some((entry, saved, expected))
                } else {
                    if !expected.is_empty() {
                        return Err(guest("context-only restore carries file heads"));
                    }
                    None
                }
            }
            _ => None,
        },
        _ => None,
    };
    let mut volumes = Vec::new();
    for binding in &parent.volumes {
        if binding.volume.class() != VolumeClass::Project {
            continue;
        }
        let workspace = fs
            .open_workspace(
                binding.volume.storage_name(&mut None).map_err(guest)?,
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(|failure| guest(failure.error))?
            .value;
        let head = workspace
            .head(WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| guest(failure.error))?
            .value;
        volumes.push((binding.clone(), head));
    }
    let mut restored_snapshots = None;
    let mut restored_context = None;
    if let Some((entry, saved, expected)) = restore {
        if expected.len() != volumes.len() || saved.volumes.len() != volumes.len() {
            return Err(guest("restore must cover every project binding"));
        }
        let mut sources = Vec::with_capacity(volumes.len());
        for (binding, head) in &volumes {
            if expected
                .iter()
                .filter(|value| value.binding == *binding && value.generation == head.id())
                .count()
                != 1
            {
                return Err(GuestError::PreconditionChanged);
            }
            let source = saved
                .volumes
                .iter()
                .find(|source| source.binding.name == binding.name)
                .ok_or_else(|| guest("saved project binding differs"))?;
            let workspace = fs
                .open_workspace(&source.retained_workspace, WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| guest(failure.error))?
                .value;
            sources.push(
                workspace
                    .generation(source.retained_generation, WorkBudget::UNBOUNDED, &cancel)
                    .await
                    .map_err(|failure| guest(failure.error))?
                    .value,
            );
        }
        let operation = crate::agent::turn_id(parent, &entry.input).map_err(guest)?;
        let specs = volumes
            .iter()
            .zip(&sources)
            .map(|((_, head), source)| RestoreSpec {
                expected: head,
                source,
                operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                permit: acyclic_fs::PublicationPermit::Unrestricted,
            })
            .collect::<Vec<_>>();
        let restored = fs
            .prepare_restore_many(&specs, commit, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| match failure.error {
                acyclic_fs::WorkspaceError::StaleGeneration => GuestError::PreconditionChanged,
                error => guest(error),
            })?
            .value;
        for ((_, head), generation) in volumes.iter_mut().zip(restored) {
            *head = generation;
        }
        restored_snapshots = Some(saved.volumes);
        restored_context = Some(saved.context);
    } else {
        for (_, head) in &volumes {
            fs.require_head(head, commit, WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| guest(failure.error))?;
        }
    }
    let mut sequence = parent.conversation_tail;
    let mut checkpoint = parent.checkpoint;
    let mut previous = if sequence == 0 {
        None
    } else {
        Some(
            read(fs.stream_provider().as_ref(), &parent.path, sequence - 1)
                .await
                .map_err(guest)?,
        )
    };
    let mut context = match &previous {
        Some(saved) => saved.context.clone(),
        None => ContextCut {
            path: acyclic_stream::StreamPath::new(format!("{}/context/0", parent.path))
                .map_err(guest)?,
            tail: 0,
            checkpoint: None,
        },
    };
    // Each segment forks a published cut once. Its descriptors may select
    // different prefixes, but no fork reads another mutation in this commit.
    let mut segment: Option<usize> = None;
    let mut records = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.checkpoint().is_some() {
            checkpoint = Some(sequence);
        }
        let selection = match entry.input.context() {
            Some(ContextAction::Restore { at, mode, .. })
                if mode.context() && entry.outcome.is_completed() =>
            {
                Some(match restored_context.take() {
                    Some(context) => context,
                    None => {
                        if *at >= parent.conversation_tail {
                            return Err(guest("restore cut is not a prior settled turn"));
                        }
                        read(fs.stream_provider().as_ref(), &parent.path, *at)
                            .await
                            .map_err(guest)?
                            .context
                    }
                })
            }
            _ => None,
        };
        if let Some(selected) = selection {
            context = selected;
            segment = None;
        }
        if !matches!(entry.input.context(), Some(ContextAction::Restore { .. })) {
            let append = entry.encode().map_err(guest)?.into();
            let request = commit
                .request_mut()
                .ok_or_else(|| guest("history commit is sealed"))?;
            match segment.and_then(|index| request.mutations.get_mut(index)) {
                Some(
                    CommitMutation::Fork { records, .. }
                    | CommitMutation::Append { records, .. }
                    | CommitMutation::SoftAppend { records, .. },
                ) => {
                    records.push(append);
                }
                None => {
                    let destination = acyclic_stream::StreamPath::new(format!(
                        "{}/context/{sequence}",
                        parent.path
                    ))
                    .map_err(guest)?;
                    request.conditions.push(CommitCondition::Absent {
                        path: destination.clone(),
                    });
                    segment = Some(request.mutations.len());
                    request.mutations.push(if context.tail != 0 {
                        CommitMutation::Fork {
                            source: context.path,
                            destination: destination.clone(),
                            exclude: Vec::new(),
                            at_tail: context.tail,
                            records: vec![append],
                        }
                    } else {
                        CommitMutation::Append {
                            path: destination.clone(),
                            records: vec![append],
                        }
                    });
                    context.path = destination;
                }
            }
            if entry.checkpoint().is_some() {
                context.checkpoint = Some(context.tail);
            }
            context.tail = context
                .tail
                .checked_add(1)
                .ok_or_else(|| guest("context cut overflow"))?;
        }
        let mut retained = Vec::with_capacity(volumes.len());
        for (binding, head) in &volumes {
            if let Some(saved) = &restored_snapshots {
                let mut saved = saved
                    .iter()
                    .find(|saved| saved.binding.name == binding.name)
                    .ok_or_else(|| guest("restored snapshot is absent"))?
                    .clone();
                saved.binding = binding.clone();
                saved.generation = head.id();
                retained.push(saved);
                continue;
            }
            if let Some(saved) = previous.as_ref().and_then(|previous| {
                previous
                    .volumes
                    .iter()
                    .find(|saved| saved.binding == *binding && saved.generation == head.id())
            }) {
                retained.push(saved.clone());
                continue;
            }
            let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.history.volume.v1");
            hash.update(parent.path.as_str().as_bytes());
            hash.update(&sequence.to_be_bytes());
            hash.update(binding.name.as_bytes());
            let mut key = [0; 16];
            hash.finalize_xof().fill(&mut key);
            let destination = WorkspaceName::new(
                format!("history-{}-{sequence}-{}", parent.path.id(), binding.name),
                &WorkBudget::UNBOUNDED,
                &cancel,
            )
            .map_err(|failure| guest(failure.error))?
            .value;
            let retained_workspace = destination.as_str().to_owned();
            let prepared = fs
                .prepare_fork_many(
                    &[ForkSpec {
                        destination,
                        source: head,
                        paths: None,
                        idempotency_key: IdempotencyKey::from_bytes(key),
                    }],
                    commit,
                    WorkBudget::UNBOUNDED,
                    &cancel,
                )
                .await
                .map_err(|failure| guest(failure.error))?
                .value;
            let prepared = prepared
                .first()
                .ok_or_else(|| guest("retained generation is absent"))?;
            retained.push(SavedVolume {
                binding: binding.clone(),
                generation: head.id(),
                retained_workspace,
                retained_generation: prepared.generation,
            });
        }
        sequence = sequence
            .checked_add(1)
            .ok_or_else(|| guest("history cut overflow"))?;
        let saved = HistoryCheckpoint {
            version: 1,
            conversation: sequence,
            checkpoint,
            context: context.clone(),
            volumes: retained,
        };
        records.push(serde_json::to_vec(&saved).map_err(guest)?.into());
        previous = Some(saved);
    }
    let history = parent.path.history().map_err(guest)?;
    let request = commit
        .request_mut()
        .ok_or_else(|| guest("history commit is sealed"))?;
    request.conditions.push(CommitCondition::Tail {
        path: history.clone(),
        expected: parent.conversation_tail,
    });
    request.mutations.push(CommitMutation::Append {
        path: history,
        records,
    });
    Ok(())
}
