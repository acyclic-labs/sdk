//! Reopen an actual native transition after its selected project head changes.
use super::{agents::settled, scripted, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentPath, HistoryCheckpoint, SpawnVolumes},
};
use acyclic_actors::guest::Composer;
use acyclic_fs::{CancellationToken, FilesystemCommit, WorkBudget};
use acyclic_stream::{
    CommitCondition, CommitConflict, CommitMutation, CommitOutcome, CommitRequest,
    StreamProviderExt as _,
};
use std::sync::{Arc, Mutex};

/// Change the real source once, after capture selected its head and before
/// the native adapter takes its snapshot. Every cut still uses that adapter.
struct BeforeCut {
    cuts: Arc<dyn crate::filesystem::ForkCuts>,
    change: Mutex<Option<acyclic_stream::BoxProviderFuture<'static, Result<()>>>>,
}

impl crate::filesystem::ForkCuts for BeforeCut {
    fn cut<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
        operation: acyclic_fs::OperationId,
        generation: Option<acyclic_fs::GenerationId>,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        std::result::Result<Option<acyclic_fs::GenerationId>, crate::filesystem::ForkCutError>,
    > {
        Box::pin(async move {
            let change = self
                .change
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            if let Some(change) = change {
                change.await?;
            }
            self.cuts
                .cut(source, destination, operation, generation)
                .await
        })
    }
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn retained(request: &CommitRequest, root: &AgentPath) -> Result<HistoryCheckpoint> {
    let history = root.history().map_err(storage)?;
    let record = request
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            CommitMutation::Append { path, records } if path == &history => records.last(),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("preparation has no history descriptor".into()))?;
    HistoryCheckpoint::decode(&record.value).map_err(storage)
}

#[tokio::test]
async fn a_refused_history_preparation_can_select_a_new_project_generation() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let project = volume(directory.path()).await?;
    let source = project.storage_name(&mut None)?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(storage)?;
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(&source, WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    let provider = scripted(
        (0..2).map(|index| crate::test_support::ScriptedTurn::Text(format!("answer {index}"))),
    );
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider)?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let client = crate::local::agent_conversation::Conversation::open(harness.connect().await?);
    client.send("first", "first").await?;
    settled(agents, root).await?;
    let actor = agents.actor(root).map_err(storage)?;
    let parent = agents.state(root).await.map_err(storage)?;
    let before = agents
        .runtime()
        .store()
        .load(&actor)
        .await
        .map_err(storage)?;
    client.send("second", "second").await?;
    let entries = settled(agents, root).await?;
    let after = agents
        .runtime()
        .store()
        .load(&actor)
        .await
        .map_err(storage)?;
    let next = after
        .state()
        .and_then(|state| state.checkpoint())
        .ok_or_else(|| Error::Invalid("settled transition has no checkpoint".into()))?;
    let previous = before.state().and_then(|state| state.checkpoint());
    let entry = entries
        .last()
        .ok_or_else(|| Error::Invalid("settled transition has no conversation".into()))?;
    let cuts = Arc::new(crate::local::workspace::LazyForkCuts(Arc::clone(
        harness
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Invalid("native transition has no workspace".into()))?,
    )));
    let composer = crate::filesystem::FilesystemSpawnVolumes::new(Arc::clone(harness.filesystem()))
        .with_fork_cuts(cuts.clone());
    let participant = composer
        .bind(
            &parent,
            &mut [crate::agent::Out::Conversation(entry.clone())],
        )
        .map_err(storage)?
        .ok_or_else(|| Error::Invalid("conversation has no volume participant".into()))?;
    let request = CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"history-retry".to_vec())
            .map_err(storage)?,
        deadline_unix_millis: None,
        conditions: vec![],
        mutations: vec![CommitMutation::Append {
            path: root.conversation().map_err(storage)?,
            records: vec![entry.clone().into()],
        }],
    };
    // These are the original production checkpoints and encoded output, not
    // a parallel transition model. Reopening probes preparation only: the
    // original actor has already published this conversation's history tail.
    workspace
        .write_text("/changed.md", "first preparation")
        .await
        .map_err(storage)?;
    let changing = workspace.clone();
    let racing = crate::filesystem::FilesystemSpawnVolumes::new(Arc::clone(harness.filesystem()))
        .with_fork_cuts(Arc::new(BeforeCut {
            cuts,
            change: Mutex::new(Some(Box::pin(async move {
                changing
                    .write_text("/changed.md", "changed during preparation")
                    .await
                    .map_err(storage)?;
                Ok(())
            }))),
        }));
    let mut raced = racing
        .open(&actor, previous, None, next, &participant)
        .map_err(storage)?;
    let failure = raced.seal(request.clone()).await;
    assert!(
        matches!(
            failure,
            Err(acyclic_actors::guest::GuestError::PreconditionChanged)
        ),
        "a source changed during preparation must request an actor retry: {failure:?}"
    );
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| storage(failure.error))?
        .value;
    // Obtain the exact source condition through the same public primitive
    // used by history capture, without changing the authored request.
    let mut guard = FilesystemCommit::new(request.clone());
    fs.require_head(&head, &mut guard, WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| storage(failure.error))?;
    let [
        source_condition @ CommitCondition::Tail {
            path: source_path, ..
        },
    ] = guard.request().conditions.as_slice()
    else {
        return Err(Error::Invalid(
            "source head has no exact lineage condition".into(),
        ));
    };
    let mut first = composer
        .open(&actor, previous, None, next, &participant)
        .map_err(storage)?;
    let original = first.seal(request.clone()).await.map_err(storage)?;
    let original_cut = retained(&original, root)?;
    assert!(original.conditions.contains(source_condition));
    workspace
        .write_text("/changed.md", "new generation")
        .await
        .map_err(storage)?;
    let selected = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| storage(failure.error))?
        .value
        .id();
    let outcome = agents.provider().commit(original).await.map_err(storage)?;
    let CommitOutcome::Conflict(conflicts) = &outcome else {
        return Err(Error::Invalid(
            "changed source did not refuse preparation".into(),
        ));
    };
    // Match the primitive's source path so an unrelated history refusal
    // cannot pass this prerequisite.
    assert!(
        conflicts.iter().any(|conflict| match conflict {
            CommitConflict::Tail { path, .. } => path == source_path,
            _ => false,
        }),
        "the selected source guard must refuse its advanced head: {conflicts:?}"
    );
    first.settle(Ok(&outcome)).await.map_err(storage)?;
    let mut retried = composer
        .open(&actor, previous, None, next, &participant)
        .map_err(storage)?;
    let retried = retried.seal(request.clone()).await;
    assert!(
        retried.is_ok(),
        "a refused history preparation must admit the new selected generation: {retried:?}"
    );
    let retried = retried.map_err(storage)?;
    let retried_cut = retained(&retried, root)?;
    assert_ne!(original_cut.volumes, retried_cut.volumes);
    assert_eq!(retried_cut.volumes.len(), 1);
    assert_eq!(retried_cut.volumes[0].generation, selected);
    let mut replay = composer
        .open(&actor, previous, None, next, &participant)
        .map_err(storage)?;
    assert_eq!(
        retained(&replay.seal(request).await.map_err(storage)?, root)?,
        retried_cut
    );
    Ok(())
}
