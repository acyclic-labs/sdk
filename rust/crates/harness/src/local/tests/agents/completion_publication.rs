//! Completion uses the original fork cut after the child's final observation.
use super::*;
use acyclic_fs::facade::FsWork as _;
use acyclic_harness_client::{AgentCommand, AgentReply};
use std::sync::Mutex;

#[tokio::test]
async fn child_source_observations_skip_publication_but_deletions_reach_the_parent() -> Result<()> {
    for remove in [false, true] {
        let checkout = tempfile::tempdir().map_err(agent)?;
        let state = tempfile::tempdir().map_err(agent)?;
        std::fs::write(checkout.path().join("source.txt"), b"original").map_err(agent)?;
        let (dispatched, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let released = Arc::new(Mutex::new(Some(released)));
        let provider = Arc::new(
            super::super::ScriptedModelProvider::new([super::super::ScriptedTurn::Text(
                "completed".into(),
            )])
            .with_context_capacity(super::super::capacity())
            .with_dispatch_observer(move |_| {
                let released = released.lock().expect("dispatch gate").take();
                let _ = dispatched.send(());
                Box::pin(async move {
                    released
                        .ok_or_else(|| Error::Invalid("unexpected second model request".into()))?
                        .await
                        .map_err(|_| Error::Storage("model dispatch gate closed".into()))
                })
            }),
        );
        let harness = Arc::new(
            super::super::open_scripted(
                crate::test_support::local_harness_config(
                    checkout.path(),
                    (
                        crate::model::Model::new("scripted".into(), "test".into(), "1".into())?,
                        provider.clone(),
                    ),
                )?
                .state_directory(state.path())
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Disabled),
            )
            .await?,
        );
        let (agents, root) = harness.agents().await?;
        let (agents, root) = (Arc::clone(agents), root.clone());
        let connection = harness.connect().await?;
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Spawn {
                parent: root,
                id: crate::agent::IntentId::try_from("source-child").map_err(agent)?,
                brief: crate::model::ModelContent::Text("complete the selected input".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("spawn did not return its child".into()));
        };
        model_dispatch_before_settlement(&harness, &child, async {
            requests
                .recv()
                .await
                .ok_or_else(|| Error::Storage("model dispatch observation closed".into()))
        })
        .await?;
        let child_state = agents.state(&child).await.map_err(agent)?;
        let project = child_state
            .volumes
            .iter()
            .find(|binding| binding.name == "project")
            .ok_or_else(|| Error::NotFound("child project".into()))?
            .volume()?;
        let parent = harness
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Invalid("native project is absent".into()))?;
        let view = parent.spawned_project_view(project).await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let before = view.snapshot(&mut accounting).await.map_err(agent)?;
        let observed = view
            .lookup("/source.txt", &mut accounting)
            .await
            .map_err(agent)?;
        view.retire_lookup(observed, &mut accounting)
            .map_err(agent)?;
        if remove {
            view.remove("/source.txt", None, &mut accounting)
                .await
                .map_err(agent)?;
        }
        let selected = view.snapshot(&mut accounting).await.map_err(agent)?;
        assert_eq!(
            before.authored_generation, selected.authored_generation,
            "the fixture must exercise source-only facts, not an authored file"
        );
        drop(accounting);
        release
            .send(())
            .map_err(|_| Error::Storage("model dispatch ended early".into()))?;
        let entries = settled(&harness, &child).await?;
        let completed = entries
            .last()
            .ok_or_else(|| Error::NotFound("child answer".into()))?;
        assert_eq!(
            ConversationEntry::decode(completed).map_err(agent)?.outcome,
            crate::agent::TurnOutcome::Completed
        );
        assert_eq!(provider.request_count(), 1);
        let branch = format!("agents/{}", child.id());
        let catalog = acyclic_fs::GitCompatRepository::new(
            parent.view().workspace().id(),
            parent.store.clone(),
        )
        .load()
        .await
        .map_err(agent)?;
        assert_eq!(
            catalog.branches.contains_key(&branch),
            remove,
            "read-only completion must not register or publish a child branch"
        );
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        match parent.view().lookup("/source.txt", &mut accounting).await {
            Ok(found) => {
                parent
                    .view()
                    .retire_lookup(found, &mut accounting)
                    .map_err(agent)?;
                assert!(
                    !remove,
                    "the completed child's source deletion did not reach its parent"
                );
            }
            Err(acyclic_fs::LazyWorkspaceError::NotFound) => {
                assert!(remove, "a read-only child deleted its parent's source file");
            }
            Err(error) => return Err(agent(error)),
        }
    }
    Ok(())
}
