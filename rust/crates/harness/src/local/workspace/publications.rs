//! Durable fork publication debt: one stream and one lazy cursor per workspace.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, ReadRequest, StreamError, StreamPath,
    StreamProvider, StreamProviderExt,
};
use futures::TryStreamExt as _;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::local) struct Publication {
    version: u32,
    pub(in crate::local) operation: OperationId,
    pub(in crate::local) branch: String,
    pub(in crate::local) source: acyclic_fs::GitGenerationRef,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    next: u64,
    completed: OperationId,
}

fn decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        Error::Unsupported(format!(
            "publication journal requires format v2; incompatible records are retained; start a fresh session: {error}"
        ))
    })
}

fn path(workspace: acyclic_fs::WorkspaceId, part: &str) -> Result<StreamPath> {
    StreamPath::new(format!(
        "harness/workspace-publications-v1/{}/{part}",
        hex::encode(workspace.into_bytes())
    ))
    .map_err(storage)
}

impl LocalWorkspace {
    pub(in crate::local) async fn defer_publication(
        &self,
        operation: OperationId,
        branch: String,
        source: acyclic_fs::GitGenerationRef,
    ) -> Result<()> {
        let workspace = self.view().workspace().id();
        let key = format!(
            "workspace-publication-v2:{}:{operation}:enqueue",
            hex::encode(workspace.into_bytes())
        );
        let stream = self.filesystem.stream_provider();
        if let Some(observed) = stream
            .inspect_idempotency(IdempotencyKey::new(key.clone()).map_err(storage)?)
            .await
            .map_err(storage)?
        {
            if !matches!(
                observed.outcome,
                acyclic_stream::IdempotencyOutcome::Append(AppendOutcome::Committed(_))
            ) {
                return Err(Error::Conflict(
                    "publication retry identity was reused".into(),
                ));
            }
            // A completion retry owns the first durable cut, even if a
            // background writer has since advanced the child's workspace.
            stream.durability_barrier().await.map_err(storage)?;
            return Ok(());
        }
        let publication = Publication {
            version: 2,
            operation,
            branch,
            source,
        };
        self.append_publication_record(
            path(workspace, "pending")?,
            serde_json::to_vec(&publication).map_err(storage)?,
            key,
        )
        .await
    }

    pub(in crate::local) async fn next_publication(
        &self,
        repository: &acyclic_fs::GitCompatRepository<LocalCoreStateStore>,
    ) -> Result<Option<(u64, Publication)>> {
        let workspace = self.view().workspace().id();
        let stream = self.filesystem.stream_provider();
        let next = match stream.head(path(workspace, "cursor")?).await {
            Ok(head) => match head.last {
                Some(record) => {
                    let cursor: Cursor = decode_record(&record.value)?;
                    if cursor.version != 2 {
                        return Err(Error::Unsupported(
                            "publication cursor requires format v2; start a fresh session".into(),
                        ));
                    }
                    // The cursor is the durable withdrawal. Recover a crash
                    // after acknowledging debt but before releasing its reply.
                    repository
                        .acknowledge_request(acyclic_fs::OperationId::from_bytes(
                            cursor.completed.into_bytes(),
                        ))
                        .await
                        .map_err(storage)?;
                    cursor.next
                }
                None => 0,
            },
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(storage(error)),
        };
        let mut records = match stream
            .read(ReadRequest {
                key: None,
                path: path(workspace, "pending")?,
                from: next,
                limit: 1,
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let Some(record) = records.try_next().await.map_err(storage)? else {
            return Ok(None);
        };
        let publication: Publication = decode_record(&record.value)?;
        if publication.version != 2 {
            return Err(Error::Unsupported(
                "deferred publication requires format v2; start a fresh session".into(),
            ));
        }
        Ok(Some((next, publication)))
    }

    pub(in crate::local) async fn acknowledge_publication(
        &self,
        sequence: u64,
        completed: OperationId,
        repository: &acyclic_fs::GitCompatRepository<LocalCoreStateStore>,
    ) -> Result<()> {
        let workspace = self.view().workspace().id();
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("publication cursor overflow".into()))?;
        self.append_publication_record(
            path(workspace, "cursor")?,
            serde_json::to_vec(&Cursor {
                version: 2,
                next,
                completed,
            })
            .map_err(storage)?,
            format!(
                "workspace-publication-v2:{}:{sequence}:ack",
                hex::encode(workspace.into_bytes())
            ),
        )
        .await?;
        repository
            .acknowledge_request(acyclic_fs::OperationId::from_bytes(completed.into_bytes()))
            .await
            .map_err(storage)
    }

    async fn append_publication_record(
        &self,
        path: StreamPath,
        value: Vec<u8>,
        key: String,
    ) -> Result<()> {
        let stream = self.filesystem.stream_provider();
        match stream
            .append(AppendRequest {
                path,
                records: vec![value.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(key).map_err(storage)?,
            })
            .await
            .map_err(storage)?
        {
            AppendOutcome::Committed(_) => {
                stream.durability_barrier().await.map_err(storage)?;
                Ok(())
            }
            AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
                "deferred publication append was refused".into(),
            )),
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    async fn fixture() -> Result<(tempfile::TempDir, LocalHarness)> {
        let directory = tempfile::tempdir().map_err(storage)?;
        let volume = super::super::super::tests::volume(directory.path()).await?;
        let local = LocalHarness::open(
            super::super::super::tests::volume_config(
                directory.path(),
                volume,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        Ok((directory, local))
    }

    #[tokio::test]
    async fn publication_drain_recovers_uninspected_transitions_before_completion() -> Result<()> {
        for recovery in ["merge", "branch", "checkout"] {
            let own = recovery == "merge";
            let (_directory, local) = fixture().await?;
            let workspace = local
                .workspace
                .as_ref()
                .ok_or_else(|| Error::Invalid("no workspace".into()))?;
            let id = workspace.view().workspace().id();
            let owner = super::super::owner::AgentOwner(crate::agent::AgentPath::root(
                &crate::agent::AgentId::from_bytes(local.root.into_bytes()),
            ));
            let scope = crate::runtime::RuntimeScope::new(
                local.signed.capabilities().clone(),
                Default::default(),
            )?;
            let configuration =
                super::super::super::checkout_tools::capture::configuration(workspace)?;
            let commands = super::super::git::Commands {
                workspace,
                signed: &local.signed,
                clock: local.clock.as_ref(),
                capture_options: &configuration.0,
                capture_policy: &configuration.1,
            };
            let context = |operation| super::super::git::CommandContext {
                owner: &owner,
                operation,
                scope: &scope,
                request: None,
            };
            commands
                .invoke(
                    "branch child",
                    context(OperationId::new()),
                    Box::pin(async { Ok(()) }),
                )
                .await?;
            let repository = acyclic_fs::GitCompatRepository::new(id, workspace.store.clone());
            let child_id = repository.load().await.map_err(storage)?.branches["child"].workspace_id;
            let child = acyclic_fs::WorkspaceResolver::resolve(
                &(
                    workspace.filesystem.clone(),
                    acyclic_fs::WorkspaceGraph::new(workspace.store.clone()),
                ),
                child_id,
            )
            .await
            .map_err(storage)?;
            child
                .write_text("/published", "completed")
                .await
                .map_err(storage)?;
            let cut = child
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            let completed = OperationId::new();
            let source = acyclic_fs::GitGenerationRef {
                workspace_id: child_id,
                generation: cut.id(),
            };
            workspace
                .defer_publication(completed, "child".into(), source)
                .await?;
            let target = workspace
                .view()
                .workspace()
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            let args: Vec<String> = match recovery {
                "merge" => vec!["merge".into(), "child".into()],
                "checkout" => vec!["checkout".into(), "-b".into(), "interrupted-parent".into()],
                _ => vec!["branch".into(), "interrupted-parent".into()],
            };
            acyclic_fs::GitCompatRepository::new(id, workspace.store.clone())
                .for_request(acyclic_fs::GitRequest {
                    operation: if own {
                        acyclic_fs::OperationId::from_bytes(completed.into_bytes())
                    } else {
                        acyclic_fs::OperationId::new()
                    },
                    merge_source: own.then_some(source),
                    unanswered: Arc::new(|_| true),
                })
                .execute_argv(
                    &args,
                    acyclic_fs::GitTreeRef::exact(id, target.id()),
                    "test",
                    1,
                )
                .await
                .map_err(storage)?;
            assert!(
                repository
                    .pending_transition()
                    .await
                    .map_err(storage)?
                    .is_some()
            );
            let mut state = crate::sync::owned::lock_owned(StateArc::clone(&workspace.state)).await;
            commands
                .drain_publications(&context(completed), &mut state)
                .await?;
            let physical = workspace.selected_workspace(&mut state, &owner).await?;
            assert!(
                repository
                    .pending_transition()
                    .await
                    .map_err(storage)?
                    .is_none()
            );
            assert!(workspace.next_publication(&repository).await?.is_none());
            let merged = state
                .active
                .as_ref()
                .unwrap_or_else(|| workspace.view())
                .workspace()
                .head(
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            let content = merged
                .read_range(
                    "/published",
                    0,
                    b"completed".len() as u64,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            assert_eq!(content.bytes.as_ref(), b"completed");
            assert_eq!(
                std::fs::read(physical.path.join("published")).map_err(storage)?,
                b"completed"
            );
            eprintln!(
                "publication recovery={recovery} native_mount={}",
                physical.mounted.is_some()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn durable_cursor_releases_only_acknowledged_publication_after_interruption() -> Result<()>
    {
        let (_directory, local) = fixture().await?;
        let workspace = local
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Invalid("no workspace".into()))?;
        let id = workspace.view().workspace().id();
        let cut = workspace
            .view()
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(storage)?
            .value;
        let completed = OperationId::new();
        let retained = acyclic_fs::OperationId::new();
        let repository = acyclic_fs::GitCompatRepository::new(id, workspace.store.clone());
        for (operation, branch) in [
            (
                acyclic_fs::OperationId::from_bytes(completed.into_bytes()),
                "completed",
            ),
            (retained, "retained"),
        ] {
            repository
                .register_branch_workspace(branch, id, None, false)
                .await
                .map_err(storage)?;
            acyclic_fs::GitCompatRepository::new(id, workspace.store.clone())
                .for_request(acyclic_fs::GitRequest {
                    operation,
                    merge_source: None,
                    unanswered: Arc::new(|_| true),
                })
                .execute_argv(
                    &["discard".into(), branch.into()],
                    acyclic_fs::GitTreeRef::exact(id, cut.id()),
                    "test",
                    1,
                )
                .await
                .map_err(storage)?;
        }
        workspace
            .defer_publication(
                completed,
                "child".into(),
                acyclic_fs::GitGenerationRef {
                    workspace_id: id,
                    generation: cut.id(),
                },
            )
            .await?;
        workspace
            .view()
            .workspace()
            .write_text("/followup", "unfinished")
            .await
            .map_err(storage)?;
        let advanced = workspace
            .view()
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(storage)?
            .value;
        workspace
            .defer_publication(
                completed,
                "child".into(),
                acyclic_fs::GitGenerationRef {
                    workspace_id: id,
                    generation: advanced.id(),
                },
            )
            .await?;
        let (_, queued) = workspace
            .next_publication(&repository)
            .await?
            .ok_or_else(|| Error::Invalid("publication lost".into()))?;
        assert_eq!(queued.source.generation, cut.id());
        // Interrupt precisely after the durable cursor, before reply release.
        workspace
            .append_publication_record(
                path(id, "cursor")?,
                serde_json::to_vec(&Cursor {
                    version: 2,
                    next: 1,
                    completed,
                })
                .map_err(storage)?,
                format!(
                    "workspace-publication-v2:{}:0:ack",
                    hex::encode(id.into_bytes())
                ),
            )
            .await?;
        assert_eq!(repository.load().await.map_err(storage)?.requests.len(), 2);
        let reopened = acyclic_fs::GitCompatRepository::new(id, workspace.store.clone());
        assert!(workspace.next_publication(&reopened).await?.is_none());
        let state = reopened.load().await.map_err(storage)?;
        assert_eq!(state.requests.len(), 1);
        assert!(state.requests.contains_key(&retained));
        // A repeated read after recovery neither retains the completed reply
        // nor withdraws the unrelated client's retry custody.
        assert!(workspace.next_publication(&reopened).await?.is_none());
        assert_eq!(reopened.load().await.map_err(storage)?.requests.len(), 1);
        // Old live-source records cannot prove a completed generation. Refuse
        // incompatible journals without inventing authority or dropping debt.
        workspace
            .append_publication_record(
                path(id, "pending")?,
                serde_json::to_vec(&serde_json::json!({
                    "version": 1, "operation": OperationId::new(),
                    "branch": "legacy", "source": id,
                }))
                .map_err(storage)?,
                "legacy-publication-control".into(),
            )
            .await?;
        assert!(matches!(
            workspace.next_publication(&reopened).await,
            Err(Error::Unsupported(_))
        ));
        workspace
            .append_publication_record(
                path(id, "cursor")?,
                serde_json::to_vec(&serde_json::json!({"version": 1, "next": 1}))
                    .map_err(storage)?,
                "legacy-cursor-control".into(),
            )
            .await?;
        assert!(matches!(
            workspace.next_publication(&reopened).await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(reopened.load().await.map_err(storage)?.requests.len(), 1);
        Ok(())
    }
}
