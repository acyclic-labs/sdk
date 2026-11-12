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

async fn cursor(
    filesystem: &acyclic_fs::LocalFs,
    workspace: acyclic_fs::WorkspaceId,
) -> Result<Option<Cursor>> {
    let stream = filesystem.stream_provider();
    let record = match stream.head(path(workspace, "cursor")?).await {
        Ok(head) => head.last,
        Err(error) if error.category() == StreamError::NotFound => None,
        Err(error) => return Err(storage(error)),
    };
    let Some(record) = record else {
        return Ok(None);
    };
    let cursor: Cursor = decode_record(&record.value)?;
    if cursor.version != 2 {
        return Err(Error::Unsupported(
            "publication cursor requires format v2; start a fresh session".into(),
        ));
    }
    Ok(Some(cursor))
}

async fn pending(
    filesystem: &acyclic_fs::LocalFs,
    workspace: acyclic_fs::WorkspaceId,
    next: u64,
) -> Result<Option<Publication>> {
    let stream = filesystem.stream_provider();
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
        Err(error) if error.category() == StreamError::NotFound => return Ok(None),
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
    Ok(Some(publication))
}

impl LocalWorkspace {
    pub(in crate::local) async fn defer_publication(
        self: &Arc<Self>,
        operation: OperationId,
        branch: String,
        source: acyclic_fs::GitGenerationRef,
    ) -> Result<()> {
        let workspace = Arc::clone(self);
        acyclic_native_runtime::TaskSpawner::current()
            .spawn(async move {
                let result = workspace
                    .enqueue_publication(operation, branch, source)
                    .await;
                // Publish the retry signal in the original append's completion owner,
                // even if its caller stopped observing before the durable acknowledgement.
                workspace
                    .publication_debt
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                result
            })
            .await
            .map_err(storage)?
    }

    async fn enqueue_publication(
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
        let next = match cursor(&self.filesystem, workspace).await? {
            Some(cursor) => {
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
        };
        Ok(pending(&self.filesystem, workspace, next)
            .await?
            .map(|publication| (next, publication)))
    }

    /// Whether `workspace` holds a handback no command has published yet,
    /// read without binding the workspace.
    pub(in crate::local) async fn owes_publication(
        filesystem: &acyclic_fs::LocalFs,
        workspace: acyclic_fs::WorkspaceId,
    ) -> Result<bool> {
        let next = cursor(filesystem, workspace)
            .await?
            .map_or(0, |cursor| cursor.next);
        Ok(pending(filesystem, workspace, next).await?.is_some())
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
    async fn cancelled_enqueue_retains_its_durable_publication_signal() -> Result<()> {
        use std::sync::atomic::Ordering::SeqCst;
        for checkpoint in ["task:submitted", "task:completed"] {
            let (_directory, local) = fixture().await?;
            let workspace = local.workspace.as_ref().unwrap();
            let cancellation = acyclic_fs::CancellationToken::new();
            let head = workspace
                .view()
                .workspace()
                .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            workspace.publication_debt.store(false, SeqCst);
            let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::seeded(89);
            let mut enqueue = Box::pin(schedule.scope(workspace.defer_publication(
                OperationId::new(),
                "child".into(),
                acyclic_fs::GitGenerationRef {
                    workspace_id: workspace.view().workspace().id(),
                    generation: head.id(),
                },
            )));
            let paused = loop {
                tokio::select! {
                    result = &mut enqueue => panic!("enqueue completed before {checkpoint}: {result:?}"),
                    boundary = boundaries.recv() => {
                        let boundary = boundary.expect("enqueue boundary");
                        if boundary.name == checkpoint { break boundary; }
                        boundary.release();
                    }
                }
            };
            drop(enqueue);
            paused.release();
            crate::test_support::run_native_schedule(&schedule, boundaries, schedule.drained())
                .await;
            assert!(workspace.publication_debt.load(SeqCst), "{checkpoint}");
            assert!(
                LocalWorkspace::owes_publication(
                    &workspace.filesystem,
                    workspace.view().workspace().id(),
                )
                .await?,
                "{checkpoint}"
            );
        }
        Ok(())
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
                verifier: &local.verifier,
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
                    commands.fs_arguments("branch child", &scope)?,
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

    /// A child's handback completes while its parent holds the checkout,
    /// and lands before the parent's next command reads the checkout.
    #[tokio::test]
    async fn handback_completes_while_the_parent_is_busy() -> Result<()> {
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
        let configuration = super::super::super::checkout_tools::capture::configuration(workspace)?;
        let commands = super::super::git::Commands {
            workspace,
            signed: &local.signed,
            verifier: &local.verifier,
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
                commands.fs_arguments("branch child", &scope)?,
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
            .write_text("/handed-back", "completed")
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
        // The parent is mid-command for the whole handback.
        let busy = crate::sync::owned::lock_owned(StateArc::clone(&workspace.state)).await;
        commands
            .hand_back(
                &context(OperationId::new()),
                "child".into(),
                acyclic_fs::GitGenerationRef {
                    workspace_id: child_id,
                    generation: cut.id(),
                },
            )
            .await?;
        assert!(LocalWorkspace::owes_publication(&workspace.filesystem, id).await?);
        drop(busy);
        // The parent's next command settles the debt before reading.
        let mut state = crate::sync::owned::lock_owned(StateArc::clone(&workspace.state)).await;
        struct PausedOwner<'a> {
            inner: &'a dyn super::super::owner::ViewOwner,
            entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        }
        impl super::super::owner::ViewOwner for PausedOwner<'_> {
            fn verify(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
                Box::pin(async move {
                    self.inner.verify().await?;
                    let entered = self.entered.lock().unwrap().take();
                    if let Some(entered) = entered {
                        let _ = entered.send(());
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            }
            fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
                self.inner.lease(operation)
            }
        }
        let (entered, draining) = tokio::sync::oneshot::channel();
        let paused = PausedOwner {
            inner: &owner,
            entered: std::sync::Mutex::new(Some(entered)),
        };
        let paused_context = super::super::git::CommandContext {
            owner: &paused,
            operation: OperationId::new(),
            scope: &scope,
            request: None,
        };
        let mut settlement = Box::pin(commands.settle_publications(&paused_context, &mut state));
        tokio::select! {
            entered = draining => entered.map_err(storage)?,
            result = &mut settlement => panic!("settlement completed before cancellation: {result:?}"),
        }
        drop(settlement);
        assert!(
            workspace
                .publication_debt
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        assert!(LocalWorkspace::owes_publication(&workspace.filesystem, id).await?);
        commands
            .settle_publications(&context(OperationId::new()), &mut state)
            .await?;
        assert!(!LocalWorkspace::owes_publication(&workspace.filesystem, id).await?);
        let physical = workspace.selected_workspace(&mut state, &owner).await?;
        assert_eq!(
            std::fs::read(physical.path.join("handed-back")).map_err(storage)?,
            b"completed"
        );
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
