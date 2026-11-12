use super::*;
use acyclic_stream::BoxProviderFuture;
use proptest::prelude::*;

struct PreparedMerge {
    repository: acyclic_fs::GitCompatRepository<acyclic_fs::LocalCoreStateStore>,
    capture: Option<(Arc<LocalWorkspace>, acyclic_fs::GenerationId)>,
    gate: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

impl ViewOwner for PreparedMerge {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let pending = self
                .repository
                .pending_transition()
                .await
                .map_err(storage)?;
            let captured = match &self.capture {
                Some((workspace, before)) => head(workspace.view().into()).await?.id() != *before,
                None => false,
            };
            if captured
                || pending.is_some_and(|pending| {
                    matches!(
                        pending.mutation,
                        acyclic_fs::GitPendingMutation::Join { .. }
                    )
                })
            {
                let gate = self.gate.lock().unwrap().take();
                if let Some((entered, release)) = gate {
                    entered
                        .send(())
                        .map_err(|_| Error::Conflict("merge observer closed".into()))?;
                    release.await.map_err(storage)?;
                }
            }
            Ok(())
        })
    }
}

struct ReconciledCheckout {
    workspace: Arc<LocalWorkspace>,
    entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ViewOwner for ReconciledCheckout {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            use acyclic_fs::GitCompatStore as _;
            let pending = self
                .workspace
                .store
                .load(self.workspace.view().workspace().id())
                .await
                .map_err(storage)?
                .and_then(|state| state.checkout_reconciliation);
            if pending.is_some()
                && matches!(
                    acyclic_fs::OperationWindowCoordinator::new(
                        self.workspace.filesystem.operation_window_store()
                    )
                    .snapshot(self.workspace.view().workspace().id())
                    .await
                    .map_err(storage)?
                    .phase,
                    acyclic_fs::OperationWindowPhase::Idle
                )
            {
                let entered = self.entered.lock().unwrap().take();
                if let Some(entered) = entered {
                    entered
                        .send(())
                        .map_err(|_| Error::Conflict("reconciliation observer closed".into()))?;
                    std::future::pending::<()>().await;
                }
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn default_child_checkout_recovers_parent_updates_and_terminal_conflicts() -> Result<()> {
    use acyclic_fs::GitCompatStore as _;
    for (conflict, unmarked) in [(false, false), (true, false), (false, true), (true, true)] {
        let directory = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(directory.path().join("shared.txt"), b"base\n").map_err(storage)?;
        let harness = LocalHarness::open(
            crate::LocalHarnessConfig::native_host(directory.path())
                .state_directory(state.path())
                .workspace_mounting(WorkspaceMounting::Physical)
                .without_default_tools(),
        )
        .await?;
        let root = harness
            .workspace
            .as_ref()
            .ok_or_else(|| Error::NotFound("root checkout".into()))?;
        let project = crate::conversation::VolumeRef::new(
            harness.project.provider().clone(),
            "child-project".into(),
            crate::conversation::VolumeClass::Project,
            crate::conversation::VolumeOwner::Project("reconciliation-child".into()),
            &mut None,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let (fork, _) = root
            .view()
            .fork(
                &project.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::new(),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)?;
        let grants = crate::Capabilities::new(
            harness
                .signed
                .capabilities()
                .iter()
                .map(str::to_owned)
                .chain([
                    project.capability(VolumeOperation::Read, &mut None)?,
                    project.capability(VolumeOperation::Write, &mut None)?,
                    project.capability(VolumeOperation::Delete, &mut None)?,
                ]),
        );
        let signed = harness
            .root_authority
            .as_ref()
            .ok_or_else(|| Error::NotFound("test authority".into()))?
            .bootstrap_scope("child", &grants, None, &mut None)?;
        let authority = crate::filesystem::NativeMountAuthority::admitted(
            harness.verifier.clone(),
            signed.copy(&mut None)?,
        );
        let bound = root
            .bound_task_view(
                fork,
                &project,
                &harness.session_bindings(),
                &super::super::owner::AgentOwner,
                &authority,
            )
            .await?;
        assert!(
            bound.selected_view().await?.0.is_none(),
            "the default view must itself be the fork"
        );
        assert_ne!(root.view().workspace().id(), bound.view().workspace().id());
        std::fs::write(bound.path.join("own.txt"), b"child edit\n").map_err(storage)?;
        if conflict {
            std::fs::write(bound.path.join("shared.txt"), b"child edit\n").map_err(storage)?;
            root.view()
                .workspace()
                .write_text("/shared.txt", "parent edit\n")
                .await
                .map_err(storage)?;
        } else {
            root.view()
                .workspace()
                .write_text("/parent.txt", "parent edit\n")
                .await
                .map_err(storage)?;
        }
        let scope = RuntimeScope::new(signed.capabilities().clone(), Default::default())?;
        let capture = capture::configuration(&bound);
        let commands = Commands {
            workspace: &bound,
            signed: &signed,
            verifier: &harness.verifier,
            clock: harness.clock.as_ref(),
            capture_options: &capture.0,
            capture_policy: &capture.1,
        };
        if unmarked {
            // A branch-switch close can leave only the core ticket. No
            // checkout marker or expired command lease may be needed to
            // discover and publish its recovery before the next capture.
            let coordinator = acyclic_fs::OperationWindowCoordinator::new(
                bound.filesystem.operation_window_store(),
            );
            let parent = head(root.view().into()).await?.id();
            let lease = coordinator
                .begin(bound.view().workspace().id(), parent, "switch", 1, 3)
                .await
                .map_err(storage)?;
            coordinator
                .observe_parent(bound.view().workspace().id(), parent)
                .await
                .map_err(storage)?;
            assert!(matches!(
                coordinator.finish(&lease, 2).await.map_err(storage)?,
                acyclic_fs::OperationWindowFinish::Reconcile(_)
            ));
            assert!(
                bound
                    .store
                    .load(bound.view().workspace().id())
                    .await
                    .map_err(storage)?
                    .and_then(|state| state.checkout_reconciliation)
                    .is_none()
            );
        }
        let (entered, prepared) = tokio::sync::oneshot::channel();
        let interrupted = ReconciledCheckout {
            workspace: bound.clone(),
            entered: std::sync::Mutex::new(Some(entered)),
        };
        let mut command = commands.invoke(
            commands.fs_arguments("status", &scope)?,
            CommandContext {
                owner: &interrupted,
                operation: OperationId::new(),
                scope: &scope,
                request: None,
            },
            Box::pin(async { Ok(()) }),
        );
        tokio::select! {
            entered = prepared => entered.map_err(storage)?,
            result = &mut command => return Err(Error::Conflict(format!("command completed before reconciliation publication: {result:?}"))),
        }
        drop(command);
        assert!(
            bound
                .store
                .load(bound.view().workspace().id())
                .await
                .map_err(storage)?
                .and_then(|state| state.checkout_reconciliation)
                .is_some()
        );
        // Replay the real reopen path after the core ticket completed, before
        // native publication. A conflict is terminal and must not run again.
        assert!(reconciliation::recover(&bound, bound.view(), &|| async { Ok(()) }).await?);
        assert!(
            bound
                .store
                .load(bound.view().workspace().id())
                .await
                .map_err(storage)?
                .and_then(|state| state.checkout_reconciliation)
                .is_none()
        );
        assert_eq!(
            std::fs::read(bound.path.join("own.txt")).map_err(storage)?,
            b"child edit\n"
        );
        if conflict {
            assert_eq!(
                std::fs::read(bound.path.join("shared.txt")).map_err(storage)?,
                b"child edit\n"
            );
            std::fs::write(bound.path.join("shared.txt"), b"parent edit\n").map_err(storage)?;
        } else {
            assert_eq!(
                std::fs::read(bound.path.join("parent.txt")).map_err(storage)?,
                b"parent edit\n"
            );
        }
        commands
            .invoke(
                commands.fs_arguments("status", &scope)?,
                CommandContext {
                    owner: &super::super::owner::AgentOwner,
                    operation: OperationId::new(),
                    scope: &scope,
                    request: None,
                },
                Box::pin(async { Ok(()) }),
            )
            .await?;
        assert_eq!(
            std::fs::read(bound.path.join("own.txt")).map_err(storage)?,
            b"child edit\n"
        );
    }
    Ok(())
}

#[tokio::test]
async fn child_commands_retain_authority_through_slow_preparation_and_replay() -> Result<()> {
    // GC-172: the recorded root merge failed after 311 seconds. Advance the
    // recovery clock while the real command is parked at its effect boundary;
    // no wall-clock sleep or model is involved. Cover clean and changed forks.
    for (command, changed) in [("merge", false), ("merge", true), ("diff", false)] {
        let directory = tempfile::tempdir().map_err(storage)?;
        let volume = crate::local::tests::volume(directory.path()).await?;
        let local = LocalHarness::open(
            crate::local::tests::volume_config(
                directory.path(),
                volume,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            )?
            .without_default_tools(),
        )
        .await?;
        let workspace = local
            .workspace
            .as_ref()
            .ok_or_else(|| Error::NotFound("checkout".into()))?;
        let scope = RuntimeScope::new(local.signed.capabilities().clone(), Default::default())?;
        let configuration = capture::configuration(workspace);
        let commands = Commands {
            workspace,
            signed: &local.signed,
            verifier: &local.verifier,
            clock: local.clock.as_ref(),
            capture_options: &configuration.0,
            capture_policy: &configuration.1,
        };
        let repository = acyclic_fs::GitCompatRepository::new(
            workspace.view().workspace().id(),
            workspace.store.clone(),
        );
        commands
            .invoke(
                commands.fs_arguments("branch agents/quality-inspection", &scope)?,
                CommandContext {
                    owner: &super::super::owner::AgentOwner,
                    operation: OperationId::new(),
                    scope: &scope,
                    request: None,
                },
                Box::pin(async { Ok(()) }),
            )
            .await?;
        let child_id =
            repository.load().await.map_err(storage)?.branches["agents/quality-inspection"]
                .workspace_id;
        let child = acyclic_fs::WorkspaceResolver::resolve(
            &(
                workspace.filesystem.clone(),
                acyclic_fs::WorkspaceGraph::new(workspace.store.clone()),
            ),
            child_id,
        )
        .await
        .map_err(storage)?;
        if changed {
            child
                .write_text("/finding.md", "retained finding\n")
                .await
                .map_err(storage)?;
        }
        let (entered_tx, entered) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let owner = PreparedMerge {
            repository,
            capture: if command == "diff" {
                let before = head(workspace.view().into()).await?.id();
                std::fs::write(
                    workspace.path.join("parent-before-diff.txt"),
                    b"captured before inspection\n",
                )
                .map_err(storage)?;
                Some((workspace.clone(), before))
            } else {
                None
            },
            gate: std::sync::Mutex::new(Some((entered_tx, release_rx))),
        };
        let operation = OperationId::new();
        let context = || CommandContext {
            owner: &owner,
            operation,
            scope: &scope,
            request: Some(acyclic_fs::GitRequest {
                operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                merge_source: None,
                unanswered: Arc::new(|_| true),
            }),
        };
        let mut merge = Box::pin(commands.invoke(
            commands.fs_arguments(&format!("{command} agents/quality-inspection"), &scope)?,
            context(),
            Box::pin(async { Ok(()) }),
        ));
        tokio::select! {
            entered = entered => entered.map_err(storage)?,
            result = &mut merge => return Err(Error::Conflict(format!("merge completed before preparation: {result:?}"))),
        }
        {
            let now = acyclic_stream::UnixMillisClock::now_unix_millis(
                &acyclic_stream::SystemUnixMillisClock,
            );
            acyclic_fs::OperationWindowCoordinator::new(
                workspace.filesystem.operation_window_store(),
            )
            .recover_workspace(
                workspace.view().workspace(),
                now + 311_000,
                Default::default(),
            )
            .await
            .map_err(storage)?;
            release
                .send(())
                .map_err(|_| Error::Conflict("merge ended before recovery".into()))?;
        }
        let result = merge.await?;
        let replay = commands
            .invoke(
                commands.fs_arguments(&format!("{command} agents/quality-inspection"), &scope)?,
                context(),
                Box::pin(async { Ok(()) }),
            )
            .await?;
        assert_eq!(
            serde_json::to_value(result).map_err(storage)?,
            serde_json::to_value(replay).map_err(storage)?
        );
        assert!(
            owner
                .repository
                .pending_transition()
                .await
                .map_err(storage)?
                .is_none()
        );
        if changed {
            assert_eq!(
                std::fs::read(workspace.path.join("finding.md")).map_err(storage)?,
                b"retained finding\n"
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn reconciliation_keys_retain_both_complete_generation_references(
        operation in any::<[u8; 16]>(),
        digest in any::<[u8; 32]>(),
        locator in any::<[u8; 32]>(),
        root in any::<[u8; 16]>(),
        origin in any::<[u8; 32]>(),
    ) {
        let operation = OperationId::from_bytes(operation);
        let mut object = acyclic_fs::ObjectId {
            kind: acyclic_fs::ObjectKind::GenerationRoot,
            digest: acyclic_fs::Digest::from_bytes(digest),
            storage: Some(acyclic_fs::ObjectStorage {
                locator: acyclic_fs::Digest::from_bytes(locator),
                root_file_id: acyclic_fs::FileId::from_bytes(root),
                origin_generation: acyclic_fs::Digest::from_bytes(origin),
            }),
        };
        let sealed = acyclic_fs::GenerationId::from_root(object);
        let original = sealed.to_bytes();
        let key = reconciliation_operation(operation, b"before", sealed, sealed);

        // Keep the canonical tag and mutate every digest, locator, root and
        // origin byte. Equal plaintext digests must not share replay custody.
        for index in 1..original.len() {
            let mut changed = [0; acyclic_fs::ObjectId::ENCODED_LEN];
            changed.copy_from_slice(original.as_slice());
            changed[index] ^= 1;
            let changed = acyclic_fs::GenerationId::from_bytes(&changed)
                .ok_or_else(|| TestCaseError::fail("canonical reference mutation refused"))?;
            prop_assert_ne!(reconciliation_operation(operation, b"before", changed, sealed), key);
            prop_assert_ne!(reconciliation_operation(operation, b"before", sealed, changed), key);
        }

        object.storage = None;
        let plain = acyclic_fs::GenerationId::from_root(object);
        prop_assert_eq!(plain.digest(), sealed.digest());
        prop_assert_ne!(reconciliation_operation(operation, b"before", plain, sealed), key);
        prop_assert_ne!(reconciliation_operation(operation, b"before", sealed, plain), key);
    }
}
