use super::*;
use crate::{
    PiScratchAllocation, PiScratchBinding, PiTaskBinding, pi_owner::PiOwnerPolicy,
    pi_root_inputs::stage_root_command, pi_runtime::PiTaskRuntime,
};
use acyclic_harness::core::SchemaRegistry;
use acyclic_harness::{
    AgentId, Capabilities,
    conversation::{Limits, VolumeClass, VolumeOwner},
    distributed::Worker,
    filesystem::{TaskWorkerAttempt, TaskWorkerOutcome},
    resources::ProviderRef,
    runtime::{TaskRunLimits, TaskStateProvider},
    scheduler::{LeaseFence, SessionLimits},
};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use std::{path::PathBuf, sync::atomic::Ordering};

struct Fixture {
    directory: tempfile::TempDir,
    session: PathBuf,
    native: PathBuf,
    resources: ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    owner: Arc<PiOwnerConfig>,
    runtime: PiTaskRuntime<LocalStream>,
    request: PiRootRequest,
    lease: acyclic_harness::distributed::WorkLease,
    provider: Arc<PiRootProvider<LocalStream>>,
    startup: NativeMaterializationWork,
    _lease_custody: NativeAllocation,
    _lock: crate::service_process::ServiceLock,
}

impl Fixture {
    async fn new(root: PiRootIntent) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = directory.path().join("native-project");
        std::fs::create_dir(&native).unwrap();
        std::fs::write(native.join("untouched.txt"), b"original native bytes").unwrap();
        let native = native.canonicalize().unwrap();
        let data = directory.path().join("service");
        let (lock, configured, private, startup) =
            crate::tests::configured_test_service_bootstrap(&data)
                .await
                .unwrap();
        let (resources, _) = ServiceResources::open(data, configured, private, &startup)
            .await
            .unwrap();
        let session = resources.session_directory("root-provider");
        std::fs::create_dir_all(&session).unwrap();
        let provider = ProviderRef::new(
            "pi-provider-tests".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )
        .unwrap();
        let agent = AgentId::from_bytes([111; 16]);
        let task = TaskId::from_bytes([112; 16]);
        let journal = VolumeRef::new(
            provider.clone(),
            "journal".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
        .unwrap();
        let scratch = VolumeRef::new(
            provider.clone(),
            "scratch".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
        .unwrap();
        let project = VolumeRef::new(
            provider.clone(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project(hex::encode([113; 16])),
            &mut None,
        )
        .unwrap();
        let grants = Capabilities::new([
            "operation:declare".into(),
            "operation:observe".into(),
            "operation:cancel".into(),
            "operation:wake".into(),
            "task:spawn:acyclic.pi@1".into(),
            "effect:plan".into(),
            "effect:run".into(),
            "conversation:bind".into(),
            format!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"),
            journal
                .directory_read_capability(ROOT_INPUT_DIRECTORY, &mut None)
                .unwrap(),
            scratch
                .capability(VolumeOperation::Read, &mut None)
                .unwrap(),
            scratch
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
            project
                .capability(VolumeOperation::Read, &mut None)
                .unwrap(),
            project
                .capability(VolumeOperation::Write, &mut None)
                .unwrap(),
        ]);
        let owner = PiOwnerConfig::new(
            &PiOwnerPolicy {
                root_task: task,
                agent,
                journal: journal.clone(),
                grants,
                limits: Limits {
                    file_bytes: 1_048_576,
                    path_bytes: 4096,
                    render_bytes: 1_048_576,
                    ..Limits::default()
                },
                run_limits: TaskRunLimits {
                    concurrency: Some(4),
                    max_steps: Some(128),
                    deadline_epoch_ms: None,
                },
                session_limits: SessionLimits {
                    active_tasks: 4,
                    total_tasks: 16,
                    depth: 4,
                    model_steps: 128,
                },
                concurrency: 4,
                maximum_payload_bytes: 1_048_576,
            },
            None,
            &startup,
        )
        .unwrap();
        crate::pi_owner::tests::publish_owner(&owner, &session, &startup).unwrap();
        let filesystem = Arc::new(Host::new(resources.fs.clone(), provider).unwrap());
        filesystem.create_volume(&journal).await.unwrap();
        let (root, projects) = match root {
            PiRootIntent::ScratchOnly {} => (PiRootIntent::ScratchOnly {}, BTreeMap::new()),
            PiRootIntent::Project {
                context_id,
                root_id,
                ..
            } => (
                PiRootIntent::Project {
                    context_id,
                    root_id,
                    paths: BTreeMap::from([(
                        hex::encode(root_id),
                        native.to_str().unwrap().into(),
                    )]),
                },
                BTreeMap::from([(hex::encode(root_id), project)]),
            ),
        };
        let request = PiRootRequest {
            binding: PiTaskBinding {
                native_session_id: "root-provider".into(),
                task_id: task,
                agent_id: agent,
                project_volumes: projects,
                scratch: PiScratchBinding {
                    volume: scratch,
                    allocation: PiScratchAllocation::Preparing,
                },
                results: None,
            },
            root,
        };
        drop(owner);
        let owner = PiOwnerConfig::load_with_native_work(
            &resources.private_state_root,
            session.strip_prefix(&resources.data).unwrap(),
            &startup,
        )
        .unwrap()
        .into_native_shared(&startup)
        .unwrap();
        let command = stage_root_command(&owner, &filesystem, &request, &startup)
            .await
            .unwrap();
        let stream = Arc::new(
            LocalStream::open(session.join("stream"), LocalStreamLimits::default())
                .await
                .unwrap(),
        );
        let runtime = PiTaskRuntime::open_with_native_work(
            stream.clone(),
            filesystem.clone(),
            &journal,
            owner.authority_with_native_work(&startup).unwrap(),
            &startup,
        )
        .await
        .unwrap();
        runtime
            .admit_root(&request.binding, &request.root, &command.0, &startup)
            .await
            .unwrap();
        let worker = Worker {
            id: "pi-root-provider-worker".into(),
            available: Default::default(),
            labels: Default::default(),
        };
        let (first, custody) = {
            let io =
                acyclic_harness::filesystem::PreparationIo::from_native_work(&startup).unwrap();
            let value = runtime
                .runtime
                .run_operation(&worker, task, &runtime.runtime.commands(), 1, Some(&io))
                .await
                .unwrap();
            let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
                unreachable!()
            };
            (
                value,
                custody
                    .into_inner()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            )
        };
        let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = first else {
            panic!("original root lease")
        };
        let fence = LeaseFence::from(&lease.reservation);
        let context = runtime
            .runtime
            .task_context_with_native_work(task, &fence, &startup)
            .await
            .unwrap();
        let preparation = PiRootPreparation::load(
            &resources.private_state_root,
            session.strip_prefix(&resources.data).unwrap(),
            &runtime,
            &filesystem,
            &request.binding,
            crate::pi_recovery::PiTaskOrigin::Root(&request.root),
            Some(context),
            &startup,
        )
        .await
        .unwrap();
        let journal = runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(task, &fence, &startup)
            .await
            .unwrap();
        journal
            .verify_with_native_work(false, &startup)
            .await
            .unwrap();
        let provider = root_provider(
            owner.clone(),
            &resources,
            filesystem.clone(),
            stream.clone(),
            preparation,
            Some(journal.into_native_shared(&startup).unwrap()),
            startup.clone(),
        )
        .unwrap();
        Self {
            directory,
            session,
            native,
            resources,
            filesystem,
            stream,
            owner,
            runtime,
            request,
            lease,
            provider,
            startup,
            _lease_custody: custody,
            _lock: lock,
        }
    }

    async fn execute(&self) -> Result<(EffectStatus, NativeAllocation)> {
        let content = acyclic_harness::runtime::content_residency_verifier(
            Arc::clone(&self.provider.preparation.context),
            self.provider.journal_owner.as_ref().unwrap().clone(),
            Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(&self.startup)?),
        )?;
        let authority = self
            .owner
            .effect_authority_with_native_work(&self.startup)?;
        let effects = crate::pi_owner::root_effects_with_native_work(
            self.stream.clone(),
            authority.0,
            authority.1,
            content,
            self.provider.clone(),
            &self.startup,
        )?;
        effects
            .run_task_effect_with_native_work(
                self.provider.execution_owner()?,
                self.provider.preparation.command.operation_id,
                &self.provider.preparation.plan,
                &self.startup,
            )
            .await
    }

    async fn dispatch(&self) -> EffectDispatch {
        let id = self
            .provider
            .effect_id_with_native_work(&self.startup)
            .unwrap();
        let attempt = EffectAttemptId::from_bytes(id.into_bytes());
        let state = self
            .provider
            .authenticate_with_native_work(attempt, false, &self.startup)
            .await
            .unwrap();
        EffectDispatch {
            provider: state.0.provider.clone(),
            effect_id: id,
            attempt_id: attempt,
            effect_kind: state.0.effect_kind.clone(),
            request: state.0.request.clone(),
            request_digest: state.0.request_digest,
            guarantee: state.0.guarantee,
        }
    }

    async fn reopen(&self) -> Arc<PiRootProvider<LocalStream>> {
        let owner = PiOwnerConfig::load_with_native_work(
            &self.resources.private_state_root,
            self.session.strip_prefix(&self.resources.data).unwrap(),
            &self.startup,
        )
        .unwrap()
        .into_native_shared(&self.startup)
        .unwrap();
        let fence = LeaseFence::from(&self.lease.reservation);
        let context = self
            .runtime
            .runtime
            .task_context_with_native_work(self.request.binding.task_id, &fence, &self.startup)
            .await
            .unwrap();
        let preparation = PiRootPreparation::load(
            &self.resources.private_state_root,
            self.session.strip_prefix(&self.resources.data).unwrap(),
            &self.runtime,
            &self.filesystem,
            &self.request.binding,
            crate::pi_recovery::PiTaskOrigin::Root(&self.request.root),
            Some(context),
            &self.startup,
        )
        .await
        .unwrap();
        let journal = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(self.request.binding.task_id, &fence, &self.startup)
            .await
            .unwrap();
        journal
            .verify_with_native_work(false, &self.startup)
            .await
            .unwrap();
        let (mut resources, _custody) =
            service_resources_with_native_work(&self.resources, &self.startup).unwrap();
        resources.shared_roots = crate::SharedRootRegistry::default();
        root_provider(
            owner,
            &resources,
            self.filesystem.clone(),
            self.stream.clone(),
            preparation,
            Some(journal.into_native_shared(&self.startup).unwrap()),
            self.startup.clone(),
        )
        .unwrap()
    }
}

fn project() -> PiRootIntent {
    PiRootIntent::Project {
        context_id: [114; 16],
        root_id: [113; 16],
        paths: BTreeMap::new(),
    }
}

#[tokio::test]
async fn allocation_retains_original_lazy_roots_and_scratch() {
    for intent in [project(), PiRootIntent::ScratchOnly {}] {
        let fixture = Fixture::new(intent).await;
        assert!(matches!(
            fixture.execute().await.unwrap().0,
            EffectStatus::Succeeded { .. }
        ));
        let dispatch = fixture.dispatch().await;
        let observed = fixture
            .provider
            .observe_resources_with_native_work(
                dispatch.effect_id,
                dispatch.attempt_id,
                dispatch.request_digest,
                &fixture.startup,
            )
            .await
            .unwrap()
            .unwrap();
        observed.0.validate(&fixture.request.binding).unwrap();
        let schema = PiRootAllocation::schema(&fixture.request.binding, &fixture.startup).unwrap();
        let mut schemas = SchemaRegistry::new();
        schemas
            .register(
                "plugin.test.allocation",
                1,
                schema.0,
                [1; 32],
                acyclic_harness::core::ExtensionForkPolicy::Inherit,
                [],
                None,
            )
            .unwrap();
        let authority = acyclic_harness::core::Authority {
            kind: acyclic_harness::core::AggregateKind::Conversation,
            id: "schema-test".into(),
        };
        let issuer =
            acyclic_harness::core::AuthorityIssuer::new("schema-test", [1; 32], authority.clone());
        let reducer = acyclic_harness::core::Reducer::new(
            authority,
            issuer.verifier(&mut None).unwrap(),
            schemas,
        );
        let validate = |value: &serde_json::Value| {
            let bytes = serde_json::to_vec(value).unwrap();
            let descriptor = acyclic_harness::conversation::FileDescriptor::from_bytes(
                &bytes,
                "application/json",
                &mut None,
            )
            .unwrap();
            let file = acyclic_harness::conversation::FileRef::new(
                fixture.owner.policy().journal.clone(),
                "allocation.json".into(),
                "schema-test".into(),
                descriptor,
                "allocation.json".into(),
                &mut None,
            )
            .unwrap();
            reducer
                .schemas()
                .validate_bytes("plugin.test.allocation", 1, &file, &bytes)
        };
        let value = serde_json::to_value(&observed.0).unwrap();
        validate(&value).unwrap();
        for mutation in 0..4 {
            let mut invalid = value.clone();
            match mutation {
                0 if fixture.request.binding.project_volumes.is_empty() => continue,
                0 => invalid["roots"].as_object_mut().unwrap().clear(),
                1 => {
                    invalid["scratch"]["generation"]["provider"]["namespace"] =
                        serde_json::json!("foreign")
                }
                2 => invalid["scratch"]["workspace_id"] = serde_json::json!(vec![0u8; 16]),
                _ => {
                    invalid
                        .as_object_mut()
                        .unwrap()
                        .insert("unknown".into(), serde_json::json!(true));
                }
            }
            assert!(
                !serde_json::from_value::<PiRootAllocation>(invalid.clone())
                    .is_ok_and(|allocation| allocation.validate(&fixture.request.binding).is_ok())
            );
            assert!(validate(&invalid).is_err());
        }
        assert!(
            fixture
                .provider
                .observe_conversation_with_native_work(
                    dispatch.effect_id,
                    dispatch.attempt_id,
                    &fixture.startup
                )
                .await
                .unwrap()
        );
        for (key, handle) in fixture.provider.take_created_handles().await {
            assert_eq!(
                handle.workspace.workspace().id().into_bytes(),
                observed.0.roots[&key].repository_workspace_id
            );
            assert_eq!(
                handle
                    .workspace
                    .read("/untouched.txt", 64)
                    .await
                    .unwrap()
                    .as_ref(),
                b"original native bytes"
            );
        }
        let proof = fixture
            .provider
            .prove_scratch_allocation_with_native_work(dispatch.attempt_id, &fixture.startup)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(proof.0.workspace_id, observed.0.scratch.workspace_id);
        assert_eq!(proof.0.generation, observed.0.scratch.generation);
        if !fixture.request.binding.project_volumes.is_empty() {
            let retained = fixture.directory.path().join("retained-project");
            std::fs::rename(&fixture.native, &retained).unwrap();
            std::fs::create_dir(&fixture.native).unwrap();
            assert!(
                fixture
                    .provider
                    .observe_allocation_with_native_work(dispatch.attempt_id, &fixture.startup)
                    .await
                    .is_err()
            );
            let reopened = fixture.reopen().await;
            let again = reopened
                .prove_scratch_allocation_with_native_work(dispatch.attempt_id, &fixture.startup)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(again.0.workspace_id, proof.0.workspace_id);
            assert_eq!(again.0.generation, proof.0.generation);
            assert_eq!(
                std::fs::read(retained.join("untouched.txt")).unwrap(),
                b"original native bytes"
            );
        } else {
            assert_eq!(
                std::fs::read(fixture.native.join("untouched.txt")).unwrap(),
                b"original native bytes"
            );
        }
    }
}

#[tokio::test]
async fn every_lost_ack_reconciles_original_resources_or_cancellation() {
    for boundary in [1, 2, 3, 4, 5] {
        for cancelled in [false, true] {
            let fixture = Fixture::new(project()).await;
            fixture.provider.fault.store(boundary, Ordering::SeqCst);
            assert!(matches!(
                fixture.execute().await.unwrap().0,
                EffectStatus::Indeterminate
            ));
            let dispatch = fixture.dispatch().await;
            let before = fixture
                .provider
                .observe_resources_with_native_work(
                    dispatch.effect_id,
                    dispatch.attempt_id,
                    dispatch.request_digest,
                    &fixture.startup,
                )
                .await
                .unwrap();
            let name = fixture
                .request
                .binding
                .scratch
                .volume
                .storage_name(&mut None)
                .unwrap();
            let query = crate::pi_fork::tests::work();
            let scratch = fixture
                .resources
                .fs
                .open_workspace_with_native_work(&name, &query, query.cancellation())
                .await
                .ok();
            let workspace = scratch.as_ref().map(|scratch| scratch.id());
            if let Some(scratch) = scratch {
                scratch
                    .write_text("/later.txt", "mutable head advanced")
                    .await
                    .unwrap();
            }
            let reopened = fixture.reopen().await;
            if cancelled {
                fixture
                    .runtime
                    .runtime
                    .task_host()
                    .cancel(fixture.request.binding.task_id, None)
                    .await
                    .unwrap();
                reopened.cancel();
            }
            let observed = reopened
                .reconcile_native(dispatch.attempt_id, &fixture.startup)
                .await
                .unwrap();
            assert_eq!(observed.is_some(), !cancelled || matches!(boundary, 2 | 4));
            if let Some(observed) = observed {
                let EffectStatus::Succeeded { result } = &observed.0.status else {
                    panic!("original resources")
                };
                let allocation = reopened
                    .read_with_native_work::<PiRootAllocation>(result, &fixture.startup)
                    .await
                    .unwrap();
                if let Some(before) = before {
                    assert!(same_allocation(&before.0, &allocation.0));
                }
                if let Some(workspace) = workspace {
                    assert_eq!(allocation.0.scratch.workspace_id, workspace.into_bytes());
                }
                assert_eq!(
                    reopened
                        .reconcile_native(dispatch.attempt_id, &fixture.startup)
                        .await
                        .unwrap()
                        .unwrap()
                        .0,
                    observed.0
                );
            } else {
                assert!(
                    fixture
                        .resources
                        .fs
                        .open_workspace_with_native_work(&name, &query, query.cancellation())
                        .await
                        .is_err()
                );
                assert!(
                    reopened
                        .dispatch_native(dispatch.clone(), &fixture.startup)
                        .await
                        .is_err()
                );
            }
            assert_eq!(
                std::fs::read(fixture.native.join("untouched.txt")).unwrap(),
                b"original native bytes"
            );
        }
    }
}

#[tokio::test]
async fn recovery_rejects_foreign_dispatch_and_replaced_native_identity() {
    let fixture = Fixture::new(project()).await;
    fixture.provider.fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    let original = fixture.dispatch().await;
    for mutation in 0..6 {
        let mut forged = original.clone();
        match mutation {
            0 => forged.attempt_id = EffectAttemptId::from_bytes([119; 16]),
            1 => forged.request_digest = [119; 32],
            2 => forged.provider = "foreign".into(),
            3 => forged.effect_id = EffectId::from_bytes([119; 16]),
            4 => forged.effect_kind = "foreign@1".into(),
            _ => forged.request = fixture.provider.preparation.plan.result_schema.clone(),
        }
        assert!(
            fixture
                .provider
                .dispatch_native(forged, &fixture.startup)
                .await
                .is_err()
        );
    }
    assert!(
        fixture
            .provider
            .reconcile_native(EffectAttemptId::from_bytes([119; 16]), &fixture.startup)
            .await
            .is_err()
    );
    let retained = fixture.directory.path().join("retained");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(
        fixture
            .provider
            .reconcile_native(original.attempt_id, &fixture.startup)
            .await
            .is_err()
    );
    assert!(!fixture.native.join("untouched.txt").exists());
    assert_eq!(
        std::fs::read(retained.join("untouched.txt")).unwrap(),
        b"original native bytes"
    );
}

#[tokio::test]
async fn partial_creation_requires_original_source_intent_and_operation() {
    for boundary in [3, 5] {
        let fixture = Fixture::new(project()).await;
        fixture.provider.fault.store(boundary, Ordering::SeqCst);
        assert!(matches!(
            fixture.execute().await.unwrap().0,
            EffectStatus::Indeterminate
        ));
        let dispatch = fixture.dispatch().await;
        let (key, volume) = fixture
            .request
            .binding
            .project_volumes
            .iter()
            .next()
            .unwrap();
        let operation = fixture
            .provider
            .creation_operation_with_native_work(
                dispatch.effect_id,
                dispatch.attempt_id,
                key,
                &fixture.startup,
            )
            .unwrap();
        let generation = fixture
            .resources
            .fs
            .create_workspace_with_operation_with_native_work(
                &volume.storage_name(&mut None).unwrap(),
                VolumeConfig::native(Lifecycle::Durable),
                operation,
                &fixture.startup,
                fixture.startup.cancellation(),
            )
            .await
            .unwrap();
        let reopened = fixture.reopen().await;
        let recovered = reopened
            .reconcile_native(dispatch.attempt_id, &fixture.startup)
            .await;
        if boundary == 3 {
            assert!(recovered.is_err());
            assert!(
                fixture
                    .resources
                    .fs
                    .open_workspace_with_native_work(
                        &fixture
                            .request
                            .binding
                            .scratch
                            .volume
                            .storage_name(&mut None)
                            .unwrap(),
                        &fixture.startup,
                        fixture.startup.cancellation()
                    )
                    .await
                    .is_err()
            );
        } else {
            let recovered = recovered.unwrap().unwrap();
            assert!(matches!(recovered.0.status, EffectStatus::Succeeded { .. }));
            let original = reopened
                .creation_with_native_work(
                    dispatch.effect_id,
                    dispatch.attempt_id,
                    key,
                    volume,
                    VolumeConfig::native(Lifecycle::Durable),
                    &fixture.startup,
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(original.workspace().id(), generation.workspace().id());
        }
        assert_eq!(
            std::fs::read(fixture.native.join("untouched.txt")).unwrap(),
            b"original native bytes"
        );
    }
    let fixture = Fixture::new(PiRootIntent::ScratchOnly {}).await;
    let name = fixture
        .request
        .binding
        .scratch
        .volume
        .storage_name(&mut None)
        .unwrap();
    let operation = acyclic_fs::OperationId::from_bytes([122; 16]);
    let original = fixture
        .resources
        .fs
        .create_workspace_with_operation_with_native_work(
            &name,
            VolumeConfig::portable(Lifecycle::Durable),
            operation,
            &fixture.startup,
            fixture.startup.cancellation(),
        )
        .await
        .unwrap();
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    let dispatch = fixture.dispatch().await;
    assert!(
        fixture
            .provider
            .reconcile_native(dispatch.attempt_id, &fixture.startup)
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .resources
            .fs
            .open_workspace_with_native_work(
                &name,
                &fixture.startup,
                fixture.startup.cancellation()
            )
            .await
            .unwrap()
            .id(),
        original.workspace().id()
    );
}

#[tokio::test]
async fn root_proofs_reject_foreign_resources_and_exhausted_work() {
    let fixture = Fixture::new(project()).await;
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Succeeded { .. }
    ));
    let handles = fixture.provider.take_created_handles().await;
    let (key, handle) = handles.iter().next().unwrap();
    let snapshot = handle
        .workspace
        .snapshot(
            &acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                &fixture.startup,
                fixture.startup.cancellation(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    for mutation in 0..6 {
        let mut candidate = snapshot;
        let mut volume = &fixture.request.binding.project_volumes[key];
        let mut budget = allocation_budget();
        if mutation == 1 {
            candidate.workspace_id = acyclic_fs::WorkspaceId::from_bytes([130; 16]);
        }
        if mutation == 2 {
            candidate.source.identity = [131; 16];
        }
        if mutation == 3 {
            volume = &fixture.request.binding.scratch.volume;
        }
        if mutation == 5 {
            budget.backend_read_operations = 1;
        }
        let query = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
        if mutation == 4 {
            query.cancellation().cancel();
        }
        let proof = prove_root_project_source_with_native_work(
            fixture.owner.clone(),
            &fixture.resources,
            fixture.filesystem.clone(),
            fixture.stream.clone(),
            &fixture.request.binding,
            &fixture.request.root,
            volume,
            candidate,
            Arc::clone(&fixture.provider.preparation.context),
            fixture.provider.execution_owner().unwrap(),
            &query,
        )
        .await;
        assert_eq!(proof.is_ok(), mutation == 0);
    }
    let substituted = Fixture::new(project()).await;
    substituted.provider.fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        substituted.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    let dispatch = substituted.dispatch().await;
    let mut allocation = substituted
        .provider
        .observe_resources_with_native_work(
            dispatch.effect_id,
            dispatch.attempt_id,
            dispatch.request_digest,
            &substituted.startup,
        )
        .await
        .unwrap()
        .unwrap();
    allocation.0.scratch.workspace_id = [120; 16];
    substituted
        .provider
        .stage_with_native_work(
            dispatch.effect_id,
            dispatch.attempt_id,
            "result",
            &allocation.0,
            &substituted.startup,
        )
        .await
        .unwrap();
    assert!(
        substituted
            .provider
            .reconcile_native(dispatch.attempt_id, &substituted.startup)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn conversation_and_unsettled_scratch_cannot_authorize_creation() {
    for fault in ["operation", "task", "cancel"] {
        let fixture = Fixture::new(PiRootIntent::ScratchOnly {}).await;
        let task = if fault == "task" {
            TaskId::from_bytes([126; 16])
        } else {
            fixture.request.binding.task_id
        };
        let query = crate::pi_fork::tests::work();
        let authority = fixture
            .owner
            .conversation_authority_with_native_work(
                task,
                fixture.request.binding.agent_id,
                &fixture.owner.policy().grants,
                &query,
            )
            .unwrap();
        if fault == "cancel" {
            query.cancellation().cancel();
        }
        let bound = crate::pi_conversation::bind_task_conversation(
            &fixture.stream,
            authority.0,
            authority.1,
            fixture.provider.execution_owner().unwrap(),
            &fixture.request.binding.scratch.volume,
            acyclic_fs::OperationId::from_bytes([125; 16]),
            fixture.owner.policy().limits,
            &query,
        )
        .await;
        if fault == "operation" {
            bound.unwrap();
            assert!(matches!(
                fixture.execute().await.unwrap().0,
                EffectStatus::Indeterminate
            ));
            let dispatch = fixture.dispatch().await;
            assert!(
                fixture
                    .provider
                    .reconcile_native(dispatch.attempt_id, &fixture.startup)
                    .await
                    .is_err()
            );
            for label in ["attachments", "result"] {
                assert!(
                    fixture
                        .provider
                        .original_artifact_with_native_work(
                            dispatch.effect_id,
                            dispatch.attempt_id,
                            label,
                            &fixture.startup
                        )
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        } else {
            assert!(bound.is_err());
        }
        for volume in fixture
            .request
            .binding
            .project_volumes
            .values()
            .chain(std::iter::once(&fixture.request.binding.scratch.volume))
        {
            assert!(
                fixture
                    .resources
                    .fs
                    .open_workspace_with_native_work(
                        &volume.storage_name(&mut None).unwrap(),
                        &fixture.startup,
                        fixture.startup.cancellation()
                    )
                    .await
                    .is_err()
            );
        }
    }
    let fixture = Fixture::new(PiRootIntent::ScratchOnly {}).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    assert!(
        fixture
            .provider
            .prove_scratch_allocation_with_native_work(
                fixture.dispatch().await.attempt_id,
                &fixture.startup
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn exhausted_or_cancelled_allocation_never_creates_selected_workspaces() {
    for fault in ["read", "allocation", "cancel"] {
        let fixture = Fixture::new(project()).await;
        let query = NativeMaterializationWork::new(
            WorkBudget {
                backend_read_operations: 0,
                ..allocation_budget()
            },
            &CancellationToken::new(),
        )
        .unwrap();
        assert!(
            fixture
                .provider
                .supports_guarantee_with_native_work(
                    ROOT_ALLOCATION_EFFECT,
                    EffectGuarantee::AtMostOnce,
                    &query
                )
                .is_err()
        );
        let custody;
        let mut payload = Vec::<u8>::new();
        if fault == "allocation" {
            let bytes = fixture.startup.budget().peak_allocation_bytes
                - fixture.startup.live_allocation_bytes();
            custody = Some(
                fixture
                    .startup
                    .claim(
                        bytes,
                        u64::from(bytes != 0),
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                    .unwrap(),
            );
            payload
                .try_reserve_exact(usize::try_from(bytes).unwrap())
                .unwrap();
            payload.resize(usize::try_from(bytes).unwrap(), 0);
        } else {
            custody = None;
            fixture.provider.cancel();
        }
        let result = fixture.execute().await;
        assert!(result.is_err() || matches!(result.unwrap().0, EffectStatus::Indeterminate));
        let observation = crate::pi_fork::tests::work();
        for volume in fixture
            .request
            .binding
            .project_volumes
            .values()
            .chain(std::iter::once(&fixture.request.binding.scratch.volume))
        {
            assert!(
                fixture
                    .resources
                    .fs
                    .open_workspace_with_native_work(
                        &volume.storage_name(&mut None).unwrap(),
                        &observation,
                        observation.cancellation()
                    )
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            std::fs::read(fixture.native.join("untouched.txt")).unwrap(),
            b"original native bytes"
        );
        drop(payload);
        drop(custody);
    }
}

#[tokio::test]
async fn output_replay_uses_original_content_after_mutable_path_replacement() {
    let fixture = Fixture::new(project()).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(
        fixture.execute().await.unwrap().0,
        EffectStatus::Indeterminate
    ));
    let dispatch = fixture.dispatch().await;
    let original = fixture
        .provider
        .original_artifact_with_native_work(
            dispatch.effect_id,
            dispatch.attempt_id,
            "result",
            &fixture.startup,
        )
        .await
        .unwrap()
        .unwrap();
    let mut allocation = fixture
        .provider
        .read_with_native_work::<PiRootAllocation>(&original.0, &fixture.startup)
        .await
        .unwrap();
    allocation.0.scratch.workspace_id = [124; 16];
    let (_, write) = pi_input_grants(&fixture.owner).unwrap();
    fixture
        .filesystem
        .put_content(
            &fixture.owner.policy().journal,
            &write,
            original.0.path(),
            &serde_json::to_vec(&allocation.0).unwrap(),
            "application/json",
            "result.json",
            fixture.owner.policy().maximum_payload_bytes,
            &IdempotencyKey::new("different-output-operation").unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    let reopened = fixture.reopen().await;
    let result = reopened
        .reconcile_native(dispatch.attempt_id, &fixture.startup)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        result.0.status,
        EffectStatus::Succeeded {
            result: original.0.clone()
        }
    );
    assert_eq!(
        reopened
            .dispatch_native(dispatch.clone(), &fixture.startup)
            .await
            .unwrap()
            .0
            .status,
        result.0.status
    );
    assert_eq!(
        fixture
            .provider
            .dispatch_native(dispatch, &fixture.startup)
            .await
            .unwrap()
            .0
            .status,
        result.0.status
    );
}
