
use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use crate::{PiScratchAllocation, PiScratchBinding, PiTaskBinding, pi_owner::PiOwnerPolicy,
    pi_root_inputs::stage_root_command, pi_runtime::PiTaskRuntime, pi_mail::{PiMailEnvelope, PiMailKind}};
use acyclic_harness::{AgentId, Capabilities,
    conversation::{Limits, VolumeClass, VolumeOwner},
    core::SchemaRegistry,
    distributed::Worker,
    filesystem::{FilesystemContentVerifier, TaskWorkerAttempt, TaskWorkerOutcome},
    resources::ProviderRef,
    runtime::{DurableTaskHost, TaskRunLimits},
    scheduler::{LeaseFence, SessionLimits}};
use acyclic_stream::{LocalStream, LocalStreamLimits};
use std::{path::PathBuf, sync::atomic::Ordering};

type Runtime = PiTaskRuntime<LocalStream>;

struct Fixture {
    directory: tempfile::TempDir,
    session: PathBuf,
    native: PathBuf,
    resources: ServiceResources,
    filesystem: Arc<Host>,
    stream: std::sync::Arc<LocalStream>,
    owner: Arc<PiOwnerConfig>,
    runtime: Runtime,
    request: PiRootRequest,
    lease: acyclic_harness::distributed::WorkLease,
    provider: Arc<PiRootProvider<LocalStream>>,
    startup: NativeMaterializationWork,
    // Data destroys the genuine worker lease before its original native guard.
    _lease_custody: NativeAllocation,
    _service_lock: crate::ServiceLock,
}

impl Fixture {
    async fn new(scratch_only: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let native = directory.path().join("native-project");
        std::fs::create_dir(&native).unwrap();
        std::fs::write(native.join("untouched.txt"), b"original native bytes").unwrap();
        let native = native.canonicalize().unwrap();
        let data = directory.path().join("service");
        let (service_lock, configured, private_root, startup) =
            crate::tests::configured_test_service_bootstrap(&data).await.unwrap();
        let (resources, sessions) = ServiceResources::open(data, configured, private_root, &startup).await.unwrap();
        assert!(sessions.is_empty());
        let session = resources.session_directory("root-provider");
        std::fs::create_dir_all(&session).unwrap();
        let agent = AgentId::from_bytes([111; 16]);
        let task = TaskId::from_bytes([112; 16]);
        let root = [113; 16];
        let provider = ProviderRef::new("pi-provider-tests".into(), "filesystem".into(), "1".into(), &mut None).unwrap();
        let journal = VolumeRef::new(provider.clone(), "journal", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
        let scratch = VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent)).unwrap();
        let project = VolumeRef::new(provider.clone(), "project", VolumeClass::Project, VolumeOwner::Project(hex::encode(root))).unwrap();
        let grants = Capabilities::new([
            "operation:declare".to_owned(), "operation:observe".to_owned(), "operation:cancel".to_owned(),
            "operation:wake".to_owned(), "task:spawn:acyclic.pi@1".to_owned(), "effect:plan".to_owned(), "conversation:bind".to_owned(),
            "effect:run".to_owned(), format!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"),
            journal.directory_read_capability(ROOT_INPUT_DIRECTORY).unwrap(),
            scratch.capability(VolumeOperation::Read).unwrap(), scratch.capability(VolumeOperation::Write).unwrap(),
            project.capability(VolumeOperation::Read).unwrap(), project.capability(VolumeOperation::Write).unwrap(),
        ]);
        let owner = PiOwnerConfig::new(&PiOwnerPolicy { root_task: task, agent, journal: journal.clone(), grants,
            limits: Limits { file_bytes: 1_048_576, path_bytes: 4096, render_bytes: 1_048_576, ..Limits::default() },
            run_limits: TaskRunLimits { concurrency: Some(4), max_steps: Some(128), deadline_epoch_ms: None },
            session_limits: SessionLimits { active_tasks: 4, total_tasks: 16, depth: 4, model_steps: 128 },
            concurrency: 4, maximum_payload_bytes: 1_048_576 }, None, &startup).unwrap();
        crate::pi_owner::tests::publish_owner(&(owner), &session, &crate::pi_fork::tests::work()).unwrap();
        let filesystem = Arc::new(Host::new(resources.fs.clone(), provider).unwrap());
        filesystem.create_volume(&journal).await.unwrap();
        let binding = PiTaskBinding { native_session_id: "root-provider".to_owned(), task_id: task, agent_id: agent,
            project_volumes: if scratch_only { BTreeMap::new() } else { BTreeMap::from([(hex::encode(root), project)]) },
            scratch: PiScratchBinding { volume: scratch, allocation: PiScratchAllocation::Preparing }, results: None };
        let intent = if scratch_only { PiRootIntent::ScratchOnly {} } else { PiRootIntent::Project {
            context_id: [114; 16], root_id: root, paths: BTreeMap::from([(hex::encode(root), native.to_str().unwrap().to_owned())]) } };
        let request = PiRootRequest { binding, root: intent };
        drop(owner);
        let owner = PiOwnerConfig::load_with_native_work(&resources.private_state_root,
            session.strip_prefix(&resources.data).unwrap(), &startup).unwrap()
            .into_native_shared(&startup).unwrap();
        let (command, command_custody) = stage_root_command(&owner, &filesystem, &request, &startup).await.unwrap();
        let stream = Arc::new(LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap());
        let authority = owner.authority_with_native_work(&startup).unwrap();
        let runtime = PiTaskRuntime::open_with_native_work(stream.clone(), filesystem.clone(),
            &journal, authority, &startup).await.unwrap();
        let (admitted_request, request_custody) = request.clone_with_native_work(&startup).unwrap();
        let (admission, admission_custody) = runtime.admit_root(
            &admitted_request.binding, &admitted_request.root, &command, &startup).await.unwrap();
        drop(admission);
        drop(admission_custody);
        drop(request_custody);
        drop(command_custody);
        let (worker_id, worker_id_custody) = startup.display(&format_args!("pi-root-provider-worker")).unwrap();
        let worker = Worker { id: worker_id, available: Default::default(), labels: Default::default() };
        let (first, lease_custody) = runtime.runtime.run_operation_with_native_work(
            &worker, task, &runtime.runtime.commands(), 1, &startup).await.unwrap();
        drop(worker);
        drop(worker_id_custody);
        let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = first.unwrap() else {
            panic!("original native Pi preparation did not yield its command lease");
        };
        let fence = LeaseFence::from(&lease.reservation);
        let context = runtime.runtime.task_context_with_native_work(task, &fence, &startup).await.unwrap();
        let (preparation, journal_owner) = async {
        let preparation = PiRootPreparation::load(&resources.private_state_root, session.strip_prefix(&resources.data).unwrap(), &runtime, &filesystem, &(request).binding, crate::pi_recovery::PiTaskOrigin::Root(&(request).root), Some(Arc::clone(&context)), &startup).await?;
        let owner = (&runtime).runtime.task_host().journal_owner_with_native_work((request).binding.task_id, &fence, &startup).await?;
        owner.verify_with_native_work(false, &startup).await?;
        Ok::<_, acyclic_harness::Error>((preparation, owner))
    }.await.unwrap();
        let journal_owner = journal_owner.into_native_shared(&startup).unwrap();
        let provider = root_provider(owner.clone(), &resources, filesystem.clone(), stream.clone(), preparation, Some(journal_owner), startup.clone()).unwrap();
        Self { directory, session, native, resources, filesystem, stream, owner, runtime,
            request, lease, provider, startup, _lease_custody: lease_custody, _service_lock: service_lock }
    }

    async fn execute(&self) -> Result<(EffectStatus, NativeAllocation)> {
        let current = Arc::clone(self.provider.journal_owner.as_ref().unwrap());
        let content = self.provider.preparation.context.content_residency_verifier_with_native_work(current, &self.startup)?;
        let (authority, custody) = self.owner.effect_authority_with_native_work(&self.startup)?;
        let effects = crate::pi_owner::root_effects_with_native_work(self.stream.clone(), authority, custody,
            content, self.provider.clone(), &self.startup)?;
        effects.run_task_effect_with_native_work(self.provider.execution_owner()?,
            self.provider.preparation.command.operation_id, &self.provider.preparation.plan, &self.startup).await
    }

    async fn dispatch_identity(&self) -> EffectDispatch {
        let id = self.provider.effect_id_with_native_work(&self.startup).unwrap();
        let attempt = EffectAttemptId::from_bytes(id.into_bytes());
        let state = self.provider.authenticate_with_native_work(attempt, false, &self.startup).await.unwrap();
        EffectDispatch { provider: state.0.provider.clone(), effect_id: id, attempt_id: attempt,
            effect_kind: state.0.effect_kind.clone(), request: state.0.request.clone(),
            request_digest: state.0.request_digest, guarantee: state.0.guarantee }
    }

    async fn reopen_provider(&self) -> Arc<PiRootProvider<LocalStream>> {
        let owner = PiOwnerConfig::load_with_native_work(&self.resources.private_state_root,
            self.session.strip_prefix(&self.resources.data).unwrap(), &self.startup).unwrap()
            .into_native_shared(&self.startup).unwrap();
        let fence = LeaseFence::from(&self.lease.reservation);
        let context = self.runtime.runtime.task_context_with_native_work(self.request.binding.task_id,
            &fence, &self.startup).await.unwrap();
        let (preparation, journal_owner) = async {
        let preparation = PiRootPreparation::load(&self.resources.private_state_root, self.session.strip_prefix(&self.resources.data).unwrap(), &self.runtime, &self.filesystem, &(self.request).binding, crate::pi_recovery::PiTaskOrigin::Root(&(self.request).root), Some(Arc::clone(&context)), &self.startup).await?;
        let owner = (&self.runtime).runtime.task_host().journal_owner_with_native_work((self.request).binding.task_id, &fence, &self.startup).await?;
        owner.verify_with_native_work(false, &self.startup).await?;
        Ok::<_, acyclic_harness::Error>((preparation, owner))
    }.await.unwrap();
        let journal_owner = journal_owner.into_native_shared(&self.startup).unwrap();
        let (mut resources, _custody) = service_resources_with_native_work(&self.resources, &self.startup).unwrap();
        resources.shared_roots = crate::SharedRootRegistry::default();
        root_provider(owner, &resources, self.filesystem.clone(), self.stream.clone(), preparation, Some(journal_owner), self.startup.clone()).unwrap()
    }

    async fn assert_no_selected_resources(&self) {
        let observation = crate::pi_fork::tests::work();
        for volume in self.request.binding.project_volumes.values().chain(std::iter::once(&self.request.binding.scratch.volume)) {
            assert!(self.resources.fs.open_workspace_with_native_work(&volume.storage_name().unwrap(), &observation, observation.cancellation()).await.is_err());
        }
        assert_eq!(std::fs::read(self.native.join("untouched.txt")).unwrap(), b"original native bytes");
        assert_eq!(self.resources.shared_roots.live_roots().await, 0);
    }
}

#[tokio::test]
async fn pi_root_provider_allocates_actual_lazy_roots_and_original_scratch_generation() {
    for scratch_only in [false, true] {
        let fixture = Fixture::new(scratch_only).await;
        let succeeded = fixture.execute().await.unwrap();
        assert!(matches!(succeeded.0, EffectStatus::Succeeded { .. }));
        let dispatch = fixture.dispatch_identity().await;
        let original = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
        let allocation = fixture.provider.read_with_native_work::<PiRootAllocation>(&original.0,
            &fixture.startup).await.unwrap();
        let actual = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap().unwrap();
        assert!(same_allocation(&allocation.0, &actual.0));
        assert_eq!(allocation.0.roots.len(), usize::from(!scratch_only));
        let schema = PiRootAllocation::schema(&fixture.request.binding, &fixture.startup).unwrap();
        let mut schemas = SchemaRegistry::new();
        schemas.register("plugin.test.allocation", 1, schema.0, [1; 32],
            acyclic_harness::core::ExtensionForkPolicy::Inherit, [], None).unwrap();
        let authority = acyclic_harness::core::Authority {
            kind: acyclic_harness::core::AggregateKind::Conversation, id: "schema-test".into(),
        };
        let issuer = acyclic_harness::core::AuthorityIssuer::new("schema-test", [1; 32], authority.clone());
        let reducer = acyclic_harness::core::Reducer::new(authority, issuer.verifier(&mut None).unwrap(), schemas);
        let validate = |value: &serde_json::Value| {
            let bytes = serde_json::to_vec(value).unwrap();
            let descriptor = acyclic_harness::conversation::FileDescriptor::from_bytes(
                &bytes, "application/json", &mut None).unwrap();
            let file = acyclic_harness::conversation::FileRef::new(fixture.owner.policy().journal.clone(),
                "allocation.json".into(), "schema-test".into(), descriptor, "allocation.json".into(), &mut None).unwrap();
            reducer.validate_custom_bytes("plugin.test.allocation", 1, &file, &bytes)
        };
        let original_value = serde_json::to_value(&allocation.0).unwrap();
        validate(&original_value).unwrap();
        for case in ["missing_root", "foreign_provider", "zero_workspace", "extra_field"] {
            let mut value = original_value.clone();
            match case {
                "missing_root" if scratch_only => continue,
                "missing_root" => value["roots"].as_object_mut().unwrap().clear(),
                "foreign_provider" => value["scratch"]["generation"]["provider"]["namespace"] = serde_json::json!("foreign"),
                "zero_workspace" => value["scratch"]["workspace_id"] = serde_json::json!(vec![0u8; 16]),
                "extra_field" => { value.as_object_mut().unwrap().insert("unknown".into(), serde_json::json!(true)); }
                _ => unreachable!(),
            }
            assert!(!serde_json::from_value::<PiRootAllocation>(value.clone())
                .is_ok_and(|allocation| allocation.validate(&fixture.request.binding).is_ok()));
            assert!(validate(&value).is_err());
        }
        assert!(fixture.provider.observe_conversation_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, &fixture.startup).await.unwrap());
        let handles = fixture.provider.take_created_handles().await;
        assert_eq!(handles.len(), allocation.0.roots.len());
        for (key, handle) in handles {
            let root = &allocation.0.roots[&key];
            assert_eq!(handle.workspace.workspace().id().into_bytes(), root.repository_workspace_id);
            assert_eq!(handle.physical.source.reference().identity, root.source_identity);
            let state = fixture.resources.store.load_lazy_workspace_with_native_work(
                handle.workspace.workspace().id(), &fixture.startup).await.unwrap().unwrap();
            assert!(state.as_state().parent_workspace_id.is_none());
            assert_eq!(handle.workspace.read("/untouched.txt", 64).await.unwrap().as_ref(), b"original native bytes");
        }
        let preparation = PiRootPreparation::load(&fixture.resources.private_state_root, fixture.session.strip_prefix(&fixture.resources.data).unwrap(), &fixture.runtime, &fixture.filesystem, &(fixture.request).binding, crate::pi_recovery::PiTaskOrigin::Root(&(fixture.request).root), None, &fixture.startup).await.unwrap();
        let observed = observe_root_allocation(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), preparation, &fixture.startup).await.unwrap().unwrap();

        assert_eq!(observed.0.0, original.0);
        assert!(same_allocation(&observed.0.1, &allocation.0));
        assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
        assert_eq!(fixture.startup.receipt().materializations, 0);
    }
}

#[tokio::test]
async fn pi_created_root_guard_uses_actual_readonly_owner_without_snapshot_or_fresh_lease() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Succeeded { .. }));
    let dispatch = fixture.dispatch_identity().await;
    let result = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
    let allocation = fixture.provider.read_with_native_work::<PiRootAllocation>(
        &result.0, &fixture.startup).await.unwrap();
    let (readonly_owner, readonly_context) = fixture.runtime.runtime.readonly_task_context_with_native_work(
        fixture.request.binding.task_id, &fixture.startup).await.unwrap();
    assert!(readonly_owner.verify_with_native_work(false, &fixture.startup).await.is_err());
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem, &(fixture.request).binding, &(fixture.request).root, Arc::clone(&readonly_context), &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    let identity = guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), preparation,
    &readonly_owner, key, &allocation.0, &fixture.startup).await.unwrap();
    assert_eq!(identity.to_bytes(), allocation.0.roots[key].native_root_identity);
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 1);
    assert_eq!(fixture.startup.receipt().materializations, before.materializations);
    let mut substituted = fixture.provider.read_with_native_work::<PiRootAllocation>(
        &result.0, &fixture.startup).await.unwrap();
    substituted.0.roots.get_mut(key).unwrap().repository_workspace_id = [201; 16];
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem, &(fixture.request).binding, &(fixture.request).root, Arc::clone(&readonly_context), &fixture.startup).await.unwrap();
    assert!(guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), preparation, &readonly_owner,
        key, &substituted.0, &fixture.startup).await.is_err());
    let original_native = fixture.directory.path().join("created-original-native-root");
    std::fs::rename(&fixture.native, &original_native).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem, &(fixture.request).binding, &(fixture.request).root, readonly_context, &fixture.startup).await.unwrap();
    assert!(guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), preparation, &readonly_owner,
        key, &allocation.0, &fixture.startup).await.is_err());
    assert_eq!(std::fs::read(original_native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_provider_empty_budget_and_precancellation_never_probe_selected_roots() {
    let fixture = Fixture::new(false).await;
    let wrong = NativeMaterializationWork::new(WorkBudget {
        backend_read_operations: 0, ..allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let before = wrong.receipt();
    assert!(fixture.provider.supports_guarantee_with_native_work(ROOT_ALLOCATION_EFFECT,
        EffectGuarantee::AtMostOnce, &wrong).is_err());
    assert_eq!(wrong.receipt(), before);
    fixture.provider.cancel();
    let outcome = fixture.execute().await;
    assert!(outcome.is_err() || matches!(outcome.unwrap().0, EffectStatus::Indeterminate));
    fixture.assert_no_selected_resources().await;
}

#[tokio::test]
async fn pi_root_original_span_exhaustion_denies_before_creation_and_releases_real_payload_custody() {
    let fixture = Fixture::new(false).await;
    let baseline = fixture.startup.live_allocation_bytes();
    let bytes = fixture.startup.budget().peak_allocation_bytes - baseline;
    let payload_custody = fixture.startup.claim(bytes, u64::from((bytes) != 0), acyclic_fs::WorkCounters::UNCHARGED).unwrap();
    let mut payload = Vec::<u8>::new();
    payload.try_reserve_exact(usize::try_from(bytes).unwrap()).unwrap();
    payload.resize(usize::try_from(bytes).unwrap(), 0);
    assert_eq!(fixture.startup.live_allocation_bytes(), fixture.startup.budget().peak_allocation_bytes);
    let before = fixture.startup.receipt();
    let failure = fixture.execute().await.err().unwrap();
    assert!(matches!(&failure, Error::NativeWork {
        cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { .. }), ..
    }));
    fixture.assert_no_selected_resources().await;
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 0);
    assert_eq!(fixture.startup.receipt().materializations, before.materializations);
    drop(failure);
    drop(payload);
    drop(payload_custody);
    assert_eq!(fixture.startup.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn pi_root_provider_all_lost_ack_boundaries_resume_original_operations_without_new_resources() {
    for boundary in [1, 2, 3, 4, 5] {
        for cancelled in [false, true] {
            let fixture = Fixture::new(false).await;
            fixture.provider.fault.store(boundary, Ordering::SeqCst);
            assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
            let dispatch = fixture.dispatch_identity().await;
            let before = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
                dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap();
            let scratch_name = fixture.request.binding.scratch.volume.storage_name().unwrap();
            let original_scratch = fixture.resources.fs.open_workspace_with_native_work(&scratch_name, &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.ok();
            let original_workspace = original_scratch.as_ref().map(|workspace| workspace.id());
            if let Some(scratch) = original_scratch {
                scratch.write_text("/later.txt", "mutable head advanced").await.unwrap();
            }
            let reopened = fixture.reopen_provider().await;
            if cancelled {
                fixture.runtime.runtime.task_host().cancel(fixture.request.binding.task_id, None).await.unwrap();
                reopened.cancel();
            }
            let observed = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap();
            assert_eq!(observed.is_some(), !cancelled || matches!(boundary, 2 | 4));
            if let Some(observed) = observed {
                let EffectStatus::Succeeded { result } = &observed.0.status else { panic!("original resources were not recovered"); };
                let recorded = reopened.read_with_native_work::<PiRootAllocation>(result, &fixture.startup).await.unwrap();
                if let Some(before) = &before { assert!(same_allocation(&before.0, &recorded.0)); }
                if let Some(id) = original_workspace { assert_eq!(recorded.0.scratch.workspace_id, id.into_bytes()); }
                let repeated = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();

                assert_eq!(repeated.0, observed.0);
            } else {
                assert!(fixture.resources.fs.open_workspace_with_native_work(&scratch_name, &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
                assert!(reopened.dispatch_native(dispatch.clone(), &fixture.startup).await.is_err());
            }
            assert_eq!(fixture.startup.receipt().materializations, 0);
            assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
        }
    }
}

#[tokio::test]
async fn pi_root_reconcile_adopts_original_workspace_created_before_its_lazy_binding_ack() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(5, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let inputs = fixture.provider.capture_inputs_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap();
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    let volume = &fixture.request.binding.project_volumes[key];
    let operation = fixture.provider.creation_operation_with_native_work(
        dispatch.effect_id, dispatch.attempt_id, key, &fixture.startup).unwrap();
    let name = {
        let mut io = acyclic_harness::store::history::native_work_io(&fixture.startup).unwrap();
        let value = volume.storage_name(&mut Some(&mut io)).unwrap();
        (value, io.into_custody())
    };
    let first = fixture.resources.fs.create_workspace_with_operation_with_native_work(
        &name.0, VolumeConfig::native(Lifecycle::Durable), operation,
        &fixture.startup, fixture.startup.cancellation()).await.unwrap();
    assert!(fixture.resources.store.load_lazy_workspace_with_native_work(
        first.workspace().id(), &fixture.startup).await.unwrap().is_none());
    let reopened = fixture.reopen_provider().await;
    let observed = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();
    assert!(matches!(observed.0.status, EffectStatus::Succeeded { .. }));
    let original = reopened.creation_with_native_work(dispatch.effect_id, dispatch.attempt_id,
        key, volume, VolumeConfig::native(Lifecycle::Durable), &fixture.startup).await.unwrap().unwrap();
    assert_eq!(original.id(), first.id());
    assert_eq!(original.workspace().id(), first.workspace().id());
    let state = fixture.resources.store.load_lazy_workspace_with_native_work(
        first.workspace().id(), &fixture.startup).await.unwrap().unwrap();
    assert_eq!(state.as_state().source.identity, inputs.0[key].source.identity);
    assert_eq!(fixture.startup.receipt().materializations, 0);
    assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_provider_rejects_foreign_attempt_digest_provider_and_substituted_native_identity() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(2, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let original = fixture.dispatch_identity().await;
    for field in ["attempt", "digest", "provider", "effect", "kind", "request"] {
        let mut forged = original.clone();
        match field {
            "attempt" => forged.attempt_id = EffectAttemptId::from_bytes([119; 16]),
            "digest" => forged.request_digest = [119; 32],
            "provider" => forged.provider = "foreign".into(),
            "effect" => forged.effect_id = EffectId::from_bytes([119; 16]),
            "kind" => forged.effect_kind = "foreign@1".into(),
            "request" => forged.request = fixture.provider.preparation.plan.result_schema.clone(),
            _ => unreachable!(),
        }
        assert!(fixture.provider.dispatch_native(forged, &fixture.startup).await.is_err());
    }
    assert!(fixture.provider.reconcile_native(EffectAttemptId::from_bytes([119; 16]), &fixture.startup).await.is_err());
    let retained = fixture.directory.path().join("original-retained");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(fixture.provider.reconcile_native(original.attempt_id, &fixture.startup).await.is_err());
    assert_eq!(fixture.startup.receipt().materializations, 0);
    assert!(!fixture.native.join("untouched.txt").exists());
    assert_eq!(std::fs::read(retained.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_provider_rejects_resource_id_substitution_and_retains_original_failure_prefix() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(2, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let mut allocation = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap().unwrap();
    allocation.0.scratch.workspace_id = [120; 16];
    fixture.provider.stage_with_native_work(dispatch.effect_id, dispatch.attempt_id,
        "result", &allocation.0, &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    assert!(fixture.provider.reconcile_native(dispatch.attempt_id, &fixture.startup).await.is_err());
    let after = fixture.startup.receipt();
    assert_eq!(after.materializations, before.materializations);
}

#[tokio::test]
async fn pi_root_provider_never_adopts_a_selected_name_created_by_another_operation() {
    let fixture = Fixture::new(true).await;
    let volume = &fixture.request.binding.scratch.volume;
    let foreign = acyclic_fs::OperationId::from_bytes([122; 16]);
    let name = volume.storage_name().unwrap();
    let initial = fixture.resources.fs.create_workspace_with_operation_with_native_work(&name,
        VolumeConfig::portable(Lifecycle::Durable), foreign, &fixture.startup, fixture.startup.cancellation()).await.unwrap();
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let original = fixture.dispatch_identity().await;
    assert!(fixture.provider.reconcile_native(original.attempt_id, &fixture.startup).await.is_err());
    let actual = fixture.resources.fs.workspace_for_creation_operation_with_native_work(&name,
        VolumeConfig::portable(Lifecycle::Durable), foreign, &fixture.startup, fixture.startup.cancellation()).await.unwrap().unwrap();
    assert_eq!(actual.id(), initial.id());
    assert_eq!(actual.workspace().id(), initial.workspace().id());
    assert!(fixture.provider.original_artifact_with_native_work(original.effect_id,
        original.attempt_id, "result", &fixture.startup).await.unwrap().is_none());
}

#[tokio::test]
async fn pi_root_provider_lost_output_ack_ignores_a_replaced_mutable_output_path() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let original = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
    let mut changed = fixture.provider.read_with_native_work::<PiRootAllocation>(&original.0, &fixture.startup).await.unwrap();
    changed.0.scratch.workspace_id = [124; 16];
    let (write, _write_custody) = pi_input_write_grant_with_native_work(&fixture.owner, &crate::pi_fork::tests::work()).unwrap();
    let substituted = fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, original.0.path(),
        &serde_json::to_vec(&changed.0).unwrap(), "application/json", "result.json",
        fixture.owner.policy().maximum_payload_bytes, &IdempotencyKey::new("different-output-operation").unwrap()).await.unwrap();
    assert_ne!(original.0, substituted);
    let reopened = fixture.reopen_provider().await;
    let before = fixture.startup.receipt();
    let result = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();

    assert_eq!(result.0.status, EffectStatus::Succeeded { result: original.0.clone() });
    let repeated = reopened.dispatch_native(dispatch.clone(), &fixture.startup).await.unwrap();

    assert_eq!(repeated.0.status, result.0.status);
    let duplicate = fixture.provider.dispatch_native(dispatch, &fixture.startup).await.unwrap();

    assert_eq!(duplicate.0.status, result.0.status);
    assert!(reopened.take_created_handles().await.is_empty());
    assert_eq!(fixture.startup.receipt().source_entries_visited, before.source_entries_visited);
    assert_eq!(fixture.startup.receipt().materializations, before.materializations);
}

#[tokio::test]
async fn pi_root_provider_rejects_a_real_conversation_bound_by_a_different_creation() {
    let fixture = Fixture::new(false).await;
    let (authority, custody) = fixture.owner.conversation_authority_with_native_work(
        fixture.request.binding.task_id, fixture.request.binding.agent_id,
        &fixture.owner.policy().grants, &fixture.startup).unwrap();
    crate::pi_conversation::bind_task_conversation(&fixture.stream, authority, custody,
        fixture.provider.execution_owner().unwrap(), &fixture.request.binding.scratch.volume,
        acyclic_fs::OperationId::from_bytes([125; 16]),
        fixture.owner.policy().limits, &fixture.startup).await.unwrap();
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    fixture.assert_no_selected_resources().await;
    let dispatch = fixture.dispatch_identity().await;
    assert!(fixture.provider.reconcile_native(dispatch.attempt_id, &fixture.startup).await.is_err());
    for label in ["attachments", "result"] {
        assert!(fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, label, &fixture.startup).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn pi_root_conversation_binding_rejects_foreign_task_and_cancellation_without_a_publication() {
    let fixture = Fixture::new(true).await;
    let (authority, custody) = fixture.owner.conversation_authority_with_native_work(
        TaskId::from_bytes([126; 16]), fixture.request.binding.agent_id,
        &fixture.owner.policy().grants, &fixture.startup).unwrap();
    assert!(crate::pi_conversation::bind_task_conversation(&fixture.stream, authority, custody,
        fixture.provider.execution_owner().unwrap(), &fixture.request.binding.scratch.volume,
        acyclic_fs::OperationId::from_bytes([127; 16]),
        fixture.owner.policy().limits, &fixture.startup).await.is_err());
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    let (authority, custody) = fixture.owner.conversation_authority_with_native_work(
        fixture.request.binding.task_id, fixture.request.binding.agent_id,
        &fixture.owner.policy().grants, &work).unwrap();
    cancellation.cancel();
    let before = work.receipt();
    assert!(crate::pi_conversation::bind_task_conversation(&fixture.stream, authority, custody,
        fixture.provider.execution_owner().unwrap(), &fixture.request.binding.scratch.volume,
        acyclic_fs::OperationId::from_bytes([127; 16]),
        fixture.owner.policy().limits, &work).await.is_err());
    assert_eq!(work.receipt(), before);
    fixture.assert_no_selected_resources().await;
}

#[tokio::test]
async fn pi_root_private_scratch_proof_is_original_readonly_and_never_probes_user_projects() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Succeeded { .. }));
    let dispatch = fixture.dispatch_identity().await;
    let observed = fixture.provider.observe_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.unwrap().unwrap();

    let before = fixture.startup.receipt();
    let proof = fixture.provider.prove_scratch_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.unwrap().unwrap();
    let binding = &fixture.provider.preparation.request.binding;
    assert_eq!(binding.task_id, fixture.request.binding.task_id);
    assert_eq!(binding.agent_id, fixture.request.binding.agent_id);
    assert_eq!(binding.scratch.volume, fixture.request.binding.scratch.volume);
    assert_eq!(proof.0.workspace_id, observed.0.1.scratch.workspace_id);
    assert_eq!(proof.0.generation, observed.0.1.scratch.generation);
    let output = fixture.provider.original_artifact_with_native_work(
        dispatch.effect_id, dispatch.attempt_id, "result", &fixture.startup,
    ).await.unwrap().unwrap();
    assert_eq!(output.0, observed.0.0);
    assert_eq!(proof.1, fixture.provider.creation_operation_with_native_work(
        dispatch.effect_id, dispatch.attempt_id, "scratch", &fixture.startup).unwrap());
    assert_eq!(fixture.startup.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(fixture.startup.receipt().source_path_components, before.source_path_components);
    let retained = fixture.directory.path().join("retained-user-project");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(fixture.provider.observe_allocation_with_native_work(dispatch.attempt_id, &fixture.startup).await.is_err());
    let preparation = PiRootPreparation::load(&fixture.resources.private_state_root, fixture.session.strip_prefix(&fixture.resources.data).unwrap(), &fixture.runtime, &fixture.filesystem, &(fixture.request).binding, crate::pi_recovery::PiTaskOrigin::Root(&(fixture.request).root), None, &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    let observer = root_provider(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), preparation, None, fixture.startup.clone()).unwrap();
    let effect = observer.effect_id_with_native_work(&fixture.startup).unwrap();
    let second = observer.prove_scratch_allocation_with_native_work(
        EffectAttemptId::from_bytes(effect.into_bytes()), &fixture.startup,
    ).await.unwrap().unwrap();
    assert_eq!(second.0.workspace_id, proof.0.workspace_id);
    assert_eq!(second.0.generation, proof.0.generation);
    assert_eq!(fixture.startup.receipt().source_path_components, before.source_path_components);
    assert_eq!(fixture.startup.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(std::fs::read(retained.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_scratch_proof_never_promotes_an_unsettled_allocation() {
    let fixture = Fixture::new(true).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    assert!(fixture.provider.prove_scratch_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.is_err());
}

#[tokio::test]
async fn pi_root_project_source_proof_uses_original_operations_and_callers_cumulative_quota() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Succeeded { .. }));
    let handles = fixture.provider.take_created_handles().await;
    let (key, handle) = handles.iter().next().unwrap();
    let snapshot = handle.workspace.snapshot(&acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(&fixture.startup, fixture.startup.cancellation()).unwrap()).await.unwrap();
    let project = &fixture.request.binding.project_volumes[key];
    let before = fixture.startup.receipt();
    prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, snapshot,
    Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.unwrap();
    let after = fixture.startup.receipt();
    assert!(after.authority_records_read > before.authority_records_read);
    assert_eq!(after.backend_write_operations, before.backend_write_operations);
    assert_eq!(after.materializations, before.materializations);
    let mut foreign = snapshot;
    foreign.workspace_id = acyclic_fs::WorkspaceId::from_bytes([130; 16]);
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, foreign,
        Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    foreign = snapshot;
    foreign.source.identity = [131; 16];
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, foreign,
        Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, &fixture.request.binding.scratch.volume,
        snapshot, Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    let cancellation = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let before = cancelled.receipt();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, snapshot,
        Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &cancelled).await.is_err());
    assert_eq!(cancelled.receipt(), before);
    let limited = NativeMaterializationWork::new(WorkBudget { backend_read_operations: 1, ..allocation_budget() },
        &CancellationToken::new()).unwrap();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, snapshot,
        Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &limited).await.is_err());
    assert!(limited.receipt().backend_read_operations <= 1);
    let retained = fixture.directory.path().join("source-proof-retained");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request.binding, &fixture.request.root, project, snapshot,
        Arc::clone(&fixture.provider.preparation.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    assert_eq!(std::fs::read(retained.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_partial_creation_without_original_source_intent_is_not_recaptured() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(3, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let (key, project) = fixture.request.binding.project_volumes.iter().next().unwrap();
    let operation = fixture.provider.creation_operation_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, key, &fixture.startup).unwrap();
    let generation = fixture.resources.fs.create_workspace_with_operation_with_native_work(&project.storage_name().unwrap(),
        VolumeConfig::native(Lifecycle::Durable), operation, &fixture.startup, fixture.startup.cancellation()).await.unwrap();
    let before = fixture.startup.receipt();
    assert!(fixture.provider.reconcile_native(dispatch.attempt_id, &fixture.startup).await.is_err());
    let after = fixture.startup.receipt();
    assert_eq!(after.backend_write_operations, before.backend_write_operations);
    assert_eq!(after.source_entries_visited, before.source_entries_visited);
    assert_eq!(after.materializations, before.materializations);
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 0);
    assert_eq!(fixture.resources.fs.open_workspace_with_native_work(&project.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.unwrap().id(), generation.workspace().id());
    assert!(fixture.resources.fs.open_workspace_with_native_work(&fixture.request.binding.scratch.volume.storage_name().unwrap(), &crate::pi_fork::tests::work(), &acyclic_fs::CancellationToken::new()).await.is_err());
    assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_output_context_preserves_actual_task_and_stages_only_original_scratch() {
    let fixture = Fixture::new(true).await;
    assert!(matches!(fixture.execute().await.unwrap().0, EffectStatus::Succeeded { .. }));
    let current_owner = fixture.provider.journal_owner.as_ref().unwrap();
    let context = Arc::clone(&fixture.provider.context);
    let output = bind_root_output_context_with_native_work(Arc::clone(&context), current_owner, fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), &(fixture.request).binding, &(fixture.request).root, &fixture.startup).await.unwrap();
    assert_eq!(output.context().durable_task_id(), Some(fixture.request.binding.task_id));
    assert_eq!(output.context().scope().grants(), context.context().scope().grants());
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(!output.context().scope().grants().contains(&fixture.owner.policy().journal.capability(operation).unwrap()));
    }
    let (published, custody) = output.context().stage_file_once_with_native_work(
        OperationId::from_bytes([133; 16]), "root/checkpoint.json", b"{\"actual\":\"root scratch\"}",
        "application/json", "checkpoint.json", &fixture.startup).await.unwrap();
    let published = (published, custody);
    assert_eq!(published.0.volume(), &fixture.request.binding.scratch.volume);
    assert_ne!(published.0.volume(), &fixture.owner.policy().journal);
    let effect = fixture.provider.effect_id_with_native_work(&fixture.startup).unwrap();
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let operation = fixture.provider.creation_operation_with_native_work(effect, attempt, "scratch", &fixture.startup).unwrap();
    let generation = fixture.provider.creation_with_native_work(effect, attempt, "scratch",
        &fixture.request.binding.scratch.volume, VolumeConfig::portable(Lifecycle::Durable),
        &fixture.startup).await.unwrap().unwrap();
    let authority = fixture.owner.conversation_authority_with_native_work(
        fixture.request.binding.task_id, fixture.request.binding.agent_id, &fixture.owner.policy().grants,
        &fixture.startup).unwrap();
    let verifier = {
        let mut io = acyclic_harness::store::history::native_work_io(&fixture.startup).unwrap();
        let value = authority.0.1.verifier(&mut Some(&mut io)).unwrap();
        (value, io.into_custody())
    };
    assert!(fixture.filesystem.root_output_source(current_owner,
        &fixture.request.binding.scratch.volume, operation, &generation, &verifier.0,
        &authority.0.2, &fixture.startup).await.is_ok());
    assert!(fixture.filesystem.root_output_source(current_owner,
        &fixture.owner.policy().journal, operation, &generation, &verifier.0,
        &authority.0.2, &fixture.startup).await.is_err());
    assert!(fixture.filesystem.root_output_source(current_owner,
        &fixture.request.binding.scratch.volume, acyclic_fs::OperationId::from_bytes([199; 16]),
        &generation, &verifier.0, &authority.0.2, &fixture.startup).await.is_err());
    let foreign_generation = fixture.resources.fs.create_workspace_with_operation_with_native_work(
        "pi-root-output-foreign-generation", VolumeConfig::portable(Lifecycle::Durable),
        acyclic_fs::OperationId::from_bytes([198; 16]), &fixture.startup,
        fixture.startup.cancellation()).await.unwrap();
    assert!(fixture.filesystem.root_output_source(current_owner,
        &fixture.request.binding.scratch.volume, operation, &foreign_generation, &verifier.0,
        &authority.0.2, &fixture.startup).await.is_err());
    fixture.runtime.runtime.task_host().cancel(fixture.request.binding.task_id, None).await.unwrap();
    assert!(fixture.filesystem.root_output_source(current_owner,
        &fixture.request.binding.scratch.volume, operation, &generation, &verifier.0,
        &authority.0.2, &fixture.startup).await.is_err());
    assert!(output.context().stage_file_once_with_native_work(OperationId::from_bytes([197; 16]),
        "root/after-owner-cancel.json", b"{}", "application/json", "cancelled.json",
        &fixture.startup).await.is_err());
    let cancellation = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let before = cancelled.receipt();
    assert!(bind_root_output_context_with_native_work(context, current_owner, fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), &(fixture.request).binding, &(fixture.request).root, &cancelled).await.is_err());
    assert_eq!(cancelled.receipt(), before);
}

#[tokio::test]
async fn pi_operator_input_original_upload_is_root_only_even_for_actual_created_child_context() {
    let fixture = crate::pi_fork::tests::Fixture::new(None).await;
    let operation = OperationId::from_bytes([137; 16]);
    let signed = fixture.owner.authority().unwrap().signed;
    let native = crate::pi_fork::tests::work();
    let (operator, _operator_custody) = stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope", &signed, &native).await.unwrap();
    assert_eq!(operator.path(), format!("{OPERATOR_INPUT_DIRECTORY}/{operation}/scope.json"));
    assert!(stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope",
        &serde_json::json!({"different":"scope"}), &native).await.is_err());
    let (regular, _regular_custody) = stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "scope", &serde_json::json!({"not":"a bearer scope"}), &native).await.unwrap();
    assert_ne!(regular, operator);
    let (write, _write_custody) = pi_input_write_grant_with_native_work(&fixture.owner, &crate::pi_fork::tests::work()).unwrap();
    fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, operator.path(), b"{\"changed\":true}",
        "application/json", "scope.json", fixture.owner.policy().maximum_payload_bytes,
        &IdempotencyKey::new("different-operator-path-upload").unwrap()).await.unwrap();
    let retry = stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope", &signed, &native).await.unwrap();
    assert_eq!(retry.0, operator);
    let plan = fixture.plan(&crate::pi_fork::tests::work()).await;
    let coordinator = fixture.coordinator(&crate::pi_fork::tests::work()).unwrap();
    let child = fixture.advance_with(&coordinator, &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.root_handle.workspace.clone(), allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let attempt = fixture.runtime.runtime.run_operation_with_native_work(&Worker { id: "operator-child-read-denial".into(),
        available: Default::default(), labels: Default::default() }, child.binding.task_id,
        &fixture.runtime.runtime.commands(), 1, &native).await.unwrap();
    let Some(TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease })) = attempt.0 else { panic!("actual created child lease"); };
    let context = fixture.runtime.runtime.task_context(child.binding.task_id, LeaseFence::from(&lease.reservation)).await.unwrap();
    assert!(context.scope().grants().contains(&fixture.owner.policy().journal.directory_read_capability(ROOT_INPUT_DIRECTORY).unwrap()));
    assert!(!context.scope().grants().contains(&fixture.owner.policy().journal.directory_read_capability(OPERATOR_INPUT_DIRECTORY).unwrap()));
    assert!(context.read_file(&operator).await.is_err());
    assert_eq!(context.read_file(&regular).await.unwrap(), b"{\"not\":\"a bearer scope\"}");
    assert!(context.read_file_with_native_work(&operator, &native).await.is_err());
    let (decoded, decoded_custody) = read_pi_input_with_native_work::<BTreeMap<String, String>>(&fixture.owner, &fixture.filesystem, operation, "scope", 4096, JsonValueSchema::None, 0, &context, &native)
        .await.unwrap().unwrap();
    assert_eq!(decoded.get("not").map(String::as_str), Some("a bearer scope"));
    assert!(decoded_custody.work().shares_ledger(&native));
    drop(decoded);
    drop(decoded_custody);
    let cancellation = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let failure = stage_pi_operator_input(&fixture.owner, &fixture.filesystem, OperationId::from_bytes([138; 16]), "scope", &signed, &cancelled).await.err().unwrap();
    assert!(matches!(failure, Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. }));
}

#[test]
fn guarded_pi_decode_rejects_original_quota_and_cancel_without_retaining_partial_dto() {
    let bytes = br#"{"kind":"message","message":"native-owned"}"#;
    let owned = (bytes.len() as u64) * 4;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + owned,
        ..allocation_budget()
    }, &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let failure = guarded_pi_decode::<PiMailEnvelope>(bytes, 8192, owned, acyclic_harness::wire::ErrorCode::Invalid, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["PiMailEnvelope", "PiMailKind", "kind", "message", "steer"]), &work).err().unwrap();
    assert!(matches!(&failure, Error::NativeWork {
        cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { .. }), ..
    }));
    assert_eq!(work.live_allocation_bytes(), baseline);
    drop(failure);
    let work = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    assert!(guarded_pi_decode::<PiMailEnvelope>(br#"{"kind":"message","message":[]}"#, 8192, owned, acyclic_harness::wire::ErrorCode::Invalid, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["PiMailEnvelope", "PiMailKind", "kind", "message", "steer"]), &work).is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(guarded_pi_decode::<PiMailEnvelope>(bytes, 8192, owned, acyclic_harness::wire::ErrorCode::Invalid, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["PiMailEnvelope", "PiMailKind", "kind", "message", "steer"]), &work),
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn pi_native_input_stage_and_observe_retain_original_key_and_immutable_receipt() {
    use acyclic_harness::conversation::ContentResidencyVerifier;
    let fixture = Fixture::new(true).await;
    let operation = OperationId::from_bytes([141; 16]);
    let payload = PiMailEnvelope { kind: PiMailKind::Message, message: "original immutable message".into() };
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    assert!(stage_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "../native-mail", &payload, &work).await.is_err());
    assert!(observe_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "native-mail", &work).await.unwrap().is_none());
    let (file, custody) = stage_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "native-mail", &payload, &work).await.unwrap();
    let retry = stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "native-mail", &payload, &work).await.unwrap();
    assert_eq!(retry.0, file);
    drop(retry);
    let (write, _write_custody) = pi_input_write_grant_with_native_work(&fixture.owner, &crate::pi_fork::tests::work()).unwrap();
    fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, file.path(),
        b"{\"later\":\"mutable head\"}", "application/json", "native-mail.json",
        fixture.owner.policy().maximum_payload_bytes, &IdempotencyKey::new("native-mail-later-head").unwrap())
        .await.unwrap();
    let (original, original_custody) = observe_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "native-mail", &work).await.unwrap().unwrap();
    assert_eq!(original, file);
    let (authority, authority_custody) = fixture.owner.authority_with_native_work(&work).unwrap();
    let verifier = FilesystemContentVerifier::new_with_native_work(fixture.filesystem.clone(),
        &authority.verifier, &authority.signed,
        fixture.owner.policy().maximum_payload_bytes, &work).unwrap();
    let bytes = verifier.read_with_native_work(&original,
        fixture.owner.policy().maximum_payload_bytes, &work).await.unwrap();
    let (decoded, decoded_custody) = guarded_pi_decode::<PiMailEnvelope>(bytes.as_ref(), fixture.owner.policy().maximum_payload_bytes, std::mem::size_of::<PiMailEnvelope>() as u64, acyclic_harness::wire::ErrorCode::Invalid, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["PiMailEnvelope", "PiMailKind", "kind", "message", "steer"]), &work).unwrap();
    assert_eq!(decoded, payload);
    drop(bytes);
    drop(original);
    drop(original_custody);
    drop(decoded);
    drop(decoded_custody);
    drop(verifier);
    drop(authority);
    drop(authority_custody);
    drop(file);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(observe_pi_input(&fixture.owner, &fixture.filesystem,
        OperationId::from_bytes([142; 16]), "native-mail", &work).await.unwrap().is_none());
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(stage_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "native-mail", &PiMailEnvelope { kind: PiMailKind::Message, message: "changed retry".into() }, &work)
        .await.is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(stage_pi_input(&fixture.owner, &fixture.filesystem,
        OperationId::from_bytes([143; 16]), "native-mail", &payload, &work).await,
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), baseline);
    let query = crate::pi_fork::tests::work();
    assert!(observe_pi_input(&fixture.owner, &fixture.filesystem,
        OperationId::from_bytes([143; 16]), "native-mail", &query).await.unwrap().is_none());
    fixture.assert_no_selected_resources().await;

}

#[tokio::test]
async fn pi_fresh_source_guard_preserves_identity_and_denies_unselected_or_cancelled_work() {
    let fixture = Fixture::new(false).await;
    let context = fixture.provider.context.context();
    let owner = fixture.provider.execution_owner().unwrap();
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    let work = &fixture.startup;
    let original = guard_root_preparation_source_with_native_work(&context, owner,
        &fixture.provider.preparation, key, None, &work).await.unwrap();
    assert_eq!(guard_root_preparation_source_with_native_work(&context, owner,
        &fixture.provider.preparation, key, Some(original), &work).await.unwrap(), original);
    fixture.assert_no_selected_resources().await;
    let moved = fixture.directory.path().join("original-native-root");
    std::fs::rename(&fixture.native, &moved).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    let failure = guard_root_preparation_source_with_native_work(&context, owner,
        &fixture.provider.preparation, key, Some(original), &work).await.err().unwrap();
    assert!(matches!(failure, Error::NativeWork {
        cause: NativeTreeMaterializationError::AllocationChanged { expected, .. }, custody: None,
    } if expected == original));
    assert_eq!(std::fs::read(moved.join("untouched.txt")).unwrap(), b"original native bytes");
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 0);
    let cancellation = fixture.startup.cancellation();
    let before = work.receipt();
    assert!(matches!(guard_root_preparation_source_with_native_work(&context, owner,
        &fixture.provider.preparation, "foreign-unselected-root", None, &work).await,
        Err(Error::NativeBoundary { code: ErrorCode::Unauthorized, .. })));
    assert_eq!(work.receipt(), before);
    cancellation.cancel();
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    assert!(matches!(guard_root_preparation_source_with_native_work(&context, owner,
        &fixture.provider.preparation, key, None, &work).await,
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, custody: None })));
    assert_eq!(work.receipt(), before);
    fixture.assert_no_selected_resources().await;
}
