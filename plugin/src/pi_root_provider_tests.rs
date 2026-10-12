//! Real local producers and the production Pi workflow; no allocation mocks.

use super::*;
use crate::{PiScratchAllocation, PiScratchBinding, PiTaskBinding, pi_owner::PiOwnerPolicy,
    pi_root_inputs::stage_root_command, pi_runtime::PiTaskRuntime};
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

type Runtime = PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

struct Fixture {
    directory: tempfile::TempDir,
    session: PathBuf,
    native: PathBuf,
    resources: ServiceResources,
    filesystem: Arc<Host>,
    stream: StreamClient<LocalStream>,
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
        let provider = ProviderRef::new("pi-provider-tests", "filesystem", "1").unwrap();
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
        let owner = PiOwnerConfig::new(PiOwnerPolicy { root_task: task, agent, journal: journal.clone(), grants,
            limits: Limits { file_bytes: 1_048_576, path_bytes: 4096, render_bytes: 1_048_576, ..Limits::default() },
            run_limits: TaskRunLimits { concurrency: Some(4), max_steps: Some(128), deadline_epoch_ms: None },
            session_limits: SessionLimits { active_tasks: 4, total_tasks: 16, depth: 4, model_steps: 128 },
            concurrency: 4, maximum_payload_bytes: 1_048_576 }).unwrap();
        owner.save_new(&session).unwrap();
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
        let stream = StreamClient::new(Arc::new(LocalStream::open(session.join("stream"), LocalStreamLimits::default()).await.unwrap()));
        let authority = owner.authority_with_native_work(&startup).unwrap();
        let runtime = PiTaskRuntime::open_with_native_work(stream.clone(), filesystem.clone(),
            &journal, authority, &startup).await.unwrap();
        let (admitted_request, request_custody) = request.clone_with_native_work(&startup).unwrap();
        let (admission, admission_custody) = runtime.admit_root_with_native_work(
            admitted_request.binding, admitted_request.root, command, &startup).await.unwrap();
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
        let (preparation, journal_owner) = PiRootPreparation::load_for_dispatch(&resources.private_state_root,
            session.strip_prefix(&resources.data).unwrap(), &runtime, &filesystem,
            &request, &fence, Arc::clone(&context), &startup).await.unwrap();
        let journal_owner = journal_owner.into_native_shared(&startup).unwrap();
        let provider = root_provider(owner.clone(), &resources, filesystem.clone(), stream.clone(), preparation,
            journal_owner, context, startup.clone()).unwrap();
        Self { directory, session, native, resources, filesystem, stream, owner, runtime,
            request, lease, provider, startup, _lease_custody: lease_custody, _service_lock: service_lock }
    }

    async fn execute(&self) -> Result<NativeOwned<EffectStatus>> {
        let current = Arc::clone(self.provider.journal_owner.as_ref().unwrap());
        let content = self.provider.context.content_residency_verifier_with_native_work(current, &self.startup)?;
        let (authority, custody) = self.owner.effect_authority_with_native_work(&self.startup)?;
        let effects = crate::pi_owner::root_effects_with_native_work(self.stream.clone(), authority, custody,
            content, self.provider.clone(), &self.startup)?;
        let (value, custody) = effects.run_task_effect_with_native_work(self.provider.execution_owner()?,
            self.provider.preparation.command().operation_id, self.provider.preparation.plan(), &self.startup).await?;
        Ok(NativeOwned { value, _custody: custody })
    }

    async fn dispatch_identity(&self) -> EffectDispatch {
        let id = self.provider.effect_id_with_native_work(&self.startup).unwrap();
        let attempt = EffectAttemptId::from_bytes(id.into_bytes());
        let state = self.provider.authenticate_with_native_work(attempt, false, &self.startup).await.unwrap();
        EffectDispatch { provider: state.value.provider.clone(), effect_id: id, attempt_id: attempt,
            effect_kind: state.value.effect_kind.clone(), request: state.value.request.clone(),
            request_digest: state.value.request_digest, guarantee: state.value.guarantee }
    }

    async fn reopen_provider(&self) -> Arc<PiRootProvider<LocalStream>> {
        let owner = PiOwnerConfig::load_with_native_work(&self.resources.private_state_root,
            self.session.strip_prefix(&self.resources.data).unwrap(), &self.startup).unwrap()
            .into_native_shared(&self.startup).unwrap();
        let fence = LeaseFence::from(&self.lease.reservation);
        let context = self.runtime.runtime.task_context_with_native_work(self.request.binding.task_id,
            &fence, &self.startup).await.unwrap();
        let (preparation, journal_owner) = PiRootPreparation::load_for_dispatch(&self.resources.private_state_root,
            self.session.strip_prefix(&self.resources.data).unwrap(), &self.runtime,
            &self.filesystem, &self.request, &fence, Arc::clone(&context), &self.startup).await.unwrap();
        let journal_owner = journal_owner.into_native_shared(&self.startup).unwrap();
        let (value, custody) = service_resources_with_native_work(&self.resources, &self.startup).unwrap();
        let mut resources = NativeOwned { value, _custody: custody };
        resources.value.shared_roots = crate::SharedRootRegistry::default();
        root_provider(owner, &resources.value, self.filesystem.clone(), self.stream.clone(), preparation,
            journal_owner, context, self.startup.clone()).unwrap()
    }

    async fn assert_no_selected_resources(&self) {
        for volume in self.request.binding.project_volumes.values().chain(std::iter::once(&self.request.binding.scratch.volume)) {
            assert!(self.resources.fs.open_workspace(volume.storage_name().unwrap()).await.is_err());
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
        assert!(matches!(succeeded.value, EffectStatus::Succeeded { .. }));
        let dispatch = fixture.dispatch_identity().await;
        let original = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
        let allocation = fixture.provider.read_with_native_work::<PiRootAllocation>(&original.value,
            &fixture.startup).await.unwrap();
        let actual = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap().unwrap();
        assert!(same_allocation(&allocation.value, &actual.value));
        assert_eq!(allocation.value.roots.len(), usize::from(!scratch_only));
        let schema = PiRootAllocation::schema(&fixture.request.binding).unwrap();
        let original_value = serde_json::to_value(&allocation.value).unwrap();
        acyclic_harness::tool::validate_value(&schema, &original_value, "allocation").unwrap();
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
            assert!(acyclic_harness::tool::validate_value(&schema, &value, "allocation").is_err());
        }
        assert!(fixture.provider.observe_conversation_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, &fixture.startup).await.unwrap());
        let handles = fixture.provider.take_created_handles().await;
        assert_eq!(handles.len(), allocation.value.roots.len());
        for (key, handle) in handles {
            let root = &allocation.value.roots[&key];
            assert_eq!(handle.workspace.workspace().id().into_bytes(), root.repository_workspace_id);
            assert_eq!(handle.physical.source.reference().identity, root.source_identity);
            let state = fixture.resources.store.load_lazy_workspace_with_native_work(
                handle.workspace.workspace().id(), &fixture.startup).await.unwrap().unwrap();
            assert!(state.as_state().parent_workspace_id.is_none());
            assert_eq!(handle.workspace.read("/untouched.txt", 64).await.unwrap().as_ref(), b"original native bytes");
        }
        let preparation = PiRootPreparation::load(&fixture.resources.private_state_root,
            fixture.session.strip_prefix(&fixture.resources.data).unwrap(), &fixture.runtime,
            &fixture.filesystem, &fixture.request, &fixture.startup).await.unwrap();
        let observed = observe_root_allocation(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), preparation, &fixture.startup).await.unwrap().unwrap();
        let observed = NativeOwned { value: observed.0, _custody: observed.1 };
        assert_eq!(observed.value.0, original.value);
        assert!(same_allocation(&observed.value.1, &allocation.value));
        assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
        assert_eq!(fixture.startup.receipt().materializations, 0);
    }
}

#[tokio::test]
async fn pi_created_root_guard_uses_actual_readonly_owner_without_snapshot_or_fresh_lease() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Succeeded { .. }));
    let dispatch = fixture.dispatch_identity().await;
    let result = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
    let allocation = fixture.provider.read_with_native_work::<PiRootAllocation>(
        &result.value, &fixture.startup).await.unwrap();
    let (readonly_owner, readonly_context) = fixture.runtime.runtime.readonly_task_context_with_native_work(
        fixture.request.binding.task_id, &fixture.startup).await.unwrap();
    assert!(readonly_owner.verify_with_native_work(false, &fixture.startup).await.is_err());
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem,
        &fixture.request, Arc::clone(&readonly_context), &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    let identity = guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), preparation,
    &readonly_owner, key, &allocation.value, &fixture.startup).await.unwrap();
    assert_eq!(identity.to_bytes(), allocation.value.roots[key].native_root_identity);
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 1);
    assert_eq!(fixture.startup.receipt().materializations, before.materializations);
    let mut substituted = fixture.provider.read_with_native_work::<PiRootAllocation>(
        &result.value, &fixture.startup).await.unwrap();
    substituted.value.roots.get_mut(key).unwrap().repository_workspace_id = [201; 16];
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem,
        &fixture.request, Arc::clone(&readonly_context), &fixture.startup).await.unwrap();
    assert!(guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), preparation, &readonly_owner,
        key, &substituted.value, &fixture.startup).await.is_err());
    let original_native = fixture.directory.path().join("created-original-native-root");
    std::fs::rename(&fixture.native, &original_native).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    let preparation = load_original_preparation_with_native_work(&fixture.owner, &fixture.filesystem,
        &fixture.request, readonly_context, &fixture.startup).await.unwrap();
    assert!(guard_created_root_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), preparation, &readonly_owner,
        key, &allocation.value, &fixture.startup).await.is_err());
    assert_eq!(std::fs::read(original_native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_provider_empty_budget_and_precancellation_never_probe_selected_roots() {
    let fixture = Fixture::new(false).await;
    assert!(NativeMaterializationWork::new(WorkBudget::ZERO, &CancellationToken::new()).is_err());
    let wrong = NativeMaterializationWork::new(WorkBudget {
        backend_read_operations: 0, ..allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let before = wrong.receipt();
    assert!(fixture.provider.supports_guarantee_with_native_work(ROOT_ALLOCATION_EFFECT,
        EffectGuarantee::AtMostOnce, &wrong).is_err());
    assert_eq!(wrong.receipt(), before);
    fixture.provider.cancel();
    let outcome = fixture.execute().await;
    assert!(outcome.is_err() || matches!(outcome.unwrap().value, EffectStatus::Indeterminate));
    fixture.assert_no_selected_resources().await;
}

#[tokio::test]
async fn pi_root_original_span_exhaustion_denies_before_creation_and_releases_real_payload_custody() {
    let fixture = Fixture::new(false).await;
    let baseline = fixture.startup.live_allocation_bytes();
    let bytes = fixture.startup.budget().peak_allocation_bytes - baseline;
    let payload_custody = fixture.startup.claim(bytes).unwrap();
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
            assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
            let dispatch = fixture.dispatch_identity().await;
            let before = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
                dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap();
            let scratch_name = fixture.request.binding.scratch.volume.storage_name().unwrap();
            let original_scratch = fixture.resources.fs.open_workspace(&scratch_name).await.ok();
            let original_workspace = original_scratch.as_ref().map(|workspace| workspace.id());
            if let Some(scratch) = original_scratch {
                scratch.write_text("/later.txt", "mutable head advanced").await.unwrap();
            }
            let reopened = fixture.reopen_provider().await;
            if cancelled {
                fixture.runtime.runtime.task_host().cancel(fixture.request.binding.task_id).await.unwrap();
                reopened.cancel();
            }
            let observed = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap();
            assert_eq!(observed.is_some(), !cancelled || matches!(boundary, 2 | 4));
            if let Some((value, custody)) = observed {
                let observed = NativeOwned { value, _custody: custody };
                let EffectStatus::Succeeded { result } = &observed.value.status else { panic!("original resources were not recovered"); };
                let recorded = reopened.read_with_native_work::<PiRootAllocation>(result, &fixture.startup).await.unwrap();
                if let Some(before) = &before { assert!(same_allocation(&before.value, &recorded.value)); }
                if let Some(id) = original_workspace { assert_eq!(recorded.value.scratch.workspace_id, id.into_bytes()); }
                let repeated = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();
                let repeated = NativeOwned { value: repeated.0, _custody: repeated.1 };
                assert_eq!(repeated.value, observed.value);
            } else {
                assert!(fixture.resources.fs.open_workspace(&scratch_name).await.is_err());
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
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let inputs = fixture.provider.capture_inputs_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap();
    let key = fixture.request.binding.project_volumes.keys().next().unwrap();
    let volume = &fixture.request.binding.project_volumes[key];
    let operation = fixture.provider.creation_operation_with_native_work(
        dispatch.effect_id, dispatch.attempt_id, key, &fixture.startup).unwrap();
    let (value, custody) = volume.storage_name_with_native_work(&fixture.startup).unwrap();
    let name = NativeOwned { value, _custody: custody };
    let first = fixture.resources.fs.create_workspace_with_operation_with_native_work(
        &name.value, VolumeConfig::native(Lifecycle::Durable), operation,
        &fixture.startup, fixture.startup.cancellation()).await.unwrap();
    assert!(fixture.resources.store.load_lazy_workspace_with_native_work(
        first.workspace().id(), &fixture.startup).await.unwrap().is_none());
    let reopened = fixture.reopen_provider().await;
    let (value, custody) = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();
    let observed = NativeOwned { value, _custody: custody };
    assert!(matches!(observed.value.status, EffectStatus::Succeeded { .. }));
    let original = reopened.creation_with_native_work(dispatch.effect_id, dispatch.attempt_id,
        key, volume, VolumeConfig::native(Lifecycle::Durable), &fixture.startup).await.unwrap().unwrap();
    assert_eq!(original.id(), first.id());
    assert_eq!(original.workspace().id(), first.workspace().id());
    let state = fixture.resources.store.load_lazy_workspace_with_native_work(
        first.workspace().id(), &fixture.startup).await.unwrap().unwrap();
    assert_eq!(state.as_state().source.identity, inputs.value[key].source.identity);
    assert_eq!(fixture.startup.receipt().materializations, 0);
    assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_provider_rejects_foreign_attempt_digest_provider_and_substituted_native_identity() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(2, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let original = fixture.dispatch_identity().await;
    for field in ["attempt", "digest", "provider", "effect", "kind", "request"] {
        let mut forged = original.clone();
        match field {
            "attempt" => forged.attempt_id = EffectAttemptId::from_bytes([119; 16]),
            "digest" => forged.request_digest = [119; 32],
            "provider" => forged.provider = "foreign".into(),
            "effect" => forged.effect_id = EffectId::from_bytes([119; 16]),
            "kind" => forged.effect_kind = "foreign@1".into(),
            "request" => forged.request = fixture.provider.preparation.plan().result_schema.clone(),
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
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let mut allocation = fixture.provider.observe_resources_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, dispatch.request_digest, &fixture.startup).await.unwrap().unwrap();
    allocation.value.scratch.workspace_id = [120; 16];
    fixture.provider.stage_with_native_work(dispatch.effect_id, dispatch.attempt_id,
        "result", &allocation.value, &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    assert!(fixture.provider.reconcile_native(dispatch.attempt_id, &fixture.startup).await.is_err());
    let after = fixture.startup.receipt();
    assert!(after.backend_read_operations > before.backend_read_operations);
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
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
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
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let original = fixture.provider.original_artifact_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, "result", &fixture.startup).await.unwrap().unwrap();
    let mut changed = fixture.provider.read_with_native_work::<PiRootAllocation>(&original.value, &fixture.startup).await.unwrap();
    changed.value.scratch.workspace_id = [124; 16];
    let (_, write) = pi_input_grants(&fixture.owner).unwrap();
    let substituted = fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, original.value.path(),
        &serde_json::to_vec(&changed.value).unwrap(), "application/json", "result.json",
        fixture.owner.policy().maximum_payload_bytes, &IdempotencyKey::new("different-output-operation").unwrap()).await.unwrap();
    assert_ne!(original.value, substituted);
    let reopened = fixture.reopen_provider().await;
    let before = fixture.startup.receipt();
    let result = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap().unwrap();
    let result = NativeOwned { value: result.0, _custody: result.1 };
    assert_eq!(result.value.status, EffectStatus::Succeeded { result: original.value.clone() });
    let repeated = reopened.dispatch_native(dispatch.clone(), &fixture.startup).await.unwrap();
    let repeated = NativeOwned { value: repeated.0, _custody: repeated.1 };
    assert_eq!(repeated.value.status, result.value.status);
    let duplicate = fixture.provider.dispatch_native(dispatch, &fixture.startup).await.unwrap();
    let duplicate = NativeOwned { value: duplicate.0, _custody: duplicate.1 };
    assert_eq!(duplicate.value.status, result.value.status);
    assert!(reopened.take_created_handles().await.is_empty());
    assert_eq!(fixture.startup.receipt().source_entries_visited, before.source_entries_visited);
    assert_eq!(fixture.startup.receipt().materializations, before.materializations);
}

#[tokio::test]
async fn pi_root_provider_lost_binding_ack_reuses_original_event_and_cancelled_owner_never_creates() {
    for cancelled in [false, true] {
        let fixture = Fixture::new(false).await;
        fixture.provider.fault.store(5, Ordering::SeqCst);
        assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
        fixture.assert_no_selected_resources().await;
        let dispatch = fixture.dispatch_identity().await;
        assert!(fixture.provider.observe_conversation_with_native_work(dispatch.effect_id,
            dispatch.attempt_id, &fixture.startup).await.unwrap());
        let authority = fixture.owner.conversation_authority(fixture.request.binding.task_id,
            fixture.request.binding.agent_id, fixture.owner.policy().grants.clone()).unwrap();
        let before = acyclic_harness::store::StreamAggregate::open_measured(&fixture.stream, authority.0.clone(),
            authority.1.verifier(), SchemaRegistry::new(), allocation_budget(), &CancellationToken::new()).await.unwrap().value;
        let original_revision = before.reducer().revision();
        let reopened = fixture.reopen_provider().await;
        if cancelled { reopened.cancel(); }
        let observed = reopened.reconcile_native(dispatch.attempt_id, &fixture.startup).await.unwrap();
        assert_eq!(observed.is_some(), !cancelled);
        let observed = observed.map(|(value, custody)| NativeOwned { value, _custody: custody });
        let after = acyclic_harness::store::StreamAggregate::open_measured(&fixture.stream, authority.0,
            authority.1.verifier(), SchemaRegistry::new(), allocation_budget(), &CancellationToken::new()).await.unwrap().value;
        assert_eq!(after.reducer().revision(), original_revision);
        assert_eq!(after.reducer().conversation().unwrap().agent, Some(fixture.request.binding.agent_id));
        if cancelled { fixture.assert_no_selected_resources().await; }
        drop(observed);
    }
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
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
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
async fn pi_input_publication_exhaustion_and_cancellation_do_not_create_original_operations() {
    let fixture = Fixture::new(true).await;
    let operation = OperationId::from_bytes([129; 16]);
    let value = serde_json::json!({"control": "bounded"});
    let query = NativeMaterializationWork::new(allocation_budget(), &CancellationToken::new()).unwrap();
    for cancelled in [false, true] {
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(if cancelled { allocation_budget() } else { WorkBudget::default() }, &cancellation).unwrap();
        if cancelled { cancellation.cancel(); }
        assert!(stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "mail", &value, &work).await.is_err());
        assert!(observe_pi_input(&fixture.owner, &fixture.filesystem, operation, "mail", &query).await.unwrap().is_none());
    }
    assert!(stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "../mail", &value, &query).await.is_err());
    fixture.assert_no_selected_resources().await;
}

#[tokio::test]
async fn pi_root_private_scratch_proof_is_original_readonly_and_never_probes_user_projects() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Succeeded { .. }));
    let dispatch = fixture.dispatch_identity().await;
    let observed = fixture.provider.observe_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.unwrap().unwrap();
    let observed = NativeOwned { value: observed.0, _custody: observed.1 };
    let before = fixture.startup.receipt();
    let proof = fixture.provider.prove_scratch_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.unwrap().unwrap();
    assert_eq!(proof.task(), fixture.request.binding.task_id);
    assert_eq!(proof.agent(), fixture.request.binding.agent_id);
    assert_eq!(proof.volume(), &fixture.request.binding.scratch.volume);
    assert_eq!(proof.workspace_id(), observed.value.1.scratch.workspace_id);
    assert_eq!(proof.generation(), &observed.value.1.scratch.generation);
    assert_eq!(proof.output(), &observed.value.0);
    assert_eq!(proof.operation(), fixture.provider.creation_operation_with_native_work(
        dispatch.effect_id, dispatch.attempt_id, "scratch", &fixture.startup).unwrap());
    assert_eq!(fixture.startup.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(fixture.startup.receipt().source_path_components, before.source_path_components);
    let retained = fixture.directory.path().join("retained-user-project");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(fixture.provider.observe_allocation_with_native_work(dispatch.attempt_id, &fixture.startup).await.is_err());
    let preparation = PiRootPreparation::load(&fixture.resources.private_state_root,
        fixture.session.strip_prefix(&fixture.resources.data).unwrap(), &fixture.runtime,
        &fixture.filesystem, &fixture.request, &fixture.startup).await.unwrap();
    let before = fixture.startup.receipt();
    let second = prove_root_scratch_allocation_with_native_work(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), preparation, &fixture.startup).await.unwrap().unwrap();
    assert_eq!(second.workspace_id(), proof.workspace_id());
    assert_eq!(second.generation(), proof.generation());
    assert_eq!(fixture.startup.receipt().source_path_components, before.source_path_components);
    assert_eq!(fixture.startup.receipt().backend_write_operations, before.backend_write_operations);
    assert_eq!(std::fs::read(retained.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_scratch_proof_never_promotes_an_unsettled_allocation() {
    let fixture = Fixture::new(true).await;
    fixture.provider.fault.store(4, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    assert!(fixture.provider.prove_scratch_allocation_with_native_work(dispatch.attempt_id,
        &fixture.startup).await.is_err());
}

#[tokio::test]
async fn pi_root_project_source_proof_uses_original_operations_and_callers_cumulative_quota() {
    let fixture = Fixture::new(false).await;
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Succeeded { .. }));
    let handles = fixture.provider.take_created_handles().await;
    let (key, handle) = handles.iter().next().unwrap();
    let (snapshot, custody) = handle.workspace.snapshot_with_native_work(&fixture.startup).await.unwrap();
    let snapshot = NativeOwned { value: snapshot, _custody: custody };
    let project = &fixture.request.binding.project_volumes[key];
    let before = fixture.startup.receipt();
    prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, snapshot.value,
    Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.unwrap();
    let after = fixture.startup.receipt();
    assert!(after.backend_read_operations > before.backend_read_operations);
    assert!(after.authority_records_read > before.authority_records_read);
    assert_eq!(after.backend_write_operations, before.backend_write_operations);
    assert_eq!(after.materializations, before.materializations);
    let mut foreign = snapshot.value;
    foreign.workspace_id = acyclic_fs::WorkspaceId::from_bytes([130; 16]);
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, foreign,
        Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    foreign = snapshot.value;
    foreign.source.identity = [131; 16];
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, foreign,
        Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, &fixture.request.binding.scratch.volume,
        snapshot.value, Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    let cancellation = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let before = cancelled.receipt();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, snapshot.value,
        Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &cancelled).await.is_err());
    assert_eq!(cancelled.receipt(), before);
    let limited = NativeMaterializationWork::new(WorkBudget { backend_read_operations: 1, ..allocation_budget() },
        &CancellationToken::new()).unwrap();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, snapshot.value,
        Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &limited).await.is_err());
    assert!(limited.receipt().backend_read_operations <= 1);
    let retained = fixture.directory.path().join("source-proof-retained");
    std::fs::rename(&fixture.native, &retained).unwrap();
    std::fs::create_dir(&fixture.native).unwrap();
    assert!(prove_root_project_source_with_native_work(fixture.owner.clone(), &fixture.resources,
        fixture.filesystem.clone(), fixture.stream.clone(), &fixture.request, project, snapshot.value,
        Arc::clone(&fixture.provider.context), fixture.provider.execution_owner().unwrap(), &fixture.startup).await.is_err());
    assert_eq!(std::fs::read(retained.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_partial_creation_without_original_source_intent_is_not_recaptured() {
    let fixture = Fixture::new(false).await;
    fixture.provider.fault.store(3, Ordering::SeqCst);
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Indeterminate));
    let dispatch = fixture.dispatch_identity().await;
    let (key, project) = fixture.request.binding.project_volumes.iter().next().unwrap();
    let operation = fixture.provider.creation_operation_with_native_work(dispatch.effect_id,
        dispatch.attempt_id, key, &fixture.startup).unwrap();
    let generation = fixture.resources.fs.create_workspace_with_operation_with_native_work(&project.storage_name().unwrap(),
        VolumeConfig::native(Lifecycle::Durable), operation, &fixture.startup, fixture.startup.cancellation()).await.unwrap();
    let before = fixture.startup.receipt();
    assert!(fixture.provider.reconcile_native(dispatch.attempt_id, &fixture.startup).await.is_err());
    let after = fixture.startup.receipt();
    assert!(after.backend_read_operations > before.backend_read_operations);
    assert_eq!(after.backend_write_operations, before.backend_write_operations);
    assert_eq!(after.source_entries_visited, before.source_entries_visited);
    assert_eq!(after.materializations, before.materializations);
    assert_eq!(fixture.resources.shared_roots.live_roots().await, 0);
    assert_eq!(fixture.resources.fs.open_workspace(project.storage_name().unwrap()).await.unwrap().id(), generation.workspace().id());
    assert!(fixture.resources.fs.open_workspace(fixture.request.binding.scratch.volume.storage_name().unwrap()).await.is_err());
    assert_eq!(std::fs::read(fixture.native.join("untouched.txt")).unwrap(), b"original native bytes");
}

#[tokio::test]
async fn pi_root_output_context_preserves_actual_task_and_stages_only_original_scratch() {
    let fixture = Fixture::new(true).await;
    assert!(matches!(fixture.execute().await.unwrap().value, EffectStatus::Succeeded { .. }));
    let current_owner = fixture.provider.journal_owner.as_ref().unwrap();
    let context = Arc::clone(&fixture.provider.context);
    let output = bind_root_output_context_with_native_work(Arc::clone(&context), current_owner, fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(),
    &fixture.request, &fixture.startup).await.unwrap();
    assert!(Arc::ptr_eq(&output, &context));
    assert_eq!(output.context().durable_task_id(), Some(fixture.request.binding.task_id));
    assert_eq!(output.context().scope().grants(), context.context().scope().grants());
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        assert!(!output.context().scope().grants().contains(&fixture.owner.policy().journal.capability(operation).unwrap()));
    }
    let (published, custody) = output.context().stage_file_once_with_native_work(
        OperationId::from_bytes([133; 16]), "root/checkpoint.json", b"{\"actual\":\"root scratch\"}",
        "application/json", "checkpoint.json", &fixture.startup).await.unwrap();
    let published = NativeOwned { value: published, _custody: custody };
    assert_eq!(published.value.volume(), &fixture.request.binding.scratch.volume);
    assert_ne!(published.value.volume(), &fixture.owner.policy().journal);
    let effect = fixture.provider.effect_id_with_native_work(&fixture.startup).unwrap();
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let operation = fixture.provider.creation_operation_with_native_work(effect, attempt, "scratch", &fixture.startup).unwrap();
    let generation = fixture.provider.creation_with_native_work(effect, attempt, "scratch",
        &fixture.request.binding.scratch.volume, VolumeConfig::portable(Lifecycle::Durable),
        &fixture.startup).await.unwrap().unwrap();
    let (value, custody) = fixture.owner.conversation_authority_with_native_work(
        fixture.request.binding.task_id, fixture.request.binding.agent_id, &fixture.owner.policy().grants,
        &fixture.startup).unwrap();
    let authority = NativeOwned { value, _custody: custody };
    let (value, custody) = authority.value.1.verifier_with_native_work(&fixture.startup).unwrap();
    let verifier = NativeOwned { value, _custody: custody };
    assert!(fixture.filesystem.native_root_output_source_with_native_work(current_owner,
        &fixture.request.binding.scratch.volume, operation, &generation, &verifier.value,
        &authority.value.2, &fixture.startup).await.is_ok());
    assert!(fixture.filesystem.native_root_output_source_with_native_work(current_owner,
        &fixture.owner.policy().journal, operation, &generation, &verifier.value,
        &authority.value.2, &fixture.startup).await.is_err());
    assert!(fixture.filesystem.native_root_output_source_with_native_work(current_owner,
        &fixture.request.binding.scratch.volume, acyclic_fs::OperationId::from_bytes([199; 16]),
        &generation, &verifier.value, &authority.value.2, &fixture.startup).await.is_err());
    let foreign_generation = fixture.resources.fs.create_workspace_with_operation_with_native_work(
        "pi-root-output-foreign-generation", VolumeConfig::portable(Lifecycle::Durable),
        acyclic_fs::OperationId::from_bytes([198; 16]), &fixture.startup,
        fixture.startup.cancellation()).await.unwrap();
    assert!(fixture.filesystem.native_root_output_source_with_native_work(current_owner,
        &fixture.request.binding.scratch.volume, operation, &foreign_generation, &verifier.value,
        &authority.value.2, &fixture.startup).await.is_err());
    fixture.runtime.runtime.task_host().cancel(fixture.request.binding.task_id).await.unwrap();
    assert!(fixture.filesystem.native_root_output_source_with_native_work(current_owner,
        &fixture.request.binding.scratch.volume, operation, &generation, &verifier.value,
        &authority.value.2, &fixture.startup).await.is_err());
    assert!(output.context().stage_file_once_with_native_work(OperationId::from_bytes([197; 16]),
        "root/after-owner-cancel.json", b"{}", "application/json", "cancelled.json",
        &fixture.startup).await.is_err());
    let cancellation = CancellationToken::new();
    let cancelled = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    cancellation.cancel();
    let before = cancelled.receipt();
    assert!(bind_root_output_context_with_native_work(context, current_owner,
        fixture.owner.clone(), &fixture.resources, fixture.filesystem.clone(), fixture.stream.clone(),
        &fixture.request, &cancelled).await.is_err());
    assert_eq!(cancelled.receipt(), before);
}

#[tokio::test]
async fn pi_operator_input_original_upload_is_root_only_even_for_actual_created_child_context() {
    let fixture = crate::pi_fork::tests::Fixture::new().await;
    let operation = OperationId::from_bytes([137; 16]);
    let signed = fixture.owner.authority().unwrap().signed;
    let native = NativeMaterializationWork::new(allocation_budget(), &CancellationToken::new()).unwrap();
    let (operator, _operator_custody) = stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope", &signed, &native).await.unwrap();
    assert_eq!(operator.path(), format!("{OPERATOR_INPUT_DIRECTORY}/{operation}/scope.json"));
    assert!(stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope",
        &serde_json::json!({"different":"scope"}), &native).await.is_err());
    let (regular, _regular_custody) = stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "scope", &serde_json::json!({"not":"a bearer scope"}), &native).await.unwrap();
    assert_ne!(regular, operator);
    let (_, write) = pi_input_grants(&fixture.owner).unwrap();
    fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, operator.path(), b"{\"changed\":true}",
        "application/json", "scope.json", fixture.owner.policy().maximum_payload_bytes,
        &IdempotencyKey::new("different-operator-path-upload").unwrap()).await.unwrap();
    let retry = stage_pi_operator_input(&fixture.owner, &fixture.filesystem, operation, "scope", &signed, &native).await.unwrap();
    assert_eq!(retry.0, operator);
    let plan = fixture.plan().await;
    let coordinator = fixture.coordinator();
    let child = fixture.advance_with(&coordinator, &fixture.context, &fixture.journal,
        &fixture.fence, &plan, fixture.source.clone(), allocation_budget(), CancellationToken::new()).await.unwrap().unwrap();
    let attempt = fixture.runtime.runtime.run_operation(&Worker { id: "operator-child-read-denial".into(),
        available: Default::default(), labels: Default::default() }, child.binding.task_id,
        &fixture.runtime.runtime.commands(), 1).await.unwrap().unwrap();
    let TaskWorkerAttempt::Progress(TaskWorkerOutcome::Yielded { lease }) = attempt else { panic!("actual created child lease"); };
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
    #[derive(Deserialize)]
    struct Payload { _text: String }
    let bytes = br#"{"_text":"native-owned"}"#;
    let owned = (bytes.len() as u64) * 4;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget {
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + owned,
        ..allocation_budget()
    }, &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let failure = guarded_pi_decode::<Payload>(bytes, 8192, owned, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["Payload", "sender", "message", "_text"]), &work).err().unwrap();
    assert!(matches!(&failure, Error::NativeWork {
        cause: NativeTreeMaterializationError::Work(WorkError::BudgetExceeded { .. }), ..
    }));
    assert!(work.receipt().source_bytes_read > 0);
    assert_eq!(work.live_allocation_bytes(), baseline);
    drop(failure);
    let work = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    assert!(guarded_pi_decode::<Payload>(br#"{"_text":[]}"#, 8192, owned, JsonValueSchema::None,
        pi_serde_diagnostic_labels_bytes(&["Payload", "_text"]), &work).is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(guarded_pi_decode::<Payload>(bytes, 8192, owned, JsonValueSchema::None,
        pi_serde_diagnostic_labels_bytes(&["Payload", "_text"]), &work),
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn pi_native_input_stage_and_observe_retain_original_key_and_immutable_receipt() {
    use acyclic_harness::filesystem::ContentResidencyVerifier;
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Payload { sender: String, message: String }
    let fixture = Fixture::new(true).await;
    let operation = OperationId::from_bytes([141; 16]);
    let payload = Payload { sender: "root".into(), message: "original immutable message".into() };
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(allocation_budget(), &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let (file, custody) = stage_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "native-mail", &payload, &work).await.unwrap();
    let retry = stage_pi_input(&fixture.owner, &fixture.filesystem, operation, "native-mail", &payload, &work).await.unwrap();
    assert_eq!(retry.0, file);
    drop(retry);
    let (_, write) = pi_input_grants(&fixture.owner).unwrap();
    fixture.filesystem.put_content(&fixture.owner.policy().journal, &write, file.path(),
        b"{\"later\":\"mutable head\"}", "application/json", "native-mail.json",
        fixture.owner.policy().maximum_payload_bytes, &IdempotencyKey::new("native-mail-later-head").unwrap())
        .await.unwrap();
    let (original, original_custody) = observe_pi_input(
        &fixture.owner, &fixture.filesystem, operation, "native-mail", &work).await.unwrap().unwrap();
    assert_eq!(original, file);
    let (value, authority_custody) = fixture.owner.authority_with_native_work(&work).unwrap();
    let authority = NativeOwned { value, _custody: authority_custody };
    let verifier = FilesystemContentVerifier::new_with_native_work(fixture.filesystem.clone(),
        &authority.value.verifier, &authority.value.signed,
        fixture.owner.policy().maximum_payload_bytes, &work).unwrap();
    let bytes = verifier.read_with_native_work(&original,
        fixture.owner.policy().maximum_payload_bytes, &work).await.unwrap();
    let (decoded, decoded_custody) = guarded_pi_decode::<Payload>(bytes.as_ref(), fixture.owner.policy().maximum_payload_bytes, std::mem::size_of::<Payload>() as u64, JsonValueSchema::None, pi_serde_diagnostic_labels_bytes(&["Payload", "sender", "message", "_text"]), &work).unwrap();
    assert_eq!(decoded, payload);
    drop(bytes);
    drop(original);
    drop(original_custody);
    drop(decoded);
    drop(decoded_custody);
    drop(verifier);
    drop(authority);
    drop(file);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(work.receipt().backend_read_operations > 0);
    assert!(observe_pi_input(&fixture.owner, &fixture.filesystem,
        OperationId::from_bytes([142; 16]), "native-mail", &work).await.unwrap().is_none());
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(stage_pi_input(&fixture.owner, &fixture.filesystem,
        operation, "native-mail", &Payload { sender: "root".into(), message: "changed retry".into() }, &work)
        .await.is_err());
    assert_eq!(work.live_allocation_bytes(), baseline);
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(stage_pi_input(&fixture.owner, &fixture.filesystem,
        OperationId::from_bytes([143; 16]), "native-mail", &payload, &work).await,
        Err(Error::NativeWork { cause: NativeTreeMaterializationError::Cancelled, .. })));
    assert_eq!(work.receipt(), before);
    assert_eq!(work.live_allocation_bytes(), baseline);
}

#[tokio::test]
async fn pi_fresh_source_guard_uses_actual_root_context_read_and_original_identity() {
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
}

#[tokio::test]
async fn pi_fresh_source_guard_never_probes_absent_selection_or_cancelled_original_work() {
    let fixture = Fixture::new(false).await;
    let context = fixture.provider.context.context();
    let owner = fixture.provider.execution_owner().unwrap();
    let cancellation = fixture.startup.cancellation();
    let work = &fixture.startup;
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
