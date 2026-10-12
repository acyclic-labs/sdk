
use super::*;
use crate::{pi_root_inputs::PiRootPreparation, pi_runtime::PiTaskRuntime};
use acyclic_harness::{
    IdempotencyKey, OperationId, effect_host::TaskEffectPlan, workflow::WorkflowCommand,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use std::sync::Arc;

type Host = FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type Runtime =
    PiTaskRuntime<LocalStream, acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

struct Fixture {
    runtime: Runtime,
    filesystem: Arc<Host>,
    raw: acyclic_fs::LocalFs,
    owner: PiOwnerConfig,
    request: PiRootRequest,
    command: WorkflowCommand,
    session: tempfile::TempDir,
    private_root: acyclic_fs::NativeHostPrivateStateRoot,
    work: acyclic_fs::NativeMaterializationWork,
    _command_custody: acyclic_fs::NativeAllocation,
}

async fn open_runtime(session: &Path, owner: &PiOwnerConfig, filesystem: Arc<Host>,
    work: &acyclic_fs::NativeMaterializationWork) -> Runtime {
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(session.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    ));
    PiTaskRuntime::open_with_native_work(
        stream, filesystem, &owner.policy.journal,
        owner.authority_with_native_work(work).unwrap(), work,
    )
    .await
    .unwrap()
}

async fn replace(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    name: &str,
    media_type: &str,
    bytes: &[u8],
) -> FileRef {
    let authority = owner.authority().unwrap();
    let write = ContentGrant::verify(
        &authority.verifier,
        &authority.signed,
        &owner.policy.journal,
        VolumeOperation::Write,
    )
    .unwrap();
    filesystem
        .put_content(
            &owner.policy.journal,
            &write,
            &format!("pi-inputs/{name}.json"),
            bytes,
            media_type,
            &format!("{name}.json"),
            owner.policy.maximum_payload_bytes,
            &IdempotencyKey::new(format!("fixture-replacement-{name}")).unwrap(),
        )
        .await
        .unwrap()
}

async fn alter_command(
    case: &str,
    owner: &PiOwnerConfig,
    filesystem: &Host,
    request: &PiRootRequest,
    mut command: WorkflowCommand,
) -> WorkflowCommand {
    if case.is_empty() {
        return command;
    }
    if case == "operation" {
        command.operation_id = OperationId::from_bytes([99; 16]);
        return command;
    }
    let authority = owner.authority().unwrap();
    let read = ContentGrant::verify_directory_read(
        &authority.verifier,
        &authority.signed,
        &owner.policy.journal,
        ROOT_INPUT_DIRECTORY,
    )
    .unwrap();
    let mut plan: TaskEffectPlan = serde_json::from_slice(
        &filesystem
            .read_content(&command.payload, &read, owner.policy.maximum_payload_bytes)
            .await
            .unwrap(),
    )
    .unwrap();
    match case {
        "provider" => plan.provider = "foreign".to_owned(),
        "effect" => plan.effect_kind = "foreign@1".to_owned(),
        "guarantee" => plan.guarantee = acyclic_harness::core::EffectGuarantee::ExactlyOnce,
        "request" => {
            let mut changed = request.clone();
            changed.binding.native_session_id = "different-session".to_owned();
            plan.request =
                replace(owner, filesystem, "root", "application/json", &serde_json::to_vec(&changed).unwrap()).await;
        }
        "schema" => {
            plan.result_schema = replace(
                owner,
                filesystem,
                "allocation-schema",
                "application/schema+json",
                &serde_json::to_vec(&json!({"type":"object"})).unwrap(),
            )
            .await;
        }
        "encoding" | "duplicate_schema" => {
            let schema =
                crate::pi_root_allocation::PiRootAllocation::schema(&request.binding).unwrap();
            let bytes = if case == "encoding" {
                serde_json::to_vec_pretty(&schema).unwrap()
            } else {
                let encoded = serde_json::to_vec(&schema).unwrap();
                let mut duplicate = br#"{"type":"object","#.to_vec();
                duplicate.extend_from_slice(encoded.get(1..).unwrap());
                duplicate
            };
            plan.result_schema = replace(
                owner,
                filesystem,
                "allocation-schema",
                "application/schema+json",
                &bytes,
            )
            .await;
        }
        "path" | "media" => {}
        _ => panic!("unknown preparation control: {case}"),
    }
    let name = if case == "path" {
        "other-preparation"
    } else {
        "preparation"
    };
    let media = if case == "media" {
        "application/schema+json"
    } else {
        "application/json"
    };
    command.payload = replace(owner, filesystem, name, media, &serde_json::to_vec(&plan).unwrap()).await;
    command
}

impl Fixture {
    async fn new(case: &str) -> Self {
        let session = tempfile::tempdir().unwrap();
        let (owner, filesystem, request, raw) = root_input_fixture(session.path(), None, |_| {}).await;
        let work = work();
        let private_root = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
            session.path(), &work,
        ).unwrap();
        let staged = stage_root_command(&owner, &filesystem, &request, &work).await.unwrap();
        let command = alter_command(case, &owner, &filesystem, &request, staged.0).await;
        let filesystem = Arc::new(filesystem);
        let runtime = open_runtime(session.path(), &owner, filesystem.clone(), &work).await;
        let admission = runtime.admit_root_with_native_work(
            request.binding.clone(), request.root.clone(), command.clone(), &work,
        ).await.unwrap();
        assert!(matches!(admission.0, acyclic_harness::Admission::Accepted(_)));
        drop(admission.0);
        drop(admission.1);
        Self {
            runtime,
            filesystem,
            raw,
            owner,
            request,
            command,
            session,
            private_root,
            work,
            _command_custody: staged.1,
        }
    }

    async fn head(&self) -> acyclic_fs::GenerationId {
        self.raw
            .open_workspace(self.owner.policy.journal.storage_name().unwrap())
            .await
            .unwrap()
            .head()
            .await
            .unwrap()
            .id()
    }

    async fn load(&self) -> Result<PiRootPreparation> {
        PiRootPreparation::load(
            &self.private_root, Path::new(""),
            &self.runtime,
            &self.filesystem,
            &self.request,
            &self.work,
        )
        .await
    }

    async fn assert_idle(&self) {
        assert!(matches!(
            self.runtime
                .runtime
                .task_host()
                .recover_work(self.request.binding.task_id)
                .await
                .unwrap(),
            acyclic_harness::distributed::WorkPull::Idle
        ));
        assert!(!self.session.path().join("unopened-selected-root").exists());
    }

    async fn reopen(mut self) -> Self {
        drop(self.runtime);
        drop(self.filesystem);
        drop(self.raw);
        self.raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            self.session.path().join("filesystem"),
        )).await.unwrap();
        self.filesystem = Arc::new(FilesystemHost::new(
            self.raw.clone(), self.owner.policy.journal.provider().clone(),
        ).unwrap());
        self.runtime = open_runtime(self.session.path(), &self.owner, self.filesystem.clone(), &self.work).await;
        self
    }
}

#[tokio::test]
async fn pi_preparation_dispatch_requires_current_uncancelled_original_execution() {
    use acyclic_harness::{
        distributed::{WorkPull, Worker},
        runtime::DurableTaskHost,
        scheduler::LeaseFence,
    };

    let fixture = Fixture::new("").await;
    let host = fixture.runtime.runtime.task_host();
    let WorkPull::Claimed(lease) = host
        .pull_work(&Worker {
            id: "pi-root-preparation".to_owned(),
            available: Default::default(),
            labels: Default::default(),
        })
        .await
        .unwrap()
    else {
        panic!("root did not obtain its original execution lease");
    };
    host.start_task(&lease).await.unwrap();
    let fence = LeaseFence::from(&lease.reservation);
    let head = fixture.head().await;
    let context = fixture.runtime.runtime.task_context_with_native_work(
        fixture.request.binding.task_id, &fence, &fixture.work,
    ).await.unwrap();
    let (preparation, owner) = PiRootPreparation::load_for_dispatch(
        &fixture.private_root, Path::new(""),
        &fixture.runtime,
        &fixture.filesystem,
        &fixture.request,
        &fence,
        context.clone(),
        &fixture.work,
    )
    .await
    .unwrap();
    assert_eq!(preparation.request(), &fixture.request);
    assert_eq!(preparation.command(), &fixture.command);
    assert!(Arc::ptr_eq(preparation.native_context(), &context));
    owner.verify_dispatch().await.unwrap();

    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(
        PiRootPreparation::load_for_dispatch(
            &fixture.private_root, Path::new(""),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
            &stale,
            context.clone(),
            &fixture.work,
        )
        .await
        .is_err()
    );
    host.cancel(fixture.request.binding.task_id).await.unwrap();
    assert!(owner.verify_dispatch().await.is_err());
    assert!(
        PiRootPreparation::load_for_dispatch(
            &fixture.private_root, Path::new(""),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
            &fence,
            context,
            &fixture.work,
        )
        .await
        .is_err()
    );
    assert_eq!(fixture.head().await, head);
    assert!(
        !fixture
            .session
            .path()
            .join("unopened-selected-root")
            .exists()
    );
    for volume in fixture
        .request
        .binding
        .project_volumes
        .values()
        .chain(std::iter::once(&fixture.request.binding.scratch.volume))
    {
        assert!(
            fixture
                .raw
                .open_workspace(volume.storage_name().unwrap())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn pi_preparation_reads_original_pins_after_changed_heads_and_cold_reopen() {
    let fixture = Fixture::new("").await;
    fixture.assert_idle().await;
    let loaded = fixture.load().await.unwrap();
    assert_eq!(loaded.command(), &fixture.command);
    assert_eq!(loaded.request(), &fixture.request);
    assert_eq!(loaded.plan().provider, ROOT_ALLOCATION_PROVIDER);
    drop(loaded);
    let mut changed = fixture.request.clone();
    changed.binding.native_session_id = "changed-head".to_owned();
    replace(
        &fixture.owner,
        &fixture.filesystem,
        "root",
        "application/json",
        &serde_json::to_vec(&changed).unwrap(),
    )
    .await;
    replace(
        &fixture.owner,
        &fixture.filesystem,
        "allocation-schema",
        "application/schema+json",
        b"true",
    )
    .await;
    let head = fixture.head().await;
    assert_eq!(fixture.load().await.unwrap().request(), &fixture.request);
    assert_eq!(fixture.head().await, head);
    fixture.assert_idle().await;
    let fixture = fixture.reopen().await;
    assert_eq!(fixture.load().await.unwrap().request(), &fixture.request);
    assert_eq!(fixture.load().await.unwrap().command(), &fixture.command);
    assert_eq!(fixture.head().await, head);
    fixture.assert_idle().await;
}

#[tokio::test]
async fn pi_preparation_rejects_other_original_keys_and_any_policy_drift() {
    let fixture = Fixture::new("").await;
    let head = fixture.head().await;
    for case in ["key", "bytes", "concurrency", "depth"] {
        let other = tempfile::tempdir().unwrap();
        let mut owner = PiOwnerConfig::load(fixture.session.path()).unwrap();
        match case {
            "key" => owner = PiOwnerConfig::new(&owner.policy, None, &fixture.work).unwrap(),
            "bytes" => owner.policy.maximum_payload_bytes /= 2,
            "concurrency" => owner.policy.concurrency += 1,
            "depth" => owner.policy.session_limits.depth += 1,
            _ => unreachable!(),
        }
        owner.save_new(other.path()).unwrap();
        let private_root = acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
            other.path(), &fixture.work,
        ).unwrap();
        let result = PiRootPreparation::load(
            &private_root, Path::new(""),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
            &fixture.work,
        )
        .await;
        assert!(
            matches!(result, Err(Error::Unauthorized(_))),
            "accepted {case}"
        );
        assert_eq!(fixture.head().await, head);
        fixture.assert_idle().await;
    }
    assert!(fixture.load().await.is_ok());
}

#[tokio::test]
async fn pi_preparation_rejects_admitted_command_artifact_substitution_before_allocation() {
    for case in [
        "operation",
        "provider",
        "effect",
        "guarantee",
        "request",
        "schema",
        "path",
        "media",
        "encoding",
        "duplicate_schema",
    ] {
        let fixture = Fixture::new(case).await;
        let head = fixture.head().await;
        assert!(fixture.load().await.is_err(), "accepted {case}");
        assert_eq!(fixture.head().await, head, "mutated storage for {case}");
        fixture.assert_idle().await;
    }
}

#[tokio::test]
async fn pi_preparation_native_state_retains_original_root_and_shared_context() {
    let fixture = Fixture::new("").await;
    let PiRootIntent::Project { context_id, root_id, paths } = &fixture.request.root else {
        panic!("ordinary fixture has no selected project");
    };
    // Ordinary fixture configuration, not a native metadata producer.
    let mut state = AdapterState {
        version: ADAPTER_STATE_VERSION,
        task_binding: PluginTaskBinding::Pi(Box::new(fixture.request.binding.clone())),
        root_context_id: *context_id,
        root_id: *root_id,
        roots: paths.iter().map(|(key, path)| (key.clone(), RootBinding {
            root_id: *root_id, path: PathBuf::from(path), allocation: RootAllocation::Preparing,
        })).collect(),
        ..AdapterState::default()
    };
    let head = fixture.head().await;
    let original = PiRootPreparation::load_native_from_state(
        &fixture.private_root, Path::new(""), &fixture.runtime, &fixture.filesystem, &state, None, &fixture.work,
    ).await.unwrap();
    assert_eq!(original.request(), &fixture.request);
    assert_eq!(original.command(), &fixture.command);
    let context = original.native_context().clone();
    let supplied = PiRootPreparation::load_native_from_state(
        &fixture.private_root, Path::new(""), &fixture.runtime, &fixture.filesystem, &state,
        Some(context.clone()), &fixture.work,
    ).await.unwrap();
    assert!(Arc::ptr_eq(supplied.native_context(), &context));
    assert_eq!(supplied.request(), &fixture.request);
    state.roots.values_mut().next().unwrap().path.push("not-original");
    assert!(PiRootPreparation::load_native_from_state(
        &fixture.private_root, Path::new(""), &fixture.runtime, &fixture.filesystem, &state,
        Some(context), &fixture.work,
    ).await.is_err());
    assert_eq!(fixture.head().await, head);
}

#[test]
fn native_preparation_operation_preserves_original_digest_and_admits_before_hashing() {
    use crate::pi_root_inputs::{preparation_operation, preparation_operation_with_native_work};
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let task = acyclic_harness::TaskId::from_bytes([93; 16]);
    let work = work();
    let operation = preparation_operation_with_native_work(task, &work).unwrap();
    assert_eq!(operation, preparation_operation(task));
    let limited = NativeMaterializationWork::new(WorkBudget {
        bytes_hashed: 0, ..crate::pi_root_provider::allocation_budget()
    }, &CancellationToken::new()).unwrap();
    let before = limited.receipt();
    assert!(preparation_operation_with_native_work(task, &limited).is_err());
    assert_eq!(limited.receipt(), before);
}
