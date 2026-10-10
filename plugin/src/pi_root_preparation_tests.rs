//! Original-admission artifact authentication, without allocation or dispatch.

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
}

async fn open_runtime(session: &Path, owner: &PiOwnerConfig, filesystem: Arc<Host>) -> Runtime {
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(session.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    ));
    PiTaskRuntime::open(
        stream,
        filesystem,
        owner.policy.journal.clone(),
        owner.authority().unwrap(),
    )
    .await
    .unwrap()
}

async fn replace_input(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    name: &str,
    media_type: &str,
    value: &impl serde::Serialize,
) -> FileRef {
    replace_bytes(
        owner,
        filesystem,
        name,
        media_type,
        &serde_json::to_vec(value).unwrap(),
    )
    .await
}

async fn replace_bytes(
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
                replace_input(owner, filesystem, "root", "application/json", &changed).await;
        }
        "schema" => {
            plan.result_schema = replace_input(
                owner,
                filesystem,
                "allocation-schema",
                "application/schema+json",
                &json!({"type":"object"}),
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
            plan.result_schema = replace_bytes(
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
    command.payload = replace_input(owner, filesystem, name, media, &plan).await;
    command
}

impl Fixture {
    async fn new(case: &str) -> Self {
        let session = tempfile::tempdir().unwrap();
        let (owner, filesystem, request, raw) = root_input_fixture(session.path(), |_| {}).await;
        let command = stage_root_command(&owner, &filesystem, &request)
            .await
            .unwrap();
        let command = alter_command(case, &owner, &filesystem, &request, command).await;
        let filesystem = Arc::new(filesystem);
        let runtime = open_runtime(session.path(), &owner, filesystem.clone()).await;
        assert!(matches!(
            runtime
                .admit_root(
                    request.binding.clone(),
                    request.root.clone(),
                    command.clone()
                )
                .await
                .unwrap(),
            acyclic_harness::Admission::Accepted(_)
        ));
        Self {
            runtime,
            filesystem,
            raw,
            owner,
            request,
            command,
            session,
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

    async fn load(&self) -> Result<PiRootPreparation, String> {
        PiRootPreparation::load(
            self.session.path(),
            &self.runtime,
            &self.filesystem,
            &self.request,
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

    async fn reopen(self) -> Self {
        let Self {
            runtime,
            filesystem,
            raw,
            owner,
            request,
            command,
            session,
        } = self;
        drop(runtime);
        drop(filesystem);
        drop(raw);
        let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap();
        let filesystem = Arc::new(
            FilesystemHost::new(raw.clone(), owner.policy.journal.provider().clone()).unwrap(),
        );
        let runtime = open_runtime(session.path(), &owner, filesystem.clone()).await;
        Self {
            runtime,
            filesystem,
            raw,
            owner,
            request,
            command,
            session,
        }
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
    let (preparation, owner) = PiRootPreparation::load_for_dispatch(
        fixture.session.path(),
        &fixture.runtime,
        &fixture.filesystem,
        &fixture.request,
        fence.clone(),
    )
    .await
    .unwrap();
    assert_eq!(preparation.request(), &fixture.request);
    assert_eq!(preparation.command(), &fixture.command);
    owner.verify_dispatch().await.unwrap();

    let mut stale = fence.clone();
    stale.reservation_id.push_str("-stale");
    assert!(
        PiRootPreparation::load_for_dispatch(
            fixture.session.path(),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
            stale,
        )
        .await
        .is_err()
    );
    host.cancel(fixture.request.binding.task_id).await.unwrap();
    assert!(owner.verify_dispatch().await.is_err());
    assert!(
        PiRootPreparation::load_for_dispatch(
            fixture.session.path(),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
            fence,
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
    let mut changed = fixture.request.clone();
    changed.binding.native_session_id = "changed-head".to_owned();
    replace_input(
        &fixture.owner,
        &fixture.filesystem,
        "root",
        "application/json",
        &changed,
    )
    .await;
    replace_input(
        &fixture.owner,
        &fixture.filesystem,
        "allocation-schema",
        "application/schema+json",
        &json!(true),
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
        let mut owner = fixture.owner.clone();
        match case {
            "key" => owner = PiOwnerConfig::new(owner.policy.clone()).unwrap(),
            "bytes" => owner.policy.maximum_payload_bytes /= 2,
            "concurrency" => owner.policy.concurrency += 1,
            "depth" => owner.policy.session_limits.depth += 1,
            _ => unreachable!(),
        }
        owner.save_new(other.path()).unwrap();
        let result = PiRootPreparation::load(
            other.path(),
            &fixture.runtime,
            &fixture.filesystem,
            &fixture.request,
        )
        .await;
        assert!(
            result
                .err()
                .unwrap()
                .contains("original runtime credentials or policy"),
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
