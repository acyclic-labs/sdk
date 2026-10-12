use super::*;
use crate::{pi_root_inputs::PiRootPreparation, pi_runtime::PiTaskRuntime};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    Admission, IdempotencyKey, OperationId, effect_host::TaskEffectPlan, workflow::WorkflowCommand,
};
use acyclic_stream::{LocalStream, LocalStreamLimits};

struct Fixture {
    session: tempfile::TempDir,
    raw: acyclic_fs::LocalFs,
    filesystem:
        Arc<FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>,
    owner: PiOwnerConfig,
    request: PiRootRequest,
    command: WorkflowCommand,
    runtime: PiTaskRuntime<LocalStream>,
    private: acyclic_fs::NativeHostPrivateStateRoot,
    work: acyclic_fs::NativeMaterializationWork,
    _command: acyclic_fs::NativeAllocation,
}

async fn open(
    session: &Path,
    owner: &PiOwnerConfig,
    filesystem: Arc<
        FilesystemHost<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    >,
    work: &acyclic_fs::NativeMaterializationWork,
) -> PiTaskRuntime<LocalStream> {
    let stream = Arc::new(
        LocalStream::open(session.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    );
    PiTaskRuntime::open_with_native_work(
        stream,
        filesystem,
        &owner.policy.journal,
        owner.authority_with_native_work(work).unwrap(),
        work,
    )
    .await
    .unwrap()
}

impl Fixture {
    async fn new() -> Self {
        let session = tempfile::tempdir().unwrap();
        let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap();
        let (owner, filesystem, request) =
            root_input_fixture(session.path(), &raw, root_policy()).await;
        let filesystem = Arc::new(filesystem);
        let work = work();
        let private =
            acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
                session.path(),
                &work,
            )
            .unwrap();
        let command = stage_root_command(&owner, &filesystem, &request, &work)
            .await
            .unwrap();
        let runtime = open(session.path(), &owner, filesystem.clone(), &work).await;
        Self {
            session,
            raw,
            filesystem,
            owner,
            request,
            command: command.0,
            runtime,
            private,
            work,
            _command: command.1,
        }
    }

    async fn head(&self) -> acyclic_fs::GenerationId {
        let query = work();
        self.raw
            .open_workspace_with_native_work(
                &self.owner.policy.journal.storage_name(&mut None).unwrap(),
                &query,
                query.cancellation(),
            )
            .await
            .unwrap()
            .head(&query, query.cancellation())
            .await
            .unwrap()
            .id()
    }

    async fn load(&self) -> Result<PiRootPreparation> {
        PiRootPreparation::load(
            &self.private,
            Path::new(""),
            &self.runtime,
            &self.filesystem,
            &self.request.binding,
            crate::pi_recovery::PiTaskOrigin::Root(&self.request.root),
            None,
            &self.work,
        )
        .await
    }

    async fn assert_idle(&self) {
        assert!(matches!(
            self.runtime
                .runtime
                .task_host()
                .recover_work(self.request.binding.task_id, &mut None)
                .await
                .unwrap(),
            acyclic_harness::distributed::WorkPull::Idle
        ));
        assert!(!self.session.path().join("unopened-selected-root").exists());
    }

    async fn replace(&self, name: &str, media: &str, bytes: &[u8]) -> FileRef {
        let authority = self.owner.authority().unwrap();
        let grant = ContentGrant::verify(
            &authority.verifier,
            &authority.signed,
            &self.owner.policy.journal,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )
        .unwrap();
        self.filesystem
            .put_content(
                &self.owner.policy.journal,
                &grant,
                &format!("{ROOT_INPUT_DIRECTORY}/{name}.json"),
                bytes,
                media,
                &format!("{name}.json"),
                self.owner.policy.maximum_payload_bytes,
                &IdempotencyKey::new(format!("fixture-replacement-{name}")).unwrap(),
                None,
                None,
            )
            .await
            .unwrap()
    }

    async fn substitute(&mut self, case: &str) {
        if case == "operation" {
            self.command.operation_id = OperationId::from_bytes([99; 16]);
            return;
        }
        let authority = self.owner.authority().unwrap();
        let grant = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.signed,
            &self.owner.policy.journal,
            ROOT_INPUT_DIRECTORY,
            &mut None,
        )
        .unwrap();
        let mut plan: TaskEffectPlan = serde_json::from_slice(
            &self
                .filesystem
                .read_content(
                    &self.command.payload,
                    &grant,
                    self.owner.policy.maximum_payload_bytes,
                    None,
                )
                .await
                .unwrap(),
        )
        .unwrap();
        match case {
            "provider" => plan.provider = "foreign".into(),
            "effect" => plan.effect_kind = "foreign@1".into(),
            "guarantee" => plan.guarantee = acyclic_harness::core::EffectGuarantee::ExactlyOnce,
            "request" => {
                let mut request = self.request.clone();
                request.binding.native_session_id = "different-session".into();
                plan.request = self
                    .replace(
                        "root",
                        "application/json",
                        &serde_json::to_vec(&request).unwrap(),
                    )
                    .await;
            }
            "schema" => {
                plan.result_schema = self
                    .replace(
                        "allocation-schema",
                        "application/schema+json",
                        br#"{"type":"object"}"#,
                    )
                    .await
            }
            "encoding" | "duplicate_schema" => {
                let schema = crate::pi_root_allocation::PiRootAllocation::schema(
                    &self.request.binding,
                    &self.work,
                )
                .unwrap();
                let bytes = if case == "encoding" {
                    serde_json::to_vec_pretty(&schema.0).unwrap()
                } else {
                    let encoded = serde_json::to_vec(&schema.0).unwrap();
                    let mut duplicate = br#"{"type":"object","#.to_vec();
                    duplicate.extend_from_slice(&encoded[1..]);
                    duplicate
                };
                plan.result_schema = self
                    .replace("allocation-schema", "application/schema+json", &bytes)
                    .await;
            }
            "path" | "media" => {}
            _ => unreachable!(),
        }
        self.command.payload = self
            .replace(
                if case == "path" {
                    "other-preparation"
                } else {
                    "preparation"
                },
                if case == "media" {
                    "application/schema+json"
                } else {
                    "application/json"
                },
                &serde_json::to_vec(&plan).unwrap(),
            )
            .await;
    }
}

#[tokio::test]
async fn pi_preparation_reads_original_pins_after_changed_heads_and_cold_reopen() {
    let mut fixture = Fixture::new().await;
    let original;
    {
        let owner = &fixture.owner;
        let files = &fixture.filesystem;
        let request = &fixture.request;
        let native = &fixture.work;
        original = stage_root_request(owner, files, request, native)
            .await
            .unwrap();
        let authority = owner.authority().unwrap();
        let signed = authority
            .issuer
            .issue(
                &owner.policy.root_task.to_string(),
                &owner.policy.grants,
                Some(owner.policy.agent),
                None,
                &mut None,
            )
            .unwrap();
        let read = ContentGrant::verify_read(&authority.verifier, &signed, &original.0).unwrap();
        assert_eq!(
            fixture
                .filesystem
                .read_content(&original.0, &read, owner.policy.maximum_payload_bytes, None)
                .await
                .unwrap()
                .as_ref(),
            serde_json::to_vec(&request).unwrap()
        );
        assert!(
            ContentGrant::verify(
                &authority.verifier,
                &signed,
                &owner.policy.journal,
                VolumeOperation::Write,
                u64::MAX,
                &mut None
            )
            .is_err()
        );
        for path in [
            "pi-inputs-sibling/root.json",
            ".system/harness-uploads/receipt.json",
        ] {
            let foreign = FileRef::new(
                original.0.volume().clone(),
                path.into(),
                original.0.version().to_owned(),
                original.0.descriptor().clone(),
                "root.json".into(),
                &mut None,
            )
            .unwrap();
            assert!(ContentGrant::verify_read(&authority.verifier, &signed, &foreign).is_err());
        }
        let mut changed = request.clone();
        let PiRootIntent::Project { paths, .. } = &mut changed.root else {
            unreachable!()
        };
        *paths.values_mut().next().unwrap() = fixture
            .session
            .path()
            .join("changed-selected-root")
            .to_str()
            .unwrap()
            .into();
        assert!(
            stage_root_request(owner, files, &changed, native)
                .await
                .is_err()
        );
        assert!(
            !fixture
                .session
                .path()
                .join("changed-selected-root")
                .exists()
        );
    }
    assert!(matches!(
        fixture
            .runtime
            .admit_root(
                &fixture.request.binding,
                &fixture.request.root,
                &fixture.command,
                &fixture.work
            )
            .await
            .unwrap()
            .0,
        Admission::Accepted(_)
    ));
    fixture.assert_idle().await;
    let loaded = fixture.load().await.unwrap();
    assert_eq!(loaded.command, fixture.command);
    assert_eq!(loaded.request, fixture.request);
    let supplied = PiRootPreparation::load(
        &fixture.private,
        Path::new(""),
        &fixture.runtime,
        &fixture.filesystem,
        &fixture.request.binding,
        crate::pi_recovery::PiTaskOrigin::Root(&fixture.request.root),
        Some(loaded.context.clone()),
        &fixture.work,
    )
    .await
    .unwrap();
    assert_eq!(supplied.request, fixture.request);
    drop(supplied);
    drop(loaded);
    let mut request = fixture.request.clone();
    request.binding.native_session_id = "changed-head".into();
    fixture
        .replace(
            "root",
            "application/json",
            &serde_json::to_vec(&request).unwrap(),
        )
        .await;
    fixture
        .replace("allocation-schema", "application/schema+json", b"true")
        .await;
    let head = fixture.head().await;
    assert_eq!(fixture.load().await.unwrap().request, fixture.request);
    assert_eq!(fixture.head().await, head);
    fixture.assert_idle().await;
    drop(fixture.runtime);
    drop(fixture.filesystem);
    drop(fixture.raw);
    fixture.raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        fixture.session.path().join("filesystem"),
    ))
    .await
    .unwrap();
    fixture.filesystem = Arc::new(
        FilesystemHost::new(
            fixture.raw.clone(),
            fixture.owner.policy.journal.provider().clone(),
        )
        .unwrap(),
    );
    fixture.runtime = open(
        fixture.session.path(),
        &fixture.owner,
        fixture.filesystem.clone(),
        &fixture.work,
    )
    .await;
    let reopened = fixture.load().await.unwrap();
    assert_eq!(reopened.request, fixture.request);
    assert_eq!(reopened.command, fixture.command);
    assert_eq!(
        stage_root_request(
            &fixture.owner,
            &fixture.filesystem,
            &fixture.request,
            &fixture.work
        )
        .await
        .unwrap()
        .0,
        original.0
    );
    assert_eq!(
        stage_root_command(
            &fixture.owner,
            &fixture.filesystem,
            &fixture.request,
            &fixture.work
        )
        .await
        .unwrap()
        .0,
        fixture.command
    );
    assert_eq!(fixture.head().await, head);
    fixture.assert_idle().await;
}

#[tokio::test]
async fn pi_preparation_rejects_other_original_keys_and_any_policy_drift() {
    let fixture = Fixture::new().await;
    assert!(matches!(
        fixture
            .runtime
            .admit_root(
                &fixture.request.binding,
                &fixture.request.root,
                &fixture.command,
                &fixture.work
            )
            .await
            .unwrap()
            .0,
        Admission::Accepted(_)
    ));
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
        publish_owner(&owner, other.path(), &fixture.work).unwrap();
        let private =
            acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(
                other.path(),
                &fixture.work,
            )
            .unwrap();
        assert!(matches!(
            PiRootPreparation::load(
                &private,
                Path::new(""),
                &fixture.runtime,
                &fixture.filesystem,
                &fixture.request.binding,
                crate::pi_recovery::PiTaskOrigin::Root(&fixture.request.root),
                None,
                &fixture.work
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(fixture.head().await, head);
        fixture.assert_idle().await;
    }
    fixture.load().await.unwrap();
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
        let mut fixture = Fixture::new().await;
        fixture.substitute(case).await;
        assert!(matches!(
            fixture
                .runtime
                .admit_root(
                    &fixture.request.binding,
                    &fixture.request.root,
                    &fixture.command,
                    &fixture.work
                )
                .await
                .unwrap()
                .0,
            Admission::Accepted(_)
        ));
        let head = fixture.head().await;
        assert!(fixture.load().await.is_err(), "{case}");
        assert_eq!(fixture.head().await, head);
        fixture.assert_idle().await;
    }
}
