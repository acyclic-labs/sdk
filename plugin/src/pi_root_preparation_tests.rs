use super::*;
use crate::{pi_root_inputs::PiRootPreparation, pi_runtime::PiTaskRuntime};
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
    runtime: PiTaskRuntime,
    private: std::path::PathBuf,
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
) -> PiTaskRuntime {
    let stream = Arc::new(
        LocalStream::open(session.join("stream"), LocalStreamLimits::default())
            .await
            .unwrap(),
    );
    PiTaskRuntime::open_with_native_work(
        stream,
        filesystem,
        &owner.policy.journal,
        owner.authority(work).unwrap(),
        work,
    )
    .await
    .unwrap()
}

impl Fixture {
    async fn new() -> Self {
        Self::with_policy(root_policy()).await
    }

    async fn with_policy(policy: crate::pi_owner::PiOwnerPolicy) -> Self {
        let session = tempfile::tempdir().unwrap();
        let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
            session.path().join("filesystem"),
        ))
        .await
        .unwrap();
        let (owner, filesystem, request) = root_input_fixture(session.path(), &raw, policy).await;
        let work = work();
        let private = session.path().to_path_buf();
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
            .open_workspace(
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
        let owner = PiOwnerConfig::load(&self.private, Path::new(""), &self.work)?;
        PiRootPreparation::load(
            &owner,
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
        let authority = self.owner.authority(&self.work).unwrap();
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
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await
            .unwrap()
    }

    async fn substitute(&mut self, case: &str) {
        if case == "operation" {
            self.command.operation_id = OperationId::from_bytes([99; 16]);
            return;
        }
        let authority = self.owner.authority(&self.work).unwrap();
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
                        &acyclic_harness::contract::canonical_json_bytes(&request).unwrap(),
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
                    .await;
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
                    duplicate.extend_from_slice(encoded.get(1..).unwrap());
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
async fn pi_approval_journal_construction_always_attaches_checked_files() {
    use acyclic_harness::{core::SchemaRegistry, filesystem::FilesystemExecutionJournal};
    let mut policy = root_policy();
    policy.grants = Capabilities::new(policy.grants.iter().map(str::to_owned).chain([
        "interaction:route".to_owned(),
        "interaction:open".to_owned(),
    ]));
    let directory = tempfile::tempdir().unwrap();
    let raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .unwrap();
    let (protected, filesystem, request) = root_input_fixture(directory.path(), &raw, policy).await;
    let work = work();
    let storage = protected.authority(&work).unwrap();
    let grant = ContentGrant::verify(
        &storage.verifier,
        &storage.signed,
        &protected.policy.journal,
        VolumeOperation::Write,
        protected.policy.maximum_payload_bytes,
        &mut None,
    )
    .unwrap();
    // Construction needs an admitted task, but does not execute its command.
    let command = WorkflowCommand {
        operation_id: crate::pi_root_inputs::preparation_operation(
            protected.policy.root_task,
            &work,
        )
        .unwrap(),
        kind: acyclic_harness::filesystem::EFFECT_TASK_COMMAND_KIND.into(),
        payload: filesystem
            .put_content(
                &protected.policy.journal,
                &grant,
                "pi-inputs/constructor.json",
                b"{}",
                "application/json",
                "constructor.json",
                protected.policy.maximum_payload_bytes,
                &IdempotencyKey::new("constructor-fixture").unwrap(),
                None,
                acyclic_harness::conversation::ContentAccess::default(),
            )
            .await
            .unwrap(),
    };
    let runtime = open(directory.path(), &protected, filesystem.clone(), &work).await;
    let task = request.binding.task_id;
    runtime
        .admit_root(&request.binding, &request.root, &command, &work)
        .await
        .unwrap();
    let acyclic_harness::distributed::WorkPull::Claimed(lease) = runtime
        .runtime
        .task_host()
        .pull_work(
            &acyclic_harness::distributed::Worker {
                id: "pi-approval-constructor".into(),
                available: Default::default(),
                labels: Default::default(),
            },
            Some(OperationId::from_bytes(task.into_bytes())),
            &mut None,
        )
        .await
        .unwrap()
    else {
        panic!("expected the admitted root lease");
    };
    runtime
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await
        .unwrap();
    let fence = acyclic_harness::scheduler::LeaseFence::from(&lease.reservation);
    let io = acyclic_harness::filesystem::PreparationIo::from_native_work(&work).unwrap();
    let conversation = protected
        .conversation_authority_with_native_work(
            task,
            protected.policy.agent,
            storage.signed.capabilities(),
            &work,
        )
        .unwrap();
    let verifier = conversation.0.1.verifier(&mut Some(&mut &io)).unwrap();
    let binding = || acyclic_harness::filesystem::FilesystemJournalBinding {
        volume: &protected.policy.journal,
        verifier: &storage.verifier,
        scope: &storage.signed,
        maximum_bytes: protected.policy.maximum_payload_bytes,
        io: Some(&io),
    };
    // Both Pi command and prepared-writeback callers use this constructor,
    // then bind their selected private interaction content volume.
    for private in [&protected.policy.journal, &request.binding.scratch.volume] {
        for attached in [false, true] {
            let owner = runtime
                .runtime
                .task_host()
                .journal_owner(task, &fence, Some(&io))
                .await
                .unwrap();
            let journal = if attached {
                runtime
                    .execution_journal(
                        owner,
                        command.operation_id,
                        command.operation_id,
                        filesystem.clone(),
                        binding(),
                    )
                    .unwrap()
            } else {
                // Negative control restores the old caller construction.
                FilesystemExecutionJournal::for_task(
                    owner,
                    command.operation_id,
                    command.operation_id,
                    filesystem.clone(),
                    binding(),
                )
                .unwrap()
            };
            let result = journal.with_interaction_owner(
                &verifier,
                &SchemaRegistry::new(),
                &conversation.0.2,
                private,
                Some(&io),
            );
            assert_eq!(result.is_ok(), attached);
            if !attached {
                assert_eq!(
                    result.err().map(|error| error.code()),
                    Some(acyclic_harness::wire::ErrorCode::Invalid)
                );
            }
        }
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
        let authority = owner.authority(native).unwrap();
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
            acyclic_harness::contract::canonical_json_bytes(&request).unwrap()
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
        &PiOwnerConfig::load(&fixture.private, Path::new(""), &fixture.work).unwrap(),
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
            &acyclic_harness::contract::canonical_json_bytes(&request).unwrap(),
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
    fixture.work = work();
    fixture.owner =
        PiOwnerConfig::load(fixture.session.path(), Path::new(""), &fixture.work).unwrap();
    fixture.raw = acyclic_fs::LocalFs::local(acyclic_fs::LocalOptions::new(
        fixture.session.path().join("filesystem"),
    ))
    .await
    .unwrap();
    fixture.filesystem = FilesystemHost::new(
        fixture.raw.clone(),
        fixture.owner.policy.journal.provider(),
        None,
    )
    .unwrap();
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
        let mut owner =
            PiOwnerConfig::load(fixture.session.path(), Path::new(""), &fixture.work).unwrap();
        match case {
            "key" => owner = PiOwnerConfig::new(&owner.policy, None, &fixture.work).unwrap(),
            "bytes" => owner.policy.maximum_payload_bytes /= 2,
            "concurrency" => owner.policy.concurrency += 1,
            "depth" => owner.policy.session_limits.depth += 1,
            _ => unreachable!(),
        }
        publish_owner(&owner, other.path(), &fixture.work).unwrap();
        let private = other.path();
        assert!(matches!(
            PiRootPreparation::load(
                &PiOwnerConfig::load(private, Path::new(""), &fixture.work).unwrap(),
                &fixture.runtime,
                &fixture.filesystem,
                &fixture.request.binding,
                crate::pi_recovery::PiTaskOrigin::Root(&fixture.request.root),
                None,
                &fixture.work,
            )
            .await,
            Err(Error::Unauthorized(_)
                | Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    ..
                })
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
