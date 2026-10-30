use super::*;
use acyclic_fs::{OperationId, WorkCounters};
use proptest::prelude::*;

fn generation_associations(digest: [u8; 32], root: [u8; 16]) -> [acyclic_fs::GenerationId; 5] {
    use acyclic_fs::{Digest, FileId, GenerationId, ObjectId, ObjectKind, ObjectStorage};
    let plain = GenerationId::new(Digest::from_bytes(digest));
    let sealed = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: plain.digest(),
        storage: Some(ObjectStorage {
            locator: Digest::from_bytes(digest),
            root_file_id: FileId::from_bytes(root),
            origin_generation: Digest::from_bytes(digest),
        }),
    };
    let mut variants = [plain, GenerationId::from_root(sealed), plain, plain, plain];
    for field in 0..3 {
        let mut other = sealed;
        let storage = other.storage.as_mut().unwrap();
        let mut changed = digest;
        changed[0] ^= 1;
        match field {
            0 => storage.locator = Digest::from_bytes(changed),
            1 => {
                let mut changed = root;
                changed[0] ^= 1;
                storage.root_file_id = FileId::from_bytes(changed);
            }
            _ => storage.origin_generation = Digest::from_bytes(changed),
        }
        variants[field + 2] = GenerationId::from_root(other);
    }
    variants
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Only an Applied record for this exact root and generation transition
    // suppresses native replay. Every other phase must continue recovery.
    #[test]
    fn completion_requires_exact_original_root_and_transition(
        root in any::<[u8; 16]>(), from in any::<[u8; 32]>(), to in any::<[u8; 32]>(),
        phase in 0usize..6, change in 0usize..4, from_case in 0usize..5, to_case in 0usize..5,
    ) {
        use acyclic_fs::{Digest, GenerationId, MaterializationIntent, MaterializationJournal, MaterializationPhase, MaterializationPlan, NativeRootIdentity};
        let from_variants = generation_associations(from, root);
        let to_variants = generation_associations(to, root);
        let from = from_variants[from_case];
        let to = to_variants[to_case];
        let phases = [MaterializationPhase::Preparing, MaterializationPhase::Prepared, MaterializationPhase::Applying, MaterializationPhase::Applied, MaterializationPhase::RollingBack, MaterializationPhase::RolledBack];
        let journal = MaterializationJournal {
            version: 1, revision: 1,
            plan: MaterializationPlan { operation_id: OperationId::from_bytes([1; 16]), intent: MaterializationIntent::GenerationTransition { from, to }, edits: Vec::new() },
            root_identity: Some(root), preparation_identity: None, preparation_progress: None,
            preimages: Vec::new(), phase: phases[phase], applied: 0, restored: 0, work: WorkCounters::UNCHARGED, custody: None,
        };
        let mut actual_root = root;
        let mutate = |id: GenerationId| { let mut object = id.object_id(); let mut bytes = object.digest.into_bytes(); bytes[0] ^= 1; object.digest = Digest::from_bytes(bytes); GenerationId::from_root(object) };
        if change == 1 { actual_root[0] ^= 1; }
        let result = completed(&journal, NativeRootIdentity::from_bytes(actual_root), if change == 2 { mutate(from) } else { from }, if change == 3 { mutate(to) } else { to });
        prop_assert_eq!(result.is_ok(), change == 0);
        if let Ok(applied) = result { prop_assert_eq!(applied, phase == 3); }
        // Negative control: checking the phase alone acknowledges a foreign root
        // or transition. The production binding check must reject that shape.
        if phase == 3 && change != 0 { prop_assert!(journal.phase == MaterializationPhase::Applied); prop_assert!(result.is_err()); }
        for other in from_variants.into_iter().filter(|other| *other != from) {
            prop_assert_eq!(other.digest(), from.digest());
            prop_assert!(completed(&journal, NativeRootIdentity::from_bytes(root), other, to).is_err());
        }
        for other in to_variants.into_iter().filter(|other| *other != to) {
            prop_assert_eq!(other.digest(), to.digest());
            prop_assert!(completed(&journal, NativeRootIdentity::from_bytes(root), from, other).is_err());
        }
    }
}

use acyclic_fs::{
    CancellationToken, LocalFs, LocalOptions, MultiRootMaterializer, WorkBudget, WorkspaceRootId,
};
use std::collections::BTreeMap;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
enum Adapter {
    Git,
    MultiRoot,
    Prepared,
}

struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    state: LocalCoreStateStore,
    from: LocalGeneration,
    to: LocalGeneration,
    work: NativeMaterializationWork,
    identity: NativeRootIdentity,
}
impl Fixture {
    async fn new() -> TestResult<Self> {
        Self::with_alias(None).await
    }
    async fn with_alias(alias: Option<&str>) -> TestResult<Self> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let directory = tempfile::tempdir()?;
        let filesystem = LocalFs::local(LocalOptions::new(directory.path().join("fs"))).await?;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(
            WorkBudget {
                bytes_encoded: 1_048_576,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        let workspace = filesystem
            .create_workspace("checkout", WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let from = workspace.head(&work, &cancellation).await?;
        for name in ["note", ".git", ".system", ".volumes"] {
            workspace
                .write_text(&format!("/{name}"), "candidate")
                .await?;
        }
        if let Some(alias) = alias {
            workspace
                .write_text(&format!("/{alias}"), "candidate")
                .await?;
        }
        let to = workspace.head(&work, &cancellation).await?;
        let root = directory.path().join("checkout");
        std::fs::create_dir(&root)?;
        for name in [".git", ".system", ".volumes"] {
            std::fs::write(root.join(name), "host-owned")?;
        }
        let identity = work.root_identity(&root)?;
        let state = LocalCoreStateStore::new(directory.path().join("core"));
        Ok(Self {
            _directory: directory,
            root,
            state,
            from,
            to,
            work,
            identity,
        })
    }
    fn publication(&self, operation: OperationId) -> crate::Result<CheckoutPublication> {
        CheckoutPublication::new(&self.root, self.identity, operation, &self.work)
    }
    async fn publish(
        &self,
        adapter: Adapter,
        operation: OperationId,
        from: &LocalGeneration,
    ) -> crate::Result<()> {
        match adapter {
            Adapter::Git => {
                crate::tools::git::publish_checkout_transition(
                    &self.root,
                    self.identity,
                    &self.state,
                    operation,
                    from.clone(),
                    self.to.clone(),
                    &|| std::future::ready(Ok(())),
                )
                .await
            }
            Adapter::MultiRoot => {
                let root = WorkspaceRootId::from_bytes([19; 16]);
                let materializer = super::super::workspace_publication::WorkspaceRootMaterializer {
                    state: self.state.clone(),
                    physical_roots: BTreeMap::from([(
                        root,
                        super::super::workspace_publication::CheckoutRoot {
                            workspace_id: self.to.workspace_id(),
                            path: self.root.clone(),
                            source_root_identity: self.identity,
                        },
                    )]),
                };
                materializer
                    .materialize(
                        operation,
                        root,
                        &self.to.workspace(),
                        from.id(),
                        self.to.id(),
                    )
                    .await
            }
            Adapter::Prepared => {
                let publication = self.publication(operation)?;
                let options = MaterializeOptions::native(publication.directory().join("target"));
                publication
                    .publish(from, &self.to, &self.state, &options, &self.work, || {
                        std::future::ready(Ok(()))
                    })
                    .await?;
                Ok(())
            }
        }
    }
    async fn assert_resume_and_acknowledgement(&self, adapter: Adapter) -> TestResult {
        let operation = OperationId::new();
        let publication = self.publication(operation)?;
        let options = MaterializeOptions::native(publication.directory().join("target"));
        let prepared = publication
            .prepare(&self.from, &self.to, &self.state, &options, &self.work)
            .await?;
        assert_eq!(prepared.phase, acyclic_fs::MaterializationPhase::Prepared);
        assert!(!self.root.join("note").exists());
        // Negative control: the low-level path cannot resume the original journal
        // from an already committed logical head. Every adapter needs the owner.
        let wrong = acyclic_fs::publish_native_generation_transition(
            &self.to,
            &self.to,
            &self.state,
            publication.request(&self.to, &self.to, &options, &self.work),
            self.work.clone(),
            || std::future::ready(Ok(())),
        )
        .await;
        assert!(wrong.is_err());
        assert!(!self.root.join("note").exists());
        self.publish(adapter, operation, &self.to).await?;
        assert_eq!(
            std::fs::read_to_string(self.root.join("note"))?,
            "candidate"
        );
        for name in [".git", ".system", ".volumes"] {
            assert_eq!(std::fs::read_to_string(self.root.join(name))?, "host-owned");
        }
        assert!(!publication.directory().exists());
        std::fs::write(self.root.join("note"), "edit after acknowledgement")?;
        self.publish(adapter, operation, &self.to).await?;
        assert_eq!(
            std::fs::read_to_string(self.root.join("note"))?,
            "edit after acknowledgement"
        );
        // Completion never bypasses a revoked guard.
        let denied = publication
            .publish(
                &self.to,
                &self.to,
                &self.state,
                &options,
                &self.work,
                || {
                    std::future::ready(Err(acyclic_fs::NativeTreeMaterializationError::Io(
                        std::io::ErrorKind::PermissionDenied.into(),
                    )
                    .into()))
                },
            )
            .await;
        assert!(denied.is_err());
        assert_eq!(
            std::fs::read_to_string(self.root.join("note"))?,
            "edit after acknowledgement"
        );
        Ok(())
    }
}

#[tokio::test]
async fn git_adapter_resumes_original_checkout_and_preserves_acknowledged_edits() -> TestResult {
    Fixture::new()
        .await?
        .assert_resume_and_acknowledgement(Adapter::Git)
        .await
}

// Internal authority model for the publication boundary. Real authenticated
// file/journal settlement is exercised by execution_journal::approvals_tests.
#[derive(Clone, Copy)]
struct PublicationDecision {
    id: crate::InteractionId,
    operation: crate::OperationId,
    digest: [u8; 32],
    approved: bool,
}
impl crate::tool::ToolApprovalVerifier for PublicationDecision {
    fn verify<'a>(
        &'a self,
        _: crate::InteractionId,
        _: crate::OperationId,
        _: [u8; 32],
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<()>> {
        Box::pin(std::future::ready(Err(crate::Error::Unsupported(
            "selected approval requires original native work".into(),
        ))))
    }
    fn verify_with_native_work<'a>(
        &'a self,
        id: crate::InteractionId,
        operation: crate::OperationId,
        digest: [u8; 32],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            if self.approved
                && id == self.id
                && operation == self.operation
                && digest == self.digest
            {
                Ok(())
            } else {
                Err(crate::Error::Unauthorized(
                    "publication decision is absent, denied or foreign".into(),
                ))
            }
        })
    }
}

#[tokio::test]
async fn preparation_and_journal_reads_keep_original_custody() -> TestResult {
    let fixture = Fixture::new().await?;
    let operation = OperationId::new();
    let publication = fixture.publication(operation)?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let journal = publication
        .prepare(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
        )
        .await?;
    assert!(
        journal
            .custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(&fixture.work))
    );
    let loaded = acyclic_fs::load_native_materialization_journal(
        &fixture.state,
        operation,
        fixture.work.clone(),
    )
    .await
    .map_err(|(cause, _)| cause)?
    .value
    .ok_or("prepared journal is absent")?;
    assert!(
        loaded
            .custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(&fixture.work))
    );
    // The former preparation boundary constructed a separate ledger from the
    // remaining counters. Its journal retains that separate owner even after
    // the caller adds the receipt back to its own counters.
    let detached =
        NativeMaterializationWork::new(fixture.work.remaining()?, fixture.work.cancellation())
            .map_err(|(cause, _)| cause)?;
    let other = fixture.publication(OperationId::new())?;
    let options = MaterializeOptions::native(other.directory().join("target"));
    let journal = acyclic_fs::materializer::prepare_native_generation_transition(
        &fixture.from,
        &fixture.to,
        &fixture.state,
        other.request(&fixture.from, &fixture.to, &options, &detached),
        detached.clone(),
    )
    .await
    .map_err(|(cause, _)| cause)?
    .value;
    assert!(
        journal
            .custody
            .as_ref()
            .is_some_and(|custody| !custody.work().shares_ledger(&fixture.work))
    );
    assert!(!fixture.root.join("note").exists());
    Ok(())
}

#[tokio::test]
async fn selected_approval_binds_original_root_transition_paths_and_creator() -> TestResult {
    let fixture = Fixture::new().await?;
    let publication = fixture.publication(OperationId::new())?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let mut journal = publication
        .prepare(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
        )
        .await?;
    let paths = vec!["note".into()];
    let encode =
        |journal: &MaterializationJournal, paths: &[String], creator| -> TestResult<Vec<u8>> {
            Ok(serde_json::to_vec(&publication.approval_value(
                &fixture.from,
                &fixture.to,
                paths,
                journal,
                creator,
            )?)?)
        };
    let original = encode(&journal, &paths, "original creator")?;
    assert_ne!(original, encode(&journal, &paths, "foreign creator")?);
    assert!(encode(&journal, &["note-other".into()], "original creator").is_err());
    assert!(encode(&journal, &[], "original creator").is_err());
    assert!(
        publication
            .approval_value(
                &fixture.to,
                &fixture.to,
                &paths,
                &journal,
                "original creator"
            )
            .is_err()
    );
    let original_identity = journal.root_identity;
    journal.root_identity = Some([0; 16]);
    assert!(encode(&journal, &paths, "original creator").is_err());
    // The former raw tuple encoder accepts a foreign-root or unselected plan;
    // callers must obtain the validated view from the shared publication owner.
    assert!(serde_json::to_vec(&journal).is_ok());
    journal.root_identity = original_identity;
    journal.phase = acyclic_fs::MaterializationPhase::Applying;
    assert!(encode(&journal, &paths, "original creator").is_err());
    journal.phase = acyclic_fs::MaterializationPhase::Prepared;
    assert_eq!(original, encode(&journal, &paths, "original creator")?);
    let approval = publication.approval_value(
        &fixture.from,
        &fixture.to,
        &paths,
        &journal,
        "original creator",
    )?;
    let confirmation = approval.digest(&fixture.work)?;
    // Independent canonical encoding proves every host retains the existing
    // BLAKE3 identity while the shared owner funds encoding and hashing.
    let mut canonical = serde_json::to_value(&approval)?;
    canonical.sort_all_objects();
    assert_eq!(
        confirmation,
        *blake3::hash(&serde_json::to_vec(&canonical)?).as_bytes()
    );
    let interaction = crate::InteractionId::from_bytes([97; 16]);
    let verifier = PublicationDecision {
        id: interaction,
        operation: crate::OperationId::from_bytes(publication.operation.into_bytes()),
        digest: confirmation,
        approved: true,
    };
    assert!(
        approval
            .authorize(&verifier, interaction, &[0; 32], &fixture.work)
            .await
            .is_err()
    );
    // The exact public digest is insufficient: declined and foreign journal
    // decisions cannot construct native publication authority.
    for refused in [
        PublicationDecision {
            approved: false,
            ..verifier
        },
        PublicationDecision {
            id: crate::InteractionId::from_bytes([98; 16]),
            ..verifier
        },
        PublicationDecision {
            operation: crate::OperationId::new(),
            ..verifier
        },
        PublicationDecision {
            digest: [0; 32],
            ..verifier
        },
    ] {
        let candidate = publication.approval_value(
            &fixture.from,
            &fixture.to,
            &paths,
            &journal,
            "original creator",
        )?;
        assert!(
            candidate
                .authorize(&refused, interaction, &confirmation, &fixture.work)
                .await
                .is_err()
        );
        assert!(
            !fixture.root.join("note").exists(),
            "unapproved exact digest reached host effect"
        );
    }
    let applied = publication
        .approval_value(
            &fixture.from,
            &fixture.to,
            &paths,
            &journal,
            "original creator",
        )?
        .authorize(&verifier, interaction, &confirmation, &fixture.work)
        .await?
        .publish(&fixture.state, &options, || std::future::ready(Ok(())))
        .await?;
    assert_eq!(original, encode(&applied, &paths, "original creator")?);
    Ok(())
}

#[tokio::test]
async fn selected_approval_refuses_a_foreign_work_owner() -> TestResult {
    for kind in 0..3 {
        let mut fixture = Fixture::new().await?;
        let parent = fixture.work.clone();
        fixture.work = parent.invocation_view()?.0;
        let publication = fixture.publication(OperationId::new())?;
        let options = MaterializeOptions::native(publication.directory().join("target"));
        let paths = vec!["note".into()];
        let journal = publication
            .prepare_selected(
                &fixture.from,
                &fixture.to,
                &fixture.state,
                &options,
                &fixture.work,
                (&paths, "original creator"),
            )
            .await?;
        let approval = publication.approval_value(
            &fixture.from,
            &fixture.to,
            &paths,
            &journal,
            "original creator",
        )?;
        let digest = approval.digest(&fixture.work)?;
        let interaction = crate::InteractionId::from_bytes([101; 16]);
        let verifier = PublicationDecision {
            id: interaction,
            operation: crate::OperationId::from_bytes(publication.operation.into_bytes()),
            digest,
            approved: true,
        };
        let foreign = match kind {
            0 => NativeMaterializationWork::new(fixture.work.budget(), &CancellationToken::new())
                .map_err(|(cause, _)| cause)?,
            1 => parent,
            _ => fixture.work.native_volume_work(fixture.work.budget())?,
        };
        assert!(!foreign.shares_invocation_context(&fixture.work));
        if kind != 2 {
            fixture.work.cancellation().cancel();
        }
        assert!(
            matches!(
                approval.authorize(&verifier, interaction, &digest, &foreign).await,
                Err(crate::Error::Unauthorized(message))
                    if message == "checkout approval changed its original work owner"
            ),
            "foreign work owner authorized"
        );
        assert!(!fixture.root.join("note").exists());
    }
    Ok(())
}

#[tokio::test]
async fn selected_approval_refuses_an_ordinary_only_verifier() -> TestResult {
    struct OrdinaryOnly;
    impl crate::tool::ToolApprovalVerifier for OrdinaryOnly {
        fn verify<'a>(
            &'a self,
            _: crate::InteractionId,
            _: crate::OperationId,
            _: [u8; 32],
        ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<()>> {
            Box::pin(std::future::ready(Err(crate::Error::Unsupported(
                "selected approval fell back to ordinary verification".into(),
            ))))
        }
    }
    let fixture = Fixture::new().await?;
    let publication = fixture.publication(OperationId::new())?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let paths = vec!["note".into()];
    let journal = publication
        .prepare_selected(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
            (&paths, "original creator"),
        )
        .await?;
    let approval = publication.approval_value(
        &fixture.from,
        &fixture.to,
        &paths,
        &journal,
        "original creator",
    )?;
    let digest = approval.digest(&fixture.work)?;
    assert!(
        matches!(
            approval
                .authorize(
                    &OrdinaryOnly,
                    crate::InteractionId::new(),
                    &digest,
                    &fixture.work
                )
                .await,
            Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "approval verifier requires original native work support",
            })
        ),
        "selected approval fell back to ordinary verification"
    );
    assert!(!fixture.root.join("note").exists());
    Ok(())
}

#[tokio::test]
async fn selected_authority_retains_cancellation_and_original_work() -> TestResult {
    let fixture = Fixture::new().await?;
    let publication = fixture.publication(OperationId::new())?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let paths = vec!["note".into()];
    let journal = publication
        .prepare_selected(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
            (&paths, "original creator"),
        )
        .await?;
    let approval = publication.approval_value(
        &fixture.from,
        &fixture.to,
        &paths,
        &journal,
        "original creator",
    )?;
    let digest = approval.digest(&fixture.work)?;
    let interaction = crate::InteractionId::from_bytes([99; 16]);
    let verifier = PublicationDecision {
        id: interaction,
        operation: crate::OperationId::from_bytes(publication.operation.into_bytes()),
        digest,
        approved: true,
    };
    let authorized = approval
        .authorize(&verifier, interaction, &digest, &fixture.work)
        .await?;
    fixture.work.cancellation().cancel();
    assert!(
        authorized
            .publish(&fixture.state, &options, || std::future::ready(Ok(())))
            .await
            .is_err()
    );
    assert!(!fixture.root.join("note").exists());

    // Actual former argument shape: a caller could supply a fresh work context
    // after authorization. Its cancellation is unrelated to the approved work.
    // The public authority no longer has an argument that can select this path.
    let fresh = NativeMaterializationWork::new(fixture.work.budget(), &CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    publication
        .publish(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fresh,
            || std::future::ready(Ok(())),
        )
        .await?;
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note"))?,
        "candidate"
    );
    Ok(())
}
#[tokio::test]
async fn multi_root_adapter_resumes_original_checkout_and_preserves_acknowledged_edits()
-> TestResult {
    Fixture::new()
        .await?
        .assert_resume_and_acknowledgement(Adapter::MultiRoot)
        .await
}
#[tokio::test]
async fn prepared_adapter_resumes_original_checkout_and_preserves_acknowledged_edits() -> TestResult
{
    Fixture::new()
        .await?
        .assert_resume_and_acknowledgement(Adapter::Prepared)
        .await
}

#[tokio::test]
async fn every_adapter_refuses_retained_internal_edits_with_old_path_control() -> TestResult {
    for adapter in [Adapter::Git, Adapter::MultiRoot, Adapter::Prepared] {
        let fixture = Fixture::new().await?;
        for name in [".system", ".volumes"] {
            std::fs::remove_file(fixture.root.join(name))?;
        }
        let operation = OperationId::new();
        let publication = fixture.publication(operation)?;
        let options = MaterializeOptions::native(publication.directory().join("target"));
        let old_request = || {
            let mut request =
                publication.request(&fixture.from, &fixture.to, &options, &fixture.work);
            request.excluded_names = &[".git"];
            request
        };
        acyclic_fs::materializer::prepare_native_generation_transition(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            old_request(),
            fixture.work.clone(),
        )
        .await
        .map_err(|(cause, _)| cause)?;
        assert!(
            fixture
                .publish(adapter, operation, &fixture.from)
                .await
                .is_err()
        );
        for name in ["note", ".system", ".volumes"] {
            assert!(!fixture.root.join(name).exists());
        }
        // The former shape accepts the same prepared journal and leaks both
        // internal names. The shared owner rejects it before any host effect.
        acyclic_fs::publish_native_generation_transition(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            old_request(),
            fixture.work.clone(),
            || std::future::ready(Ok(())),
        )
        .await
        .map_err(|(cause, _)| cause)?;
        for name in [".system", ".volumes"] {
            assert_eq!(
                std::fs::read_to_string(fixture.root.join(name))?,
                "candidate"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn every_adapter_refuses_fresh_and_retained_reserved_aliases_before_host_effect() -> TestResult
{
    for adapter in [Adapter::Git, Adapter::MultiRoot, Adapter::Prepared] {
        let fixture = Fixture::with_alias(Some(".GIT")).await?;
        let operation = OperationId::new();
        for _ in 0..2 {
            // The first call prepares a fresh transition; the second follows
            // the retained journal. Neither may publish even an allowed edit.
            assert!(
                fixture
                    .publish(adapter, operation, &fixture.from)
                    .await
                    .is_err()
            );
            assert!(!fixture.root.join("note").exists());
            for name in [".git", ".system", ".volumes"] {
                assert_eq!(
                    std::fs::read_to_string(fixture.root.join(name))?,
                    "host-owned"
                );
            }
        }
        // The actual former path publishes the forbidden spelling. On Windows
        // this resolves to the protected .git file through native lookup.
        let publication = fixture.publication(operation)?;
        let options = MaterializeOptions::native(publication.directory().join("target"));
        acyclic_fs::publish_native_generation_transition(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            publication.request(&fixture.from, &fixture.to, &options, &fixture.work),
            fixture.work.clone(),
            || std::future::ready(Ok(())),
        )
        .await
        .map_err(|(cause, _)| cause)?;
        assert_eq!(
            std::fs::read_to_string(fixture.root.join(".GIT"))?,
            "candidate"
        );
    }
    Ok(())
}

#[test]
fn every_reserved_ascii_spelling_is_refused_for_every_plan_edit() {
    use acyclic_fs::{
        Digest, GenerationId, MaterializationEdit, MaterializationIntent, MaterializationPlan,
    };
    let generation = GenerationId::new(Digest::from_bytes([0; 32]));
    for reserved in EXCLUDED_NAMES {
        for mask in 0..(1 << reserved.len()) {
            let spelling: String = reserved
                .bytes()
                .enumerate()
                .map(|(index, byte)| {
                    char::from(if mask & (1 << index) == 0 {
                        byte
                    } else {
                        byte.to_ascii_uppercase()
                    })
                })
                .collect();
            for path in [spelling.clone(), format!("{spelling}/config")] {
                for edit in [
                    MaterializationEdit::Install {
                        path: path.clone(),
                        image: vec![1],
                    },
                    MaterializationEdit::Remove { path: path.clone() },
                    MaterializationEdit::SetMetadata {
                        path: path.clone(),
                        image: vec![1],
                    },
                    MaterializationEdit::Rename {
                        from: path.clone(),
                        to: "note".into(),
                    },
                    MaterializationEdit::Rename {
                        from: "note".into(),
                        to: path.clone(),
                    },
                ] {
                    let mut plan = MaterializationPlan {
                        operation_id: OperationId::from_bytes([1; 16]),
                        intent: MaterializationIntent::GenerationTransition {
                            from: generation,
                            to: generation,
                        },
                        edits: vec![edit],
                    };
                    assert!(validate_plan(&plan).is_err(), "{path}");
                    // Exact-byte matching admits case aliases; this is the
                    // former predicate whose spelling-dependent gap is closed.
                    if path.split('/').next().unwrap() != *reserved {
                        assert!(!EXCLUDED_NAMES.contains(&path.split('/').next().unwrap()));
                    }
                    plan.edits = vec![MaterializationEdit::Remove {
                        path: format!("{path}-other"),
                    }];
                    if !path.contains('/') {
                        assert!(validate_plan(&plan).is_ok());
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn git_and_prepared_guards_keep_the_original_late_refusal() -> TestResult {
    for adapter in [Adapter::Git, Adapter::Prepared] {
        let fixture = Fixture::new().await?;
        let operation = OperationId::new();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let guard = || {
            std::future::ready(
                if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(Error::Unauthorized("revoked after initial guard".into()))
                },
            )
        };
        let result = match adapter {
            Adapter::Git => {
                crate::tools::git::publish_checkout_transition(
                    &fixture.root,
                    fixture.identity,
                    &fixture.state,
                    operation,
                    fixture.from.clone(),
                    fixture.to.clone(),
                    &guard,
                )
                .await
            }
            Adapter::Prepared => {
                let publication = fixture.publication(operation)?;
                let options = MaterializeOptions::native(publication.directory().join("target"));
                publication
                    .publish(
                        &fixture.from,
                        &fixture.to,
                        &fixture.state,
                        &options,
                        &fixture.work,
                        guard,
                    )
                    .await
                    .map(|_| ())
            }
            Adapter::MultiRoot => unreachable!("multi-root authorization owns its root map"),
        };
        assert!(
            matches!(result, Err(Error::Unauthorized(ref reason)) if reason == "revoked after initial guard")
        );
        assert!(calls.load(std::sync::atomic::Ordering::SeqCst) >= 2);
        assert!(!fixture.root.join("note").exists());
        // Negative control: the low-level error mapping erases the authority
        // cause. All adapters must preserve the shared owner's original error.
        let erased = Error::from(acyclic_fs::NativeWorkspacePublicationError::Native(
            acyclic_fs::NativeTreeMaterializationError::Io(
                std::io::ErrorKind::PermissionDenied.into(),
            ),
        ));
        assert!(!matches!(erased, Error::Unauthorized(_)));
    }
    Ok(())
}
