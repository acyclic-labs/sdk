use super::*;
use acyclic_fs::{OperationId, WorkCounters};
use proptest::prelude::*;

fn generation_associations(digest: [u8; 32], root: [u8; 16]) -> [acyclic_fs::GenerationId; 5] {
    use acyclic_fs::{Digest, FileId, GenerationId, ObjectId, ObjectKind, ObjectStorage};
    let plain = GenerationId::new(Digest::from_bytes(digest));
    let association = ObjectStorage {
        locator: Digest::from_bytes(digest),
        root_file_id: FileId::from_bytes(root),
        origin_generation: Digest::from_bytes(digest),
    };
    let sealed = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: plain.digest(),
        storage: Some(association),
    };
    let mut variants = [plain, GenerationId::from_root(sealed), plain, plain, plain];
    for field in 0..3 {
        let mut storage = association;
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
        variants[field + 2] = GenerationId::from_root(ObjectId {
            storage: Some(storage),
            ..sealed
        });
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
        let mut journal = MaterializationJournal::empty(
            MaterializationPlan { operation_id: OperationId::from_bytes([1; 16]), intent: MaterializationIntent::GenerationTransition { from, to }, edits: Vec::new() },
            Some(root),
        ).unwrap();
        journal.phase = phases[phase];
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

const USER_PATHS: [&str; 5] = [
    ".git/config",
    ".system/user.txt",
    ".volumes/home/user.txt",
    ".home-seed-v1",
    ".acyclic-sdk/user.txt",
];

struct Fixture {
    _directory: tempfile::TempDir,
    root: PathBuf,
    state: LocalCoreStateStore,
    from: LocalGeneration,
    to: LocalGeneration,
    work: NativeMaterializationWork,
    identity: NativeRootIdentity,
    paths: &'static [&'static str],
}
impl Fixture {
    async fn new() -> TestResult<Self> {
        Self::with_paths(&[]).await
    }
    async fn with_paths(paths: &'static [&'static str]) -> TestResult<Self> {
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
        if !paths.is_empty() {
            let mut original = workspace
                .begin_transaction(
                    acyclic_fs::IdempotencyKey::new(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await?
                .value;
            for name in paths {
                if let Some((parent, _)) = name.rsplit_once('/') {
                    original
                        .create_dir_all(&format!("/{parent}"), WorkBudget::UNBOUNDED, &cancellation)
                        .await?;
                }
                original.write_text(&format!("/{name}"), "original").await?;
            }
            original
                .commit::<acyclic_fs::WorkspaceError, _, _>(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    |_| {
                        std::future::ready(Ok(acyclic_fs::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await?;
        }
        let from = workspace.head(&work, &cancellation).await?;
        for name in std::iter::once("note").chain(paths.iter().copied()) {
            workspace
                .write_text(&format!("/{name}"), "candidate")
                .await?;
        }
        let to = workspace.head(&work, &cancellation).await?;
        let root = directory.path().join("checkout");
        std::fs::create_dir(&root)?;
        for name in paths {
            let path = root.join(name);
            std::fs::create_dir_all(path.parent().expect("fixture parent"))?;
            std::fs::write(path, "original")?;
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
            paths,
        })
    }
    fn publication(&self, operation: OperationId) -> crate::Result<CheckoutPublication> {
        CheckoutPublication::new(
            &self.root,
            self.identity,
            operation,
            &self.state,
            &self.work,
        )
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
                    .publish(
                        from,
                        &self.to,
                        &self.state,
                        &options,
                        (&self.work, None),
                        || std::future::ready(Ok(())),
                    )
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
        // Operations share external staging ancestors without reserving any
        // workspace name or needing write authority in the checkout's parent.
        assert!(publication.directory().starts_with(self.state.root()));
        assert!(!publication.directory().starts_with(&self.root));
        let shared_parent = publication.directory().parent().unwrap();
        let parent_identity = self.work.root_identity(shared_parent)?;
        assert!(options.destination.exists());
        let pending = self.publication(OperationId::new())?;
        let pending_options = MaterializeOptions::native(pending.directory().join("target"));
        pending
            .prepare(
                &self.from,
                &self.to,
                &self.state,
                &pending_options,
                &self.work,
            )
            .await?;
        let other_root = self.root.parent().unwrap().join("other-checkout");
        std::fs::create_dir(&other_root)?;
        for name in self.paths {
            let path = other_root.join(name);
            std::fs::create_dir_all(path.parent().expect("fixture parent"))?;
            std::fs::write(path, "original")?;
        }
        let other = CheckoutPublication::new(
            &other_root,
            self.work.root_identity(&other_root)?,
            OperationId::new(),
            &self.state,
            &self.work,
        )?;
        let other_options = MaterializeOptions::native(other.directory().join("target"));
        other
            .prepare(
                &self.from,
                &self.to,
                &self.state,
                &other_options,
                &self.work,
            )
            .await?;
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
        for name in self.paths {
            assert_eq!(
                std::fs::read_to_string(self.root.join(name))?,
                "candidate",
                "{name}"
            );
        }
        assert!(!publication.directory().exists());
        // Another operation on this root and another root own real Prepared
        // bytes. Retiring this operation must preserve both.
        assert!(pending_options.destination.exists());
        assert!(other_options.destination.exists());
        cleanup_directory(pending.directory())?;
        assert!(!pending.directory().exists());
        assert!(other_options.destination.exists());
        cleanup_directory(other.directory())?;
        assert!(!other.directory().exists());
        // Empty ancestors can already be bound into another Preparing journal,
        // before that operation creates its own directory. Keep their identities.
        assert_eq!(
            self.work.root_identity(shared_parent)?,
            parent_identity,
            "cleanup invalidated a shared preparation parent"
        );
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
                (&self.work, None),
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
    Fixture::with_paths(&USER_PATHS)
        .await?
        .assert_resume_and_acknowledgement(Adapter::Git)
        .await
}

#[tokio::test]
async fn retained_publication_resumes_its_original_transition_after_head_advances() -> TestResult {
    let fixture = Fixture::new().await?;
    let operation = OperationId::new();
    let publication = fixture.publication(operation)?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let original = publication
        .prepare(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
        )
        .await?;
    let workspace = fixture.to.workspace();
    workspace.write_text("/note", "later logical head").await?;
    let advanced = workspace
        .head(&fixture.work, fixture.work.cancellation())
        .await?;
    assert_ne!(advanced.id(), fixture.to.id());
    // Reconstructing the original operation from the later head is a real
    // transition mismatch, even when both supplied cuts are equal.
    assert!(
        publication
            .publish(
                &advanced,
                &advanced,
                &fixture.state,
                &options,
                (&fixture.work, None),
                || std::future::ready(Ok(())),
            )
            .await
            .is_err()
    );
    // A later checkout can select another workspace as well as another head.
    // Its capability cannot be used to reopen the journal's original cuts.
    let replacement = workspace
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "replacement",
            acyclic_fs::ForkOptions::from_generation(
                advanced.clone(),
                acyclic_fs::IdempotencyKey::new(),
            ),
            acyclic_fs::PublicationPermit::Unrestricted,
            &fixture.work,
            fixture.work.cancellation(),
            (),
        )
        .await?;
    assert!(
        replacement
            .generation(fixture.to.id(), &fixture.work, fixture.work.cancellation())
            .await
            .is_err()
    );
    let denied = publication
        .resume(
            &fixture.state,
            &options,
            &fixture.work,
            |_| {
                std::future::ready(Err(Error::Unauthorized(
                    "original workspace no longer granted".into(),
                )))
            },
            || std::future::ready(Ok(())),
        )
        .await;
    assert!(matches!(denied, Err(Error::Unauthorized(_))));
    assert!(
        !fixture.root.join("note").exists(),
        "failed authorization performed native work"
    );
    let resolve = |generation| {
        let workspace = &workspace;
        let work = &fixture.work;
        async move {
            workspace
                .generation(generation, work, work.cancellation())
                .await
                .map_err(Error::from)
        }
    };
    let resumed = publication
        .resume(&fixture.state, &options, &fixture.work, &resolve, || {
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("original publication disappeared")?;
    assert_eq!(resumed.plan(), original.plan());
    assert_eq!(resumed.phase, acyclic_fs::MaterializationPhase::Applied);
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note"))?,
        "candidate"
    );
    assert_eq!(
        workspace
            .head(&fixture.work, fixture.work.cancellation())
            .await?
            .id(),
        advanced.id()
    );
    std::fs::write(fixture.root.join("note"), "edit after native completion")?;
    publication
        .resume(&fixture.state, &options, &fixture.work, &resolve, || {
            std::future::ready(Ok(()))
        })
        .await?
        .ok_or("completed publication disappeared")?;
    assert_eq!(
        std::fs::read_to_string(fixture.root.join("note"))?,
        "edit after native completion"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn publication_needs_no_write_authority_in_the_checkout_parent() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    for adapter in [Adapter::Git, Adapter::MultiRoot, Adapter::Prepared] {
        let fixture = Fixture::with_paths(&USER_PATHS).await?;
        let parent = fixture.root.parent().unwrap();
        std::fs::create_dir_all(parent.join("core"))?;
        let original = std::fs::metadata(parent)?.permissions();
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o500))?;
        let forbidden = std::fs::create_dir(parent.join("forbidden-sibling"));
        if forbidden.is_ok() {
            use std::os::unix::process::CommandExt as _;
            std::fs::set_permissions(parent, original)?;
            if std::env::var_os("ACYCLIC_PUBLICATION_PERMISSION_CHILD").is_some() {
                return Err("unprivileged publication control bypassed chmod".into());
            }
            // An inherited executable descriptor also works when the build
            // directory is private to root. No binary copy or privileged skip.
            let executable = std::fs::File::open(std::env::current_exe()?)?;
            let status = std::process::Command::new("/proc/self/fd/0")
                .stdin(executable)
                .args(["--exact", "filesystem::checkout_publication::transition_tests::publication_needs_no_write_authority_in_the_checkout_parent", "--nocapture"])
                .env("ACYCLIC_PUBLICATION_PERMISSION_CHILD", "1")
                .env("TMPDIR", "/tmp")
                .current_dir("/tmp")
                .uid(65534)
                .gid(65534)
                .status()?;
            assert!(status.success(), "unprivileged publication control failed");
            return Ok(());
        }
        let published = fixture
            .publish(adapter, OperationId::new(), &fixture.from)
            .await;
        // Restore before checking either result so a failure retains cleanup authority.
        std::fs::set_permissions(parent, original)?;
        assert_eq!(
            forbidden.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        published?;
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("note"))?,
            "candidate"
        );
        for path in USER_PATHS {
            assert_eq!(
                std::fs::read_to_string(fixture.root.join(path))?,
                "candidate"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn staged_recovery_stays_out_of_capture_and_lazy_discovery_after_reopen() -> TestResult {
    use acyclic_fs::demand::{DemandSource as _, native::NativeDemandSource};
    use acyclic_fs::kernel::NamespacePath;
    let fixture = Fixture::new().await?;
    let operation = OperationId::new();
    let publication = fixture.publication(operation)?;
    let options = MaterializeOptions::native(publication.directory().join("target"));
    let prepared = publication
        .prepare(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
        )
        .await?;
    assert!(publication.directory().starts_with(fixture.state.root()));
    assert!(!publication.directory().starts_with(&fixture.root));

    // Reopen the real journal while the original operation is still Prepared.
    // No separate binding record or process-local recovery registry is needed.
    let state = LocalCoreStateStore::new(fixture.state.root().to_path_buf());
    let reopened = CheckoutPublication::new(
        &fixture.root,
        fixture.identity,
        operation,
        &state,
        &fixture.work,
    )?;
    let recovered = reopened
        .prepare(&fixture.from, &fixture.to, &state, &options, &fixture.work)
        .await?;
    assert_eq!(recovered.phase, acyclic_fs::MaterializationPhase::Prepared);
    assert_eq!(
        recovered.preparation_identity,
        prepared.preparation_identity
    );
    assert_eq!(reopened.directory(), publication.directory());

    std::fs::write(fixture.root.join("authored.txt"), "user-owned")?;
    let former = format!(
        ".acyclic-r-{}",
        uuid::Uuid::from_bytes(fixture.identity.to_bytes()).simple()
    );
    std::fs::create_dir(fixture.root.join(&former))?;
    std::fs::write(
        fixture.root.join(&former).join("authored"),
        "also user-owned",
    )?;

    let config = fixture.from.workspace().volume_config();
    let stop = fixture.work.cancellation();
    let path = |text: &str| -> TestResult<_> {
        Ok(NamespacePath::from_text_in_profile(
            text,
            config.profile,
            config.limits,
        )?)
    };
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(&fixture.root),
        config.profile,
        config.limits,
        None,
        None,
        &fixture.work,
        stop,
    )
    .await?;
    assert!(
        source
            .lookup(
                source.reference(),
                &path(&format!("/{former}"))?,
                &fixture.work,
                stop,
            )
            .await?
            .is_some()
    );
    let page = source
        .list_page(
            source.reference(),
            &NamespacePath::new(Vec::new(), config.limits)?,
            None,
            16,
            &fixture.work,
            stop,
        )
        .await?;
    assert_eq!(
        page.entries.len(),
        2,
        "recovery leaked into authored source"
    );

    let mut checkout = fixture
        .from
        .workspace()
        .checkout(
            acyclic_fs::model::GenerationSelector::Exact(fixture.from.id()),
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            stop,
        )
        .await?
        .value;
    let capture = acyclic_fs::CaptureOptions {
        materialized_view: None,
        source_root: fixture.root.clone(),
        expected_root_identity: fixture.identity,
        source_namespace: None,
        maximum_paths: None,
        maximum_extent_spans: None,
    };
    acyclic_fs::capture_baseline(
        &mut checkout,
        &capture,
        &acyclic_fs::CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        stop,
    )
    .await?;
    for text in ["/authored.txt".to_owned(), format!("/{former}/authored")] {
        assert!(
            checkout
                .lookup_no_follow(&path(&text)?, WorkBudget::UNBOUNDED, stop)
                .await?
                .value
                .record
                .is_some()
        );
    }
    assert_eq!(
        std::fs::read_to_string(options.destination.join("note"))?,
        "candidate"
    );
    Ok(())
}

#[tokio::test]
async fn recovery_name_collision_preserves_authored_data_before_publication() -> TestResult {
    for adapter in [Adapter::Git, Adapter::MultiRoot, Adapter::Prepared] {
        for populated in [false, true] {
            let fixture = Fixture::new().await?;
            let operation = OperationId::new();
            let publication = fixture.publication(operation)?;
            std::fs::create_dir_all(publication.directory())?;
            let sentinel = publication.directory().join("authored");
            if populated {
                std::fs::write(&sentinel, "user-owned")?;
            }
            for _ in 0..2 {
                assert!(
                    fixture
                        .publish(adapter, operation, &fixture.from)
                        .await
                        .is_err()
                );
                assert!(!fixture.root.join("note").exists());
                assert!(publication.directory().is_dir());
                if populated {
                    assert_eq!(std::fs::read_to_string(&sentinel)?, "user-owned");
                } else {
                    assert_eq!(std::fs::read_dir(publication.directory())?.count(), 0);
                }
            }
        }
    }
    Ok(())
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
    ) -> crate::Result<acyclic_fs::NativeFutureBundle<'a, (), crate::Error>> {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
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
async fn obsolete_staging_binding_is_refused_before_publication() -> TestResult {
    use acyclic_fs::MaterializationJournalStore as _;
    for adapter in [Adapter::Git, Adapter::MultiRoot, Adapter::Prepared] {
        let fixture = Fixture::with_paths(&USER_PATHS).await?;
        let operation = OperationId::new();
        let publication = fixture.publication(operation)?;
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
        journal.version = 5;
        let expected = journal.revision;
        journal.revision = expected + 1;
        let (stored, _) = fixture
            .state
            .materialization(fixture.work.clone())
            .compare_and_swap(operation, expected, journal)
            .await?;
        assert!(stored);
        assert!(
            fixture
                .publish(adapter, operation, &fixture.from)
                .await
                .is_err()
        );
        assert!(!fixture.root.join("note").exists());
        assert_eq!(
            std::fs::read_to_string(options.destination.join("note"))?,
            "candidate"
        );
        for path in USER_PATHS {
            assert_eq!(
                std::fs::read_to_string(fixture.root.join(path))?,
                "original"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn selected_approval_never_rebuilds_a_missing_preparation() -> TestResult {
    for (promoted, unchanged) in [(true, true), (true, false), (false, false)] {
        let fixture = Fixture::new().await?;
        let from = if unchanged {
            &fixture.to
        } else {
            &fixture.from
        };
        if promoted {
            std::fs::write(fixture.root.join("note"), "candidate")?;
        }
        let publication = fixture.publication(OperationId::new())?;
        let options = MaterializeOptions::native(publication.directory().join("target"));
        let paths = vec!["note".into()];
        let journal = publication
            .prepare_selected(
                from,
                &fixture.to,
                &fixture.state,
                &options,
                &fixture.work,
                (&paths, "creator"),
            )
            .await?;
        assert_eq!(journal.plan().edits.is_empty(), promoted);
        if promoted && !unchanged {
            // A preparation response may be lost after its empty result is durable.
            let retry = publication
                .prepare(from, &fixture.to, &fixture.state, &options, &fixture.work)
                .await?;
            assert_eq!(retry, journal);
        }
        let approval =
            publication.approval_value(from, &fixture.to, &paths, &journal, "creator")?;
        let digest = approval.digest(&fixture.work)?;
        let interaction = crate::InteractionId::new();
        let verifier = PublicationDecision {
            id: interaction,
            operation: crate::OperationId::from_bytes(publication.operation.into_bytes()),
            digest,
            approved: true,
        };
        let authorized = approval
            .authorize(&verifier, interaction, &digest, &fixture.work)
            .await?;
        std::fs::write(fixture.root.join("note"), "edit after approval")?;
        let missing = LocalCoreStateStore::new(fixture._directory.path().join("missing"));
        let state = if promoted { &fixture.state } else { &missing };
        let result = authorized
            .publish(state, &options, || std::future::ready(Ok(())))
            .await;
        if promoted {
            assert!(result?.plan().edits.is_empty());
            assert_eq!(
                state.materialization_operations_async().await?.is_empty(),
                unchanged
            );
        } else {
            assert!(matches!(result, Err(Error::Unauthorized(_))));
        }
        assert_eq!(
            std::fs::read_to_string(fixture.root.join("note"))?,
            "edit after approval"
        );
        // Rebuilding instead of carrying approval really would overwrite the
        // later edit: exercise the former unbound publication argument shape.
        if promoted && !unchanged {
            // Completed empty preparation remains available after publication
            // has retired the staging allocations.
            let retry = publication
                .prepare(from, &fixture.to, state, &options, &fixture.work)
                .await?;
            assert_eq!(retry.phase, acyclic_fs::MaterializationPhase::Applied);
            assert!(retry.plan().edits.is_empty());
            // The original namespace retains completion even without an
            // approval handle, protecting later edits on a cold retry.
            publication
                .publish(
                    from,
                    &fixture.to,
                    state,
                    &options,
                    (&fixture.work, None),
                    || std::future::ready(Ok(())),
                )
                .await?;
            assert_eq!(
                std::fs::read_to_string(fixture.root.join("note"))?,
                "edit after approval"
            );
            state
                .remove_materialization_async(publication.operation)
                .await?;
            publication
                .publish(
                    from,
                    &fixture.to,
                    state,
                    &options,
                    (&fixture.work, None),
                    || std::future::ready(Ok(())),
                )
                .await?;
            assert_eq!(
                std::fs::read_to_string(fixture.root.join("note"))?,
                "candidate"
            );
        }
    }
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
            (&fresh, None),
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
                        (&fixture.work, None),
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
