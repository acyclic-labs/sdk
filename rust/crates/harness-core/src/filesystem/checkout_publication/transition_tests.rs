use super::*;
use acyclic_fs::{OperationId, WorkCounters};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Only an Applied record for this exact root and generation transition
    // suppresses native replay. Every other phase must continue recovery.
    #[test]
    fn completion_requires_exact_original_root_and_transition(
        root in any::<[u8; 16]>(), from in any::<[u8; 32]>(), to in any::<[u8; 32]>(),
        phase in 0usize..6, change in 0usize..4,
    ) {
        use acyclic_fs::{Digest, GenerationId, MaterializationIntent, MaterializationJournal, MaterializationPhase, MaterializationPlan, NativeRootIdentity};
        let from = GenerationId::new(Digest::from_bytes(from));
        let to = GenerationId::new(Digest::from_bytes(to));
        let phases = [MaterializationPhase::Preparing, MaterializationPhase::Prepared, MaterializationPhase::Applying, MaterializationPhase::Applied, MaterializationPhase::RollingBack, MaterializationPhase::RolledBack];
        let journal = MaterializationJournal {
            version: 1, revision: 1,
            plan: MaterializationPlan { operation_id: OperationId::from_bytes([1; 16]), intent: MaterializationIntent::GenerationTransition { from, to }, edits: Vec::new() },
            root_identity: Some(root), preparation_identity: None, preparation_progress: None,
            preimages: Vec::new(), phase: phases[phase], applied: 0, restored: 0, work: WorkCounters::UNCHARGED, custody: None,
        };
        let mut actual_root = root;
        let mutate = |id: GenerationId| { let mut bytes = id.digest().into_bytes(); bytes[0] ^= 1; GenerationId::new(Digest::from_bytes(bytes)) };
        if change == 1 { actual_root[0] ^= 1; }
        let result = completed(&journal, NativeRootIdentity::from_bytes(actual_root), if change == 2 { mutate(from) } else { from }, if change == 3 { mutate(to) } else { to });
        prop_assert_eq!(result.is_ok(), change == 0);
        if let Ok(applied) = result { prop_assert_eq!(applied, phase == 3); }
        // Negative control: checking the phase alone acknowledges a foreign root
        // or transition. The production binding check must reject that shape.
        if phase == 3 && change != 0 { prop_assert!(journal.phase == MaterializationPhase::Applied); prop_assert!(result.is_err()); }
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
        acyclic_native_runtime::initialize_native_io_services()?;
        let directory = tempfile::tempdir()?;
        let filesystem = LocalFs::local(LocalOptions::new(directory.path().join("fs"))).await?;
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
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
    let applied = publication
        .publish(
            &fixture.from,
            &fixture.to,
            &fixture.state,
            &options,
            &fixture.work,
            || std::future::ready(Ok(())),
        )
        .await?;
    assert_eq!(original, encode(&applied, &paths, "original creator")?);
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
