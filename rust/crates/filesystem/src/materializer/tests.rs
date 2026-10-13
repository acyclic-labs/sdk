use super::*;
use crate::Digest;
use proptest::collection::btree_map;
use proptest::option;
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

#[derive(Debug, Error)]
#[error("test backend unavailable")]
struct BackendError;

fn allow<S: std::error::Error + 'static, B: std::error::Error + 'static>()
-> std::future::Ready<Result<(), MaterializationError<S, B>>> {
    std::future::ready(Ok(()))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_backend(
    root: &Path,
    operation: &Path,
) -> Result<std::sync::Arc<NativeTreeMaterializationBackend>, NativeTreeMaterializationError> {
    std::fs::create_dir_all(operation.join("backup"))?;
    let work = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let (root, mut allocation) = work.path(root, None)?;
    let (operation, mut operation_allocation) = work.path(operation, None)?;
    allocation.absorb(&mut operation_allocation)?;
    NativeTreeMaterializationBackend::new(root, operation, allocation, work)
}

/// Flat path model whose writes, like every journal CAS, are durable
/// steps; the step numbered `crash_at` takes effect and then "crashes".
#[derive(Default)]
struct Crashing {
    tree: Mutex<BTreeMap<String, u8>>,
    steps: AtomicU64,
    crash_at: AtomicU64,
}

impl Crashing {
    fn step(&self) -> Result<(), BackendError> {
        let step = self.steps.fetch_add(1, SeqCst) + 1;
        if step == self.crash_at.load(SeqCst) {
            return Err(BackendError);
        }
        Ok(())
    }

    fn tree(&self) -> BTreeMap<String, u8> {
        self.tree.lock().expect("tree lock").clone()
    }

    fn current(&self, path: &str) -> Result<Option<u8>, BackendError> {
        Ok(self
            .tree
            .lock()
            .map_err(|_| BackendError)?
            .get(path)
            .copied())
    }

    fn set(&self, path: &str, value: Option<u8>) -> Result<(), BackendError> {
        let mut tree = self.tree.lock().map_err(|_| BackendError)?;
        match value {
            Some(value) => tree.insert(path.to_owned(), value),
            None => tree.remove(path),
        };
        drop(tree);
        self.step()
    }
}

fn endpoints(edit: &MaterializationEdit) -> (&str, Option<u8>) {
    match edit {
        MaterializationEdit::Install { path, image } => (path, image.first().copied()),
        MaterializationEdit::Remove { path } => (path, None),
        _ => ("", None),
    }
}

impl MaterializationBackend for &Crashing {
    type Error = BackendError;

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let value = self.current(endpoints(edit).0)?;
        Ok(MaterializationPreimage {
            image: value.into_iter().collect(),
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        })
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let (path, postimage) = endpoints(edit);
        let current = self.current(path)?;
        Ok(if current == postimage {
            MaterializationObservation::Postimage
        } else if current == preimage.image.first().copied() {
            MaterializationObservation::Preimage
        } else {
            MaterializationObservation::Diverged
        })
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let (path, postimage) = endpoints(edit);
        self.set(path, postimage)
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.set(endpoints(edit).0, preimage.image.first().copied())
    }
}

/// Journal store whose compare-and-swaps are durable steps of `.1`.
struct CrashingStore<'a>(&'a MemoryMaterializationJournalStore, &'a Crashing);

impl MaterializationJournalStore for CrashingStore<'_> {
    type Error = BackendError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = std::future::Ready<Result<MaterializationJournal, BackendError>>
    where
        Self: 'a;
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        _journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'_>, Self::Error> {
        Err(BackendError)
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.0.load(operation_id).await.map_err(|_| BackendError)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let swapped = self
            .0
            .compare_and_swap(operation_id, expected_revision, replacement)
            .await
            .map_err(|_| BackendError)?;
        self.1.step()?;
        Ok(swapped)
    }
}

/// Applies `plan` to `original`, crashing after durable step `crash_at`
/// (zero never crashes).
fn crashing_apply(
    plan: &MaterializationPlan,
    original: &BTreeMap<String, u8>,
    crash_at: u64,
) -> (Crashing, MemoryMaterializationJournalStore, bool) {
    let backend = Crashing::default();
    original.clone_into(&mut backend.tree.lock().expect("tree lock"));
    backend.crash_at.store(crash_at, SeqCst);
    let journals = MemoryMaterializationJournalStore::default();
    let crashed = futures::executor::block_on(
        JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend)
            .apply(plan.clone(), &mut allow),
    )
    .is_err();
    (backend, journals, crashed)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// A crash after any durable step, followed by either recovery,
    /// leaves exactly the planned tree or exactly the original one.
    #[test]
    fn recovery_after_a_crash_at_any_durable_step_is_all_or_nothing(
        original in btree_map("[a-e]", any::<u8>(), 0..5),
        targets in btree_map("[a-e]", option::of(any::<u8>()), 1..5),
        crash in any::<u64>(),
        forward in any::<bool>(),
    ) {
        let plan = MaterializationPlan {
            operation_id: OperationId::from_bytes([7; 16]),
            intent: MaterializationIntent::GenerationTransition {

                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),

            },
            edits: targets
                .iter()
                .map(|(path, value)| match value {
                    Some(value) => MaterializationEdit::Install {
                        path: path.clone(),
                        image: vec![*value],
                    },
                    None => MaterializationEdit::Remove { path: path.clone() },
                })
                .collect(),
        };
        let mut planned = original.clone();
        for (path, value) in &targets {
            match value {
                Some(value) => planned.insert(path.clone(), *value),
                None => planned.remove(path),
            };
        }
        let (clean, _, crashed) = crashing_apply(&plan, &original, 0);
        prop_assert!(!crashed);
        prop_assert_eq!(clean.tree(), planned.clone());
        let steps = clean.steps.load(SeqCst);

        let (backend, journals, crashed) = crashing_apply(&plan, &original, 1 + crash % steps);
        prop_assert!(crashed);
        backend.crash_at.store(0, SeqCst);
        let restarted = JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend);
        let recovery = if forward {
            MaterializationRecovery::Complete
        } else {
            MaterializationRecovery::RollBack
        };
        let journal = futures::executor::block_on(restarted.recover(plan.operation_id, recovery, &mut allow))
            .expect("recover")
            .expect("journal");
        if forward {
            prop_assert_eq!(journal.phase, MaterializationPhase::Applied);
            prop_assert_eq!(backend.tree(), planned);
        } else {
            prop_assert_eq!(journal.phase, MaterializationPhase::RolledBack);
            prop_assert_eq!(backend.tree(), original);
        }
    }
}

fn plan(edits: Vec<MaterializationEdit>) -> MaterializationPlan {
    MaterializationPlan {
        operation_id: OperationId::new(),
        intent: MaterializationIntent::GenerationTransition {
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
        },
        edits,
    }
}

#[tokio::test]
async fn obsolete_journals_and_forward_rollback_are_rejected() {
    for version in [0, 2, u32::MAX, JOURNAL_VERSION] {
        let store = MemoryMaterializationJournalStore::default();
        let backend = Crashing::default();
        let materializer = JournaledMaterializer::new(CrashingStore(&store, &backend), &backend);
        let mut journal = materializer
            .prepare(
                plan(vec![MaterializationEdit::Install {
                    path: "file".into(),
                    image: vec![9],
                }]),
                #[cfg(not(target_arch = "wasm32"))]
                None,
            )
            .await
            .unwrap();
        journal.preimages.push(MaterializationPreimage {
            image: vec![0],
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        });
        if version == JOURNAL_VERSION {
            journal.phase = MaterializationPhase::RollingBack;
            journal.applied = 1;
            journal.restored = 1;
        } else {
            journal.phase = MaterializationPhase::Prepared;
        }
        journal = materializer.persist(journal).await.unwrap();
        journal.version = version;
        let operation = journal.plan.operation_id;
        let expected = next_revision(&mut journal.revision);
        store
            .compare_and_swap(operation, expected, journal)
            .await
            .unwrap();
        assert!(matches!(
            materializer
                .recover(operation, MaterializationRecovery::Complete, &mut allow)
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
        assert!(backend.tree().is_empty());
    }
}

#[tokio::test]
async fn overlapping_or_noncanonical_plans_are_rejected_before_capture() {
    let backend = Crashing::default();
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), &backend);
    for path in ["directory/file", "../file", "", "/file"] {
        assert!(matches!(
            materializer
                .apply(
                    plan(vec![
                        MaterializationEdit::Remove {
                            path: "directory".into()
                        },
                        MaterializationEdit::Install {
                            path: path.into(),
                            image: vec![1]
                        },
                    ]),
                    &mut allow
                )
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
        assert!(backend.tree().is_empty());
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn fixture() -> (
    tempfile::TempDir,
    PathBuf,
    PathBuf,
    std::sync::Arc<NativeTreeMaterializationBackend>,
) {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(operation.join("target")).unwrap();
    let backend = native_backend(&root, &operation).unwrap();
    (temporary, root, operation, backend)
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_selected_paths_complete_and_roll_back() -> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, root, operation, backend) = fixture();
    std::fs::create_dir(root.join(".git"))?;
    std::fs::write(root.join(".git/config"), b"git")?;
    std::fs::write(root.join("old"), b"old")?;
    std::fs::write(operation.join("target/new"), b"new")?;
    let plan = plan(vec![
        MaterializationEdit::Remove { path: "old".into() },
        MaterializationEdit::Install {
            path: "new".into(),
            image: vec![1],
        },
    ]);
    let operation_id = plan.operation_id;
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    materializer.apply(plan, &mut allow).await?;
    assert!(!root.join("old").exists());
    assert_eq!(std::fs::read(root.join("new"))?, b"new");
    materializer
        .recover(operation_id, MaterializationRecovery::RollBack, &mut allow)
        .await?;
    assert_eq!(std::fs::read(root.join("old"))?, b"old");
    assert!(!root.join("new").exists());
    assert_eq!(std::fs::read(root.join(".git/config"))?, b"git");
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_mutated_endpoints_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
    for endpoint in ["target/file", "file", "removed", "rollback"] {
        let (_temporary, root, operation, backend) = fixture();
        let target = operation.join("target/file");
        std::fs::write(root.join("file"), b"before")?;
        let edit = MaterializationEdit::Install {
            path: "file".into(),
            image: vec![1],
        };
        assert!(matches!(
            backend.capture(&edit).await,
            Err(NativeTreeMaterializationError::MissingEntry)
        ));
        std::fs::write(&target, b"after")?;
        let materializer = JournaledMaterializer::new(
            MemoryMaterializationJournalStore::default(),
            backend.clone(),
        );
        let journal = materializer.prepare(plan(vec![edit.clone()]), None).await?;
        let journal = materializer.capture_preimages(journal, &mut allow).await?;
        let preimage = &journal.preimages[0];
        assert_eq!(
            backend.observe(&edit, preimage).await?,
            MaterializationObservation::Preimage
        );
        if endpoint == "rollback" {
            backend.apply(&edit, preimage).await?;
        }
        if endpoint == "removed" {
            std::fs::remove_file(&target)?;
        } else {
            let changed = if endpoint == "target/file" {
                target.clone()
            } else {
                root.join("file")
            };
            std::fs::write(changed, b"external")?;
        }
        assert_eq!(
            backend.observe(&edit, preimage).await?,
            MaterializationObservation::Diverged
        );
        let result = if endpoint == "rollback" {
            backend.restore(&edit, preimage).await
        } else {
            backend.apply(&edit, preimage).await
        };
        assert!(matches!(
            result,
            Err(NativeTreeMaterializationError::ExternalMutation)
        ));
        assert!(matches!(
            materializer
                .recover(
                    journal.plan.operation_id,
                    MaterializationRecovery::Complete,
                    &mut allow,
                )
                .await,
            Err(MaterializationError::ExternalMutation)
        ));
        assert_eq!(
            std::fs::read(root.join("file"))?,
            if matches!(endpoint, "file" | "rollback") {
                b"external".as_slice()
            } else {
                b"before"
            }
        );
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_recovery_handles_both_rename_boundaries() -> Result<(), Box<dyn std::error::Error>>
{
    for applied in [false, true] {
        for recovery in [
            MaterializationRecovery::Complete,
            MaterializationRecovery::RollBack,
        ] {
            let (_temporary, root, operation, backend) = fixture();
            std::fs::write(root.join("file"), b"before")?;
            std::fs::write(operation.join("target/file"), b"after")?;
            let plan = plan(vec![MaterializationEdit::Install {
                path: "file".into(),
                image: vec![1],
            }]);
            let operation_id = plan.operation_id;
            let materializer = JournaledMaterializer::new(
                MemoryMaterializationJournalStore::default(),
                backend.clone(),
            );
            let journal = materializer.prepare(plan, None).await?;
            let mut journal = materializer.capture_preimages(journal, &mut allow).await?;
            journal.phase = MaterializationPhase::Applying;
            let journal = materializer.persist(journal).await?;
            if applied {
                backend
                    .apply(&journal.plan.edits[0], &journal.preimages[0])
                    .await?;
            } else {
                std::fs::rename(root.join("file"), operation.join("backup/file"))?;
            }
            materializer
                .recover(operation_id, recovery, &mut allow)
                .await?;
            assert_eq!(
                std::fs::read(root.join("file"))?,
                if recovery == MaterializationRecovery::Complete {
                    b"after".as_slice()
                } else {
                    b"before"
                }
            );
            assert!(
                !operation.join("backup/file").exists()
                    || recovery == MaterializationRecovery::Complete
            );
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_metadata_keeps_children_and_rejects_replaced_directories()
-> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, root, _operation, backend) = fixture();
    let directory = root.join("directory");
    std::fs::create_dir(&directory)?;
    let before = native_metadata(
        &{ #[cfg(unix)] { backend
            .held_root
            .stat(Path::new("directory"), &mut backend.work()) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new("directory"), &mut backend.work(), |path, work| backend.held_root.stat(path, work)) } }?,
    );
    let mut desired = before.clone();
    #[cfg(unix)]
    {
        desired.posix_mode = desired.posix_mode.map(|mode| mode ^ 0o200);
    }
    #[cfg(windows)]
    {
        desired.windows_attributes = desired.windows_attributes.map(|attributes| attributes ^ 2);
    }
    let edit = MaterializationEdit::SetMetadata {
        path: "directory".into(),
        image: serde_json::to_vec(&desired)?,
    };
    let materializer = JournaledMaterializer::new(
        MemoryMaterializationJournalStore::default(),
        backend.clone(),
    );
    let journal = materializer.prepare(plan(vec![edit.clone()]), None).await?;
    let journal = materializer.capture_preimages(journal, &mut allow).await?;
    let preimage = &journal.preimages[0];
    std::fs::write(directory.join("child"), b"concurrent")?;
    assert_eq!(
        backend.observe(&edit, preimage).await?,
        MaterializationObservation::Preimage
    );
    materializer
        .recover(
            journal.plan.operation_id,
            MaterializationRecovery::Complete,
            &mut allow,
        )
        .await?;
    let applied = native_metadata(
        &{ #[cfg(unix)] { backend
            .held_root
            .stat(Path::new("directory"), &mut backend.work()) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new("directory"), &mut backend.work(), |path, work| backend.held_root.stat(path, work)) } }?,
    );
    assert_eq!(applied.posix_mode, desired.posix_mode);
    assert_eq!(applied.windows_attributes, desired.windows_attributes);
    materializer
        .recover(
            journal.plan.operation_id,
            MaterializationRecovery::RollBack,
            &mut allow,
        )
        .await?;
    let restored = native_metadata(
        &{ #[cfg(unix)] { backend
            .held_root
            .stat(Path::new("directory"), &mut backend.work()) } #[cfg(windows)] { crate::native_host::with_wide_path(Path::new("directory"), &mut backend.work(), |path, work| backend.held_root.stat(path, work)) } }?,
    );
    assert_eq!(restored.posix_mode, before.posix_mode);
    assert_eq!(restored.windows_attributes, before.windows_attributes);
    assert_eq!(std::fs::read(directory.join("child"))?, b"concurrent");
    std::fs::rename(&directory, root.join("displaced"))?;
    std::fs::create_dir(&directory)?;
    assert_eq!(
        backend.observe(&edit, preimage).await?,
        MaterializationObservation::Diverged
    );
    assert!(matches!(
        backend.apply(&edit, preimage).await,
        Err(NativeTreeMaterializationError::ExternalMutation)
    ));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_dangling_links_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, root, _operation, backend) = fixture();
    std::os::unix::fs::symlink("missing", root.join("link"))?;
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    let plan = plan(vec![MaterializationEdit::Remove {
        path: "link".into(),
    }]);
    let operation = plan.operation_id;
    materializer.apply(plan, &mut allow).await?;
    assert!(std::fs::symlink_metadata(root.join("link")).is_err());
    materializer
        .recover(operation, MaterializationRecovery::RollBack, &mut allow)
        .await?;
    assert_eq!(
        std::fs::read_link(root.join("link"))?,
        PathBuf::from("missing")
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn nested_operation_is_rejected_without_effects() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("checkout");
    let operation = root.join("operation");
    std::fs::create_dir_all(operation.join("target")).unwrap();
    let sentinel = operation.join("target/sentinel");
    std::fs::write(&sentinel, b"keep").unwrap();
    assert!(matches!(
        native_backend(&root, &operation),
        Err(NativeTreeMaterializationError::InvalidLayout)
    ));
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"keep");
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn accepted_action_conflict_retains_the_journal() -> Result<(), Box<dyn std::error::Error>> {
    let (_temporary, _root, _operation, backend) = fixture();
    let materializer = JournaledMaterializer::new(
        MemoryMaterializationJournalStore::default(),
        backend.clone(),
    );
    let journal = materializer
        .prepare(
            plan(vec![MaterializationEdit::Remove {
                path: "file".into(),
            }]),
            None,
        )
        .await?;
    let operation = journal.plan.operation_id;
    let store = materializer.store();
    let completion = store.prepare_action(journal).await?;
    let mut competitor = store.load(operation).await?.unwrap();
    let expected = next_revision(&mut competitor.revision);
    assert!(
        store
            .compare_and_swap(operation, expected, competitor)
            .await?
            .0
    );
    let Err(MemoryMaterializationJournalStoreError::ActionConflict(journal)) = completion.await
    else {
        panic!("accepted action must retain its conflicting journal");
    };
    assert!(journal.is_valid_for(operation));
    let custody = journal.custody.as_ref().unwrap();
    assert!(custody.bytes() > 0);
    assert!(custody.work().shares_allocation_context(backend.work()));
    let stored = store.load(operation).await?.unwrap();
    assert_eq!(stored.revision, journal.revision);
    assert_eq!(stored.plan, journal.plan);
    Ok(())
}
