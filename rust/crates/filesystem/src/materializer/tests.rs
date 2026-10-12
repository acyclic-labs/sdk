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

#[cfg(not(target_arch = "wasm32"))]
async fn seed_native_journal(
    store: &MemoryMaterializationJournalStore,
    backend: &std::sync::Arc<NativeTreeMaterializationBackend>,
    plan: &MaterializationPlan,
    preimage: &MaterializationPreimage,
    phase: MaterializationPhase,
) {
    let crash = Crashing::default();
    let materializer = JournaledMaterializer::new(CrashingStore(store, &crash), backend.clone());
    let mut journal = materializer
        .prepare(plan.clone(), None)
        .await
        .expect("intent");
    let allocation = journal.custody.as_mut().expect("journal allocation");
    let image = copy_image(&preimage.image, allocation).expect("preimage copy");
    native_vec_push(
        &mut journal.preimages,
        MaterializationPreimage {
            image,
            custody: None,
        },
        allocation,
    )
    .expect("preimage vector");
    journal.phase = phase;
    materializer.persist(journal).await.expect("journal");
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

#[cfg(not(target_arch = "wasm32"))]
struct CrashingCompletion<'a>(MemoryMaterializationActionCompletion<'a>, &'a Crashing);
#[cfg(not(target_arch = "wasm32"))]
impl Future for CrashingCompletion<'_> {
    type Output = Result<MaterializationJournal, BackendError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match std::pin::Pin::new(&mut this.0).poll(context) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(result) => {
                std::task::Poll::Ready(result.map_err(|_| BackendError).and_then(|journal| {
                    this.1.step()?;
                    Ok(journal)
                }))
            }
        }
    }
}

impl MaterializationJournalStore for CrashingStore<'_> {
    type Error = BackendError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = CrashingCompletion<'a>
    where
        Self: 'a;
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'_>, Self::Error> {
        Ok(CrashingCompletion(
            self.0
                .prepare_action(journal)
                .await
                .map_err(|_| BackendError)?,
            self.1,
        ))
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

#[tokio::test]
async fn obsolete_journal_versions_reject_recovery_without_effects() {
    for version in [0, 2, u32::MAX] {
        let operation_id = OperationId::new();
        let journal = MaterializationJournal {
            version,
            revision: 1,
            plan: MaterializationPlan {
                operation_id,
                intent: MaterializationIntent::GenerationTransition {
                    from: GenerationId::new(Digest::from_bytes([1; 32])),
                    to: GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: vec![MaterializationEdit::Install {
                    path: "file".into(),
                    image: vec![9],
                }],
            },
            root_identity: None,
            preparation_identity: None,
            preparation_progress: None,
            work: crate::WorkCounters::UNCHARGED,
            preimages: vec![MaterializationPreimage {
                image: vec![0],
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            }],
            phase: MaterializationPhase::Prepared,
            applied: 0,
            restored: 0,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        };
        let store = MemoryMaterializationJournalStore::default();
        store
            .compare_and_swap(
                operation_id,
                0,
                duplicate_journal(&journal).expect("copy journal"),
            )
            .await
            .expect("seed obsolete journal");
        let backend = Crashing::default();
        let materializer = JournaledMaterializer::new(store, &backend);
        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::Complete, &mut allow)
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
        assert!(materializer.backend.tree().is_empty());
        assert_eq!(
            materializer
                .store
                .load(operation_id)
                .await
                .expect("retained journal"),
            Some(journal)
        );
    }
}
#[tokio::test]
async fn interrupted_rollback_cannot_be_completed_forward() {
    let operation_id = OperationId::new();
    let store = MemoryMaterializationJournalStore::default();
    let plan = MaterializationPlan {
        operation_id,
        intent: MaterializationIntent::GenerationTransition {
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
        },
        edits: vec![MaterializationEdit::Install {
            path: "file".to_owned(),
            image: vec![9],
        }],
    };
    store
        .compare_and_swap(
            operation_id,
            0,
            MaterializationJournal {
                version: JOURNAL_VERSION,
                revision: 1,
                plan,
                root_identity: None,
                preparation_identity: None,
                preparation_progress: None,
                work: crate::WorkCounters::UNCHARGED,
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
                preimages: vec![MaterializationPreimage {
                    image: vec![0],
                    #[cfg(not(target_arch = "wasm32"))]
                    custody: None,
                }],
                phase: MaterializationPhase::RollingBack,
                applied: 1,
                restored: 1,
            },
        )
        .await
        .expect("store journal");
    let backend = Crashing::default();
    let materializer = JournaledMaterializer::new(store, &backend);
    assert!(matches!(
        materializer
            .recover(operation_id, MaterializationRecovery::Complete, &mut allow)
            .await,
        Err(MaterializationError::IncompatibleJournal)
    ));
}

#[tokio::test]
async fn overlapping_or_noncanonical_plans_are_rejected_before_capture() {
    let backend = Crashing::default();
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), &backend);
    let plan = MaterializationPlan {
        operation_id: OperationId::new(),
        intent: MaterializationIntent::GenerationTransition {
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
        },
        edits: vec![
            MaterializationEdit::Remove {
                path: "directory".to_owned(),
            },
            MaterializationEdit::Install {
                path: "directory/file".to_owned(),
                image: vec![1],
            },
        ],
    };
    assert!(matches!(
        materializer.apply(plan, &mut allow).await,
        Err(MaterializationError::IncompatibleJournal)
    ));
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_applies_and_rolls_back_selected_paths() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(root.join(".git")).expect("git directory");
    std::fs::create_dir_all(&target).expect("target directory");
    std::fs::write(root.join("old.txt"), b"old").expect("old file");
    std::fs::write(root.join(".git/config"), b"git").expect("git file");
    std::fs::write(target.join("new.txt"), b"new").expect("new file");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["old.txt".to_owned(), "new.txt".to_owned()],
        )
        .expect("plan");
    let operation_id = plan.operation_id;
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    materializer
        .apply(plan, &mut allow)
        .await
        .expect("apply tree");
    assert!(!root.join("old.txt").exists());
    assert_eq!(std::fs::read(root.join("new.txt")).expect("new"), b"new");
    assert_eq!(
        std::fs::read(root.join(".git/config")).expect("git"),
        b"git"
    );
    materializer
        .recover(operation_id, MaterializationRecovery::RollBack, &mut allow)
        .await
        .expect("rollback");
    assert_eq!(std::fs::read(root.join("old.txt")).expect("old"), b"old");
    assert!(!root.join("new.txt").exists());
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_pauses_before_overwriting_an_external_mutation() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&target).expect("target");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    std::fs::write(target.join("file.txt"), b"after").expect("after");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["file.txt".to_owned()],
        )
        .expect("plan");
    let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
    let store = MemoryMaterializationJournalStore::default();
    seed_native_journal(
        &store,
        &backend,
        &plan,
        &preimage,
        MaterializationPhase::Prepared,
    )
    .await;
    std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

    assert!(matches!(
        JournaledMaterializer::new(store, backend)
            .apply(plan, &mut allow)
            .await,
        Err(MaterializationError::ExternalMutation)
    ));
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("external remains"),
        b"external"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_rejects_tampered_or_missing_staged_targets() {
    for replacement in [Some(b"tampered".as_slice()), None] {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend = native_backend(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = plan.edits.first().expect("install edit");
        let preimage = backend.capture(edit).await.expect("capture");
        match replacement {
            Some(bytes) => std::fs::write(target.join("file.txt"), bytes).expect("tamper"),
            None => std::fs::remove_file(target.join("file.txt")).expect("delete target"),
        }
        assert_eq!(
            backend.observe(edit, &preimage).await.expect("observe"),
            MaterializationObservation::Diverged
        );
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("live remains"),
            b"before"
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_rejects_missing_staged_target_during_capture() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(operation.join("target")).expect("target");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    let backend = native_backend(&root, &operation).expect("native backend");
    let edit = MaterializationEdit::Install {
        path: "file.txt".to_owned(),
        image: Vec::new(),
    };

    assert!(matches!(
        backend.capture(&edit).await,
        Err(NativeTreeMaterializationError::MissingEntry)
    ));
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("live remains"),
        b"before"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_rolls_back_interrupted_install_from_durable_backup() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    let backup = operation.join("backup");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&target).expect("target");
    std::fs::create_dir_all(&backup).expect("backup");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    std::fs::write(target.join("file.txt"), b"after").expect("after");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["file.txt".to_owned()],
        )
        .expect("plan");
    let edit = &plan.edits[0];
    let preimage = backend.capture(edit).await.expect("capture");

    std::fs::rename(root.join("file.txt"), backup.join("file.txt"))
        .expect("simulate crash after backup rename");
    assert_eq!(
        backend.observe(edit, &preimage).await.expect("observe"),
        MaterializationObservation::Interrupted
    );
    backend
        .restore(edit, &preimage)
        .await
        .expect("restore interrupted install");
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("restored live"),
        b"before"
    );
    assert!(!backup.join("file.txt").exists());
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_revalidates_witnesses_inside_apply() {
    for mutate_target in [false, true] {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend = native_backend(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = plan.edits.first().expect("install edit");
        let preimage = backend.capture(edit).await.expect("capture");
        assert_eq!(
            backend.observe(edit, &preimage).await.expect("observe"),
            MaterializationObservation::Preimage
        );
        let changed = if mutate_target {
            target.join("file.txt")
        } else {
            root.join("file.txt")
        };
        std::fs::write(&changed, b"external").expect("mutate after observe");
        assert!(matches!(
            backend.apply(edit, &preimage).await,
            Err(NativeTreeMaterializationError::ExternalMutation)
        ));
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_refuses_to_roll_back_over_an_external_write() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&target).expect("target");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    std::fs::write(target.join("file.txt"), b"after").expect("after");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["file.txt".to_owned()],
        )
        .expect("plan");
    let edit = plan.edits.first().expect("install edit");
    let preimage = backend.capture(edit).await.expect("capture");
    backend.apply(edit, &preimage).await.expect("apply");
    std::fs::write(root.join("file.txt"), b"external").expect("external write");

    assert!(matches!(
        backend.restore(edit, &preimage).await,
        Err(NativeTreeMaterializationError::ExternalMutation)
    ));
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("external content survives"),
        b"external"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_journals_directory_metadata_without_replacing_children() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    std::fs::create_dir_all(root.join("directory")).expect("directory");
    std::fs::create_dir_all(operation.join("target")).expect("target");
    std::fs::write(root.join("directory/child.txt"), b"child").expect("child");
    let before = native_metadata(
        &std::fs::symlink_metadata(root.join("directory")).expect("before metadata"),
    );
    let mut desired = before.clone();
    desired.readonly = !before.readonly;
    #[cfg(windows)]
    {
        const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
        desired.windows_attributes = before
            .windows_attributes
            .map(|attributes| attributes ^ FILE_ATTRIBUTE_HIDDEN);
        desired.readonly = desired
            .windows_attributes
            .is_some_and(|attributes| attributes & 1 != 0);
    }
    #[cfg(unix)]
    {
        desired.posix_mode = before.posix_mode.map(|mode| mode ^ 0o200);
    }
    let backend = native_backend(&root, &operation).expect("native backend");
    let operation_id = OperationId::new();
    let plan = MaterializationPlan {
        operation_id,
        intent: MaterializationIntent::GenerationTransition {
            from: GenerationId::new(Digest::from_bytes([1; 32])),
            to: GenerationId::new(Digest::from_bytes([2; 32])),
        },
        edits: vec![MaterializationEdit::SetMetadata {
            path: "directory".to_owned(),
            image: serde_json::to_vec(&desired).expect("metadata image"),
        }],
    };
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    materializer
        .apply(plan, &mut allow)
        .await
        .expect("apply metadata");
    assert_eq!(
        std::fs::read(root.join("directory/child.txt")).expect("child remains"),
        b"child"
    );
    let applied = native_metadata(
        &std::fs::symlink_metadata(root.join("directory")).expect("applied metadata"),
    );
    assert_eq!(applied.readonly, desired.readonly);
    #[cfg(windows)]
    assert_eq!(applied.windows_attributes, desired.windows_attributes);
    #[cfg(unix)]
    assert_eq!(applied.posix_mode, desired.posix_mode);

    materializer
        .recover(operation_id, MaterializationRecovery::RollBack, &mut allow)
        .await
        .expect("rollback metadata");
    let restored = native_metadata(
        &std::fs::symlink_metadata(root.join("directory")).expect("restored metadata"),
    );
    assert_eq!(restored.readonly, before.readonly);
    #[cfg(windows)]
    assert_eq!(restored.windows_attributes, before.windows_attributes);
    #[cfg(unix)]
    assert_eq!(restored.posix_mode, before.posix_mode);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn metadata_edit_preserves_concurrent_child_content() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    std::fs::create_dir_all(root.join("directory")).expect("checkout directory");
    std::fs::create_dir_all(operation.join("target")).expect("target directory");
    let child = root.join("directory/child.txt");
    std::fs::write(&child, b"before").expect("child file");
    let backend = native_backend(&root, &operation).expect("native backend");
    let mut desired = native_metadata(
        &std::fs::symlink_metadata(root.join("directory")).expect("directory metadata"),
    );
    #[cfg(unix)]
    {
        desired.posix_mode = desired.posix_mode.map(|mode| mode ^ 0o200);
    }
    #[cfg(windows)]
    {
        desired.windows_attributes = desired.windows_attributes.map(|attributes| attributes ^ 2);
    }
    let edit = MaterializationEdit::SetMetadata {
        path: "directory".to_owned(),
        image: serde_json::to_vec(&desired).expect("metadata image"),
    };
    let preimage = backend.capture(&edit).await.expect("capture metadata");
    std::fs::write(&child, b"concurrent child write").expect("update child");
    assert_eq!(
        backend.observe(&edit, &preimage).await.expect("observe"),
        MaterializationObservation::Preimage
    );
    backend
        .apply(&edit, &preimage)
        .await
        .expect("apply metadata");
    assert_eq!(
        std::fs::read(&child).expect("read child"),
        b"concurrent child write"
    );
    backend
        .restore(&edit, &preimage)
        .await
        .expect("restore metadata");
    assert_eq!(
        std::fs::read(&child).expect("read restored child"),
        b"concurrent child write"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn metadata_edit_rejects_a_replaced_directory() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let directory = root.join("directory");
    std::fs::create_dir_all(&directory).expect("checkout directory");
    std::fs::create_dir_all(operation.join("target")).expect("target directory");
    let backend = native_backend(&root, &operation).expect("native backend");
    let edit = MaterializationEdit::SetMetadata {
        path: "directory".to_owned(),
        image: serde_json::to_vec(&native_metadata(
            &std::fs::symlink_metadata(&directory).expect("directory metadata"),
        ))
        .expect("metadata image"),
    };
    let preimage = backend.capture(&edit).await.expect("capture metadata");
    std::fs::rename(&directory, root.join("displaced")).expect("replace binding");
    std::fs::create_dir(&directory).expect("replacement directory");
    assert_eq!(
        backend.observe(&edit, &preimage).await.expect("observe"),
        MaterializationObservation::Diverged
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_recognizes_apply_before_progress_was_persisted() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&target).expect("target");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    std::fs::write(target.join("file.txt"), b"after").expect("after");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["file.txt".to_owned()],
        )
        .expect("plan");
    let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
    let store = MemoryMaterializationJournalStore::default();
    seed_native_journal(
        &store,
        &backend,
        &plan,
        &preimage,
        MaterializationPhase::Applying,
    )
    .await;
    backend
        .apply(&plan.edits[0], &preimage)
        .await
        .expect("physical apply");

    let recovered = JournaledMaterializer::new(store, backend)
        .recover(
            plan.operation_id,
            MaterializationRecovery::Complete,
            &mut allow,
        )
        .await
        .expect("recover")
        .expect("journal");
    assert_eq!(recovered.phase, MaterializationPhase::Applied);
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("after remains"),
        b"after"
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_tree_backend_pauses_rollback_after_external_mutation() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&target).expect("target");
    std::fs::write(root.join("file.txt"), b"before").expect("before");
    std::fs::write(target.join("file.txt"), b"after").expect("after");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["file.txt".to_owned()],
        )
        .expect("plan");
    let operation_id = plan.operation_id;
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    materializer.apply(plan, &mut allow).await.expect("apply");
    std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

    assert!(matches!(
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack, &mut allow)
            .await,
        Err(MaterializationError::ExternalMutation)
    ));
    assert_eq!(
        std::fs::read(root.join("file.txt")).expect("external remains"),
        b"external"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn native_tree_backend_preserves_dangling_symlinks_on_rollback() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = temporary.path().join("operation");
    let target = operation.join("target");
    std::fs::create_dir_all(&root).expect("checkout directory");
    std::fs::create_dir_all(&target).expect("target directory");
    symlink("missing-target", root.join("link")).expect("dangling symlink");
    let backend = native_backend(&root, &operation).expect("native backend");
    let plan = backend
        .plan_paths(
            OperationId::new(),
            GenerationId::new(Digest::from_bytes([1; 32])),
            GenerationId::new(Digest::from_bytes([2; 32])),
            ["link".to_owned()],
        )
        .expect("plan");
    let operation_id = plan.operation_id;
    let materializer =
        JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
    materializer
        .apply(plan, &mut allow)
        .await
        .expect("remove link");
    assert!(std::fs::symlink_metadata(root.join("link")).is_err());
    materializer
        .recover(operation_id, MaterializationRecovery::RollBack, &mut allow)
        .await
        .expect("restore link");
    assert_eq!(
        std::fs::read_link(root.join("link")).expect("restored symlink"),
        PathBuf::from("missing-target")
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_tree_backend_rejects_operation_directory_inside_checkout() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let root = temporary.path().join("checkout");
    let operation = root.join("operation");
    let sentinel = operation.join("target/sentinel");
    std::fs::create_dir_all(sentinel.parent().expect("sentinel parent")).expect("target directory");
    std::fs::write(&sentinel, b"keep").expect("sentinel");

    assert!(matches!(
        native_backend(&root, &operation),
        Err(NativeTreeMaterializationError::InvalidLayout)
    ));
    assert_eq!(std::fs::read(sentinel).expect("sentinel remains"), b"keep");
}
