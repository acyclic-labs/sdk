use super::*;
use crate::Digest;
use proptest::{collection::btree_map, option, prelude::*};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::SeqCst},
    },
};

#[derive(Debug, Error)]
#[error("interrupted")]
struct Interrupted;
#[derive(Default)]
struct Model {
    tree: Mutex<BTreeMap<String, u8>>,
    steps: AtomicU64,
    crash: AtomicU64,
}
impl Model {
    fn step(&self) -> Result<(), Interrupted> {
        if self.steps.fetch_add(1, SeqCst) + 1 == self.crash.load(SeqCst) {
            Err(Interrupted)
        } else {
            Ok(())
        }
    }
    fn current(&self, path: &str) -> Option<u8> {
        self.tree.lock().unwrap().get(path).copied()
    }
    fn set(&self, path: &str, value: Option<u8>) -> Result<(), Interrupted> {
        let mut tree = self.tree.lock().unwrap();
        match value {
            Some(value) => tree.insert(path.into(), value),
            None => tree.remove(path),
        };
        drop(tree);
        self.step()
    }
}
fn endpoint(edit: &MaterializationEdit) -> (&str, Option<u8>) {
    match edit {
        MaterializationEdit::Install { path, image } => (path, image.first().copied()),
        MaterializationEdit::Remove { path } => (path, None),
        _ => unreachable!(),
    }
}
fn preimage(image: Vec<u8>) -> MaterializationPreimage {
    MaterializationPreimage {
        image,
        #[cfg(not(target_arch = "wasm32"))]
        custody: None,
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
impl MaterializationBackend for &Model {
    type Error = Interrupted;
    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Interrupted> {
        Ok(preimage(
            self.current(endpoint(edit).0).into_iter().collect(),
        ))
    }
    async fn observe(
        &self,
        edit: &MaterializationEdit,
        before: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Interrupted> {
        let (path, after) = endpoint(edit);
        let current = self.current(path);
        Ok(if current == after {
            MaterializationObservation::Postimage
        } else if current == before.image.first().copied() {
            MaterializationObservation::Preimage
        } else {
            MaterializationObservation::Diverged
        })
    }
    async fn apply(
        &self,
        edit: &MaterializationEdit,
        _: &MaterializationPreimage,
    ) -> Result<(), Interrupted> {
        let (path, value) = endpoint(edit);
        self.set(path, value)
    }
    async fn restore(
        &self,
        edit: &MaterializationEdit,
        before: &MaterializationPreimage,
    ) -> Result<(), Interrupted> {
        self.set(endpoint(edit).0, before.image.first().copied())
    }
}
struct CrashStore<'a>(&'a MemoryMaterializationJournalStore, &'a Model);
impl MaterializationJournalStore for CrashStore<'_> {
    type Error = Interrupted;
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = std::future::Ready<Result<MaterializationJournal, Interrupted>>
    where
        Self: 'a;
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        _: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'_>, Interrupted> {
        Err(Interrupted)
    }
    async fn load(
        &self,
        operation: OperationId,
    ) -> Result<Option<MaterializationJournal>, Interrupted> {
        self.0.load(operation).await.map_err(|_| Interrupted)
    }
    async fn compare_and_swap(
        &self,
        operation: OperationId,
        expected: u64,
        journal: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Interrupted> {
        let result = self
            .0
            .compare_and_swap(operation, expected, journal)
            .await
            .map_err(|_| Interrupted)?;
        self.1.step()?;
        Ok(result)
    }
}
type ModelError = MaterializationError<Interrupted, Interrupted>;
fn authorize<E>() -> std::future::Ready<Result<(), E>> {
    std::future::ready(Ok(()))
}
fn crash_apply(
    plan: &MaterializationPlan,
    original: &BTreeMap<String, u8>,
    crash: u64,
) -> (Model, MemoryMaterializationJournalStore, bool) {
    let model = Model::default();
    *model.tree.lock().unwrap() = original.clone();
    model.crash.store(crash, SeqCst);
    let store = MemoryMaterializationJournalStore::default();
    let failed = futures::executor::block_on(
        JournaledMaterializer::new(CrashStore(&store, &model), &model)
            .apply(plan.clone(), &mut authorize::<ModelError>),
    )
    .is_err();
    (model, store, failed)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn recovery_after_every_durable_boundary(original in btree_map("[a-e]", any::<u8>(), 0..5), targets in btree_map("[a-e]", option::of(any::<u8>()), 1..5), crash in any::<u64>(), forward in any::<bool>()) {
        let plan = plan(targets.iter().map(|(path, value)| match value {
            Some(value) => MaterializationEdit::Install { path: path.clone(), image: vec![*value] }, None => MaterializationEdit::Remove { path: path.clone() },
        }).collect());
        let mut target = original.clone();
        for (path, value) in &targets { match value { Some(value) => target.insert(path.clone(), *value), None => target.remove(path) }; }
        let (clean, _, failed) = crash_apply(&plan, &original, 0); prop_assert!(!failed); prop_assert_eq!(&*clean.tree.lock().unwrap(), &target);
        let (model, store, failed) = crash_apply(&plan, &original, 1 + crash % clean.steps.load(SeqCst)); prop_assert!(failed); model.crash.store(0, SeqCst);
        let mode = if forward { MaterializationRecovery::Complete } else { MaterializationRecovery::RollBack };
        let recovered = futures::executor::block_on(JournaledMaterializer::new(CrashStore(&store, &model), &model).recover(plan.operation_id, mode, &mut authorize::<ModelError>)).unwrap().unwrap();
        prop_assert_eq!(recovered.phase, if forward { MaterializationPhase::Applied } else { MaterializationPhase::RolledBack });
        prop_assert_eq!(&*model.tree.lock().unwrap(), if forward { &target } else { &original });
    }
}
#[tokio::test]
async fn invalid_intent_and_progress_reject_without_effects() {
    let model = Model::default();
    let store = MemoryMaterializationJournalStore::default();
    let materializer = JournaledMaterializer::new(store, &model);
    let invalid = plan(vec![
        MaterializationEdit::Remove {
            path: "directory".into(),
        },
        MaterializationEdit::Install {
            path: "directory/file".into(),
            image: vec![1],
        },
    ]);
    assert!(matches!(
        materializer
            .apply(
                invalid,
                &mut authorize::<
                    MaterializationError<MemoryMaterializationJournalStoreError, Interrupted>,
                >
            )
            .await,
        Err(MaterializationError::IncompatibleJournal)
    ));
    for (version, phase) in [
        (0, MaterializationPhase::Prepared),
        (2, MaterializationPhase::Prepared),
        (u32::MAX, MaterializationPhase::Prepared),
        (JOURNAL_VERSION, MaterializationPhase::RollingBack),
    ] {
        let plan = plan(vec![MaterializationEdit::Install {
            path: "file".into(),
            image: vec![1],
        }]);
        let operation = plan.operation_id;
        let journal = MaterializationJournal {
            version,
            revision: 1,
            plan,
            root_identity: None,
            preparation_identity: None,
            preparation_progress: None,
            work: crate::WorkCounters::UNCHARGED,
            preimages: vec![preimage(vec![0])],
            phase,
            applied: u32::from(phase == MaterializationPhase::RollingBack),
            restored: u32::from(phase == MaterializationPhase::RollingBack),
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        };
        materializer
            .store
            .compare_and_swap(operation, 0, journal)
            .await
            .unwrap();
        assert!(matches!(
            materializer
                .recover(
                    operation,
                    MaterializationRecovery::Complete,
                    &mut authorize::<
                        MaterializationError<MemoryMaterializationJournalStoreError, Interrupted>,
                    >
                )
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
    }
    assert!(model.tree.lock().unwrap().is_empty());
}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;
    type Failure = MaterializationError<
        MemoryMaterializationJournalStoreError,
        NativeTreeMaterializationError,
    >;
    fn fixture() -> (
        tempfile::TempDir,
        PathBuf,
        PathBuf,
        std::sync::Arc<NativeTreeMaterializationBackend>,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir_all(operation.join("target")).unwrap();
        std::fs::create_dir(operation.join("backup")).unwrap();
        let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
        for value in budget.as_object_mut().unwrap().values_mut() {
            *value = serde_json::Value::from(100_000_000_u64);
        }
        let work = NativeMaterializationWork::new(
            serde_json::from_value(budget).unwrap(),
            &crate::CancellationToken::new(),
        )
        .unwrap();
        let (owned_root, mut allocation) = work.path(&root, None).unwrap();
        let (owned_operation, mut operation_allocation) = work.path(&operation, None).unwrap();
        allocation.absorb(&mut operation_allocation).unwrap();
        let backend =
            NativeTreeMaterializationBackend::new(owned_root, owned_operation, allocation, work)
                .unwrap();
        (temporary, root, operation, backend)
    }
    #[tokio::test]
    async fn selected_tree_round_trip_preserves_unselected_entries() {
        let (_temporary, root, operation, backend) = fixture();
        std::fs::create_dir(root.join(".git")).unwrap();
        std::fs::write(root.join(".git/config"), b"git").unwrap();
        std::fs::write(root.join("old"), b"old").unwrap();
        std::fs::write(operation.join("target/new"), b"new").unwrap();
        let plan = plan(vec![
            MaterializationEdit::Remove { path: "old".into() },
            MaterializationEdit::Install {
                path: "new".into(),
                image: vec![1],
            },
        ]);
        let operation = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer
            .apply(plan.clone(), &mut authorize::<Failure>)
            .await
            .unwrap();
        materializer
            .apply(plan, &mut authorize::<Failure>)
            .await
            .unwrap();
        assert!(!root.join("old").exists());
        assert_eq!(std::fs::read(root.join("new")).unwrap(), b"new");
        materializer
            .recover(
                operation,
                MaterializationRecovery::RollBack,
                &mut authorize::<Failure>,
            )
            .await
            .unwrap();
        assert_eq!(std::fs::read(root.join("old")).unwrap(), b"old");
        assert!(!root.join("new").exists());
        assert_eq!(std::fs::read(root.join(".git/config")).unwrap(), b"git");
    }
    #[tokio::test]
    async fn install_recovers_lost_acknowledgement_and_rejects_external_changes() {
        let (_temporary, root, operation, backend) = fixture();
        let path = root.join("file");
        let target = operation.join("target/file");
        std::fs::write(&path, b"before").unwrap();
        std::fs::write(&target, b"after").unwrap();
        let edit = MaterializationEdit::Install {
            path: "file".into(),
            image: vec![1],
        };
        let before = backend.capture(&edit).await.unwrap();
        std::fs::write(&target, b"tampered").unwrap();
        assert!(backend.apply(&edit, &before).await.is_err());
        std::fs::write(&target, b"after").unwrap();
        std::fs::write(&path, b"external").unwrap();
        assert!(backend.apply(&edit, &before).await.is_err());
        std::fs::write(&path, b"before").unwrap();
        // Recapture after fixture writes changed the actual inode metadata.
        let before = backend.capture(&edit).await.unwrap();
        std::fs::rename(&path, operation.join("backup/file")).unwrap();
        assert_eq!(
            backend.observe(&edit, &before).await.unwrap(),
            MaterializationObservation::Interrupted
        );
        backend.apply(&edit, &before).await.unwrap();
        assert_eq!(
            backend.observe(&edit, &before).await.unwrap(),
            MaterializationObservation::Postimage
        );
        backend.apply(&edit, &before).await.unwrap();
        std::fs::write(&path, b"external").unwrap();
        assert!(backend.restore(&edit, &before).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"external");
    }
    #[tokio::test]
    async fn metadata_round_trip_preserves_children_and_rejects_replaced_directory() {
        let (_temporary, root, _operation, backend) = fixture();
        let directory = root.join("directory");
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("child"), b"before").unwrap();
        let original = native_metadata(
            &backend
                .stat(&(&backend.held_root, Path::new("directory")))
                .unwrap(),
        );
        let mut desired = original.clone();
        #[cfg(unix)]
        {
            desired.posix_mode = desired.posix_mode.map(|mode| mode ^ 0o200);
        }
        #[cfg(windows)]
        {
            desired.windows_attributes =
                desired.windows_attributes.map(|attributes| attributes ^ 2);
        }
        let edit = MaterializationEdit::SetMetadata {
            path: "directory".into(),
            image: serde_json::to_vec(&desired).unwrap(),
        };
        let before = backend.capture(&edit).await.unwrap();
        std::fs::write(directory.join("child"), b"concurrent").unwrap();
        backend.apply(&edit, &before).await.unwrap();
        backend.restore(&edit, &before).await.unwrap();
        assert_eq!(
            std::fs::read(directory.join("child")).unwrap(),
            b"concurrent"
        );
        assert_eq!(
            native_metadata(
                &backend
                    .stat(&(&backend.held_root, Path::new("directory")))
                    .unwrap()
            ),
            original
        );
        std::fs::rename(&directory, root.join("displaced")).unwrap();
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            backend.observe(&edit, &before).await.unwrap(),
            MaterializationObservation::Diverged
        );
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn removal_round_trip_preserves_dangling_link() {
        let (_temporary, root, _operation, backend) = fixture();
        std::os::unix::fs::symlink("missing", root.join("link")).unwrap();
        let edit = MaterializationEdit::Remove {
            path: "link".into(),
        };
        let before = backend.capture(&edit).await.unwrap();
        backend.apply(&edit, &before).await.unwrap();
        backend.restore(&edit, &before).await.unwrap();
        assert_eq!(
            std::fs::read_link(root.join("link")).unwrap(),
            Path::new("missing")
        );
    }
    #[test]
    fn nested_operation_is_rejected_without_effects() {
        let (_temporary, root, _operation, backend) = fixture();
        let operation = root.join("operation");
        std::fs::create_dir_all(operation.join("target")).unwrap();
        std::fs::write(operation.join("target/sentinel"), b"keep").unwrap();
        assert!(matches!(
            validate_native_preparation_location(&root, &operation, &backend.work()),
            Err(NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(
            std::fs::read(operation.join("target/sentinel")).unwrap(),
            b"keep"
        );
    }
}
