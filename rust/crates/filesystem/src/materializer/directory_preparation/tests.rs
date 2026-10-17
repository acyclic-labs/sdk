use super::*;
use proptest::prelude::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn work() -> TestResult<NativeMaterializationWork> {
    NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error.into())
}

async fn interrupted_preparation(prefix: usize, foreign: Option<usize>) -> TestResult {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("checkout");
    let parent = temporary.path().join("staging");
    let operation = parent.join("operation");
    std::fs::create_dir(&root)?;
    std::fs::create_dir(&parent)?;
    let store = crate::LocalCoreStateStore::new(temporary.path().join("state"));
    let original = work()?;
    let operation_id = OperationId::new();
    let root_identity = original.root_identity(&root)?.to_bytes();
    let mut journal = MaterializationJournal {
        version: JOURNAL_VERSION,
        revision: 1,
        plan: MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(crate::Digest::from_bytes([1; 32])),
                to: GenerationId::new(crate::Digest::from_bytes([2; 32])),
            },
            edits: Vec::new(),
        },
        root_identity: Some(root_identity),
        preparation_identity: None,
        preparation_progress: Some(MaterializationPreparationProgress {
            parent: original.root_identity(&parent)?.to_bytes(),
            pending: true,
        }),
        work: original.journal_work()?,
        preimages: Vec::new(),
        phase: MaterializationPhase::Preparing,
        applied: 0,
        restored: 0,
        custody: None,
    };
    journal.custody = Some(original.claim(
        journal_owned_bytes(&journal)?,
        0,
        crate::WorkCounters::UNCHARGED,
    )?);
    let (stored, journal) = store
        .materialization(original.clone())
        .compare_and_swap(operation_id, 0, journal)
        .await?;
    assert!(stored);
    drop(journal);
    drop(original);
    let paths = [
        operation.clone(),
        operation.join("target"),
        operation.join("backup"),
    ];
    for path in paths.iter().take(prefix) {
        std::fs::create_dir(path)?;
    }
    if let Some(index) = foreign {
        std::fs::write(paths[index].join("foreign"), "must survive")?;
    }
    let recovery = work()?;
    let store = store.materialization(recovery.clone());
    let journal = store
        .load(operation_id)
        .await?
        .ok_or("missing preparation")?;
    assert!(
        journal
            .preparation_progress
            .ok_or("missing progress")?
            .pending
    );
    let result = NativeTreeMaterializationBackend::prepare_allocations(
        &store,
        journal,
        &root,
        &operation,
        &recovery,
        &mut || std::future::ready(Ok(())),
    )
    .await;
    if let Some(index) = foreign {
        assert!(matches!(
            result,
            Err(NativeWorkspacePublicationError::Native(
                NativeTreeMaterializationError::ExternalMutation
            ))
        ));
        assert_eq!(
            std::fs::read_to_string(paths[index].join("foreign"))?,
            "must survive"
        );
        return Ok(());
    }
    let journal = result?;
    assert!(journal.preparation_progress.is_none());
    let identities = journal.preparation_identity.ok_or("missing identities")?;
    for (path, expected) in
        paths
            .iter()
            .zip([identities.operation, identities.target, identities.backup])
    {
        assert_eq!(recovery.root_identity(path)?.to_bytes(), expected);
    }
    assert_eq!(recovery.root_identity(&root)?.to_bytes(), root_identity);
    let durable = store
        .load(operation_id)
        .await?
        .ok_or("missing acknowledgement")?;
    assert_eq!(durable, journal);
    Ok(())
}

#[test]
fn pending_preparation_recovers_every_directory_prefix() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // These are all mkdir prefixes between the durable pending and identity writes.
    for prefix in 0..=3 {
        runtime.block_on(interrupted_preparation(prefix, None))?;
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn pending_preparation_never_adopts_foreign_contents(index in 0usize..3) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(interrupted_preparation(index + 1, Some(index))).unwrap();
    }
}
