use super::*;

pub(super) fn callback_runtime(
    source: Arc<MemorySource>,
    root: &std::path::Path,
    work: &crate::NativeMaterializationWork,
) -> Result<Runtime, Box<dyn std::error::Error>> {
    Ok(Runtime {
        source,
        work: Mutex::new(work.clone()),
        active_work: Mutex::new(Default::default()),
        original_work: Mutex::new(None),
        callback_guard: None,
        root: root.to_path_buf(),
        metadata_root: HostRoot::open(root, &mut &*work)?,
        staging: Mutex::new(None),
        creating_name: RwLock::new((MountPath::root(), None)),
        writable: true,
        metadata_baselines: Mutex::new(HashMap::new()),
        projection: Mutex::new(HashMap::new()),
        projector: Arc::new(Projector::new()),
        metadata_probes: Mutex::new(HashMap::new()),
        metadata_edits: FileLocks::default(),
        post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
        callbacks: CallbackGate::start()?,
        _allocation: work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
    })
}

fn cached_name_query(runtime: &Runtime, file_id: FileId, basis: ReadBasis) -> HRESULT {
    let mut path = windows_path("leaf");
    with_creating_name(&runtime.creating_name, &mut path, (file_id, basis), |_| {
        leaf_name_query(runtime)
    })
}

fn leaf_name_query(runtime: &Runtime) -> HRESULT {
    let name = HSTRING::from("leaf");
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(runtime).cast_mut().cast(),
        FilePathName: PCWSTR(name.as_ptr()),
        ..Default::default()
    };
    // SAFETY: the runtime, callback data and UTF-16 name remain live for the call.
    unsafe { query_name(&raw const data) }
}

#[tokio::test]
async fn a_scoped_name_query_rejects_an_intervening_source_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let path = windows_path("leaf");
    let lookup = source.create_file(&path, FileMetadata::default(), None, &work)?;
    let basis = ReadBasis::sample(source.as_ref()).ok_or("source view is unstable")?;
    let root = tempfile::tempdir()?;
    let runtime = callback_runtime(Arc::clone(&source), root.path(), &work)?;
    assert_eq!(
        cached_name_query(&runtime, lookup.node.file_id, basis),
        HR_OK
    );
    source.remove(&path, None, &work)?;
    assert_eq!(
        cached_name_query(&runtime, lookup.node.file_id, basis),
        HR_FILE_NOT_FOUND
    );
    assert!(source.lookup(&path)?.is_none());
    Ok(())
}

#[tokio::test]
async fn a_scoped_name_query_keeps_the_callback_budget_and_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let source = windows_checkout_source().await?;
    let lookup = source.create_file(&windows_path("leaf"), FileMetadata::default(), None, &work)?;
    let basis = ReadBasis::sample(source.as_ref()).ok_or("source view is unstable")?;
    let root = tempfile::tempdir()?;
    let mut runtime = callback_runtime(source, root.path(), &work)?;
    let cancellation = crate::CancellationToken::new();
    let limited = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: 1,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    runtime.work = Mutex::new(limited.clone());
    assert!(cached_name_query(&runtime, lookup.node.file_id, basis).is_err());
    assert_eq!(limited.receipt().allocation_operations, 1);
    let cancellable =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
    runtime.work = Mutex::new(cancellable.clone());
    let before = cancellable.receipt();
    cancellation.cancel();
    assert_eq!(
        cached_name_query(&runtime, lookup.node.file_id, basis),
        HR_FILE_INVALID
    );
    assert_eq!(cancellable.receipt(), before);
    Ok(())
}

async fn stale_name_runtime(
    work: &crate::NativeMaterializationWork,
) -> Result<(Runtime, FileId, ReadBasis, tempfile::TempDir), Box<dyn std::error::Error>> {
    let source = windows_checkout_source().await?;
    let path = windows_path("leaf");
    let lookup = source.create_file(&path, FileMetadata::default(), None, work)?;
    let basis = ReadBasis::sample(source.as_ref()).ok_or("source view is unstable")?;
    source.remove(&path, None, work)?;
    assert_ne!(ReadBasis::sample(source.as_ref()), Some(basis));
    let root = tempfile::tempdir()?;
    let runtime = callback_runtime(source, root.path(), work)?;
    Ok((runtime, lookup.node.file_id, basis, root))
}

#[tokio::test]
async fn stale_listing_facts_keep_the_original_lookup_allocation_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    let cancellation = crate::CancellationToken::new();
    let defaults = crate::performance::BOUNDARY_BUDGET;
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: defaults.allocation_operations,
            peak_allocation_bytes: defaults.peak_allocation_bytes,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let (mut runtime, _, _, _root) = stale_name_runtime(&work).await?;
    runtime.work = Mutex::new(original.clone());
    assert_eq!(leaf_name_query(&runtime), HR_FILE_NOT_FOUND);
    let receipt = original.receipt();
    for peak in [
        receipt.peak_allocation_bytes,
        defaults.peak_allocation_bytes,
    ] {
        let budget = crate::WorkBudget {
            allocation_operations: receipt.allocation_operations,
            peak_allocation_bytes: peak,
            ..crate::WorkBudget::UNBOUNDED
        };
        let plain = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let (mut runtime, _, _, _root) = stale_name_runtime(&work).await?;
        runtime.work = Mutex::new(plain.clone());
        let expected = leaf_name_query(&runtime);
        if peak == defaults.peak_allocation_bytes {
            assert_eq!(expected, HR_FILE_NOT_FOUND);
        }
        let limited = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let (mut runtime, file_id, basis, _root) = stale_name_runtime(&work).await?;
        runtime.work = Mutex::new(limited.clone());
        assert_eq!(cached_name_query(&runtime, file_id, basis), expected);
        let fallback = limited.receipt();
        let baseline = plain.receipt();
        assert_eq!(
            fallback.allocation_operations,
            baseline.allocation_operations
        );
        assert_eq!(
            fallback.peak_allocation_bytes,
            baseline.peak_allocation_bytes
        );
        assert_eq!(
            fallback.backend_read_operations,
            baseline.backend_read_operations
        );
        assert_eq!(fallback.object_bytes_read, baseline.object_bytes_read);
    }
    Ok(())
}
