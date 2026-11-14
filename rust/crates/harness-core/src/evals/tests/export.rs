use super::*;
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    ForkOptions, LazyWorkspace, Lifecycle, MemoryLazyWorkspaceStore, PublicationPermit,
    VolumeConfig, VolumeInitialization,
};

type Cases = Computed<View<Snapshot>, Provider>;

pub(super) async fn selected_case_export(
    source: Arc<Cases>,
    work: &NativeMaterializationWork,
) -> TestResult {
    let fs = acyclic_fs::Fs::memory();
    let cancel = work.cancellation();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let (root, _) = LazyWorkspace::attach(
        &fs,
        "case-source",
        source,
        MemoryLazyWorkspaceStore::default(),
        VolumeInitialization {
            config,
            access_policy: None,
        },
        acyclic_fs::OperationId::new(),
        &mut accounting(work),
    )
    .await?;
    let omitted = "unselected private specimen";
    let later = "later unselected bytes";
    root.workspace().write_text("/private.txt", omitted).await?;
    let cut = root.snapshot(&mut accounting(work)).await?;
    let (witness, _) = root
        .fork_snapshot(
            "case-witness",
            cut.clone(),
            acyclic_fs::IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut accounting(work),
        )
        .await?;
    let selected = "/00000000000000000001.json".to_owned();
    witness.stat(&selected, &mut accounting(work)).await?;
    let exact = root
        .exactify_cut(cut, &[selected.clone()], &witness, &mut accounting(work))
        .await?
        .ok_or("original case was not retained")?;
    root.workspace().write_text("/private.txt", later).await?;
    // Filter before building the transfer closure, including all body objects.
    let export = exact
        .workspace()
        .fork::<_, _, acyclic_fs::WorkspaceError>(
            "selected-case",
            ForkOptions::from_generation(exact.clone(), acyclic_fs::IdempotencyKey::new())
                .inherit_paths(vec![selected.clone()]),
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            cancel,
            (),
        )
        .await?
        .value;
    let bytes = export
        .read(&selected, u64::MAX, WorkBudget::UNBOUNDED, cancel)
        .await?
        .value;
    let case: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(case["cut"]["through"], 1);
    assert_eq!(case["cut"]["checkpoint"]["conversation"], 1);
    assert_eq!(case["outcome"]["checkpoint"]["conversation"], 2);
    for omitted_path in ["/private.txt", "/00000000000000000000.json"] {
        assert!(
            export
                .stat(omitted_path, WorkBudget::UNBOUNDED, cancel)
                .await
                .is_err()
        );
    }
    let checkout = export
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    let manifest = checkout
        .export_manifest(WorkBudget::UNBOUNDED, cancel)
        .await?
        .value;
    let mut cursor = acyclic_fs::kernel::TransferCursor::START;
    let destination = acyclic_fs::Fs::memory();

    loop {
        let batch = fs
            .export_generation_batch(
                &manifest,
                cursor,
                1,
                export.volume_config().limits.maximum_object_bytes,
                WorkBudget::UNBOUNDED,
                cancel,
            )
            .await?
            .value;
        let bodies = batch
            .objects
            .iter()
            .map(|object| object.bytes.clone())
            .collect::<Vec<_>>();
        destination
            .import_generation_batch(&manifest, cursor, &bodies, 1, WorkBudget::UNBOUNDED, cancel)
            .await?;
        let Some(next) = batch.next else {
            break;
        };
        assert!(next.next_object() > cursor.next_object());
        cursor = next;
    }
    let restored = destination
        .restore_volume(
            &manifest,
            acyclic_fs::OperationId::new(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    let mut restored_checkout = restored
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    let original = restored_checkout
        .read_file_range(
            &path(&selected),
            acyclic_fs::ByteRange {
                offset: 0,
                length: bytes.len() as u64,
            },
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    assert_eq!(original.bytes.as_ref(), bytes.as_ref());
    let listing = restored_checkout
        .list_directory(&path("/"), None, 2, WorkBudget::UNBOUNDED, cancel)
        .await?
        .value;
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(
        listing.entries[0].name.as_bytes(),
        selected.trim_start_matches('/').as_bytes()
    );
    assert!(!listing.has_more);
    Ok(())
}
