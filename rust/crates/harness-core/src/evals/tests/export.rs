use super::*;
use acyclic_fs::{
    CheckoutMode, ForkOptions, GenerationSelector, LazyWorkspace, Lifecycle,
    MemoryLazyWorkspaceStore, PublicationPermit, VolumeConfig, VolumeInitialization,
};

type Cases = Computed<View<Snapshot>, StreamProvider<MemoryStream>>;

pub(super) async fn selected_case_export(
    source: Arc<Cases>,
    work: &NativeMaterializationWork,
) -> TestResult {
    let fs = acyclic_fs::Fs::memory();
    let cancel = work.cancellation();
    let (root, _) = LazyWorkspace::attach(
        &fs,
        "case-source",
        source,
        MemoryLazyWorkspaceStore::default(),
        VolumeInitialization {
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
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
            IdempotencyKey::unique(),
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
            ForkOptions::from_generation(exact.clone(), IdempotencyKey::unique())
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
    let surplus_destination = acyclic_fs::Fs::memory();
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
        for target in [&destination, &surplus_destination] {
            let imported = target
                .import_generation_batch(
                    &manifest,
                    cursor,
                    &bodies,
                    1,
                    WorkBudget::UNBOUNDED,
                    cancel,
                )
                .await?
                .value;
            assert_eq!(
                imported.next_object(),
                cursor.next_object() + bodies.len() as u64
            );
        }
        let Some(next) = batch.next else {
            break;
        };
        assert!(next.next_object() > cursor.next_object());
        cursor = next;
    }
    // A valid, present source object must not be accepted as extra scope data.
    let private_checkout = root
        .workspace()
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    let private_manifest = private_checkout
        .export_manifest(WorkBudget::UNBOUNDED, cancel)
        .await?
        .value;
    let extra = private_manifest
        .objects
        .iter()
        .copied()
        .find(|object| !manifest.objects.contains(object))
        .ok_or("no unselected source object")?;
    let extra_body = fs
        .export_object(
            extra,
            private_manifest.config.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await?
        .value;
    surplus_destination
        .import_object(extra, extra_body.bytes, WorkBudget::UNBOUNDED, cancel)
        .await?;
    let mut surplus = manifest.clone();
    surplus.objects.insert(0, extra);
    let rejected = surplus_destination
        .restore_volume(
            &surplus,
            acyclic_fs::OperationId::new(),
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await
        .err()
        .ok_or("surplus scope object was accepted")?;
    assert!(matches!(
        rejected.error,
        acyclic_fs::FsError::InvalidExportManifest
    ));
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
    assert_eq!(manifest.file_count, 1);
    let rebuilt = restored_checkout
        .export_manifest(WorkBudget::UNBOUNDED, cancel)
        .await?
        .value;
    assert_eq!(
        rebuilt.objects, manifest.objects,
        "transfer closure changed after restoration"
    );
    Ok(())
}
