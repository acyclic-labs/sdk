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
    let mut found_case = false;
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
        for object in &batch.objects {
            let contains = |needle: &[u8]| {
                object
                    .bytes
                    .windows(needle.len())
                    .any(|part| part == needle)
            };
            assert!(
                !contains(omitted.as_bytes()),
                "private bytes entered the transfer closure"
            );
            assert!(
                !contains(later.as_bytes()),
                "later bytes entered the transfer closure"
            );
            assert!(
                !contains(b"\"through\":0"),
                "unselected case entered the transfer closure"
            );
            found_case |= contains(b"\"through\":1");
        }
        let Some(next) = batch.next else {
            break;
        };
        assert!(next.next_object() > cursor.next_object());
        cursor = next;
    }
    assert!(
        found_case,
        "the selected Computed case body was not transferable"
    );
    Ok(())
}
