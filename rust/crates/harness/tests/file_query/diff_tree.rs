use super::*;
use acyclic_harness::client::{DiffLimits, DiffView};
use proptest::{prelude::*, test_runner::TestRunner};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Workspace = acyclic_fs::Workspace<MemoryAuthorityBackend, MemoryObjectBackend>;
type Transaction = acyclic_fs::Transaction<MemoryAuthorityBackend, MemoryObjectBackend>;
type Generation = acyclic_fs::Generation<MemoryAuthorityBackend, MemoryObjectBackend>;

const LIMITS: DiffLimits = DiffLimits {
    files: 64,
    changes: 64,
    file_bytes: 4096,
    render_bytes: 65_536,
    work: 1_048_576,
};

async fn workspace(fixture: &Fixture) -> TestResult<Workspace> {
    Ok(fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .await?
        .value)
}

async fn begin(workspace: &Workspace) -> TestResult<Transaction> {
    Ok(workspace
        .begin_transaction(
            acyclic_fs::IdempotencyKey::new(),
            BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .await?
        .value)
}

async fn commit(transaction: &mut Transaction) -> TestResult<Generation> {
    let result = transaction
        .commit(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await?
        .value;
    match result {
        acyclic_fs::TransactionCommit::Committed(generation) => Ok(generation),
        _ => Err("test mutation did not commit".into()),
    }
}

#[test]
fn moved_directories_expand_descendants_and_retire_previous_rows() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    TestRunner::new(proptest::test_runner::Config::with_cases(8)).run(
        &("[a-z]{1,4}", 1_usize..4),
        |(suffix, count)| {
            let result: TestResult = runtime.block_on(async {
                let fixture = Fixture::new().await?;
                let workspace = workspace(&fixture).await?;
                let cancel = acyclic_fs::CancellationToken::new();
                let source = format!("src-{suffix}");
                let destination = format!("lib-{suffix}");
                let mut seed = begin(&workspace).await?;
                seed.create_dir_all(&format!("/{source}/deep"), BOUNDARY_BUDGET, &cancel)
                    .await?;
                for index in 0..count {
                    seed.write_text(&format!("/{source}/deep/{index}.txt"), "base\n")
                        .await?;
                }
                seed.write_text("/untouched", "unchanged\n").await?;
                let base = commit(&mut seed).await?;
                let mut stream = fixture.source.observe_diff(base.id(), 64).await?;
                let snapshot = stream.next().await.ok_or("snapshot")??;
                let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                view.apply(&snapshot, fixture.reader.as_ref(), LIMITS)
                    .await?;
                let old = format!("{source}/deep/0.txt");
                fixture.write(&[(&old, Some("edited\n"))]).await?;
                view.apply(
                    &stream.next().await.ok_or("edit")??,
                    fixture.reader.as_ref(),
                    LIMITS,
                )
                .await?;
                assert!(view.files()[&old].patch.contains("+edited"));
                let mut transaction = begin(&workspace).await?;
                transaction
                    .rename(
                        &format!("/{source}"),
                        &format!("/{destination}"),
                        false,
                        BOUNDARY_BUDGET,
                        &cancel,
                    )
                    .await?;
                commit(&mut transaction).await?;
                let publication = stream.next().await.ok_or("move")??;
                // Negative control: the former identity-only selection omits children,
                // leaving the modified row and failing to publish the new paths.
                let mut anchors_only = publication.clone();
                anchors_only.paths.retain(|path| {
                    !path.path.starts_with(&format!("{source}/"))
                        && !path.path.starts_with(&format!("{destination}/"))
                });
                let mut broken = view.clone();
                broken
                    .apply(&anchors_only, fixture.reader.as_ref(), LIMITS)
                    .await?;
                view.apply(&publication, fixture.reader.as_ref(), LIMITS)
                    .await?;
                assert_ne!(broken.files(), view.files());
                assert_eq!(view.files().len(), count * 2);
                for index in 0..count {
                    let removed = &view.files()[&format!("{source}/deep/{index}.txt")];
                    assert!(removed.patch.contains("-base"));
                    let added = &view.files()[&format!("{destination}/deep/{index}.txt")];
                    assert!(
                        added
                            .patch
                            .contains(if index == 0 { "+edited" } else { "+base" })
                    );
                }
                assert!(!view.files().contains_key("untouched"));
                let mut reopened = fixture.source.observe_diff(base.id(), 64).await?;
                let snapshot = reopened.next().await.ok_or("reopen")??;
                let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                replay
                    .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
                    .await?;
                assert_eq!(replay.files(), view.files());
                Ok(())
            });
            result.map_err(|error| TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn root_metadata_does_not_interrupt_file_diffs_or_replay() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture.write(&[("visible", Some("before"))]).await?;
    let workspace = workspace(&fixture).await?;
    let cancel = acyclic_fs::CancellationToken::new();
    let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
    let mut stream = fixture.source.observe_diff(base.id(), 64).await?;
    let snapshot = stream.next().await.ok_or("snapshot")??;
    let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    view.apply(&snapshot, fixture.reader.as_ref(), LIMITS)
        .await?;
    let mut transaction = begin(&workspace).await?;
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Exact(base.id()),
            acyclic_fs::model::CheckoutMode::read_only_pinned(),
            BOUNDARY_BUDGET,
            &cancel,
        )
        .await?
        .value;
    let root = checkout.root().root_file_id;
    let mut root_metadata = checkout
        .read_metadata(root, BOUNDARY_BUDGET, &cancel)
        .await?
        .value;
    root_metadata.modified_ns = acyclic_fs::kernel::MetadataField::Value(1_700_000_000_000_000_000);
    transaction
        .set_metadata("/", root_metadata, BOUNDARY_BUDGET, &cancel)
        .await?;
    commit(&mut transaction).await?;
    let metadata = stream.next().await.ok_or("root metadata")??;
    assert!(metadata.paths.is_empty());
    view.apply(&metadata, fixture.reader.as_ref(), LIMITS)
        .await?;
    fixture.write(&[("visible", Some("after"))]).await?;
    view.apply(
        &stream.next().await.ok_or("file edit")??,
        fixture.reader.as_ref(),
        LIMITS,
    )
    .await?;
    assert!(view.files()["visible"].patch.contains("+after"));
    let mut replay = fixture.source.observe_diff(base.id(), 64).await?;
    let snapshot = replay.next().await.ok_or("replay")??;
    let mut reopened = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    reopened
        .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert_eq!(reopened.files(), view.files());
    Ok(())
}

#[tokio::test]
async fn one_changed_path_in_a_cold_large_checkout_keeps_its_own_work_bound() -> TestResult {
    let fixture = Fixture::new().await?;
    let maximum_changes = u32::try_from(LIMITS.changes)?;
    let names: Vec<_> = (0..4097).map(|index| format!("file-{index:04}")).collect();
    let mut writes: Vec<_> = names
        .iter()
        .map(|name| (name.as_str(), Some("base")))
        .collect();
    writes.push(("zz-changed", Some("before")));
    fixture.write(&writes).await?;
    let workspace = workspace(&fixture).await?;
    let cancel = acyclic_fs::CancellationToken::new();
    let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
    fixture.write(&[("zz-changed", Some("after"))]).await?;
    let current = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
    let stat = current
        .stat("/zz-changed", BOUNDARY_BUDGET, &cancel)
        .await?
        .value;
    // Run the negative control before either candidate query warms the path
    // cache. An output bound used as a cold scan bound cannot find this path.
    let old = current
        .paths_for_file_id(stat.file_id, maximum_changes, BOUNDARY_BUDGET, &cancel)
        .await?
        .value;
    assert!(old.is_empty());
    let mut stream = fixture
        .source
        .observe_diff(base.id(), maximum_changes)
        .await?;
    let snapshot = stream.next().await.ok_or("snapshot")??;
    assert_eq!(snapshot.paths.len(), 1);
    assert_eq!(
        snapshot.paths.first().ok_or("missing diff path")?.path,
        "zz-changed"
    );
    // Semantic records and rendered paths have separate bounds: the host also
    // changes directory/metadata records when replacing this one visible file.
    let changes = base
        .diff_to(&current, maximum_changes, BOUNDARY_BUDGET, &cancel)
        .await?
        .value
        .0;
    let paths = changes
        .changed_paths(4, BOUNDARY_BUDGET, &cancel)
        .await?
        .value
        .0;
    assert_eq!(paths.len(), 1);
    let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    view.apply(&snapshot, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert!(view.files()["zz-changed"].patch.contains("+after"));
    let limited = fixture
        .reader
        .file_query(
            &fixture.volume,
            acyclic_fs::WorkBudget {
                items_examined: 0,
                ..BOUNDARY_BUDGET
            },
        )
        .await;
    assert!(match limited {
        Err(_) => true,
        Ok(limited) => limited
            .observe_diff(base.id(), maximum_changes)
            .await
            .is_err(),
    });
    Ok(())
}

#[tokio::test]
async fn directory_replacement_and_restoration_preserve_both_endpoints() -> TestResult {
    let fixture = Fixture::new().await?;
    let workspace = workspace(&fixture).await?;
    let cancel = acyclic_fs::CancellationToken::new();
    let mut seed = begin(&workspace).await?;
    seed.create_dir_all("/dir", BOUNDARY_BUDGET, &cancel)
        .await?;
    seed.write_text("/dir/a.txt", "base\n").await?;
    let base = commit(&mut seed).await?;
    let mut stream = fixture.source.observe_diff(base.id(), 64).await?;
    let snapshot = stream.next().await.ok_or("snapshot")??;
    let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    let mut transaction = begin(&workspace).await?;
    transaction
        .remove("/dir/a.txt", BOUNDARY_BUDGET, &cancel)
        .await?;
    transaction.remove("/dir", BOUNDARY_BUDGET, &cancel).await?;
    transaction.write_text("/dir", "replacement\n").await?;
    let file = commit(&mut transaction).await?;
    let error = file
        .stat("/dir/a.txt", BOUNDARY_BUDGET, &cancel)
        .await
        .unwrap_err()
        .error;
    assert!(matches!(error, acyclic_fs::WorkspaceError::NotDirectory));
    // Negative control: the old NotFound-only classifier rejects this endpoint.
    assert!(!matches!(error, acyclic_fs::WorkspaceError::NotFound));
    let replaced = stream.next().await.ok_or("replacement")??;
    view.apply(&replaced, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert_eq!(view.files().len(), 2);
    assert!(view.files()["dir"].patch.contains("+replacement"));
    assert!(view.files()["dir/a.txt"].patch.contains("-base"));
    let mut reopened = fixture.source.observe_diff(base.id(), 64).await?;
    let snapshot = reopened.next().await.ok_or("reopen")??;
    let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    replay
        .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert_eq!(replay.files(), view.files());
    let mut restore = begin(&workspace).await?;
    restore.remove("/dir", BOUNDARY_BUDGET, &cancel).await?;
    restore
        .create_dir_all("/dir", BOUNDARY_BUDGET, &cancel)
        .await?;
    restore.write_text("/dir/a.txt", "base\n").await?;
    commit(&mut restore).await?;
    view.apply(
        &stream.next().await.ok_or("restore")??,
        fixture.reader.as_ref(),
        LIMITS,
    )
    .await?;
    assert!(view.files().is_empty());
    Ok(())
}
