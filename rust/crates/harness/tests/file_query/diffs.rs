use super::*;
use acyclic_harness::client::{DiffLimits, DiffView};
use proptest::{prelude::*, test_runner::TestRunner};

const LIMITS: DiffLimits = DiffLimits {
    files: 16,
    changes: 16,
    file_bytes: 4096,
    render_bytes: 65_536,
    work: 1_048_576,
};

#[test]
fn large_equal_length_edits_keep_other_rows_inside_the_query_budget()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(8));
    runner.run(&(0_usize..512, any::<usize>()), |(extra, position)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            let fixture = Fixture::counted().await?;
            let baseline = "a".repeat(262_144 + extra);
            let mut edited = baseline.clone();
            let position = position % edited.len();
            edited.replace_range(position..position + 1, "b");
            fixture
                .write(&[
                    ("large-a", Some(&baseline)),
                    ("large-b", Some(&baseline)),
                    ("small", Some("before")),
                ])
                .await?;
            let cancel = acyclic_fs::CancellationToken::new();
            let workspace = fixture
                .filesystem
                .open_workspace(
                    fixture.volume.storage_name(&mut None)?,
                    BOUNDARY_BUDGET,
                    &cancel,
                )
                .await?
                .value;
            let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
            let budget = acyclic_fs::WorkBudget {
                bytes_hashed: 131_072,
                object_bytes_read: 131_072,
                ..BOUNDARY_BUDGET
            };
            let source = fixture.reader.file_query(&fixture.volume, budget).await?;
            let mut stream = source.observe_diff(base.id(), 16).await?;
            stream.next().await.ok_or("snapshot")??;
            fixture
                .write(&[
                    ("large-a", Some(&edited)),
                    ("large-b", Some(&edited)),
                    ("small", Some("after!")),
                ])
                .await?;
            fixture.reads.store(0, Ordering::SeqCst);
            let changed = stream.next().await.ok_or("edit")??;
            assert_eq!(changed.paths.len(), 3);
            assert!(changed.paths.iter().all(|path| !path.matches_base));
            assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
            let mut view = DiffView::new(fixture.volume.clone(), changed.previous.clone())?;
            view.apply(&changed, fixture.reader.as_ref(), LIMITS)
                .await?;
            assert_eq!(view.files().len(), 3);
            assert_eq!(
                view.files()["large-a"].omitted_bytes,
                Some(baseline.len() as u64)
            );
            assert!(view.files()["small"].patch.contains("+after!"));
            fixture
                .write(&[("large-a", Some(&baseline)), ("large-b", Some(&baseline))])
                .await?;
            fixture.reads.store(0, Ordering::SeqCst);
            let restored = stream.next().await.ok_or("restore")??;
            assert!(restored.paths.iter().all(|path| path.matches_base));
            assert_eq!(fixture.reads.load(Ordering::SeqCst), 0);
            view.apply(&restored, fixture.reader.as_ref(), LIMITS)
                .await?;
            assert_eq!(
                view.files().keys().map(String::as_str).collect::<Vec<_>>(),
                ["small"]
            );
            // Negative control: the former body-comparison path cannot even read
            // one endpoint under this same budget, irrespective of object caching.
            assert!(
                base.read_range("/large-a", 0, baseline.len() as u64, budget, &cancel)
                    .await
                    .is_err()
            );
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[test]
fn oversized_edits_and_restorations_match_the_baseline_without_body_reads()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner.run(&(0_usize..512, any::<usize>()), |(extra, position)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            let baseline = "a".repeat(65_537 + extra);
            let mut edited = baseline.clone();
            let position = position % edited.len();
            edited.replace_range(position..position + 1, "b");
            let fixture = Fixture::new().await?;
            fixture.write(&[("large.txt", Some(&baseline))]).await?;
            let cancel = acyclic_fs::CancellationToken::new();
            let workspace = fixture
                .filesystem
                .open_workspace(
                    fixture.volume.storage_name(&mut None)?,
                    BOUNDARY_BUDGET,
                    &cancel,
                )
                .await?
                .value;
            let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
            let mut stream = fixture.source.observe_diff(base.id(), 16).await?;
            let snapshot = stream.next().await.ok_or("snapshot")??;
            let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
            let limits = DiffLimits {
                file_bytes: 65_536,
                ..LIMITS
            };
            view.apply(&snapshot, &NoReads, limits).await?;

            fixture.write(&[("large.txt", Some(&edited))]).await?;
            let changed = stream.next().await.ok_or("edit")??;
            assert!(
                !changed
                    .paths
                    .first()
                    .ok_or("missing diff path")?
                    .matches_base,
                "length alone is insufficient"
            );
            view.apply(&changed, &NoReads, limits).await?;
            assert_eq!(
                view.files()["large.txt"].omitted_bytes,
                Some(baseline.len() as u64)
            );

            fixture.write(&[("large.txt", Some(&baseline))]).await?;
            let restored = stream.next().await.ok_or("restore")??;
            assert!(
                restored
                    .paths
                    .first()
                    .ok_or("missing diff path")?
                    .matches_base
            );
            let encoded = serde_json::to_vec(&restored)?;
            assert_eq!(
                serde_json::from_slice::<acyclic_harness::client::DiffPublication>(&encoded)?,
                restored
            );
            // Negative control: discard the authorized equality fact, as the
            // old size-only shortcut did, and the real reducer retains its row.
            let mut broken = view.clone();
            let mut size_only = restored.clone();
            size_only
                .paths
                .first_mut()
                .ok_or("missing negative-control path")?
                .matches_base = false;
            broken.apply(&size_only, &NoReads, limits).await?;
            assert!(!broken.files().is_empty());

            assert_eq!(
                view.apply(&restored, &NoReads, limits).await?,
                ["large.txt"]
            );
            assert!(view.files().is_empty());
            assert_eq!(view.line_counts(), (0, 0));
            let mut reopened = fixture.source.observe_diff(base.id(), 16).await?;
            let snapshot = reopened.next().await.ok_or("reopen")??;
            let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
            replay.apply(&snapshot, &NoReads, limits).await?;
            assert_eq!(replay.files(), view.files());
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[test]
fn diff_query_omits_large_bodies_and_follows_resize_then_delete()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner.run(&(16_u64..64, 1_usize..64), |(bound, extra)| {
        let result: std::result::Result<(), Box<dyn std::error::Error>> = runtime.block_on(async {
            for baseline in [None, Some("baseline\n")] {
                let fixture = Fixture::new().await?;
                if baseline.is_some() {
                    fixture.write(&[("note.txt", baseline)]).await?;
                }
                let cancel = acyclic_fs::CancellationToken::new();
                let workspace = fixture
                    .filesystem
                    .open_workspace(
                        fixture.volume.storage_name(&mut None)?,
                        BOUNDARY_BUDGET,
                        &cancel,
                    )
                    .await?
                    .value;
                let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
                let mut stream = fixture.source.observe_diff(base.id(), 16).await?;
                let snapshot = stream.next().await.ok_or("snapshot")??;
                let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                let limits = DiffLimits {
                    file_bytes: bound,
                    ..LIMITS
                };
                view.apply(&snapshot, &NoReads, limits).await?;
                let large = "x".repeat(usize::try_from(bound)? + extra);
                fixture.write(&[("note.txt", Some(large.as_str()))]).await?;
                let added = stream.next().await.ok_or("addition")??;
                assert_eq!(
                    added
                        .paths
                        .first()
                        .ok_or("missing diff path")?
                        .largest_file_bytes,
                    Some(large.len() as u64)
                );
                view.apply(&added, &NoReads, limits).await?;
                assert_eq!(
                    view.files()["note.txt"].omitted_bytes,
                    Some(large.len() as u64)
                );
                assert!(view.files()["note.txt"].patch.is_empty());
                assert_eq!(view.line_counts(), (0, 0));
                fixture.write(&[("note.txt", Some("small\n"))]).await?;
                let resized = stream.next().await.ok_or("resize")??;
                view.apply(&resized, fixture.reader.as_ref(), limits)
                    .await?;
                assert_eq!(view.files()["note.txt"].omitted_bytes, None);
                fixture.write(&[("note.txt", None)]).await?;
                let deleted = stream.next().await.ok_or("deletion")??;
                assert_eq!(deleted.paths.len(), 1);
                assert!(
                    deleted
                        .paths
                        .first()
                        .ok_or("missing diff path")?
                        .current
                        .is_none()
                );
                let changed = view
                    .apply(&deleted, fixture.reader.as_ref(), limits)
                    .await?;
                assert_eq!(changed, ["note.txt"]);
                if baseline.is_none() {
                    assert!(
                        deleted
                            .paths
                            .first()
                            .ok_or("missing diff path")?
                            .base
                            .is_none()
                    );
                    assert!(view.files().is_empty());
                    assert_eq!(view.line_counts(), (0, 0));
                }
                let mut reopened = fixture.source.observe_diff(base.id(), 16).await?;
                let snapshot = reopened.next().await.ok_or("reopen")??;
                let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                replay
                    .apply(&snapshot, fixture.reader.as_ref(), limits)
                    .await?;
                assert_eq!(replay.files(), view.files());
            }
            Ok(())
        });
        result.map_err(|error| TestCaseError::fail(error.to_string()))
    })?;
    Ok(())
}

#[test]
fn diff_query_snapshots_and_follow_reconstruct_every_endpoint()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut runner = TestRunner::new(proptest::test_runner::Config::with_cases(32));
    runner.run(
        &prop::collection::vec("[a-zλ雪\\n]{0,32}", 1..6),
        |texts| {
            let result: std::result::Result<(), Box<dyn std::error::Error>> =
                runtime.block_on(async {
                    for baseline_text in [None, Some("baseline\n")] {
                        let fixture = Fixture::new().await?;
                        if baseline_text.is_some() {
                            fixture.write(&[("note.txt", baseline_text)]).await?;
                        }
                        let cancellation = acyclic_fs::CancellationToken::new();
                        let workspace = fixture
                            .filesystem
                            .open_workspace(
                                fixture.volume.storage_name(&mut None)?,
                                BOUNDARY_BUDGET,
                                &cancellation,
                            )
                            .await?
                            .value;
                        let baseline = workspace.head(BOUNDARY_BUDGET, &cancellation).await?.value;
                        let mut stream = fixture.source.observe_diff(baseline.id(), 16).await?;
                        let first = stream.next().await.ok_or("missing snapshot")??;
                        assert!(first.paths.is_empty());
                        let mut view =
                            DiffView::new(fixture.volume.clone(), first.previous.clone())?;
                        view.apply(&first, fixture.reader.as_ref(), LIMITS).await?;
                        let values = texts
                            .iter()
                            .map(|text| Some(text.as_str()))
                            .chain(std::iter::once(None))
                            .chain(baseline_text.map(Some));
                        let mut current_text = baseline_text;
                        for text in values {
                            if text == current_text {
                                let error = fixture.write(&[("note.txt", text)]).await.unwrap_err();
                                assert!(error.to_string().contains("no pending mutation"));
                                assert!(stream.next().now_or_never().is_none());
                                continue;
                            }
                            fixture.write(&[("note.txt", text)]).await?;
                            current_text = text;
                            let publication = stream.next().await.ok_or("missing delta")??;
                            assert_eq!(&publication.previous, view.generation());
                            let bytes = serde_json::to_vec(&publication)?;
                            let decoded = serde_json::from_slice(&bytes)?;
                            assert_eq!(publication, decoded);
                            view.apply(&publication, fixture.reader.as_ref(), LIMITS)
                                .await?;
                            if text == baseline_text {
                                assert!(view.files().is_empty());
                            } else {
                                let patch =
                                    diffy::Patch::from_str(&view.files()["note.txt"].patch)?;
                                assert_eq!(
                                    diffy::apply(baseline_text.unwrap_or_default(), &patch)?,
                                    text.unwrap_or_default()
                                );
                            }
                            let mut reopened =
                                fixture.source.observe_diff(baseline.id(), 16).await?;
                            let snapshot =
                                reopened.next().await.ok_or("reopened snapshot missing")??;
                            let mut replay =
                                DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
                            replay
                                .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
                                .await?;
                            assert_eq!(replay.files(), view.files());
                        }
                    }
                    Ok(())
                });
            result.map_err(|error| TestCaseError::fail(error.to_string()))
        },
    )?;
    Ok(())
}

#[tokio::test]
async fn diff_query_rejects_truncation_instead_of_publishing_partial_paths()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    fixture
        .write(&[("a", Some("base")), ("b", Some("base"))])
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &cancellation,
        )
        .await?
        .value;
    let base = workspace.head(BOUNDARY_BUDGET, &cancellation).await?.value;
    fixture
        .write(&[("a", Some("changed")), ("b", Some("changed"))])
        .await?;
    assert!(fixture.source.observe_diff(base.id(), 1).await.is_err());
    assert!(fixture.source.observe_diff(base.id(), 0).await.is_err());
    Ok(())
}

#[tokio::test]
async fn native_range_restoration_removes_oversized_row_on_follow_and_reopen()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let fixture = Fixture::new().await?;
    let baseline = "a".repeat(131_072);
    fixture.write(&[("large.txt", Some(&baseline))]).await?;
    let cancel = acyclic_fs::CancellationToken::new();
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &cancel,
        )
        .await?
        .value;
    let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
    let mut stream = fixture.source.observe_diff(base.id(), 16).await?;
    let snapshot = stream.next().await.ok_or("snapshot")??;
    let mut view = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    let limits = DiffLimits {
        file_bytes: 65_536,
        ..LIMITS
    };
    view.apply(&snapshot, &NoReads, limits).await?;
    for replacement in *b"ba" {
        let mut transaction = workspace
            .begin_transaction(
                acyclic_fs::workspace::IdempotencyKey::new(),
                BOUNDARY_BUDGET,
                &cancel,
            )
            .await?
            .value;
        transaction
            .write_range(
                "/large.txt",
                65_537,
                vec![replacement],
                BOUNDARY_BUDGET,
                &cancel,
            )
            .await?;
        transaction
            .commit::<acyclic_fs::workspace::WorkspaceError, _, _>(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                BOUNDARY_BUDGET,
                &cancel,
                |_| {
                    std::future::ready(Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?;
        let publication = stream.next().await.ok_or("range publication")??;
        assert_eq!(
            publication
                .paths
                .first()
                .ok_or("missing diff path")?
                .matches_base,
            replacement == b'a'
        );
        view.apply(&publication, &NoReads, limits).await?;
        if replacement == b'b' {
            // A real change remains visible despite identical file sizes.
            assert_eq!(view.files()["large.txt"].omitted_bytes, Some(131_072));
        } else {
            assert!(view.files().is_empty());
            let mut reopened = fixture.source.observe_diff(base.id(), 16).await?;
            let snapshot = reopened.next().await.ok_or("reopen")??;
            let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
            replay.apply(&snapshot, &NoReads, limits).await?;
            assert!(replay.files().is_empty());
        }
    }
    Ok(())
}

#[tokio::test]
async fn bounded_restoration_comparison_preserves_other_rows_and_reopen()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new().await?;
    let baseline = "a".repeat(131_072);
    fixture
        .write(&[("a-large", Some(&baseline)), ("z-small", Some("before"))])
        .await?;
    let cancel = acyclic_fs::CancellationToken::new();
    let workspace = fixture
        .filesystem
        .open_workspace(
            fixture.volume.storage_name(&mut None)?,
            BOUNDARY_BUDGET,
            &cancel,
        )
        .await?
        .value;
    let base = workspace.head(BOUNDARY_BUDGET, &cancel).await?.value;
    let budget = acyclic_fs::WorkBudget {
        source_bytes_read: 65_536,
        ..BOUNDARY_BUDGET
    };
    let source = fixture.reader.file_query(&fixture.volume, budget).await?;
    let mut stream = source.observe_diff(base.id(), 16).await?;
    let initial = stream.next().await.ok_or("snapshot")??;
    let mut view = DiffView::new(fixture.volume.clone(), initial.previous.clone())?;
    view.apply(&initial, fixture.reader.as_ref(), LIMITS)
        .await?;
    let mut transaction = workspace
        .begin_transaction(
            acyclic_fs::workspace::IdempotencyKey::new(),
            BOUNDARY_BUDGET,
            &cancel,
        )
        .await?
        .value;
    transaction
        .write_range(
            "/a-large",
            1,
            vec![b'a'; baseline.len() - 2],
            BOUNDARY_BUDGET,
            &cancel,
        )
        .await?;
    transaction.write_text("/z-small", "after!").await?;
    let current = transaction
        .commit::<acyclic_fs::workspace::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            BOUNDARY_BUDGET,
            &cancel,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    let acyclic_fs::workspace::TransactionCommit::Committed(current) = current else {
        return Err("range rewrite did not commit".into());
    };
    // Negative control: the former mandatory comparison cannot finish under
    // this same query allowance. The optional hint cannot own publication.
    let failed = current
        .matches_content(&base, "/a-large", budget, &cancel)
        .await;
    assert!(matches!(
        failed,
        Err(acyclic_fs::OperationFailure {
            error: acyclic_fs::WorkspaceError::Work(acyclic_fs::WorkError::BudgetExceeded { .. }),
            ..
        })
    ));
    let changed = stream.next().await.ok_or("range publication")??;
    assert_eq!(changed.paths.len(), 2);
    assert!(
        !changed
            .paths
            .iter()
            .find(|p| p.path == "a-large")
            .ok_or("large path")?
            .matches_base
    );
    view.apply(&changed, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert_eq!(
        view.files()["a-large"].omitted_bytes,
        Some(baseline.len() as u64)
    );
    assert!(view.files()["z-small"].patch.contains("+after!"));
    let mut reopened = source.observe_diff(base.id(), 16).await?;
    let snapshot = reopened.next().await.ok_or("reopened snapshot")??;
    let mut replay = DiffView::new(fixture.volume.clone(), snapshot.previous.clone())?;
    replay
        .apply(&snapshot, fixture.reader.as_ref(), LIMITS)
        .await?;
    assert_eq!(view.files(), replay.files());
    Ok(())
}
