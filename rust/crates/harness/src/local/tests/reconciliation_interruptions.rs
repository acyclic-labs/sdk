//! Every durable reconciliation cut recovers before checkout capture.
use super::super::checkout_tools::reconciliation;
use super::*;
use acyclic_fs::{CancellationToken, GitCompatStore, WorkBudget, facade::FsWork as _};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn open(path: &Path, expected: &str) -> Result<LocalHarness> {
    let provider = scripted([
        super::checked_commands::call(0, "bash", &format!("test $(cat upstream.txt) = {expected}")),
        ScriptedTurn::Text("done".into()),
    ]);
    LocalHarness::open(crate::test_support::local_harness_config(
        path,
        (model()?, provider),
    )?)
    .await
}

async fn generation(
    view: &super::super::workspace::View,
) -> Result<acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>>
{
    Ok(view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(super::super::storage)?
        .value)
}

async fn assert_published(harness: &LocalHarness, expected: &str) -> Result<()> {
    let checkout = harness.workspace().unwrap();
    assert_eq!(
        std::fs::read(checkout.path.join("upstream.txt")).map_err(super::super::storage)?,
        expected.as_bytes()
    );
    let selected = checkout.selected_view().await?.unwrap();
    let head = generation(&selected).await?;
    assert_eq!(
        head.read(
            "/upstream.txt",
            128,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new()
        )
        .await
        .map_err(super::super::storage)?
        .value
        .as_ref(),
        expected.as_bytes()
    );
    // This runs the stock bash binding, including its pre-command baseline
    // capture. A stale disk must never replace the already rebased generation.
    super::checkout_reopen::run(harness).await?;
    assert_eq!(
        generation(&selected).await?.id(),
        head.id(),
        "capture changed the rebased generation"
    );
    Ok(())
}

async fn sweep(text: &str) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf base > upstream.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        ScriptedTurn::Text("done".into()),
    ]);
    let mut harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider),
    )?)
    .await?;
    super::checkout_reopen::run(&harness).await?;
    // Learn the finite native effect boundaries on the real publication path.
    // Later attempts change only this same file, with identical tree structure.
    let mut effects = 0;
    let mut cut = 0;
    loop {
        let expected = format!("{text}-{cut}");
        let checkout = harness.workspace().unwrap();
        let selected = checkout.selected_view().await?.unwrap();
        let from = generation(&selected).await?;
        let windows = acyclic_fs::OperationWindowCoordinator::new(
            checkout.filesystem.operation_window_store(),
        );
        windows
            .begin(selected.workspace().id(), from.id(), "interrupted", 0, 1)
            .await
            .map_err(super::super::storage)?;
        checkout
            .view()
            .workspace()
            .write_text("/upstream.txt", &expected)
            .await
            .map_err(super::super::storage)?;
        reconciliation::prepare(
            checkout,
            acyclic_fs::OperationId::new(),
            acyclic_fs::GitGenerationRef {
                workspace_id: selected.workspace().id(),
                generation: from.id(),
            },
        )
        .await?;
        windows
            .observe_parent(
                selected.workspace().id(),
                generation(checkout.view()).await?.id(),
            )
            .await
            .map_err(super::super::storage)?;
        // Cut 0 measures a complete run. Cuts 1..=4 stop after the source
        // marker, ticket claim, rebase commit, and ticket acknowledgement.
        let ticket = if cut == 0 || cut >= 2 {
            Some(
                windows
                    .claim_reconcile(selected.workspace().id(), 2)
                    .await
                    .map_err(super::super::storage)?
                    .unwrap(),
            )
        } else {
            None
        };
        if cut == 0 || cut >= 3 {
            let ticket = ticket.as_ref().unwrap();
            let limits = acyclic_fs::OperationReconcileLimits::default();
            selected
                .workspace()
                .live_rebase(
                    acyclic_fs::IdempotencyKey::from_bytes(ticket.ticket.into_bytes()),
                    limits.maximum_generations,
                    limits.maximum_changes,
                    limits.maximum_conflicts,
                )
                .await
                .map_err(super::super::storage)?;
        }
        if cut == 0 || cut >= 4 {
            windows
                .complete_reconcile(selected.workspace().id(), ticket.as_ref().unwrap().ticket)
                .await
                .map_err(super::super::storage)?;
        }
        if cut == 0 || cut >= 5 {
            let count = AtomicUsize::new(0);
            let result = reconciliation::recover(checkout, &selected, &|| {
                let index = count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if cut != 0 && index == cut - 5 {
                        Err(Error::Conflict("injected publication interruption".into()))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
            if cut == 0 {
                effects = count.load(Ordering::SeqCst);
                result?;
            } else if cut - 5 < effects {
                assert!(result.is_err(), "publication ignored interruption {cut}");
                assert_eq!(
                    count.load(Ordering::SeqCst),
                    cut - 4,
                    "publication stopped before the selected effect boundary"
                );
            } else {
                result?;
            }
        }
        drop(harness);
        harness = open(directory.path(), &expected).await?;
        assert_published(&harness, &expected).await?;
        if cut == 5 + effects {
            break;
        }
        cut += 1;
    }
    Ok(())
}

async fn negative_control(text: &str) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf base > upstream.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model()?, provider),
    )?)
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let checkout = harness.workspace().unwrap();
    let selected = checkout.selected_view().await?.unwrap();
    let from = generation(&selected).await?;
    let windows =
        acyclic_fs::OperationWindowCoordinator::new(checkout.filesystem.operation_window_store());
    let lease = windows
        .begin(
            selected.workspace().id(),
            from.id(),
            "negative-control",
            0,
            1,
        )
        .await
        .map_err(super::super::storage)?;
    let expected = format!("{text}-broken");
    checkout
        .view()
        .workspace()
        .write_text("/upstream.txt", &expected)
        .await
        .map_err(super::super::storage)?;
    reconciliation::prepare(
        checkout,
        acyclic_fs::OperationId::new(),
        acyclic_fs::GitGenerationRef {
            workspace_id: selected.workspace().id(),
            generation: from.id(),
        },
    )
    .await?;
    windows
        .finish_workspace(
            selected.workspace(),
            &lease,
            2,
            acyclic_fs::OperationReconcileLimits::default(),
        )
        .await
        .map_err(super::super::storage)?;
    let root = checkout.view().workspace().id();
    let mut state = GitCompatStore::load(&checkout.store, root)
        .await
        .map_err(super::super::storage)?
        .unwrap();
    let revision = state.revision;
    state.revision += 1;
    state.checkout_reconciliation = None; // deliberately broken: acknowledge before publishing
    assert!(
        GitCompatStore::compare_and_swap(&checkout.store, root, revision, state)
            .await
            .map_err(super::super::storage)?
    );
    drop(harness);
    let harness = open(directory.path(), &expected).await?;
    let bytes =
        std::fs::read(directory.path().join("upstream.txt")).map_err(super::super::storage)?;
    let failure =
        std::panic::catch_unwind(|| assert_eq!(bytes, expected.as_bytes(), "unpublished rebase"));
    assert!(
        failure.is_err(),
        "the crash invariant missed premature acknowledgement"
    );
    // No capture is allowed in this deliberately corrupted case.
    assert_eq!(
        generation(&harness.workspace().unwrap().selected_view().await?.unwrap())
            .await?
            .read(
                "/upstream.txt",
                128,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(super::super::storage)?
            .value
            .as_ref(),
        expected.as_bytes()
    );
    Ok(())
}

#[tokio::test]
async fn premature_publication_acknowledgement_is_detected() -> Result<()> {
    negative_control("upstream").await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    // For arbitrary authored bytes, exhaust every durable protocol boundary
    // and every native publication guard, then reopen and capture through the
    // actual LocalHarness. No sleep, scheduler continuation or mirror frame.
    #[test]
    fn reopen_never_captures_over_an_unpublished_rebase(text in "[a-z]{1,12}") {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(sweep(&text)).unwrap();
    }
}
