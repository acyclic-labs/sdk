//! Every durable reconciliation cut recovers before checkout capture.
use super::super::checkout_tools::reconciliation;
use super::*;
use acyclic_fs::{CancellationToken, GitCompatStore, WorkBudget};
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn open(path: &Path, state: &Path) -> Result<LocalHarness> {
    LocalHarness::open(super::checkout_config(path, (model()?, text()))?.state_directory(state))
        .await
}

async fn capture(harness: &LocalHarness) -> Result<()> {
    let workspace = harness.workspace().unwrap();
    let tools = Box::pin(super::super::checkout_tools::CheckoutTools::new(
        Arc::clone(workspace),
        harness.project(),
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        Arc::clone(harness.task_files()),
    ))
    .await?;
    let mut state = super::super::workspace::lock_owned(super::super::workspace::StateArc::clone(
        &workspace.state,
    ))
    .await;
    let super::super::checkout_tools::State {
        active, watcher, ..
    } = &mut *state;
    let view = active.as_ref().unwrap_or_else(|| workspace.view());
    Box::pin(tools.capture(
        (workspace, view),
        watcher,
        OperationId::new(),
        0,
        acyclic_fs::PublicationPermit::Unrestricted,
        None,
    ))
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
        expected.as_bytes(),
        "unpublished rebase"
    );
    let selected = checkout.selected_view().await?.0.unwrap();
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
    // Use the real pre-command capture directly, without a model turn or
    // process per cut. Native projection can change unassignable timestamps;
    // file identity and the entire immutable payload must remain unchanged.
    let before = head
        .stat(
            "/upstream.txt",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(super::super::storage)?
        .value;
    capture(harness).await?;
    let after = generation(&selected).await?;
    assert_eq!(
        after
            .stat(
                "/upstream.txt",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(super::super::storage)?
            .value
            .file_id,
        before.file_id
    );
    assert_eq!(
        after
            .read_range_if_unchanged(
                &head,
                "/upstream.txt",
                acyclic_fs::ByteRange {
                    offset: 0,
                    length: expected.len() as u64,
                },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(super::super::storage)?
            .value
            .bytes
            .as_ref(),
        expected.as_bytes()
    );
    Ok(())
}

async fn sweep(text: &str) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state_directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf base > upstream.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        ScriptedTurn::Text("done".into()),
    ]);
    let mut harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state_directory.path()),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    // Learn the finite native effect boundaries on the real publication path.
    // Later attempts change only this same file, with identical tree structure.
    let mut effects = 0;
    let mut cut = 0;
    loop {
        let expected = format!("{text}-{cut}");
        let checkout = harness.workspace().unwrap();
        let selected = checkout.selected_view().await?.0.unwrap();
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
        // Reconstruct the exact production binding from durable selection and
        // reconciliation records; retain unrelated scratch mounts and task I/O.
        // No native projection or model needs restarting at these cuts.
        {
            let mut state = super::super::workspace::lock_owned(
                super::super::workspace::StateArc::clone(&checkout.state),
            )
            .await;
            state.active = None;
            state.watcher = None;
            state.initialized = false;
        }
        Box::pin(super::super::checkout_tools::CheckoutTools::new(
            Arc::clone(checkout),
            harness.project(),
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            Arc::clone(harness.task_files()),
        ))
        .await?;
        assert_published(&harness, &expected).await?;
        if cut == 5 + effects {
            // One complete host reopen additionally checks binding and mount
            // reconstruction after every durable interruption was recovered.
            drop(harness);
            harness = open(directory.path(), state_directory.path()).await?;
            assert_published(&harness, &expected).await?;
            break;
        }
        cut += 1;
    }
    Ok(())
}

async fn negative_control(text: &str) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let state_directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([
        super::checked_commands::call(0, "bash", "printf base > upstream.txt"),
        super::checked_commands::call(1, "fs", "commit -m base"),
        super::checked_commands::call(2, "fs", "checkout -b agents/child"),
        ScriptedTurn::Text("done".into()),
    ]);
    let harness = LocalHarness::open(
        super::checkout_config(directory.path(), (model()?, provider))?
            .state_directory(state_directory.path()),
    )
    .await?;
    super::checkout_reopen::run(&harness).await?;
    let checkout = harness.workspace().unwrap();
    let selected = checkout.selected_view().await?.0.unwrap();
    let from = generation(&selected).await?;
    let windows =
        acyclic_fs::OperationWindowCoordinator::new(checkout.filesystem.operation_window_store());
    let lease = windows
        .begin(
            selected.workspace().id(),
            from.id(),
            "negative-control",
            0,
            10,
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
    let finished = windows
        .finish_workspace(
            selected.workspace(),
            &lease,
            2,
            acyclic_fs::OperationReconcileLimits::default(),
        )
        .await
        .map_err(super::super::storage)?;
    assert!(matches!(
        finished,
        acyclic_fs::WorkspaceOperationFinish::Reconciled(
            acyclic_fs::WorkspaceRebase::Rebased(_)
                | acyclic_fs::WorkspaceRebase::AlreadyRebased(_)
                | acyclic_fs::WorkspaceRebase::Current(_)
        )
    ));
    // Establish the healthy pre-fault cut: the SDK head has rebased, while
    // physical publication is still pending. The mutant below removes its fence.
    assert_eq!(
        generation(&selected)
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
    assert_eq!(
        std::fs::read(checkout.path.join("upstream.txt")).map_err(super::super::storage)?,
        b"base"
    );
    let root = checkout.view().workspace().id();
    let mut state = GitCompatStore::load(&checkout.store, root)
        .await
        .map_err(super::super::storage)?
        .unwrap();
    assert!(state.checkout_reconciliation.is_some());
    let revision = state.revision;
    state.revision += 1;
    state.checkout_reconciliation = None; // deliberately broken: acknowledge before publishing
    assert!(
        GitCompatStore::compare_and_swap(&checkout.store, root, revision, state)
            .await
            .map_err(super::super::storage)?
    );
    drop(harness);
    let harness = open(directory.path(), state_directory.path()).await?;
    // Acknowledging before publication permits capture over the rebased SDK
    // head. The same complete crash invariant used by the positive sweep must
    // detect this deliberately broken variant; preservation cannot hold here.
    let failure = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(assert_published(
        &harness, &expected,
    )))
    .await
    .expect_err("the crash invariant missed premature acknowledgement");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("unpublished rebase"),
        "unexpected invariant failure: {message}"
    );
    Ok(())
}

#[tokio::test]
async fn premature_publication_acknowledgement_is_detected() -> Result<()> {
    negative_control("upstream").await
}

#[test]
fn reopen_never_captures_over_an_unpublished_rebase() {
    // Preserve the same four generated/shrinkable byte cases and every cut;
    // run independent cases concurrently instead of serial model/process turns.
    std::thread::scope(|scope| {
        let cases: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
                        source_file: Some(file!()),
                        ..ProptestConfig::with_cases(1)
                    });
                    runner
                        .run(&"[a-z]{1,12}", |text| {
                            runtime
                                .block_on(sweep(&text))
                                .map_err(|error| TestCaseError::fail(error.to_string()))
                        })
                        .unwrap();
                })
            })
            .collect();
        for case in cases {
            case.join().unwrap();
        }
    });
}
