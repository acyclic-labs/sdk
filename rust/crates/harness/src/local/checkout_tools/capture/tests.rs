//! Inject lost continuity at the actual native capture boundaries.
use super::*;
use acyclic_fs::{ByteRange, WatchInvalidationReason};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct Injection {
    stage: &'static str,
    remaining: usize,
    delivered: Arc<AtomicUsize>,
    path: PathBuf,
}

impl Cursor {
    pub(super) fn inject(&mut self, stage: &str) -> Result<()> {
        inject(&mut self.injection, &mut self.watcher, stage)
    }
}

pub(super) fn inject(
    injection: &mut Option<Injection>,
    watcher: &mut NativeWatch,
    stage: &str,
) -> Result<()> {
    if let Some(injection) = injection
        && injection.stage == stage
        && injection.remaining != 0
    {
        injection.remaining -= 1;
        injection.delivered.fetch_add(1, Ordering::Relaxed);
        std::fs::write(&injection.path, b"after").map_err(storage)?;
        // A zero-deadline fence deterministically marks native continuity
        // unconfirmed, the same invalidation delivered by a lost OS batch.
        watcher.fence(Duration::ZERO).map_err(storage)?;
    }
    Ok(())
}

async fn fixture() -> Result<(tempfile::TempDir, LocalHarness, CheckoutTools)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    std::fs::write(directory.path().join("marker"), b"before").map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        crate::test_support::local_harness_config(
            directory.path(),
            (
                model,
                Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ),
        )?
        .without_default_tools(),
    )
    .await?;
    let physical = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("workspace".into()))?;
    let tools = CheckoutTools::new(
        physical.clone(),
        &harness.project,
        Arc::new(acyclic_stream::SystemUnixMillisClock),
        &harness.signed,
        harness.task_files.clone(),
    )
    .await?;
    Ok((directory, harness, tools))
}

async fn contents(tools: &CheckoutTools) -> Result<Vec<u8>> {
    let cancellation = CancellationToken::new();
    let workspace = tools.workspace.view().workspace();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    let config = workspace.volume_config();
    let path = acyclic_fs::native_capture::host_path_to_namespace(
        Path::new("marker"),
        config.profile,
        config.limits,
    )
    .map_err(storage)?;
    Ok(checkout
        .read_file_range(
            &path,
            ByteRange {
                offset: 0,
                length: 5,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value
        .bytes
        .to_vec())
}

async fn run(tools: &CheckoutTools, slot: &mut Option<Cursor>) -> Result<()> {
    tools
        .capture(
            (&tools.workspace, tools.workspace.view()),
            slot,
            OperationId::new(),
            0,
            PublicationPermit::Unrestricted,
            None,
        )
        .await
}

async fn recovers(stage: &'static str) -> Result<()> {
    let (directory, harness, tools) = fixture().await?;
    let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
    // Exercise a real full scan for its boundaries; fence/poll also exercise
    // the ordinary persistent-cursor path on Linux and Windows.
    if matches!(stage, "begin" | "watch-tree" | "baseline" | "finish") {
        cursor.initial = Some(WatchBatch::RescanRequired {
            epoch: 1,
            reason: WatchInvalidationReason::NativeRescanRequired,
        });
    }
    cursor.injection = Some(Injection {
        stage,
        remaining: 1,
        delivered: Arc::new(AtomicUsize::new(0)),
        path: directory.path().join("marker"),
    });
    let mut slot = Some(cursor);
    run(&tools, &mut slot).await?;
    assert_eq!(
        slot.as_ref()
            .and_then(|cursor| cursor.injection.as_ref())
            .map(|injection| injection.delivered.load(Ordering::Relaxed)),
        Some(1)
    );
    assert_eq!(contents(&tools).await?, b"after");
    // The next command must remain usable with the persistent watcher.
    run(&tools, &mut slot).await?;
    assert_eq!(contents(&tools).await?, b"after");
    drop(slot);
    drop(tools);
    harness.close().await
}

#[tokio::test]
async fn unconfirmed_fence_converges() -> Result<()> {
    recovers("fence").await
}
#[tokio::test]
async fn invalidated_poll_converges() -> Result<()> {
    recovers("poll").await
}
#[tokio::test]
async fn invalidated_begin_converges() -> Result<()> {
    recovers("begin").await
}
#[tokio::test]
async fn invalidated_watch_tree_converges() -> Result<()> {
    recovers("watch-tree").await
}
#[tokio::test]
async fn invalidated_baseline_converges() -> Result<()> {
    recovers("baseline").await
}
#[tokio::test]
async fn invalidated_finish_converges() -> Result<()> {
    recovers("finish").await
}
#[tokio::test]
async fn invalidated_registration_converges() -> Result<()> {
    recovers("register").await
}
#[tokio::test]
async fn invalidated_replay_converges() -> Result<()> {
    recovers("replay").await
}

#[tokio::test]
async fn repeated_invalidation_is_bounded_and_never_publishes_a_candidate() -> Result<()> {
    let (directory, harness, tools) = fixture().await?;
    let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
    cursor.initial = Some(WatchBatch::RescanRequired {
        epoch: 1,
        reason: WatchInvalidationReason::QueueOverflow,
    });
    let delivered = Arc::new(AtomicUsize::new(0));
    cursor.injection = Some(Injection {
        stage: "baseline",
        remaining: usize::MAX,
        delivered: delivered.clone(),
        path: directory.path().join("marker"),
    });
    let mut slot = Some(cursor);
    let generation = tools
        .workspace
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(storage)?
        .value
        .id();
    assert!(matches!(
        run(&tools, &mut slot).await,
        Err(Error::Conflict(_))
    ));
    assert!(slot.is_none(), "a failed cursor must be discarded");
    assert_eq!(delivered.load(Ordering::Relaxed), converge::MAXIMUM_RESCANS);
    assert_eq!(
        tools
            .workspace
            .view()
            .workspace()
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(storage)?
            .value
            .id(),
        generation
    );
    run(&tools, &mut slot).await?;
    assert_eq!(contents(&tools).await?, b"after");
    drop(slot);
    drop(tools);
    harness.close().await
}

#[tokio::test]
async fn overflow_and_unconfirmed_initial_batches_rebaseline() -> Result<()> {
    let (directory, harness, tools) = fixture().await?;
    for reason in [
        WatchInvalidationReason::NativeRescanRequired,
        WatchInvalidationReason::QueueOverflow,
        WatchInvalidationReason::AmbiguousRename,
    ] {
        let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
        cursor.initial = Some(WatchBatch::RescanRequired { epoch: 7, reason });
        std::fs::write(directory.path().join("marker"), b"after").map_err(storage)?;
        let mut slot = Some(cursor);
        run(&tools, &mut slot).await?;
        assert_eq!(contents(&tools).await?, b"after");
    }
    drop(tools);
    harness.close().await
}

#[tokio::test]
async fn root_invalidation_is_never_retried_as_overflow() -> Result<()> {
    let (_directory, harness, tools) = fixture().await?;
    let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
    cursor.initial = Some(WatchBatch::RescanRequired {
        epoch: 7,
        reason: WatchInvalidationReason::RootChanged,
    });
    let mut slot = Some(cursor);
    assert!(matches!(
        run(&tools, &mut slot).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(slot.is_none());
    drop(tools);
    harness.close().await
}

#[tokio::test]
async fn replay_rescan_requirement_is_a_transition() -> Result<()> {
    let (directory, harness, tools) = fixture().await?;
    std::fs::hard_link(
        directory.path().join("marker"),
        directory.path().join("alias"),
    )
    .map_err(storage)?;
    let cancellation = CancellationToken::new();
    let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
    let workspace = tools.workspace.view().workspace();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    let config = workspace.volume_config();
    let path = acyclic_fs::native_capture::host_path_to_namespace(
        Path::new("marker"),
        config.profile,
        config.limits,
    )
    .map_err(storage)?;
    let batch = WatchBatch::Changes {
        epoch: 1,
        first_sequence: 0,
        next_sequence: 1,
        changes: vec![acyclic_fs::WatchChange::Modified(path)],
        original: Default::default(),
    };
    // Replaying one alias cannot establish all hard-link identities. The
    // capture layer returns a typed RescanRequired, independently of the OS.
    assert!(
        !converge::replay(
            &mut cursor,
            &mut checkout,
            batch,
            &tools.workspace,
            (&tools.capture_options, &tools.capture_policy),
            &cancellation,
        )
        .await?
    );
    assert!(!checkout.has_pending_mutations());
    cursor.initial = Some(WatchBatch::RescanRequired {
        epoch: 1,
        reason: WatchInvalidationReason::NativeRescanRequired,
    });
    let mut slot = Some(cursor);
    run(&tools, &mut slot).await?;
    drop(slot);
    drop(tools);
    harness.close().await
}

#[tokio::test]
async fn rescan_preserves_the_original_publication_fence() -> Result<()> {
    let (directory, harness, tools) = fixture().await?;
    let cancellation = CancellationToken::new();
    let workspace = tools.workspace.view().workspace();
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    let original = checkout.generation_id();
    // A different capture commits after admission of the original candidate.
    std::fs::write(directory.path().join("marker"), b"other").map_err(storage)?;
    let mut other = None;
    run(&tools, &mut other).await?;
    let winner = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    assert_ne!(original, winner);
    std::fs::write(directory.path().join("marker"), b"after").map_err(storage)?;
    let mut cursor = Cursor::initial(&tools.workspace, &tools.capture_policy)?;
    converge::capture(
        &mut cursor,
        &mut checkout,
        &tools.workspace,
        (&tools.capture_options, &tools.capture_policy),
        true,
        &cancellation,
    )
    .await?;
    let outcome = checkout
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map_err(storage)?
        .value;
    assert!(!matches!(
        outcome,
        LivePublicationObservation::Committed { .. }
            | LivePublicationObservation::AlreadyCommitted { .. }
    ));
    assert_eq!(
        workspace
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        winner
    );
    assert_eq!(contents(&tools).await?, b"other");
    drop((cursor, other, checkout));
    drop(tools);
    harness.close().await
}
