//! Rebinding a captured publication must reuse its authenticated content.
use super::*;

pub(super) async fn cost_fixture() -> Result<(
    tempfile::TempDir,
    tempfile::TempDir,
    crate::LocalHarness,
    CheckoutPublisher,
)> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let model = crate::model::Model::new("scripted".into(), "publication".into(), "1".into())?;
    let provider = Arc::new(crate::test_support::ScriptedModelProvider::new([]));
    let harness = crate::LocalHarness::open(
        crate::test_support::local_harness_config(directory.path(), (model, provider))?
            .state_directory(state.path()),
    )
    .await?;
    let tools = Arc::new(
        CheckoutTools::new(
            harness
                .workspace
                .as_ref()
                .ok_or_else(|| Error::NotFound("checkout".into()))?
                .clone(),
            &harness.project,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            &harness.signed,
            &harness.verifier,
            None,
        )
        .await?,
    );
    let publisher = CheckoutPublisher::new(
        tools,
        harness.files.clone(),
        &harness.project,
        &harness.verifier,
        harness.limits.file_bytes,
    );
    Ok((directory, state, harness, publisher))
}

#[tokio::test]
async fn observed_publication_does_not_reread_the_published_payload() -> Result<()> {
    let (_directory, _state, harness, publisher) = cost_fixture().await?;
    let operation = OperationId::new();
    let bytes = vec![b'x'; 1024 * 1024];
    let (captured, _) = publisher
        .stage_once(
            operation,
            "large.txt",
            &bytes,
            "text/plain",
            "large.txt",
            Default::default(),
        )?
        .await?;
    let view = publisher.tools.workspace.view();
    let writer = publisher.writer_for(&harness.project, view.workspace())?;
    let staged = writer
        .observe_staged_once(operation, None, publisher.maximum, Default::default())?
        .await?
        .0
        .ok_or_else(|| Error::NotFound("staged publication".into()))?;
    assert_ne!(
        staged.version(),
        captured.version(),
        "fixture must finish its own physical capture"
    );
    eprintln!(
        "WRITE_REBIND_IDENTITIES equal={}",
        same_payloads(&publisher, &staged, &captured).await?
    );
    let started = std::time::Instant::now();
    let original = publisher.observed(view, operation, staged.clone()).await?;
    eprintln!(
        "WRITE_REBIND bytes={} elapsed_us={}",
        bytes.len(),
        started.elapsed().as_micros()
    );
    assert_eq!(original, captured);
    let budget = acyclic_fs::WorkBudget {
        output_bytes: 64 * 1024,
        ..acyclic_fs::WorkBudget::UNBOUNDED
    };
    let bounded = publisher
        .observed_with_budget(view, operation, staged.clone(), budget)
        .await;
    assert!(
        bounded.is_ok(),
        "rebinding must authenticate metadata without rereading 1 MiB of published content: {bounded:?}"
    );
    assert_eq!(bounded?, captured);
    let old = FullRead::enter();
    let refused = publisher
        .observed_with_budget(view, operation, staged.clone(), budget)
        .await;
    let fallback = publisher.observed(view, operation, staged).await?;
    drop(old);
    let failure = refused.expect_err("old full read must exceed the logical output budget");
    let refusal = failure.to_string();
    assert!(
        refusal.contains("output_bytes") || refusal.contains("OutputBytes"),
        "{failure:?}"
    );
    assert_eq!(
        fallback, captured,
        "different payload layouts keep the original content fallback"
    );
    eprintln!("WRITE_REBIND_NEGATIVE old_read_refused={failure}");

    harness.disconnect().await?;
    Ok(())
}

async fn same_payloads(
    publisher: &CheckoutPublisher,
    staged: &FileRef,
    captured: &FileRef,
) -> Result<bool> {
    let view = publisher.tools.workspace.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let mut payloads = Vec::new();
    let config = view.workspace().volume_config();
    let mut allocations = acyclic_fs::kernel::allocation::AllocationLedger::default();
    let mut counters = acyclic_fs::WorkCounters::UNCHARGED;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::borrowed_standalone(
        &mut allocations,
        &mut counters,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let (path, _allocation) = acyclic_fs::kernel::NamespacePath::parse_in_profile(
        "/large.txt",
        config.profile,
        config.limits,
        &mut accounting,
        &cancellation,
    )
    .map_err(storage)?;
    for file in [staged, captured] {
        let generation = view
            .workspace()
            .generation(
                publisher.staged_generation(file)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        let (records, _allocation) = generation
            .lookup_paths(
                std::slice::from_ref(&path),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        payloads.push(
            records
                .entries
                .first()
                .and_then(|entry| entry.record.as_ref())
                .map(|record| record.payload),
        );
    }
    Ok(payloads.first() == payloads.get(1))
}

struct FullRead;

impl FullRead {
    fn enter() -> Self {
        assert!(!REREAD_MATCHING_PAYLOAD.replace(true));
        Self
    }
}

impl Drop for FullRead {
    fn drop(&mut self) {
        REREAD_MATCHING_PAYLOAD.set(false);
    }
}

#[tokio::test]
async fn differing_captured_bytes_keep_the_original_publication_reference() -> Result<()> {
    let (directory, _state, harness, publisher) = cost_fixture().await?;
    let operation = OperationId::new();
    let original = vec![b'a'; 1024 * 1024];
    KILL_BEFORE_CAPTURE.set(true);
    let pending = publisher
        .stage_once(
            operation,
            "changed.txt",
            &original,
            "text/plain",
            "changed.txt",
            Default::default(),
        )?
        .await;
    KILL_BEFORE_CAPTURE.set(false);
    assert!(pending.is_err());
    let view = publisher.tools.workspace.view();
    let writer = publisher.writer_for(&harness.project, view.workspace())?;
    let staged = writer
        .observe_staged_once(operation, None, publisher.maximum, Default::default())?
        .await?
        .0
        .ok_or_else(|| Error::NotFound("pending publication".into()))?;
    std::fs::write(
        directory.path().join("changed.txt"),
        vec![b'b'; original.len()],
    )
    .map_err(storage)?;
    let mut state = publisher.tools.state.lock().await;
    publisher
        .tools
        .finish_owed_write(
            (&publisher.tools.workspace, view),
            &mut state.watcher,
            PublicationPermit::Unrestricted,
        )?
        .await?;
    assert_eq!(
        publisher.observed(view, operation, staged.clone()).await?,
        staged
    );
    drop(state);
    harness.disconnect().await?;
    Ok(())
}

#[tokio::test]
async fn first_publications_preserve_the_imported_baseline_and_external_edits() -> Result<()> {
    for once in [false, true] {
        let (directory, _state, harness, publisher) = cost_fixture().await?;
        std::fs::write(directory.path().join("seed.txt"), b"before\n").map_err(storage)?;
        std::fs::write(directory.path().join("external.txt"), b"external\n").map_err(storage)?;
        let path = if once { "created.txt" } else { "seed.txt" };
        let operation = OperationId::new();
        let (file, _) = if once {
            publisher
                .stage_once(
                    operation,
                    path,
                    b"after\n",
                    "text/plain",
                    path,
                    Default::default(),
                )?
                .await?
        } else {
            publisher
                .stage(
                    operation,
                    path,
                    b"after\n",
                    "text/plain",
                    path,
                    Default::default(),
                )?
                .await?
        };
        let workspace = publisher.tools.workspace.view().workspace();
        let cancellation = acyclic_fs::CancellationToken::new();
        let baseline = workspace
            .operation_generation(
                acyclic_fs::IdempotencyKey::from_bytes(
                    capture::baseline_operation(workspace.id()).into_bytes(),
                ),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .expect("the initial import precedes the first authored publication");
        let head = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        // A mutable-head substitute loses the old bytes of an overwritten file.
        assert_ne!(baseline.id(), head.id());
        for (generation, path, expected) in [
            (&baseline, "/seed.txt", b"before\n".as_slice()),
            (&baseline, "/external.txt", b"external\n".as_slice()),
            (&head, "/external.txt", b"external\n".as_slice()),
            (
                &head,
                if once { "/created.txt" } else { "/seed.txt" },
                b"after\n".as_slice(),
            ),
        ] {
            assert_eq!(
                generation
                    .read(path, 64, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(storage)?
                    .value
                    .as_ref(),
                expected,
            );
        }
        assert_eq!(file.version(), hex::encode(head.id().to_bytes().as_slice()));
        assert_eq!(
            std::fs::read(directory.path().join(path)).map_err(storage)?,
            b"after\n"
        );
        harness.disconnect().await?;
    }
    Ok(())
}
