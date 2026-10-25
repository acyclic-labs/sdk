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
    let model = crate::model::Model::new(
        "scripted".into(),
        "publication".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
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
            Arc::clone(&harness.task_files),
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
        .observe_staged_once(operation, publisher.maximum, Default::default())?
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

    harness.close().await?;
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
        let records = generation
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
                .first()
                .and_then(Option::as_ref)
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
        .observe_staged_once(operation, publisher.maximum, Default::default())?
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
        )
        .await?;
    assert_eq!(
        publisher.observed(view, operation, staged.clone()).await?,
        staged
    );
    drop(state);
    harness.close().await?;
    Ok(())
}

#[tokio::test]
async fn fresh_checkout_writes_publish_external_and_authored_changes_in_one_capture() -> Result<()>
{
    let mut counts = Vec::new();
    for once in [false, true] {
        let (directory, _state, harness, publisher) = cost_fixture().await?;
        let workspace = publisher.tools.workspace.view().workspace();
        let cancellation = acyclic_fs::CancellationToken::new();
        let before = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        std::fs::write(
            directory.path().join("external-write-cost"),
            b"external bytes\n",
        )
        .map_err(storage)?;
        let started = std::time::Instant::now();
        let operation = OperationId::new();
        let (file, _) = if once {
            publisher
                .stage_once(
                    operation,
                    "authored-write-cost",
                    b"authored bytes\n",
                    "text/plain",
                    "authored-write-cost",
                    Default::default(),
                )?
                .await?
        } else {
            publisher
                .stage(
                    operation,
                    "authored-write-cost",
                    b"authored bytes\n",
                    "text/plain",
                    "authored-write-cost",
                    Default::default(),
                )?
                .await?
        };
        let elapsed = started.elapsed();
        let after = workspace
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        for (path, expected) in [
            ("/external-write-cost", b"external bytes\n".as_slice()),
            ("/authored-write-cost", b"authored bytes\n".as_slice()),
        ] {
            assert_eq!(
                after
                    .read(path, 64, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(storage)?
                    .value
                    .as_ref(),
                expected
            );
        }
        assert_eq!(
            file.version(),
            hex::encode(after.id().digest().into_bytes())
        );
        let mut generation = after;
        let mut count = 0;
        while generation.id() != before.id() {
            let parents = generation
                .parents(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            let [parent] = parents.as_slice() else {
                return Err(Error::Invalid(
                    "write ancestry must retain its original parent".into(),
                ));
            };
            generation = workspace
                .generation(*parent, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(storage)?
                .value;
            count += 1;
            assert!(count <= 4, "write ancestry must reach its original head");
        }
        eprintln!(
            "WRITE_CAPTURE once={once} generations={count} elapsed_us={}",
            elapsed.as_micros()
        );
        counts.push(count);
        harness.close().await?;
    }
    assert_eq!(
        counts,
        [2, 2],
        "fresh writes must share one post-write capture with external edits"
    );
    Ok(())
}
