//! Recovery of interrupted write captures keeps each written reference.
use super::*;

// Recovery returns the reference the write returned, and it stays editable.
// Before recovery rebound its reference, observing the operation returned
// the staged generation.
#[tokio::test]
async fn a_recovered_write_returns_the_written_reference() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let (observed, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let observed = observed.ok_or_else(|| Error::Invalid("the write was not recovered".into()))?;
    assert_eq!(observed.version(), written.version());
    publisher
        .stage_at(OperationId::new(), &observed, b"edited\n", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"edited\n"
    );
    Ok(())
}

/// Whether the reference that a replayed write returns names its finished
/// capture, after the first attempt stopped between its physical write and
/// that write's capture, and whether an edit through it then succeeds. The
/// replay either reruns the write or, as the write tool does on restart,
/// recovers the operation first.
async fn replay_after_an_interrupted_capture_is_settled(skip: bool, recover: bool) -> Result<bool> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let write = || {
        publisher.stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )
    };
    KILL_BEFORE_CAPTURE.set(true);
    let interrupted = match write() {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    KILL_BEFORE_CAPTURE.set(false);
    assert!(
        interrupted.is_some(),
        "the first attempt was not interrupted"
    );
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"agent bytes\n"
    );
    REPLAY_SKIPS_CAPTURE.set(skip);
    let replayed = if recover {
        match publisher.observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        ) {
            Ok(observed) => observed.await.map(|(file, _)| file),
            Err(error) => Err(error),
        }
    } else {
        match write() {
            Ok(staged) => staged.await.map(|(file, _)| Some(file)),
            Err(error) => Err(error),
        }
    };
    REPLAY_SKIPS_CAPTURE.set(false);
    let replayed = replayed?
        .ok_or_else(|| Error::Invalid("the interrupted write was not recovered".into()))?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(write_capture(operation, replayed.path()), 1).into_bytes(),
    );
    let capture = publisher
        .tools
        .workspace
        .view()
        .workspace()
        .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let settled = capture.is_some_and(|capture| {
        hex::encode(capture.id().to_bytes().as_slice()) == replayed.version()
    });
    let edited = match publisher.stage_at(OperationId::new(), &replayed, b"edited\n", None, None) {
        Ok(staged) => staged.await.is_ok(),
        Err(_) => false,
    };
    Ok(settled && edited)
}

#[tokio::test]
async fn a_replay_finishes_an_interrupted_capture() -> Result<()> {
    assert!(replay_after_an_interrupted_capture_is_settled(false, false).await?);
    // Negative control: a replay that skips the pending capture returns the
    // staged reference, not the capture of the write's own host metadata.
    assert!(!replay_after_an_interrupted_capture_is_settled(true, false).await?);
    Ok(())
}

#[tokio::test]
async fn recovery_finishes_an_interrupted_capture() -> Result<()> {
    assert!(replay_after_an_interrupted_capture_is_settled(false, true).await?);
    // Negative control: recovery that skips the pending capture returns the
    // staged reference, not the capture of the write's own host metadata.
    assert!(!replay_after_an_interrupted_capture_is_settled(true, true).await?);
    Ok(())
}

/// The reference that names `file`'s staged generation: the parent of the
/// capture that `file` names.
async fn staged_reference(harness: &Fixture, file: &FileRef) -> Result<FileRef> {
    let workspace = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .clone();
    let encoded = hex::decode(file.version()).map_err(storage)?;
    let generation = acyclic_fs::GenerationId::from_bytes(&encoded)
        .ok_or_else(|| Error::Invalid("file version is not a canonical generation".into()))?;
    let stop = acyclic_fs::CancellationToken::new();
    let parents = workspace
        .generation(generation, acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(storage)?
        .value
        .parents(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .await
        .map_err(storage)?
        .value;
    let [staged] = parents.as_slice() else {
        return Err(Error::Invalid("the capture has no unique parent".into()));
    };
    FileRef::new(
        file.volume().clone(),
        file.path().to_owned(),
        hex::encode(staged.to_bytes().as_slice()),
        file.descriptor().clone(),
        file.display_name().to_owned(),
        &mut None,
    )
}

// Recovery finds the write's capture by its operation, however many commits
// have buried it, so a retried edit through the recovered reference replays.
// Negative control: the staged reference, which recovery returned once the
// capture lay deeper than a bounded walk from the head, makes that retry fail.
#[tokio::test]
async fn a_recovered_write_keeps_its_reference_however_far_the_head_moves() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let (written, _) = publisher
        .stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let edit = OperationId::new();
    publisher
        .stage_at(edit, &written, b"edited\n", None, None)?
        .await?;
    // Bury the capture well past any walk bounded at 64 with commits that
    // leave the head's content, and so the checkout, unchanged.
    let workspace = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .clone();
    for _ in 0..33 {
        workspace
            .write_text("/buried", "x")
            .await
            .map_err(storage)?;
        workspace.remove("/buried").await.map_err(storage)?;
    }
    let (recovered, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let recovered =
        recovered.ok_or_else(|| Error::Invalid("the write was not recovered".into()))?;
    assert_eq!(recovered.version(), written.version());
    publisher
        .stage_at(edit, &recovered, b"edited\n", None, None)?
        .await?;
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"edited\n"
    );
    let staged = staged_reference(&harness, &written).await?;
    assert_ne!(staged.version(), written.version());
    let retried = match publisher.stage_at(edit, &staged, b"edited\n", None, None) {
        Ok(retry) => retry.await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(matches!(&retried, Err(Error::Conflict(_))), "{retried:?}");
    Ok(())
}

// Another write between an interrupted capture and its recovery cannot take
// the interrupted write's capture: the next capture first finishes the
// capture the interrupted write durably owes, under that write's own
// operation. Recovery then returns that capture, the reference an
// uninterrupted write returns. Negative control: when the other write's
// capture records the interrupted write as its own, no capture exists under
// the write's operation and recovery returns the staged reference.
#[tokio::test]
async fn another_write_before_recovery_leaves_the_interrupted_capture_its_own() -> Result<()> {
    let (directory, harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    KILL_BEFORE_CAPTURE.set(true);
    let interrupted = match publisher.stage_once(
        operation,
        "result.txt",
        b"agent bytes
",
        "text/plain",
        "result.txt",
        crate::conversation::ContentAccess::default(),
    ) {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    KILL_BEFORE_CAPTURE.set(false);
    assert!(
        interrupted.is_some(),
        "the first attempt was not interrupted"
    );
    publisher
        .stage_once(
            OperationId::new(),
            "other.txt",
            b"other bytes
",
            "text/plain",
            "other.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let (recovered, _) = publisher
        .observe_staged_once(
            operation,
            1024,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let recovered = recovered
        .ok_or_else(|| Error::Invalid("the interrupted write was not recovered".into()))?;
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        super::super::capture::capture_operation(write_capture(operation, "result.txt"), 1)
            .into_bytes(),
    );
    let capture = harness
        .workspace
        .as_ref()
        .ok_or_else(|| Error::NotFound("checkout".into()))?
        .view()
        .workspace()
        .operation_generation(
            key,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(storage)?
        .value
        .ok_or_else(|| Error::Invalid("another write took the interrupted capture".into()))?;
    assert_eq!(
        recovered.version(),
        hex::encode(capture.id().to_bytes().as_slice())
    );
    assert_eq!(
        std::fs::read(directory.path().join("result.txt")).map_err(storage)?,
        b"agent bytes
"
    );
    assert_eq!(
        std::fs::read(directory.path().join("other.txt")).map_err(storage)?,
        b"other bytes
"
    );
    Ok(())
}

#[tokio::test]
async fn a_clean_capture_records_its_settlement_without_native_mutations() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    publisher
        .stage(
            OperationId::new(),
            "same.txt",
            b"same bytes",
            "text/plain",
            "same.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let tools = &publisher.tools;
    let view = tools.workspace.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let staged = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    let operation = publication_operation(OperationId::new(), "same.txt");
    let owed = (
        operation,
        acyclic_fs::GitGenerationRef {
            workspace_id: view.workspace().id(),
            generation: staged,
        },
    );
    serialized(&tools.state, async |mut state| {
        tools
            .owe_write_capture((&*tools.workspace, view), &mut state.watcher, owed)
            .await?;
        tools
            .finish_owed_write(
                (&*tools.workspace, view),
                &mut state.watcher,
                PublicationPermit::Unrestricted,
            )?
            .await
    })
    .await?;
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(
            OperationId::from_bytes(operation.into_bytes().as_slice()),
            1,
        )
        .into_bytes(),
    );
    let settled = view
        .workspace()
        .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert!(
        settled.is_some(),
        "a capture with no changes must still durably settle its debt"
    );
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("same.txt"), b"later user edit").map_err(storage)?;
    serialized(&tools.state, async |mut state| {
        tools
            .owe_write_capture((&*tools.workspace, view), &mut state.watcher, owed)
            .await?;
        tools
            .finish_owed_write(
                (&*tools.workspace, view),
                &mut state.watcher,
                PublicationPermit::Unrestricted,
            )?
            .await
    })
    .await?;
    assert_eq!(
        view.workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before
    );
    assert_eq!(
        std::fs::read(directory.path().join("same.txt")).map_err(storage)?,
        b"later user edit"
    );
    Ok(())
}

#[tokio::test]
async fn a_clean_write_capture_cannot_capture_a_later_user_edit() -> Result<()> {
    let (directory, _harness, publisher) = fixture().await?;
    let source = publisher
        .stage(
            OperationId::new(),
            "same.txt",
            b"same bytes",
            "text/plain",
            "same.txt",
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .0;
    let operation = OperationId::new();
    let captured = publisher
        .stage_at(operation, &source, b"same bytes", None, None)?
        .await?
        .0;
    let view = publisher.tools.workspace.view();
    let cancellation = acyclic_fs::CancellationToken::new();
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(write_capture(operation, "same.txt"), 1).into_bytes(),
    );
    let settlement = view
        .workspace()
        .operation_generation(key, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert!(
        settlement.is_some(),
        "even a clean capture must durably settle its operation"
    );
    let before = view
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(directory.path().join("same.txt"), b"later user edit").map_err(storage)?;
    let replayed = publisher
        .stage_at(operation, &source, b"same bytes", None, None)?
        .await?
        .0;
    assert_eq!(replayed, captured);
    assert_eq!(
        std::fs::read(directory.path().join("same.txt")).map_err(storage)?,
        b"later user edit"
    );
    assert_eq!(
        view.workspace()
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before
    );
    Ok(())
}

/// How a write that stopped after it owed its write, before the write
/// reached disk, is finished.
#[derive(Clone, Copy)]
enum Finish {
    Replay,
    AnotherWriteThenRecovery,
    Recovery,
}

/// Whether, after the interruption, `finish` leaves the checkout holding the
/// write's bytes, returns the capture under the write's own operation, and
/// keeps that reference editable.
async fn an_unwritten_owed_write_is_settled(finish: Finish, unapplied: bool) -> Result<bool> {
    let (directory, _harness, publisher) = fixture().await?;
    let operation = OperationId::new();
    let write = || {
        publisher.stage_once(
            operation,
            "result.txt",
            b"agent bytes\n",
            "text/plain",
            "result.txt",
            crate::conversation::ContentAccess::default(),
        )
    };
    KILL_BEFORE_WRITE.set(true);
    let interrupted = match write() {
        Ok(staged) => staged.await.err(),
        Err(error) => Some(error),
    };
    KILL_BEFORE_WRITE.set(false);
    assert!(
        interrupted.is_some(),
        "the first attempt was not interrupted"
    );
    assert!(
        !directory.path().join("result.txt").exists(),
        "the interrupted write reached disk"
    );
    let recover = || async {
        publisher
            .observe_staged_once(
                operation,
                1024,
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .map(|(file, _)| file)
    };
    OWED_WRITE_UNAPPLIED.set(unapplied);
    let finished = match finish {
        Finish::Replay => async { Ok(Some(write()?.await?.0)) }.await,
        Finish::Recovery => recover().await,
        Finish::AnotherWriteThenRecovery => {
            let other = async {
                publisher
                    .stage_once(
                        OperationId::new(),
                        "other.txt",
                        b"other bytes\n",
                        "text/plain",
                        "other.txt",
                        crate::conversation::ContentAccess::default(),
                    )?
                    .await
            }
            .await;
            match other {
                Ok(_) => recover().await,
                Err(error) => Err(error),
            }
        }
    };
    OWED_WRITE_UNAPPLIED.set(false);
    let Ok(Some(finished)) = finished else {
        return Ok(false);
    };
    let written = std::fs::read(directory.path().join("result.txt")).ok();
    let key = acyclic_fs::IdempotencyKey::from_bytes(
        capture::capture_operation(write_capture(operation, finished.path()), 1).into_bytes(),
    );
    let capture = publisher
        .tools
        .workspace
        .view()
        .workspace()
        .operation_generation(
            key,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map_err(storage)?
        .value;
    let settled = capture.is_some_and(|capture| {
        hex::encode(capture.id().to_bytes().as_slice()) == finished.version()
    });
    let edited = match publisher.stage_at(OperationId::new(), &finished, b"edited\n", None, None) {
        Ok(staged) => staged.await.is_ok(),
        Err(_) => false,
    };
    Ok(written.as_deref() == Some(b"agent bytes\n".as_slice()) && settled && edited)
}

// A write that stopped after owing its write, before the write, is
// published by whichever entry point finishes the debt, so its capture
// holds its bytes and its reference stays editable. Negative control:
// finishing the debt without publishing the write, as before, captures the
// missing file and leaves the write's reference unsettled or stale.
#[tokio::test]
async fn an_owed_write_that_never_reached_disk_is_published_before_its_capture() -> Result<()> {
    for finish in [
        Finish::Replay,
        Finish::AnotherWriteThenRecovery,
        Finish::Recovery,
    ] {
        assert!(an_unwritten_owed_write_is_settled(finish, false).await?);
        assert!(!an_unwritten_owed_write_is_settled(finish, true).await?);
    }
    Ok(())
}
