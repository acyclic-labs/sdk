use super::*;
use crate::task_volume::{ChildTurnResult, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use loom::thread;

fn stopping(broken: bool, assert_property: bool) {
    let provider = Arc::new(MemoryStream::default());
    let path = StreamPath::new("child-controller-drain").unwrap();
    let volume =
        loom::future::block_on(Box::pin(TaskVolume::open(provider.clone(), path.clone()))).unwrap();
    let reference = Component::new("worker").unwrap();
    let key = |file| FileKey::Child {
        reference: reference.clone(),
        file,
    };
    let status = ChildStatus {
        task: TaskId::from_bytes([7; 16]),
        state: ChildState::Running,
        settled: false,
        inputs: 1,
    };
    let cut = loom::future::block_on(Box::pin(volume.head())).unwrap();
    let cut = loom::future::block_on(Box::pin(volume.append(
        cut,
        &FileValue::new(key(ChildFile::Status), serde_json::to_vec(&status).unwrap()),
    )))
    .unwrap();
    let mut observation = loom::future::block_on(Box::pin(volume.observe(
        Query::Child(reference.clone()),
        None,
        8,
    )))
    .unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let writer = loom::future::block_on(Box::pin(TaskVolume::open_existing(
        provider.clone(),
        path.clone(),
    )))
    .unwrap();
    let control = FileValue::new(key(ChildFile::Control), Control::Cancel.bytes().to_vec());
    // Publication/follow registration races are explored independently by the
    // task-files models. Start this controller model at their accepted cut.
    loom::future::block_on(Box::pin(writer.append(cut, &control))).unwrap();
    let signal = acyclic_fs::CancellationToken::new();
    let inherited = signal.clone();
    let writer = thread::spawn(move || inherited.cancel());
    let effect = async {
        cancellation.cancelled().await;
        let cut = volume.head().await.map_err(storage)?;
        let result = ChildTurnResult {
            task: status.task,
            turn: 1,
            outcome: crate::Outcome::Cancelled,
        };
        let cut = volume
            .append(
                cut,
                &FileValue::new(
                    key(ChildFile::Result(1)),
                    serde_json::to_vec(&result).unwrap(),
                ),
            )
            .await
            .map_err(storage)?;
        volume
            .append(
                cut,
                &FileValue::new(
                    key(ChildFile::Status),
                    serde_json::to_vec(&ChildStatus {
                        state: ChildState::Cancelled,
                        settled: true,
                        ..status
                    })
                    .unwrap(),
                ),
            )
            .await
            .map_err(storage)?;
        Ok(())
    };
    let controls = async {
        signal.cancelled().await;
        // Retire the signaler before serial publication and reopen. Only the
        // real cancel/register/wake interleavings belong to this model.
        writer.join().unwrap();
        let change = observation.changes.next().await.unwrap().map_err(storage)?;
        // The accepted cancel intent survives the controller's observation error.
        if change
            .file
            .as_ref()
            .is_none_or(|file| file.key != key(ChildFile::Control))
        {
            return Err(Error::Invalid("control follow lost its file".into()));
        }
        Err(Error::Conflict("control writer stopped".into()))
    };
    if broken {
        crate::task_volume::model_checking::set_fault(
            crate::task_volume::model_checking::Fault::DropActiveChild,
        );
    }
    let result = loom::future::block_on(Box::pin(drain(effect, controls, &cancellation)));
    let reopened =
        loom::future::block_on(Box::pin(TaskVolume::open_existing(provider, path))).unwrap();
    let snapshot = loom::future::block_on(Box::pin(
        reopened.snapshot(&Query::Child(reference.clone()), 8),
    ))
    .unwrap();
    let answer = snapshot
        .files
        .iter()
        .find(|file| file.key == key(ChildFile::Result(1)))
        .and_then(|file| ChildTurnResult::parse(&file.bytes).ok());
    let settled = snapshot
        .files
        .iter()
        .find(|file| file.key == key(ChildFile::Status))
        .and_then(|file| ChildStatus::parse(&file.bytes).ok());
    if assert_property {
        assert!(
            result.is_err()
                && answer.is_some_and(|result| matches!(result.outcome, crate::Outcome::Cancelled))
                && settled
                    .is_some_and(|status| status.settled && status.state == ChildState::Cancelled),
            "child effect was dropped before settlement"
        );
    }
}

#[test]
fn control_failure_drains_the_actual_invocation_future() {
    // One cancel signaler and one controller worker. The accepted-file cut is
    // shared with the independent publication models; cancel/register/wakeup
    // uses the real filesystem token locks. Reopen verifies serial settlement.
    loom::model(|| stopping(false, true));
    let failure = std::panic::catch_unwind(|| loom::model(|| stopping(true, true)))
        .expect_err("dropping the active child was not detected");
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("child effect was dropped before settlement"),
        "unexpected negative control: {message}"
    );
    // Removing the sole property assertion must make its negative control
    // undetectable. A different panic cannot masquerade as model coverage.
    loom::model(|| stopping(true, false));
}
