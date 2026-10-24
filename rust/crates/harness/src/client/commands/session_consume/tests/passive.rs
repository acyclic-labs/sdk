use super::*;

async fn paused_command_remains_recoverable<const BROKEN: bool>(task_root: bool) {
    let volume = Arc::new(
        files::TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("passive-command").unwrap(),
        )
        .await
        .unwrap(),
    );
    let session = uuid::Uuid::from_bytes([11; 16]);
    let operation = OperationId::from_bytes([12; 16]);
    let id = component(operation.to_string()).unwrap();
    let key = |file| {
        if task_root {
            files::FileKey::Task {
                task: component(TaskId::from_bytes([13; 16]).to_string()).unwrap(),
                file,
            }
        } else {
            files::FileKey::Session {
                session: component(session.to_string()).unwrap(),
                file,
            }
        }
    };
    let request = files::FileValue::new(
        key(files::TaskFile::Request(id.clone())),
        serde_json::to_vec(&CommandResolution::Prompt {
            text: "pending human approval".into(),
            source: None,
            mentions: Vec::new(),
        })
        .unwrap(),
    );
    volume
        .append(volume.head().await.unwrap(), &request)
        .await
        .unwrap();
    let mut commands = CommandSession::open(volume.clone(), session, 65_536, 32)
        .await
        .unwrap();
    assert!(matches!(
        commands.consume(&request, |_, _, _| async { Err(Error::Indeterminate(operation)) }).await,
        Err(Error::Indeterminate(found)) if found == operation
    ));
    let result_key = key(files::TaskFile::ExecutionResult(id));
    if BROKEN {
        // The old first-delivery path archived a passive wait as a terminal
        // command result. Reopen then suppressed the still-unanswered request.
        let bytes =
            archive::output_bytes(1, &Err(Error::Indeterminate(operation)), 65_536).unwrap();
        commands
            .publish(result_key.clone(), bytes, true)
            .await
            .unwrap();
    }
    drop(commands);
    let mut reopened = CommandSession::open(volume.clone(), session, 65_536, 32)
        .await
        .unwrap();
    let mut recovered = 0;
    let result = reopened
        .consume_with_recovery::<true, _, _, _, _>(
            &request,
            |_, _, _| -> futures::future::Ready<Result<Value>> {
                panic!("claimed command was dispatched again")
            },
            |_, _, _| {
                recovered += 1;
                async { Ok(json!({"done":true})) }
            },
        )
        .await
        .unwrap();
    assert_eq!(recovered, 1, "passive approval was archived as terminal");
    assert_eq!(result, Some(json!({"done":true})));
    assert_eq!(
        volume
            .snapshot(&files::Query::File(result_key), 1)
            .await
            .unwrap()
            .files
            .len(),
        1
    );
}

#[test]
fn passive_approval_keeps_first_and_recovered_command_deliveries_open() {
    for task_root in [false, true] {
        futures::executor::block_on(paused_command_remains_recoverable::<false>(task_root));
        let failure = std::panic::catch_unwind(|| {
            futures::executor::block_on(paused_command_remains_recoverable::<true>(task_root));
        })
        .expect_err("terminal-publication negative control escaped the recovery oracle");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("passive approval was archived as terminal"),
            "negative control failed before the recovery oracle: {message}"
        );
    }
}
