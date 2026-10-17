use super::*;

async fn local() -> (tempfile::TempDir, Arc<LocalHarness>) {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(
        crate::test_support::ScriptedModelProvider::new([]).with_context_capacity(
            crate::context::ModelContextCapacity {
                context_tokens: 32768,
                output_tokens: 4096,
            },
        ),
    );
    let model = crate::model::Model::new(
        "test".into(),
        "restore".into(),
        "1".into(),
        serde_json::Value::Null,
    )
    .unwrap();
    let config = crate::test_support::local_harness_config(directory.path(), (model, provider))
        .unwrap()
        .without_default_tools();
    (
        directory,
        Arc::new(Box::pin(LocalHarness::open(config)).await.unwrap()),
    )
}

async fn restored<const GATE_INPUT: bool>(completed: bool) {
    let (_directory, local) = Box::pin(local()).await;
    let operation = OperationId::new();
    let request = prepare_local_prompt(operation, "saved").unwrap();
    let files = local.task_files();
    let cut = files.inner.head().await.unwrap();
    let cut = files.inner.append(cut, &request).await.unwrap();
    let result = FileValue::new(
        key(operation, TaskFile::ExecutionResult).unwrap(),
        br#"{"sequence":1,"ok":{"text":"done"}}"#.to_vec(),
    );
    if completed {
        files.inner.append(cut, &result).await.unwrap();
    }
    let mut client = Box::pin(LocalFileClient::open(local.clone(), operation))
        .await
        .unwrap();
    if !completed {
        let local = local.clone();
        client.host = Box::pin(async move {
            let files = local.task_files();
            let cut = files.inner.head().await?;
            files.inner.append(cut, &result).await?;
            Ok(())
        });
    }
    let mut output = Vec::new();
    let outcome = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        if GATE_INPUT {
            futures::stream::pending::<Result<String>>().next().await;
        }
        Box::pin(client.input(operation, futures::stream::pending(), &mut output)).await
    })
    .await;
    assert!(
        matches!(outcome, Ok(Ok(HeadlessOutcome::Completed))),
        "restored operation waited for stdin"
    );
    Box::pin(client.finish(Ok(HeadlessOutcome::Completed)))
        .await
        .unwrap();
    drop(local);
}
#[tokio::test]
async fn saved_jsonl_work_progresses_while_stdin_remains_open() {
    restored::<false>(true).await;
    restored::<false>(false).await;
}
#[tokio::test]
#[should_panic(expected = "restored operation waited for stdin")]
async fn gating_restoration_on_another_frame_is_detected_negative_control() {
    restored::<true>(true).await;
}

struct Sink(bool);
impl std::io::Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0 {
            Err(std::io::Error::other("broken output"))
        } else {
            Ok(bytes.len())
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
async fn teardown<const JOIN: bool>(output_failure: bool) {
    let (_directory, local) = Box::pin(local()).await;
    let operation = OperationId::new();
    let mut client = Box::pin(LocalFileClient::open(local.clone(), operation))
        .await
        .unwrap();
    Box::pin(client.submit(&prepare_local_prompt(operation, "active").unwrap()))
        .await
        .unwrap();
    let joined = std::rc::Rc::new(std::cell::Cell::new(false));
    let marker = joined.clone();
    // Model a native job whose awaiting future was already polled and parked.
    let (release, wait) = futures::channel::oneshot::channel();
    client.host = Box::pin(async move {
        wait.await.unwrap();
        marker.set(true);
        Ok(())
    });
    assert!(client.host.as_mut().now_or_never().is_none());
    let failure = Box::pin(client.input(
        operation,
        futures::stream::iter([Ok("invalid second frame".into())]),
        &mut Sink(output_failure),
    ))
    .await;
    assert!(failure.is_err());
    release.send(()).unwrap();
    if JOIN {
        assert!(Box::pin(client.finish(failure)).await.is_err());
    } else {
        drop(client);
        super::super::super::disconnect_local(local.clone())
            .await
            .unwrap();
    }
    assert!(
        joined.get(),
        "mounts retired before the active worker was joined"
    );
}
#[tokio::test]
async fn input_or_output_failure_joins_the_active_host_before_disconnect() {
    teardown::<true>(false).await;
    teardown::<true>(true).await;
}
#[tokio::test]
#[should_panic(expected = "mounts retired before the active worker was joined")]
async fn dropping_the_awaiting_future_is_detected_negative_control() {
    teardown::<false>(false).await;
}
