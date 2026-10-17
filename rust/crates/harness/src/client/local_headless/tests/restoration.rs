// Restored headless work and failure teardown through the shared oracle. Each
// run gets its own current-thread runtime on a paused clock: the stdin wait is
// observed by virtual time, which advances only once the runtime has nothing
// left to run, never by a wall-clock deadline.
use super::*;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};

#[derive(Debug)]
enum Case {
    /// Saved work, already completed or completed by the host, with stdin open.
    Restored { completed: bool },
    /// A second-frame input or an output failure while the host is parked.
    Teardown { output_failure: bool },
}

#[derive(Debug, PartialEq)]
enum Fault {
    /// Gates restoration on another stdin frame.
    GateInput,
    /// Drops the awaiting host future instead of joining it.
    DropAwaiting,
}

#[derive(Debug)]
enum Seen {
    Restored { completed: bool, progressed: bool },
    Joined { joined: bool },
}

fn oracle() -> Oracle<Case, Fault, Seen> {
    Oracle::declare(run)
        .invariant("progress", |seen| match seen {
            Seen::Restored {
                completed,
                progressed: false,
            } => Err(format!(
                "restored operation (completed: {completed}) waited for stdin"
            )),
            _ => Ok(()),
        })
        .invariant("joined", |seen| match seen {
            Seen::Joined { joined: false } => {
                Err("mounts retired before the active worker was joined".into())
            }
            _ => Ok(()),
        })
        .control(
            "progress",
            Case::Restored { completed: true },
            Fault::GateInput,
        )
        .control(
            "joined",
            Case::Teardown {
                output_failure: false,
            },
            Fault::DropAwaiting,
        )
        .build()
}

fn run(
    case: &Case,
    fault: Option<&Fault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    let seen = runtime.block_on(async {
        match case {
            Case::Restored { completed } => Seen::Restored {
                completed: *completed,
                progressed: Box::pin(restored(*completed, fault == Some(&Fault::GateInput))).await,
            },
            Case::Teardown { output_failure } => Seen::Joined {
                joined: Box::pin(teardown(
                    *output_failure,
                    fault != Some(&Fault::DropAwaiting),
                ))
                .await,
            },
        }
    });
    see.see(seen)
}

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

/// Whether restored work completed while stdin stayed open.
async fn restored(completed: bool, gate_input: bool) -> bool {
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
    // Virtual time: this elapses only after every runnable task is parked.
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(3600), async {
        if gate_input {
            futures::stream::pending::<Result<String>>().next().await;
        }
        Box::pin(client.input(operation, futures::stream::pending(), &mut output)).await
    })
    .await;
    if !matches!(outcome, Ok(Ok(HeadlessOutcome::Completed))) {
        return false;
    }
    Box::pin(client.finish(Ok(HeadlessOutcome::Completed)))
        .await
        .unwrap();
    drop(local);
    true
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

/// Whether the parked host was joined before the mounts retired.
async fn teardown(output_failure: bool, join: bool) -> bool {
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
    if join {
        assert!(Box::pin(client.finish(failure)).await.is_err());
    } else {
        drop(client);
        super::super::super::disconnect_local(local.clone())
            .await
            .unwrap();
    }
    joined.get()
}

#[test]
fn saved_work_progresses_with_stdin_open_and_failures_join_the_active_host() {
    let oracle = oracle();
    for completed in [true, false] {
        oracle.check(&Case::Restored { completed });
    }
    for output_failure in [false, true] {
        oracle.check(&Case::Teardown { output_failure });
    }
}
