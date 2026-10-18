use super::*;
use crate::task_volume::{Component, Query};
use futures::{FutureExt as _, StreamExt as _};
use std::cell::RefCell;
use tokio::sync::{mpsc, oneshot};

struct Probe {
    broken: bool,
    reached: mpsc::UnboundedSender<&'static str>,
    proceed: mpsc::UnboundedReceiver<()>,
}
thread_local! { static PROBE: RefCell<Option<Probe>> = const { RefCell::new(None) }; }

pub(in crate::local::children) async fn probe(
    files: &crate::task_volume::TaskFiles<acyclic_stream::LocalStream>,
    reference: &Component,
    cursor: &mut super::super::controller::follow::Follow,
) -> Result<()> {
    let Some(mut probe) = PROBE.with(|slot| slot.borrow_mut().take()) else {
        return Ok(());
    };
    let query = Query::Child(reference.clone());
    let mut retained = if probe.broken {
        Some(
            files
                .inner
                .follow(query.clone(), cursor.cursor.clone())
                .await
                .map_err(storage)?,
        )
    } else {
        None
    };
    probe.reached.send("before follow probe").unwrap();
    probe.proceed.recv().await.unwrap();
    if let Some(changes) = &mut retained {
        while let Some(next) = changes.next().now_or_never() {
            cursor.cursor = next.unwrap().map_err(storage)?.cursor;
        }
    } else {
        while let Some(next) =
            super::super::controller::follow::next(files, query.clone(), cursor).now_or_never()
        {
            next.unwrap().map_err(storage)?;
        }
    }
    probe.reached.send("after follow probe").unwrap();
    probe.proceed.recv().await.unwrap();
    Ok(())
}

async fn dispatch(broken: bool) -> Result<bool> {
    use crate::{
        client::{CommandResolution, CommandTool},
        model::Model,
    };
    let directory = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new([ScriptedTurn::Text("delegated answer".into())])
            .with_context_capacity(ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            }),
    );
    let model = Model::new(
        "scripted".into(),
        "delegate-probe".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model, provider.clone()),
    )?)
    .await?;
    let stream = harness.task_stream();
    let (reached, mut events) = mpsc::unbounded_channel();
    let (proceed, receiver) = mpsc::unbounded_channel();
    PROBE.with(|slot| {
        *slot.borrow_mut() = Some(Probe {
            broken,
            reached,
            proceed: receiver,
        })
    });
    let run = harness.dispatch_command(
        OperationId::new(),
        None,
        CommandResolution::Tool {
            tool: CommandTool::Spawn,
            input: serde_json::json!({"task":"delegate this task", "name":"delegate"}),
        },
    );
    let handshake = async {
        assert_eq!(events.recv().await, Some("before follow probe"));
        let (entered, entering) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let writer = stream.test_publication_barrier(entered, released);
        let consumer = async {
            entering.await.unwrap();
            proceed.send(()).unwrap();
            assert_eq!(events.recv().await, Some("after follow probe"));
            release.send(()).unwrap();
        };
        futures::join!(writer, consumer);
        stream.test_publication_available()
    };
    futures::pin_mut!(run, handshake);
    let available = match futures::future::select(handshake, run).await {
        futures::future::Either::Left((available, run)) => {
            if available {
                proceed.send(()).unwrap();
                assert_eq!(run.await?, serde_json::json!({"agent":"delegate"}));
                assert_eq!(provider.requests().len(), 1);
            }
            available
        }
        futures::future::Either::Right(_) => {
            panic!("delegate bypassed the follow/publication handshake")
        }
    };
    Ok(available)
}

fn property(available: bool, assert_property: bool) {
    if assert_property {
        assert!(
            available,
            "speculative child follow retained the publication read permit before its own write"
        );
    }
}

#[tokio::test]
async fn delegate_never_parks_a_follow_read_ahead_of_its_own_publication() -> Result<()> {
    property(dispatch(false).await?, true);
    let broken = dispatch(true).await?;
    std::panic::catch_unwind(|| property(broken, true))
        .expect_err("retained publication permit was not detected");
    property(broken, false);
    Ok(())
}
