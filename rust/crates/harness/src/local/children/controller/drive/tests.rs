use super::*;
use crate::{
    context::ModelContextCapacity,
    model::Model,
    runtime::RuntimeTask,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
use futures::FutureExt as _;
use tokio::sync::oneshot;

async fn outcome_after_ready_child_io(broken: bool) -> Result<bool> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(
        ScriptedModelProvider::new(std::iter::empty::<ScriptedTurn>()).with_context_capacity(
            ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: 4096,
            },
        ),
    );
    let model = Model::new(
        "scripted".into(),
        "coordinator-gate".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(crate::test_support::local_harness_config(
        directory.path(),
        (model, provider),
    )?)
    .await?;
    let Admission::Accepted(RuntimeTask::Durable { task_id: task, .. }) =
        harness.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent admission");
    };
    let host = Arc::clone(harness.runtime.task_host());
    let (entered, locked) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let child = async {
        host.test_coordinator_barrier(entered, released).await;
        Ok((Component::new("gate-holder").unwrap(), 0))
    };
    let mut child: BoxProviderFuture<'_, Result<(Component, u64)>> = Box::pin(child);
    assert!(futures::poll!(child.as_mut()).is_pending());
    locked.await.unwrap();
    // External IO has completed. Only polling the child can now release the
    // actual coordinator gate; awaiting the parent outcome alone cannot progress.
    release.send(()).unwrap();
    let mut jobs = FuturesUnordered::new();
    jobs.push(child);
    let parent = std::future::pending::<Result<()>>();
    futures::pin_mut!(parent);
    let mut parent_done = false;
    let mut pending = VecDeque::new();
    // The fixture is quiescent and has no outcome payload to fetch. Suppress
    // cooperative budget yields so Pending identifies the held gate, not a
    // scheduler slice. A broken driver fails this probe instead of hanging.
    let output = if broken {
        tokio::task::unconstrained(host.terminal_status(task)).now_or_never()
    } else {
        tokio::task::unconstrained(during(
            host.terminal_status(task),
            parent.as_mut(),
            &mut parent_done,
            &mut jobs,
            &mut pending,
        ))
        .now_or_never()
    };
    let available = match output {
        Some(output) => {
            assert!(output?.is_none());
            true
        }
        None => false,
    };
    if available && !broken {
        assert_eq!(pending.len(), 1);
        assert!(!parent_done);
    }
    drop(jobs);
    drop(host);
    harness.disconnect().await?;
    Ok(available)
}

fn progresses(available: bool, check: bool) {
    if check {
        assert!(
            available,
            "parent outcome parked the child which owns its coordinator gate"
        );
    }
}

#[tokio::test]
async fn parent_outcome_drives_the_child_which_releases_its_coordinator_gate() -> Result<()> {
    progresses(outcome_after_ready_child_io(false).await?, true);
    let broken = outcome_after_ready_child_io(true).await?;
    std::panic::catch_unwind(|| progresses(broken, true))
        .expect_err("parked coordinator owner was not detected");
    progresses(broken, false);
    Ok(())
}
