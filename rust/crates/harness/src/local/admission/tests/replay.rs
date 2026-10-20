use super::*;
use crate::{TaskId, goal::GoalContinuationPolicy};
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};

#[derive(Clone, Copy, Debug)]
struct Case {
    completed: bool,
    reopened: bool,
    same_root: bool,
}

#[derive(Debug)]
enum Fault {
    WrongRoot,
    ChargeReplay,
    ExecuteReplay,
}

#[derive(Debug)]
enum Seen {
    Task {
        actual: Option<TaskId>,
        expected: Option<TaskId>,
    },
    Head {
        actual: (Option<Turn>, GenerationRef),
        expected: (Option<Turn>, GenerationRef),
    },
    Effects {
        unchanged: bool,
    },
}

async fn write_goal(harness: &LocalHarness, contents: &str) {
    use crate::conversation::{ContentPublisher, Limits};
    crate::filesystem::FilesystemContentPublisher::new(
        harness.files.clone(),
        harness.scratch(),
        &harness.verifier,
        &harness.signed,
        Limits::default().file_bytes,
        None,
    )
    .expect("goal publisher")
    .stage_once(
        OperationId::new(),
        "GOAL.md",
        contents.as_bytes(),
        "text/markdown",
        "GOAL.md",
        None,
    )
    .expect("goal write future")
    .await
    .expect("goal write");
}

async fn run(
    bootstrap: &bootstrap::Bootstrap,
    case: &Case,
    fault: Option<&Fault>,
    see: &mut Observer<'_, Seen>,
) -> std::result::Result<(), Stop> {
    let (root, _) = bootstrap.restore().expect("independent snapshot");
    let directory = root.directory.path().join("root");
    let provider = Arc::new(ScriptedModelProvider::new([
        ScriptedTurn::Text("root".into()),
        ScriptedTurn::Text("successor".into()),
    ]));
    let config = || {
        bootstrap
            .config(&directory, provider.clone())
            .expect("fixture config")
            .goal_continuations(Some(GoalContinuationPolicy {
                max_continuations: 1,
            }))
    };
    let harness = LocalHarness::open(config()).await.expect("open fixture");
    let operation = OperationId::new();
    let task = crate::runtime::task_id_for_admission(operation);
    admitted_task(
        harness
            .submit(operation, "start")
            .await
            .expect("submit root"),
    )
    .expect("root admission");
    harness.resume_turn(task).await.expect("settle root");
    write_goal(&harness, "Keep going").await;
    let next = harness
        .continue_goal(task, operation)
        .await
        .expect("create continuation")
        .expect("continuation admitted");
    assert_eq!(
        next,
        crate::runtime::task_id_for_admission(crate::goal::continuation_operation(task, operation))
    );
    assert_eq!(
        harness
            .current_turn()
            .await
            .expect("initial HEAD")
            .0
            .expect("published HEAD")
            .automatic_turns,
        1
    );
    if case.completed {
        harness.resume_turn(next).await.expect("settle successor");
    }
    // A replay must follow the retained input even after the goal changes.
    write_goal(&harness, "Changed objective").await;
    let harness = if case.reopened {
        drop(harness);
        LocalHarness::open(config())
            .await
            .expect("reopen retained state")
    } else {
        harness
    };
    let expected_head = harness.current_turn().await.expect("reference HEAD");
    let expected_requests = provider.requests();
    let expected_remaining = provider.remaining();
    let expected_task = case.same_root.then_some(next);
    let correct_root = if case.same_root {
        operation
    } else {
        OperationId::new()
    };
    for _ in 0..3 {
        let root = if matches!(fault, Some(Fault::WrongRoot)) {
            if case.same_root {
                OperationId::new()
            } else {
                operation
            }
        } else {
            correct_root
        };
        let actual = harness
            .continue_goal(task, root)
            .await
            .expect("replay settle");
        see.see(Seen::Task {
            actual,
            expected: expected_task,
        })?;
        if matches!(fault, Some(Fault::ChargeReplay)) {
            // Restore the defect at the actual durable publication boundary.
            let (head, generation) = harness.current_turn().await.expect("charged HEAD");
            let mut charged = head.expect("retained successor");
            charged.automatic_turns += 1;
            harness
                .write_turn(
                    &charged,
                    &generation,
                    &crate::IdempotencyKey::new(format!(
                        "fault-replay:{}",
                        charged.automatic_turns
                    ))
                    .expect("control key"),
                )
                .await
                .expect("charge replay");
        }
        see.see(Seen::Head {
            actual: harness.current_turn().await.expect("observed HEAD"),
            expected: expected_head.clone(),
        })?;
        if matches!(fault, Some(Fault::ExecuteReplay)) {
            harness
                .resume_turn(next)
                .await
                .expect("execute replay control");
        }
        see.see(Seen::Effects {
            unchanged: provider.requests() == expected_requests
                && provider.remaining() == expected_remaining,
        })?;
    }
    Ok(())
}

#[test]
fn replayed_settle_follows_the_retained_successor_without_new_effects() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime");
    let bootstrap = runtime
        .block_on(bootstrap::Bootstrap::new())
        .expect("bootstrap snapshot");
    let oracle = Oracle::declare(move |input, fault, see| {
        runtime.block_on(run(&bootstrap, input, fault, see))
    })
    .invariant("retained-task", |event| match event {
        Seen::Task { actual, expected } if actual != expected => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .invariant("durable-head", |event| match event {
        Seen::Head { actual, expected } if actual != expected => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .invariant("model-effects", |event| match event {
        Seen::Effects { unchanged: false } => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .control(
        "retained-task",
        Case {
            completed: false,
            reopened: true,
            same_root: true,
        },
        Fault::WrongRoot,
    )
    .control(
        "durable-head",
        Case {
            completed: false,
            reopened: true,
            same_root: true,
        },
        Fault::ChargeReplay,
    )
    .control(
        "model-effects",
        Case {
            completed: false,
            reopened: true,
            same_root: true,
        },
        Fault::ExecuteReplay,
    )
    .build();
    // Exhaust every bounded state, rather than sampling histories or timing races.
    for completed in [false, true] {
        for reopened in [false, true] {
            for same_root in [false, true] {
                oracle.check(&Case {
                    completed,
                    reopened,
                    same_root,
                });
            }
        }
    }
}
