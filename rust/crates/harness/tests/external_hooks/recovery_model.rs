use super::{
    intake_model::{claim, consumer, publish, suspend},
    session_runtime::{Fixture, VolumeBinding},
    *,
};
use acyclic_harness::{TaskId, task_volume::Component};
use proptest::prelude::*;
use std::sync::Arc;

#[path = "../../src/local/external/recovery.rs"]
mod recovery;

#[test]
fn competing_recoveries_settle_the_retained_slot_before_delivery() {
    model(false, true, false);
}

#[test]
#[should_panic(expected = "reply exposed an unfinished workflow input")]
fn negative_control_detects_returning_a_result_without_recovery() {
    model(true, true, false);
}

#[test]
fn an_uncached_request_recovers_the_previous_accepted_input() {
    model(false, false, false);
}

#[test]
#[should_panic(expected = "reply exposed an unfinished workflow input")]
fn negative_control_detects_classifying_an_accepted_input_as_idle() {
    model(false, false, true);
}

fn model(broken: bool, answered: bool, idle: bool) {
    // Cached and uncached requests must settle the same retained input. The
    // truth table covers flag projection; each model explores the competing
    // settlements and the negative controls leave the original input occupied.
    let initial = false;
    shuttle::check_dfs(
        move || {
            shuttle::future::block_on(Box::pin(async move {
                let fixture = Arc::new(Fixture::new(VolumeBinding::Separate).await.unwrap());
                let task = TaskId::from_bytes([81; 16]);
                let folder = Component::new(task.to_string()).unwrap();
                let start = request(
                    ExternalHost::Pi,
                    "session_start",
                    json!({"sessionId":"session", "cwd":"/project"}),
                )
                .unwrap();
                let file = publish(&fixture, &folder, &start).await;
                consumer(&fixture, fixture.publisher.as_ref())
                    .accept(&folder, &file)
                    .await
                    .unwrap();
                let fence = claim(&fixture, task).await;
                suspend(&fixture, task, &fence, 2).await;
                let pending = request(
                    ExternalHost::Pi,
                    "tool_call",
                    json!({
                        "sessionId":"session", "toolCallId":"pending", "toolName":"write",
                        "input":{"path":"note", "content":"pending"}
                    }),
                )
                .unwrap();
                let file = publish(&fixture, &folder, &pending).await;
                consumer(&fixture, fixture.publisher.as_ref())
                    .accept(&folder, &file)
                    .await
                    .unwrap();
                let fence = claim(&fixture, task).await;
                let (revision, _) = fixture
                    .runtime
                    .task_host()
                    .workflow_input(task, &fence, None)
                    .await
                    .unwrap()
                    .unwrap();
                let mut calls = Vec::new();
                // A cached attach, a cached tool result, and the next uncached tool
                // all have to settle the old slot before exposing a reply/admission.
                for _ in 0..2 {
                    let fixture = fixture.clone();
                    let fence = fence.clone();
                    let revision = revision + 1;
                    calls.push(shuttle::future::spawn(async move {
                        recovery::recover_before_delivery(initial, answered, idle, || async {
                            shuttle::future::yield_now().await;
                            if !broken {
                                fixture
                                    .runtime
                                    .task_host()
                                    .suspend_workflow(
                                        task,
                                        &fence,
                                        (revision, None),
                                        None,
                                        None,
                                        &mut None,
                                    )
                                    .await?;
                            }
                            Ok(())
                        })
                        .await?;
                        Ok::<_, Error>(())
                    }));
                }
                let mut settled = 0;
                for call in calls {
                    if call.await.unwrap().is_ok() {
                        settled += 1;
                    }
                }
                assert!(settled > 0);
                let next = request(
                    ExternalHost::Pi,
                    "tool_call",
                    json!({
                        "sessionId":"session", "toolCallId":"next", "toolName":"write",
                        "input":{"path":"note", "content":"next"}
                    }),
                )
                .unwrap();
                let file = publish(&fixture, &folder, &next).await;
                let accepted = consumer(&fixture, fixture.publisher.as_ref())
                    .accept(&folder, &file)
                    .await;
                assert!(
                    accepted.is_ok(),
                    "reply exposed an unfinished workflow input: {accepted:?}"
                );
            }));
        },
        None,
    );
}

#[test]
fn every_request_state_selects_exactly_the_required_recovery_branch() {
    use std::cell::Cell;
    for initial in [false, true] {
        for answered in [false, true] {
            for idle in [false, true] {
                let called = Cell::new(0);
                futures::executor::block_on(recovery::recover_before_delivery(
                    initial,
                    answered,
                    idle,
                    || async {
                        called.set(called.get() + 1);
                        Ok(())
                    },
                ))
                .unwrap();
                assert_eq!(called.get(), usize::from(answered || (!initial && !idle)));
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Flag projection preserves every recovery failure, rather than replying
    // from a result whose workflow settlement is still uncertain.
    #[test]
    fn recovery_failure_is_returned_for_every_recovery_state(
        initial in any::<bool>(), answered in any::<bool>(), idle in any::<bool>(), message in ".{0,128}",
    ) {
        let answer = futures::executor::block_on(recovery::recover_before_delivery(initial, answered, idle, || async {
            Err(Error::Storage(message.clone()))
        }));
        match answer {
            Err(Error::Storage(actual)) => {
                prop_assert!(answered || (!initial && !idle));
                prop_assert_eq!(actual, message);
            }
            Ok(()) => prop_assert!(!answered && (initial || idle)),
            other => prop_assert!(false, "unexpected recovery result: {other:?}"),
        }
    }
}
