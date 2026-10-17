//! The provider's atomic cursor and push are the contract at this boundary;
//! Shuttle instruments every state lock and wake in this small provider.
use super::*;
use shuttle::{
    future,
    sync::{Arc, Mutex},
};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct State {
    ready: bool,
    revision: u64,
    wake: Option<Waker>,
    session_phase: u8,
}

mod native_files;
mod settlement;
struct Changes {
    state: Arc<Mutex<State>>,
    cursor: u64,
}

impl Stream for Changes {
    type Item = Result<()>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let revision = {
            let mut state = self.state.lock().unwrap();
            if self.cursor == state.revision {
                state.wake = Some(cx.waker().clone());
                return Poll::Pending;
            }
            state.revision
        };
        self.cursor = revision;
        Poll::Ready(Some(Ok(())))
    }
}

fn model(broken: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                // Lock order: sole provider-state lock. Drop before yielding/waking.
                let state = Arc::new(Mutex::new(State::default()));
                let writer = state.clone();
                let producer = future::spawn(async move {
                    future::yield_now().await;
                    let wake = {
                        let mut state = writer.lock().unwrap();
                        state.ready = true;
                        state.revision += 1;
                        state.wake.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                });
                let step = || async {
                    let ready = state.lock().unwrap().ready;
                    future::yield_now().await;
                    Ok::<_, Error>(ready.then_some(7))
                };
                let subscribe = async {
                    let cursor = state.lock().unwrap().revision;
                    future::yield_now().await;
                    Ok(Changes {
                        state: state.clone(),
                        cursor,
                    })
                };
                let result = if broken {
                    // Mutation of the real algorithm: omit only its second check.
                    match step().await.unwrap() {
                        Some(value) => Some(value),
                        None => {
                            let mut changes = subscribe.await.unwrap();
                            changes.next().await.unwrap().unwrap();
                            step().await.unwrap()
                        }
                    }
                } else {
                    follow(
                        (),
                        |cursor| {
                            let step = &step;
                            async move { Ok((cursor, step().await?)) }
                        },
                        subscribe,
                        std::future::pending(),
                    )
                    .await
                    .unwrap()
                };
                assert_eq!(result, Some(7));
                producer.await.unwrap();
            });
        },
        None,
    );
}

#[test]
fn passive_completion_between_snapshot_and_follow_cannot_be_lost() {
    model(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_detects_omitting_the_second_read() {
    model(true);
}

#[test]
fn shutdown_does_not_drop_an_active_step() {
    shuttle::check_dfs(
        || {
            future::block_on(async {
                let finished = Arc::new(Mutex::new(false));
                let result = follow(
                    (),
                    |cursor| {
                        let finished = &finished;
                        async move {
                            future::yield_now().await;
                            *finished.lock().unwrap() = true;
                            Ok::<_, crate::Error>((cursor, None::<()>))
                        }
                    },
                    async { Ok(futures::stream::pending::<Result<()>>()) },
                    async { Ok(()) },
                )
                .await
                .unwrap();
                assert!(finished.lock().unwrap().to_owned());
                assert_eq!(result, None);
            })
        },
        None,
    );
}

fn revision_model(broken: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                let state = Arc::new(Mutex::new(State::default()));
                let writer = state.clone();
                let producer = future::spawn(async move {
                    future::yield_now().await;
                    let wake = {
                        let mut state = writer.lock().unwrap();
                        state.ready = true;
                        state.revision += 1;
                        state.wake.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                });
                let step = |previous| {
                    let state = &state;
                    async move {
                        let revision = state.lock().unwrap().revision;
                        let (cursor, result) = at_revision(previous, Some(revision), || async {
                            let ready = state.lock().unwrap().ready;
                            future::yield_now().await;
                            Ok(ready.then_some(7))
                        })
                        .await?;
                        // Broken sampling after the work can consume a change that
                        // arrived after its final read, losing the subsequent push.
                        let cursor = if broken {
                            Some(state.lock().unwrap().revision)
                        } else {
                            cursor
                        };
                        Ok((cursor, result))
                    }
                };
                let subscribe = async {
                    let cursor = state.lock().unwrap().revision;
                    future::yield_now().await;
                    Ok(Changes {
                        state: state.clone(),
                        cursor,
                    })
                };
                assert_eq!(
                    follow(None, step, subscribe, std::future::pending())
                        .await
                        .unwrap(),
                    Some(7)
                );
                producer.await.unwrap();
            });
        },
        None,
    );
}

#[test]
fn a_change_during_work_is_not_consumed_by_the_processed_revision() {
    revision_model(false);
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_detects_sampling_the_processed_revision_after_work() {
    revision_model(true);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    // Exactly the already processed revision is skipped; all new revisions
    // execute once and retain their pre-work cursor and error identity.
    #[test]
    fn only_the_processed_revision_can_skip_work(
        previous in proptest::option::of(proptest::prelude::any::<u64>()),
        observed in proptest::option::of(proptest::prelude::any::<u64>()), same in proptest::prelude::any::<bool>(),
    ) {
        let observed = if same { previous } else { observed };
        let advance = observed.is_none() || previous != observed;
        let called = std::cell::Cell::new(0);
        let result = futures::executor::block_on(at_revision(previous, observed, || async {
            called.set(called.get() + 1);
            Ok(Some(7))
        })).unwrap();
        proptest::prop_assert_eq!(called.get(), usize::from(advance));
        proptest::prop_assert_eq!(result, (observed, advance.then_some(7)));
        let result = futures::executor::block_on(at_revision(previous, observed, || async {
            Err::<Option<()>, _>(crate::Error::Conflict("retained failure".into()))
        }));
        proptest::prop_assert!(match result {
            Ok((cursor,None)) => !advance && cursor == previous,
            Err(crate::Error::Conflict(message)) => advance && message == "retained failure",
            _ => false,
        }, "revision shortcut changed the retained error or cursor");
    }
}
