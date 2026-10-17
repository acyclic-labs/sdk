//! Model the production select/settlement boundary. Process completion and
//! provider follows are atomic contracts; their internal locks are not modeled.
use super::*;
use shuttle::{
    future,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    cancelled: bool,
    reaped: bool,
    settled: bool,
}

#[test]
fn cancellation_and_follow_failure_both_await_physical_cleanup() {
    for failed_follow in [false, true] {
        model(false, failed_follow);
    }
}

#[test]
#[should_panic(expected = "settled while the native process was still active")]
fn negative_control_detects_dropping_execution_before_cleanup() {
    model(true, false);
}

fn model(broken: bool, failed_follow: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                // Model lock order: this is the only effect-state lock. Every
                // guard retires before yielding, awaiting, or polling a future.
                let state = Arc::new(Mutex::new(State::default()));
                let process_state = state.clone();
                let mut process = future::spawn(async move {
                    future::yield_now().await;
                    {
                        let state = process_state.lock().unwrap();
                        if !state.cancelled {
                            assert!(!state.settled, "native effect after settlement");
                        }
                    }
                    future::yield_now().await;
                    process_state.lock().unwrap().reaped = true;
                });
                // The physical job remains independently owned when this async
                // waiter is dropped, as with run_blocking_io in the checkout path.
                let execution = async {
                    (&mut process).await.unwrap();
                    Ok(())
                };
                let cancelled = async {
                    future::yield_now().await;
                    if failed_follow {
                        Err(crate::Error::Storage("follow ended".into()))
                    } else {
                        Ok(())
                    }
                };
                let stop = || state.lock().unwrap().cancelled = true;
                let result = if broken {
                    tokio::pin!(execution, cancelled);
                    // Deliberately broken variant: cancellation abandons the
                    // waiter and returns without joining the physical job.
                    tokio::select! {
                        biased;
                        _ = &mut cancelled => {stop(); Ok(((), Some(())))}
                        result = &mut execution => result.map(|value| (value, None)),
                    }
                } else {
                    settle(execution, cancelled, stop).await
                };
                {
                    let mut state = state.lock().unwrap();
                    state.settled = true;
                    assert!(
                        state.reaped,
                        "settled while the native process was still active"
                    );
                    if state.cancelled {
                        assert_eq!(result.is_err(), failed_follow);
                    }
                }
            });
        },
        None,
    );
}
