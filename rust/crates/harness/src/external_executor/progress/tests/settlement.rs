//! The production settlement step and follow share the instrumented provider.
use super::*;
use crate::external_executor::settlement::{self, Slot};

const BUSY: u8 = 0;
const IDLE: u8 = 1;
const QUEUED: u8 = 2;
const SETTLED: u8 = 3;

fn model(restarted: bool, missing_settlement: bool) {
    shuttle::check_dfs(
        move || {
            future::block_on(async {
                // One provider-state mutex; never held across an await or wake.
                let state = Arc::new(Mutex::new(State::default()));
                let producer = state.clone();
                let exit = future::spawn(async move {
                    future::yield_now().await;
                    let wake = {
                        let mut state = producer.lock().unwrap();
                        state.ready = true;
                        // A restarted owner can find an already accepted wake.
                        state.session_phase = if restarted { QUEUED } else { IDLE };
                        state.revision += 1;
                        state.wake.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                });
                let advance = || async {
                    future::yield_now().await;
                    let mut state = state.lock().unwrap();
                    if state.session_phase == QUEUED {
                        state.session_phase = SETTLED;
                    }
                    Ok(())
                };
                let inspect = || async {
                    future::yield_now().await;
                    Ok(match state.lock().unwrap().session_phase {
                        SETTLED => Slot::Settled,
                        IDLE => Slot::Idle(1),
                        BUSY | QUEUED => Slot::Busy,
                        _ => unreachable!(),
                    })
                };
                let resume = |revision| {
                    let state = &state;
                    async move {
                        assert_eq!(revision, 1);
                        future::yield_now().await;
                        let mut state = state.lock().unwrap();
                        assert_eq!(state.session_phase, IDLE);
                        state.session_phase = QUEUED;
                        Ok(())
                    }
                };
                let result = follow(
                    (),
                    |cursor| {
                        let (state, advance, inspect, resume) =
                            (&state, &advance, &inspect, &resume);
                        async move {
                            if !state.lock().unwrap().ready {
                                return Ok((cursor, None));
                            }
                            let settled = if missing_settlement {
                                // Old ordering: only an idle slot gets driven. An
                                // accepted resume is busy and no new wake can arrive.
                                match inspect().await? {
                                    Slot::Settled => true,
                                    Slot::Busy => false,
                                    Slot::Idle(revision) => {
                                        resume(revision).await?;
                                        advance().await?;
                                        matches!(inspect().await?, Slot::Settled)
                                    }
                                }
                            } else {
                                settlement::step(advance, inspect, resume).await?
                            };
                            Ok((cursor, settled.then_some(())))
                        }
                    },
                    async {
                        let cursor = state.lock().unwrap().revision;
                        future::yield_now().await;
                        Ok(Changes {
                            state: state.clone(),
                            cursor,
                        })
                    },
                    std::future::pending(),
                )
                .await
                .unwrap();
                assert_eq!(result, Some(()));
                assert_eq!(state.lock().unwrap().session_phase, SETTLED);
                exit.await.unwrap();
            });
        },
        None,
    );
}

#[test]
fn custody_exit_settles_fresh_and_retained_session_inputs() {
    for restarted in [false, true] {
        model(restarted, false);
    }
}

#[test]
#[should_panic(expected = "deadlock")]
fn negative_control_detects_a_missing_retained_input_settlement() {
    model(true, true);
}
