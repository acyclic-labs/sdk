//! Dedicated command: cargo rustc --test native_stage_loom ... -- --cfg acyclic_fs_loom.
#![cfg(acyclic_fs_loom)]

#[path = "../src/materializer/stage_state.rs"]
mod stage_state;
#[path = "../src/sync.rs"]
mod sync;

use loom::sync::{Arc, Mutex};
use stage_state::StageState;
use std::future::{Ready, ready};

// The external journal store's atomic acceptance contract: owner tail plus
// journal tail are checked together. Cancellation is monotonic. Physical I/O
// occurs between acceptance and completion; invalidation there is uncertain,
// never permission to acknowledge completion or start another physical action.
#[derive(Default)]
struct Store {
    cancelled: bool,
    owner: u8,
    journal: u8,
    accepted: bool,
    physical: u8,
    acknowledged: u8,
}

#[test]
fn staging_checks_invalidation_before_acceptance_and_before_the_next_action() {
    check(false);
}

#[test]
#[should_panic(expected = "stale staging fence accepted")]
fn removing_the_store_fence_is_detected() {
    check(true);
}

fn check(broken_fence: bool) {
    for changed in 0..3 {
        loom::model(move || {
            // The environment lock models the external atomic journal store;
            // StageState's production lock is independently loom-instrumented.
            // Neither lock may enclose the other. Callbacks reacquire the phase
            // lock through another method to detect accidental lock nesting.
            let store = Arc::new(Mutex::new(Store::default()));
            let owned_state = StageState::<u8, Ready<Result<u8, ()>>>::new(0);
            let state = &owned_state;
            let writer = {
                let store = Arc::clone(&store);
                loom::thread::spawn(move || {
                    let mut store = store.lock().unwrap();
                    match changed {
                        0 => store.cancelled = true,
                        1 => store.owner += 1,
                        _ => store.journal += 1,
                    }
                })
            };
            let mut actions = 0;
            for _ in 0..2 {
                let authorize = async {
                    state.assert_unlocked();
                    let store = store.lock().unwrap();
                    if store.cancelled || store.owner != 0 {
                        Err(())
                    } else {
                        Ok(())
                    }
                };
                let acceptance_store = &store;
                let accepted = futures::executor::block_on(state.before(
                    authorize,
                    |revision| async move {
                        state.assert_unlocked();
                        let mut store = acceptance_store.lock().unwrap();
                        let current =
                            !store.cancelled && store.owner == 0 && store.journal == revision;
                        if !current && !broken_fence {
                            return Err(());
                        }
                        assert!(current, "stale staging fence accepted");
                        assert!(!store.accepted);
                        store.accepted = true;
                        Ok(ready(Ok(revision)))
                    },
                    || (),
                ));
                if accepted.is_err() {
                    break;
                }
                {
                    let mut store = store.lock().unwrap();
                    assert!(store.accepted, "physical action preceded acceptance");
                    store.physical += 1;
                    actions += 1;
                }
                let completed = futures::executor::block_on(state.completed(
                    |completion| async {
                        state.assert_unlocked();
                        let revision = completion.await?;
                        let mut store = store.lock().unwrap();
                        if store.cancelled || store.owner != 0 || store.journal != revision {
                            return Err(());
                        }
                        store.journal += 1;
                        store.accepted = false;
                        store.acknowledged += 1;
                        Ok(store.journal)
                    },
                    || (),
                ));
                if completed.is_err() {
                    break;
                }
            }
            let (journal, completion, uncertain) = owned_state.into_inner();
            assert!(completion.is_none());
            let store = store.lock().unwrap();
            assert_eq!(store.physical, actions);
            if uncertain {
                assert!(journal.is_none());
                assert_eq!(store.physical, store.acknowledged + 1);
            } else {
                assert_eq!(store.physical, store.acknowledged);
            }
            drop(store);
            writer.join().unwrap();
        });
    }
}
