use super::*;
use crate::kernel::{
    CommitFootprint, FileKind, FilePayload, FileRecord, FileRecordChange, GenerationDiff,
    InlineFileData,
};
use crate::{Digest, FileId, ObjectId, ObjectKind};
use loom::sync::{Arc, Mutex};

#[test]
fn provisional_metadata_receipt_crash_retry_uses_the_production_admission_state() {
    use super::super::{Admission, AdmissionState, BatchAnchor};
    for correct in [false, true] {
        loom::model(move || {
            let expected = super::super::tests::anchor();
            let mut actual = expected;
            if !correct {
                actual.head.digest = Digest::ZERO;
            }
            let shared = Arc::new(Mutex::new((
                BatchAnchor::Provisional(expected),
                AdmissionState::Provisional(Admission {
                    anchor: expected,
                    cost: crate::AuthorityGroupCost::default(),
                }),
            )));
            let receipt = Arc::clone(&shared);
            let receipt = loom::thread::spawn(move || {
                let mut state = receipt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                assert!(state.0.committed().is_none());
                assert_eq!(state.0.commit(actual).is_ok(), correct);
                let durable = state.0.committed();
                if state.1.settle(durable) {
                    assert!(correct);
                }
            });
            let crash = Arc::clone(&shared);
            let crash = loom::thread::spawn(move || {
                let mut state = crash
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.1.ready().is_none() {
                    assert!(!state.1.settle(None));
                }
            });
            assert!(receipt.join().is_ok());
            assert!(crash.join().is_ok());
            let mut state = shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(state.1.ready().is_none() || correct);
            if state.1.ready().is_none() {
                state.1 = AdmissionState::Provisional(Admission {
                    anchor: expected,
                    cost: crate::AuthorityGroupCost::default(),
                });
                let durable = state.0.committed();
                assert_eq!(state.1.settle(durable), correct);
            }
        });
    }
}

fn delta(key: u8) -> GenerationDiff {
    let record = |value| FileRecord {
        file_id: FileId::from_bytes([key; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: ObjectId {
            storage: None,
            kind: ObjectKind::Metadata,
            digest: Digest::ZERO,
        },
        payload: FilePayload::InlineRegular(
            InlineFileData::new(&[value]).unwrap_or_else(|_| unreachable!("one inline byte fits")),
        ),
    };
    GenerationDiff {
        files: vec![FileRecordChange {
            file_id: record(0).file_id,
            before: Some(record(0)),
            after: Some(record(1)),
        }],
        bindings: Vec::new(),
        truncated: false,
    }
}

struct Model {
    state: State<usize, u8>,
    tree: [u8; 2],
    locators: [bool; 2],
    leaders: usize,
}

#[test]
fn selected_prefix_has_one_preparation_owner() {
    loom::model(|| {
        let state = Arc::new(Mutex::new(State::<usize, u8>::new(Vec::with_capacity(2))));
        let owners = Arc::new(loom::sync::atomic::AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let state = Arc::clone(&state);
            let owners = Arc::clone(&owners);
            workers.push(loom::thread::spawn(move || {
                if state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .own_preparation()
                {
                    owners.fetch_add(1, loom::sync::atomic::Ordering::SeqCst);
                }
            }));
        }
        for worker in workers {
            assert!(worker.join().is_ok());
        }
        assert_eq!(owners.load(loom::sync::atomic::Ordering::SeqCst), 1);
        let mut state = state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.abort();
        assert!(
            !state.own_preparation(),
            "crashed prefix owner was resurrected"
        );
    });
}

#[test]
fn pending_admission_and_collection_owner_withdrawal_preserve_the_survivor() {
    for withdrawn in 0..2 {
        loom::model(move || {
            let mut state = State::<Option<usize>, usize>::new(Vec::with_capacity(2));
            assert_eq!(state.enqueue(None), Ok(0));
            assert_eq!(state.enqueue(None), Ok(1));
            assert!(!state.can_collect(Option::is_some));
            state.close();
            assert!(!state.can_collect(Option::is_some));
            let state = Arc::new(Mutex::new(state));
            let departing = Arc::clone(&state);
            let departing = loom::thread::spawn(move || {
                departing
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .withdraw(Some(withdrawn));
            });
            let arriving = Arc::clone(&state);
            let arriving = loom::thread::spawn(move || {
                let mut state = arriving
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.members[1 - withdrawn].value = Some(1 - withdrawn);
                if state.can_collect(Option::is_some) {
                    state
                        .collect::<()>(|_, _| Ok(true), |value| Ok(value.is_some()))
                        .unwrap();
                }
            });
            departing.join().unwrap();
            arriving.join().unwrap();
            let mut state = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.can_collect(Option::is_some) {
                state
                    .collect::<()>(|_, _| Ok(true), |value| Ok(value.is_some()))
                    .unwrap();
            }
            assert_eq!(
                state.phase,
                Phase::Preparing,
                "pending withdrawal revoked a surviving proposal"
            );
            assert_eq!(
                state.selected().copied().collect::<Vec<_>>(),
                vec![Some(1 - withdrawn)]
            );
            assert!(matches!(state.plan(1 - withdrawn), Plan::Prefix(None)));
            assert!(state.planned(1 - withdrawn, 1));
            assert!(state.ready(1 - withdrawn));
            assert!(state.handoff());
            assert!(state.complete());
        });
    }
}

#[test]
fn owned_combiner_client_withdrawal_cannot_cancel_the_durability_owner() {
    loom::model(|| {
        let mut state = State::new(Vec::with_capacity(1));
        let index = state
            .enqueue(0)
            .unwrap_or_else(|_| unreachable!("valid owned-combiner model transition"));
        state
            .collect::<()>(|_, _| Ok(true), |_| Ok(true))
            .unwrap_or_else(|_| unreachable!("valid owned-combiner model transition"));
        assert!(state.planned(index, 1));
        assert!(state.ready(index));
        assert!(state.handoff());
        let state = Arc::new(Mutex::new(state));
        let client = Arc::clone(&state);
        let client = loom::thread::spawn(move || {
            client
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .withdraw(Some(index));
        });
        let owner = Arc::clone(&state);
        let owner = loom::thread::spawn(move || {
            assert!(
                owner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .complete(),
                "reply cancellation revoked the durability owner"
            );
        });
        client
            .join()
            .unwrap_or_else(|_| unreachable!("valid owned-combiner model transition"));
        owner
            .join()
            .unwrap_or_else(|_| unreachable!("valid owned-combiner model transition"));
        assert_eq!(
            state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .phase,
            Phase::Complete
        );
    });
}

fn model(overlap: bool, admit_overlap: bool) {
    loom::model(move || {
        let keys = [0, usize::from(!overlap)];
        let model = Arc::new(Mutex::new(Model {
            state: State::new(Vec::with_capacity(2)),
            tree: [0; 2],
            locators: [false; 2],
            leaders: 0,
        }));
        let mut enqueuers = Vec::new();
        for operation in 0..2 {
            let model = Arc::clone(&model);
            enqueuers.push(loom::thread::spawn(move || {
                // Each worker can lose enqueue to collection, seal, or crash.
                let Ok(index) = model
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .state
                    .enqueue(operation)
                else {
                    return;
                };
                {
                    let mut model = model.lock().unwrap_or_else(|error| error.into_inner());
                    if model.state.phase == Phase::Collecting {
                        model
                            .state
                            .collect(
                                |a, b| {
                                    let a = delta(u8::from(keys[*a] != 0));
                                    let b = delta(u8::from(keys[*b] != 0));
                                    Ok::<_, crate::WorkError>(
                                        admit_overlap
                                            || CommitFootprint {
                                                reads: None,
                                                writes: &a,
                                            }
                                            .independent(
                                                CommitFootprint {
                                                    reads: None,
                                                    writes: &b,
                                                },
                                                u64::MAX,
                                            )
                                            .independent?,
                                    )
                                },
                                |_| Ok(true),
                            )
                            .unwrap_or_else(|_| {
                                unreachable!("two one-record deltas fit the model budget")
                            });
                    }
                }
                {
                    let mut model = model.lock().unwrap_or_else(|error| error.into_inner());
                    if model.state.phase == Phase::Preparing
                        && matches!(model.state.plan(index), Plan::Prefix(_))
                    {
                        assert!(model.state.planned(index, u8::from(index != 0)));
                    }
                }
                {
                    let mut model = model.lock().unwrap_or_else(|error| error.into_inner());
                    model.leaders += usize::from(model.state.ready(index));
                    assert!(model.leaders <= 1, "sealing capability duplicated");
                }
                let mut model = model.lock().unwrap_or_else(|error| error.into_inner());
                if model.state.phase == Phase::Sealing {
                    assert_eq!(model.leaders, 1, "sealing capability lost");
                    let selected: Vec<_> = model.state.selected().copied().collect();
                    for operation in selected {
                        assert_eq!(
                            model.tree[keys[operation]], 0,
                            "overlapping precondition admitted into one seal"
                        );
                        assert!(!model.locators[operation], "operation published twice");
                        model.tree[keys[operation]] = 1;
                        model.locators[operation] = true;
                    }
                    assert!(model.state.handoff());
                    assert!(model.state.complete());
                    assert!(!model.state.complete(), "seal completed twice");
                }
            }));
        }
        // The root is the independent crash actor. Keeping only two worker
        // threads exposes enqueue, collect, plan, ready and seal as separate
        // transitions, including a crash between preparation and readiness.
        model
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .state
            .abort();
        for thread in enqueuers {
            assert!(thread.join().is_ok());
        }
        let mut model = model.lock().unwrap_or_else(|error| error.into_inner());
        if model.state.phase == Phase::Aborted {
            assert!(!model.state.complete(), "aborted round resurrected");
        }
        recover(&mut model, keys);
        for key in 0..2 {
            let committed = (0..2)
                .filter(|operation| keys[*operation] == key && model.locators[*operation])
                .count();
            assert_eq!(
                model.tree[key] as usize, committed,
                "batch differs from every valid serial history"
            );
        }
        assert_eq!(
            model.locators.iter().filter(|value| **value).count(),
            if overlap { 1 } else { 2 }
        );
    });
}

#[test]
fn combiner_enqueue_seal_crash_retry_is_serializable() {
    model(false, false);
    model(true, false);
}

#[test]
#[should_panic(expected = "overlapping precondition admitted into one seal")]
fn overlapping_admission_negative_control() {
    model(true, true);
}

fn recover(model: &mut Model, keys: [usize; 2]) {
    // Recovery uses the operation locator before evaluating the original
    // before-record. A lost acknowledgement cannot duplicate a member.
    for (operation, key) in keys.iter().copied().enumerate() {
        if !model.locators[operation] && model.tree[key] == 0 {
            let mut retry = State::new(Vec::with_capacity(2));
            let index = retry
                .enqueue(operation)
                .unwrap_or_else(|_| unreachable!("fresh retry has capacity"));
            retry
                .collect(
                    |a, b| {
                        CommitFootprint {
                            reads: None,
                            writes: &delta(u8::from(keys[*a] != 0)),
                        }
                        .independent(
                            CommitFootprint {
                                reads: None,
                                writes: &delta(u8::from(keys[*b] != 0)),
                            },
                            u64::MAX,
                        )
                        .independent
                    },
                    |_| Ok(true),
                )
                .unwrap_or_else(|_| unreachable!("one retry has no pair to compare"));
            assert!(matches!(retry.plan(index), Plan::Prefix(None)));
            assert!(retry.planned(index, 1u8));
            assert!(retry.ready(index));
            model.tree[key] = 1;
            model.locators[operation] = true;
            assert!(retry.handoff());
            assert!(retry.complete());
            assert!(
                retry.enqueue(operation).is_err(),
                "a sealed round accepted a retry"
            );
        }
    }
}
