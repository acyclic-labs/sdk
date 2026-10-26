use super::*;
use proptest::prelude::*;

mod active;
mod orphans;
mod recovery;

fn file(reference: &Component, kind: ChildFile, body: Vec<u8>) -> FileValue {
    FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: kind,
        },
        body,
    )
}

fn stop_handoffs(task: TaskId, ignore_stop_at_release: bool, assert_property: bool) {
    let reference = Component::new("worker").unwrap();
    // Completion callback, accepted completion follow, and ancestor stop are
    // independent. Exhaust all six orders through the production predicates.
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let status = ChildStatus {
            task,
            state: ChildState::Running,
            settled: false,
            inputs: 1,
        };
        let mut values = BTreeMap::new();
        for value in [
            file(&reference, ChildFile::Request, b"work".to_vec()),
            file(
                &reference,
                ChildFile::Status,
                serde_json::to_vec(&status).unwrap(),
            ),
            file(
                &reference,
                ChildFile::Result(1),
                serde_json::to_vec(&crate::task_volume::ChildTurnResult {
                    task,
                    turn: 1,
                    outcome: crate::Outcome::Cancelled,
                })
                .unwrap(),
            ),
        ] {
            values.insert(value.key.clone(), value);
        }
        let mut active = BTreeSet::from([reference.clone()]);
        let mut completions = BTreeSet::new();
        let (mut position, mut stopping, mut scheduled) = (9, false, 0);
        for event in order {
            match event {
                0 => {
                    completions.insert((10, reference.clone()));
                }
                1 => {
                    position = 10;
                    let done = file(
                        &reference,
                        ChildFile::Status,
                        serde_json::to_vec(&ChildStatus {
                            settled: true,
                            ..status
                        })
                        .unwrap(),
                    );
                    values.insert(done.key.clone(), done);
                }
                _ => stopping = true,
            }
            let ready = release_completed(position, &mut active, &mut completions);
            if !active.contains(&reference)
                && (event == 2 || ready.contains(&reference))
                && needs_work(
                    &reference,
                    &values,
                    stopping && !(ignore_stop_at_release && event != 2),
                )
                .unwrap()
            {
                active.insert(reference.clone());
                scheduled += 1;
            }
        }
        if assert_property {
            assert_eq!(
                scheduled, 1,
                "ancestor stop lost at completion handoff: {order:?}"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn ancestor_stop_survives_every_completion_handoff(task in any::<[u8;16]>()) {
        stop_handoffs(TaskId::from_bytes(task), false, true);
    }

    #[test]
    fn resume_acknowledges_only_real_completed_receive_checkpoints(turns in 1u64..9) {
        use crate::workflow::{MachineCheckpoint, MachineRegistry, ResumableMachine};
        use std::borrow::Cow;
        let seed: serde_json::Value = serde_json::from_slice(include_bytes!("../../../../../harness-fuzz/corpus/child_control/child-agent-turn")).unwrap();
        let machine = Arc::new(crate::filesystem::ChildAgentMachine::new());
        let mut registry = MachineRegistry::default();
        registry.register(machine.clone(), &mut None).unwrap();
        let initial = MachineCheckpoint { machine: machine.identity().clone(), revision: 0, state: machine.initialize(&seed["initial"]).unwrap() };
        let mut checkpoint = registry.step(&initial, Cow::Owned(serde_json::Value::Null), None).unwrap().0;
        let task: TaskId = serde_json::from_value(seed["input"]["commands"][0]["value"]["Succeeded"]["task_id"].clone()).unwrap();
        for inputs in 1..=turns {
            let status = Some(ChildStatus { task, state: ChildState::Paused, settled: true, inputs });
            prop_assert!(!settled_turn(status, task, inputs, Some(checkpoint.revision)));
            let mut incoming = seed["input"].clone();
            incoming["commands"][0]["operation_id"] = checkpoint.state["receive"]["operation_id"].clone();
            incoming["commands"][0]["value"]["Succeeded"]["sequence"] = serde_json::json!(inputs);
            checkpoint = registry.step(&checkpoint, Cow::Owned(incoming), None).unwrap().0;
            prop_assert!(!settled_turn(status, task, inputs, Some(checkpoint.revision)));
            let mut completed = seed["model_result"].clone();
            completed["commands"][0]["operation_id"] = checkpoint.state["model"]["operation_id"].clone();
            checkpoint = registry.step(&checkpoint, Cow::Owned(completed), None).unwrap().0;
            prop_assert!(settled_turn(status, task, inputs, Some(checkpoint.revision)));
            prop_assert_eq!(checkpoint.state["stage"].as_str(), Some("receive"));
        }
    }

    #[test]
    fn lifecycle_only_resume_never_replays_a_settled_turn(
        task in any::<[u8;16]>(), inputs in 1u64..u64::MAX / 2,
    ) {
        let task = TaskId::from_bytes(task);
        let revision = Some(inputs * 2 + 1);
        prop_assert!(!settled_turn(None, task, inputs, revision));
        for state in [ChildState::Running, ChildState::Paused, ChildState::Interrupted, ChildState::Cancelled, ChildState::Archived] {
            let status = ChildStatus { task, state, settled: true, inputs };
            prop_assert_eq!(settled_turn(Some(status), task, inputs, revision), matches!(state, ChildState::Running | ChildState::Paused | ChildState::Interrupted));
            for invalid in [None, Some(1), Some(inputs * 2)] {
                prop_assert!(!settled_turn(Some(status), task, inputs, invalid));
            }
            prop_assert!(!settled_turn(Some(status), task, inputs + 1, revision));
            prop_assert!(!settled_turn(Some(status), task, u64::MAX, revision));
            let unfinished = ChildStatus { settled: false, ..status };
            prop_assert!(!settled_turn(Some(unfinished), task, inputs, revision));
            let mut other = task.into_bytes(); other[0] ^= 1;
            prop_assert!(!settled_turn(Some(status), TaskId::from_bytes(other), inputs, revision));
        }
    }

    // Exhaust the callback before, between, and after the two ordered follow
    // records. Only the follow may update cached files; completion releases its
    // worker only after the accepted status cursor has been observed.
    #[test]
    fn completed_jobs_never_hide_new_inputs(prefix in 1u64..9, suffix in 1u64..9) {
        let reference = Component::new("worker").unwrap();
        let task = TaskId::from_bytes([8;16]);
        for callback_at in 0..3 {
            let mut values = BTreeMap::new();
            let request = file(&reference, ChildFile::Request, b"work".to_vec());
            values.insert(request.key.clone(), request);
            for sequence in 1..prefix {
                let value = file(&reference, ChildFile::Inbox(Component::new(format!("message-{sequence}")).unwrap()), vec![1]);
                values.insert(value.key.clone(), value);
            }
            let status = ChildStatus { task, state: ChildState::Running, settled: false, inputs: prefix };
            let started = file(&reference, ChildFile::Status, serde_json::to_vec(&status).unwrap());
            values.insert(started.key.clone(), started);
            let result = file(&reference, ChildFile::Result(prefix), serde_json::to_vec(&crate::task_volume::ChildTurnResult {
                task, turn: prefix, outcome: crate::Outcome::Cancelled,
            }).unwrap());
            values.insert(result.key.clone(), result);
            let mut active = BTreeSet::from([reference.clone()]);
            let mut completions = BTreeSet::new();
            let mut position = 9;
            let mut scheduled = 0;
            let mut followed = 0;
            for step in 0..3 {
                let input_changed = if step == callback_at {
                    completions.insert((10, reference.clone()));
                    false
                } else {
                    followed += 1;
                    position += 1;
                    if followed == 1 {
                        let done = file(&reference, ChildFile::Status, serde_json::to_vec(&ChildStatus { settled: true, ..status }).unwrap());
                        values.insert(done.key.clone(), done);
                        false
                    } else {
                        for sequence in prefix..prefix+suffix {
                            let value = file(&reference, ChildFile::Inbox(Component::new(format!("message-{sequence}")).unwrap()), vec![2]);
                            values.insert(value.key.clone(), value);
                        }
                        true
                    }
                };
                let ready = release_completed(position, &mut active, &mut completions);
                if (input_changed || ready.contains(&reference))
                    && !active.contains(&reference) && needs_work(&reference, &values, false).unwrap() {
                    scheduled += 1;
                    active.insert(reference.clone());
                }
            }
            prop_assert!(completions.is_empty());
            prop_assert_eq!(scheduled, 1);
            prop_assert_eq!(input_count(&reference, &values), prefix + suffix);
        }
    }
}

#[test]
fn ancestor_stop_handoff_negative_control_requires_its_property_assertion() {
    let task = TaskId::from_bytes([8; 16]);
    std::panic::catch_unwind(|| stop_handoffs(task, true, true))
        .expect_err("dropping ancestor stop at worker release was not detected");
    // The negative control must cease failing if its property is deleted.
    stop_handoffs(task, true, false);
}

mod stopped;

#[cfg(unix)]
mod errors;

#[cfg(unix)]
mod turn_failure;

fn completion_work_bound(scan_unrelated: bool) {
    let reference = |n: u64| Component::new(format!("worker-{n:06}")).unwrap();
    let mut completions = (0..100_000)
        .map(|n| (n + 100, reference(n)))
        .collect::<BTreeSet<_>>();
    let mut active = completions
        .iter()
        .map(|(_, reference)| reference.clone())
        .collect::<BTreeSet<_>>();
    let (ready, trace) = acyclic_test_support::differential::observe::<_, _, (&str, u64)>(
        || (),
        |_| {
            if scan_unrelated {
                // Restore the old per-record full completion scan.
                let ready = completions
                    .iter()
                    .filter(|(accepted, _)| {
                        acyclic_test_support::differential::record((
                            "child-completion-examined",
                            1u64,
                        ));
                        *accepted <= 102
                    })
                    .map(|(_, reference)| reference.clone())
                    .collect::<BTreeSet<_>>();
                for reference in &ready {
                    active.remove(reference);
                }
                completions.retain(|(accepted, _)| *accepted > 102);
                ready
            } else {
                release_completed(102, &mut active, &mut completions)
            }
        },
    );
    assert_eq!(ready, (0..3).map(reference).collect());
    assert_eq!(active.len(), 99_997);
    assert_eq!(completions.len(), 99_997);
    let examined: u64 = trace
        .iter()
        .filter(|(name, _)| *name == "child-completion-examined")
        .map(|(_, work)| work)
        .sum();
    assert!(
        examined <= 4,
        "completion release visited unrelated children"
    );
}

#[test]
fn completion_release_visits_only_the_observed_prefix_with_one_hundred_thousand_children() {
    completion_work_bound(false);
}

#[test]
#[should_panic(expected = "completion release visited unrelated children")]
fn negative_control_scanning_all_completions_exceeds_the_transition_work_bound() {
    completion_work_bound(true);
}
