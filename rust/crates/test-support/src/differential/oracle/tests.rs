use super::{Builder, Controlled, Oracle};
use std::panic::{AssertUnwindSafe, catch_unwind};

// A two-step counter: each step records (observed, expected) for the running
// total and for the step count. Faults corrupt exactly the named aspect.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Fault {
    SkipAdd,
    SkipCount,
    Both,
    Cascade,
    Halt,
    DiscardStop,
    Panic,
    StopSilently,
    IgnoreStop,
    None,
}

#[derive(Debug)]
struct Step {
    total: (u32, u32),
    count: (u32, u32),
}

fn declare() -> Builder<Vec<u8>, Fault, Step, super::Uncontrolled> {
    Oracle::declare(|input: &Vec<u8>, fault: Option<&Fault>, see| {
        let (mut total, mut count, mut expected) = (0, 0, 0);
        for (index, &byte) in input.iter().enumerate() {
            expected += u32::from(byte);
            let last = index + 1 == input.len();
            match fault.copied() {
                Some(Fault::SkipAdd) if last => count += 1,
                Some(Fault::SkipCount) if last => total += u32::from(byte),
                Some(Fault::Both) if last => {}
                // Total breaks one step early; count breaks on the last step.
                Some(Fault::Cascade) if index + 2 == input.len() => count += 1,
                Some(Fault::Cascade) if last => total += u32::from(byte),
                Some(Fault::Halt) if last => {
                    see.see(Step {
                        total: (0, 1),
                        count: (index as u32 + 1, index as u32 + 1),
                    })?;
                    return Err(super::Stop);
                }
                Some(Fault::Panic) if last => panic!("fixture setup broke"),
                Some(Fault::StopSilently) if last => return Err(super::Stop),
                _ => {
                    total += u32::from(byte);
                    count += 1;
                }
            }
            let step = Step {
                total: (total, expected),
                count: (count, index as u32 + 1),
            };
            let ignore = matches!(fault, Some(Fault::IgnoreStop | Fault::DiscardStop));
            if ignore && last {
                let bad = || Step {
                    total: (0, 1),
                    count: (0, 0),
                };
                let _ = see.see(bad());
                if fault == Some(&Fault::IgnoreStop) {
                    let _ = see.see(bad());
                }
            } else {
                see.see(step)?;
            }
        }
        Ok(())
    })
    .invariant("total", |step| {
        (step.total.0 == step.total.1)
            .then_some(())
            .ok_or_else(|| format!("{:?}", step.total))
    })
    .invariant("count", |step| {
        (step.count.0 == step.count.1)
            .then_some(())
            .ok_or_else(|| format!("{:?}", step.count))
    })
}

fn controlled(controls: &[(&'static str, Fault)]) -> Builder<Vec<u8>, Fault, Step, Controlled> {
    let (first, rest) = controls.split_first().unwrap();
    let mut builder = declare().control(first.0, vec![1, 2, 3], first.1);
    for (target, fault) in rest {
        builder = builder.control(target, vec![4, 5], *fault);
    }
    builder
}

fn rejected(controls: &[(&'static str, Fault)], expected: &str) {
    let panic = catch_unwind(AssertUnwindSafe(|| {
        controlled(controls).build().check(&vec![7, 8]);
    }))
    .expect_err("oracle accepted a broken control set");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or_default();
    assert!(message.contains(expected), "{message}");
}

#[test]
fn targeted_controls_and_healthy_cases_pass() {
    let oracle = controlled(&[("total", Fault::SkipAdd), ("count", Fault::SkipCount)]).build();
    for case in [vec![0], vec![1, 2, 3, 4], vec![255; 16]] {
        oracle.check(&case);
    }
}

#[test]
fn a_control_also_caught_at_its_exposing_event_fails_deletion() {
    rejected(
        &[("total", Fault::Both), ("count", Fault::SkipCount)],
        "still detected after deleting total",
    );
}

#[test]
fn later_consequences_or_a_halt_after_deletion_are_allowed() {
    for fault in [Fault::Cascade, Fault::Halt] {
        let oracle = controlled(&[("total", fault), ("count", Fault::SkipCount)]).build();
        oracle.check(&vec![9]);
    }
}

#[test]
fn a_surviving_mutant_or_wrong_target_is_rejected() {
    rejected(
        &[("total", Fault::None), ("count", Fault::SkipCount)],
        "not detected by its target",
    );
    rejected(
        &[("total", Fault::SkipCount), ("count", Fault::SkipAdd)],
        "not detected by its target",
    );
}

#[test]
fn panics_and_silent_or_ignored_stops_are_infrastructure_failures() {
    for (fault, expected) in [
        (
            Fault::Panic,
            "infrastructure failure, not a detection: fixture setup broke",
        ),
        (Fault::StopSilently, "stopped without a violation"),
        (Fault::IgnoreStop, "fixture continued after Stop"),
        (Fault::DiscardStop, "fixture discarded Stop"),
    ] {
        rejected(&[("total", fault), ("count", Fault::SkipCount)], expected);
    }
}

#[test]
fn every_invariant_needs_a_known_control() {
    rejected(
        &[("total", Fault::SkipAdd)],
        "every invariant needs a control",
    );
    rejected(
        &[
            ("total", Fault::SkipAdd),
            ("count", Fault::SkipCount),
            ("missing", Fault::SkipAdd),
        ],
        "every invariant needs a control",
    );
}

#[test]
fn a_failing_healthy_case_and_zero_selection_are_rejected() {
    let oracle = controlled(&[("total", Fault::SkipAdd), ("count", Fault::SkipCount)]).build();
    assert!(catch_unwind(AssertUnwindSafe(|| oracle.check(&Vec::new()))).is_err());
    std::mem::forget(oracle);
    let unused = controlled(&[("total", Fault::SkipAdd), ("count", Fault::SkipCount)]).build();
    let panic = catch_unwind(AssertUnwindSafe(|| drop(unused))).expect_err("zero cases passed");
    assert_eq!(
        panic.downcast_ref::<&str>().copied(),
        Some("oracle selected zero healthy cases")
    );
}
