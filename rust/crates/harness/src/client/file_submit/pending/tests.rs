//! Full input-derived queue histories, through one oracle and its real faults.
mod controls;
mod faults;
mod fixture;
mod observation;
mod reference;

use faults::Fault;
pub(super) use faults::{admitted, checked, retired};
use fixture::Input;
use proptest::{
    prelude::*,
    test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner},
};

type Action = Option<(u8, u8)>;
const COMMANDS: usize = 3;
const BYTES: u64 = 12;

fn actions() -> impl Strategy<Value = Vec<Action>> {
    proptest::collection::vec(proptest::option::of((0_u8..6, 0_u8..9)), 0..24)
}

fn config(name: &'static str) -> Config {
    Config {
        source_file: Some(file!()),
        test_name: Some(name),
        ..Config::with_cases(256)
    }
}

#[test]
fn admission_and_retirement_move_entry_identity_and_bytes_together() {
    let oracle = controls::oracle();
    TestRunner::new(config(concat!(
        module_path!(),
        "::admission_and_retirement_move_entry_identity_and_bytes_together"
    )))
    .run(&actions(), |actions| {
        oracle.check(&Input {
            actions,
            probe: None,
        });
        Ok(())
    })
    .unwrap();
}

#[test]
fn preflight_preserves_command_priority_and_never_wraps_bytes() {
    let oracle = controls::oracle();
    TestRunner::new(config(concat!(
        module_path!(),
        "::preflight_preserves_command_priority_and_never_wraps_bytes"
    )))
    .run(&(actions(), any::<u64>()), |(actions, probe)| {
        oracle.check(&Input {
            actions,
            probe: Some(probe),
        });
        Ok(())
    })
    .unwrap();
}

#[test]
fn mandatory_histories_cover_boundaries_and_identity_reuse() {
    let oracle = controls::oracle();
    for actions in [
        vec![],
        vec![None],
        // Same request basename in distinct task namespaces is not a conflict.
        vec![Some((0, 1)), Some((1, 1)), None, None],
        vec![Some((0, 0)), Some((1, 0)), Some((2, 0)), Some((3, 0))],
        vec![Some((0, 4)), Some((1, 4)), Some((2, 4)), Some((0, 1))],
        vec![Some((0, 8)), Some((1, 4)), Some((2, 1))],
        vec![Some((0, 3)), None, Some((0, 1)), None, None],
        vec![Some((2, 1)), Some((0, 2)), None, None],
    ] {
        for probe in [0, 1, BYTES, u64::MAX] {
            oracle.check(&Input {
                actions: actions.clone(),
                probe: Some(probe),
            });
        }
    }
}

#[test]
fn negative_control_leaking_retired_bytes_fails_and_shrinks() {
    let oracle = controls::oracle();
    oracle.check(&Input {
        actions: vec![Some((0, 1)), None],
        probe: None,
    });
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    // The same fixture and named ownership invariant used by Oracle. This
    // extra probe establishes shrinking; it is not another fault detector.
    let result = runner.run(&actions(), |actions| {
        let event = fixture::run(
            &Input {
                actions,
                probe: None,
            },
            Some(Fault::LeakRefund),
        );
        observation::ownership(&event).map_err(TestCaseError::fail)
    });
    let Err(TestError::Fail(_, minimal)) = result else {
        panic!("leaked bytes escaped the ownership invariant: {result:?}");
    };
    assert!(
        minimal.iter().any(Option::is_none),
        "no retirement: {minimal:?}"
    );
    assert!(
        minimal.iter().flatten().any(|(_, size)| *size > 0),
        "no funded payload: {minimal:?}"
    );
    assert!(minimal.len() <= 6, "did not shrink: {minimal:?}");
    eprintln!("LEAK_REFUND_SHRUNK {minimal:?}");
    // Proptest can classify a panic as Fail. The final witness must also
    // satisfy the shared runner's named-detection and deletion discipline;
    // an infrastructure panic can never establish successful shrinking.
    controls::builder()
        .control(
            "ownership",
            Input {
                actions: minimal.clone(),
                probe: None,
            },
            Fault::LeakRefund,
        )
        .build()
        .check(&Input {
            actions: minimal,
            probe: None,
        });
}
