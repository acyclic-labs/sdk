//! Admission outcomes are the oracle: an independent queue of generated
//! (identity, size) pairs predicts every result, never the owner's counters.
use super::*;
use crate::client::FileClientDurability;
use proptest::{
    prelude::*,
    test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner},
};

const COMMANDS: usize = 3;
const BYTES: u64 = 12;

fn cursor(position: u64) -> Cursor {
    Cursor {
        volume: [0; 32],
        epoch: uuid::Uuid::nil(),
        schema: 0,
        query: [0; 32],
        position,
        accepted: position,
        durable: position,
        commit: [0; 32],
    }
}

fn write(id: u8, size: u8) -> Write {
    Write::new(
        FileKey::parse(&format!(".tasks/task/requests/op{id}.json")).unwrap(),
        vec![id; usize::from(size)],
    )
}

/// `None` retires the oldest entry; `Some((id, size))` admits a request.
fn check(actions: &[Option<(u8, u8)>]) -> std::result::Result<(), TestCaseError> {
    let mut owner = PendingRequests::new(FileClientLimits {
        commands: COMMANDS,
        bytes: BYTES,
        durability: FileClientDurability::Accepted,
    });
    // (identity, size, admission step) of every admitted, unretired request.
    let mut reference: VecDeque<(u8, u8, u64)> = VecDeque::new();
    for (step, action) in actions.iter().enumerate() {
        match *action {
            None => {
                let expected = reference.pop_front();
                let retired = owner.retire_front();
                prop_assert_eq!(
                    retired.map(|done| (done.write.bytes.len(), done.cursor.position)),
                    expected.map(|(_, size, admitted)| (usize::from(size), admitted))
                );
            }
            Some((id, size)) => {
                let used: u64 = reference.iter().map(|(_, size, _)| u64::from(*size)).sum();
                let expected = if reference.iter().any(|(held, _, _)| *held == id) {
                    "conflict"
                } else if reference.len() >= COMMANDS {
                    "commands"
                } else if used + u64::from(size) > BYTES {
                    "bytes"
                } else {
                    "admitted"
                };
                let preflight = owner.check(u64::from(size));
                let admitted = owner.admit(cursor(step as u64), write(id, size));
                let actual = match &admitted {
                    Ok(()) => "admitted",
                    Err(FileClientError::CommandQuota { limit }) if *limit == COMMANDS => {
                        "commands"
                    }
                    Err(FileClientError::ByteQuota { limit }) if *limit == BYTES => "bytes",
                    Err(FileClientError::Harness(Error::Conflict(_))) => "conflict",
                    Err(other) => return Err(TestCaseError::fail(format!("{other:?}"))),
                };
                prop_assert_eq!(actual, expected);
                if expected != "conflict" {
                    prop_assert_eq!(preflight.is_ok(), expected == "admitted");
                }
                if expected == "admitted" {
                    reference.push_back((id, size, step as u64));
                }
            }
        }
        prop_assert_eq!(owner.len(), reference.len());
    }
    Ok(())
}

fn actions() -> impl Strategy<Value = Vec<Option<(u8, u8)>>> {
    proptest::collection::vec(proptest::option::of((0_u8..5, 0_u8..9)), 0..24)
}

proptest! {
    #![proptest_config(Config::with_cases(256))]
    #[test]
    fn admission_and_retirement_move_entry_identity_and_bytes_together(actions in actions()) {
        check(&actions)?;
    }
}

#[test]
fn negative_control_leaking_retired_bytes_fails_and_shrinks() {
    LEAK_RETIRED_BYTES.with(|flag| flag.set(true));
    // Seeded so the shrunk witness is reproducible.
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: 256,
            failure_persistence: None,
            ..Config::default()
        },
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let result = runner.run(&actions(), |actions| check(&actions));
    LEAK_RETIRED_BYTES.with(|flag| flag.set(false));
    let Err(TestError::Fail(_, minimal)) = result else {
        panic!("leaked bytes escaped the admission oracle: {result:?}");
    };
    // Every witness must fund a request, retire it, then have the leaked
    // bytes reject a request the reference admits; leaks need a retirement.
    assert!(
        minimal.iter().any(Option::is_none),
        "witness must retire: {minimal:?}"
    );
    assert!(minimal.len() <= 6, "did not shrink: {minimal:?}");
}
