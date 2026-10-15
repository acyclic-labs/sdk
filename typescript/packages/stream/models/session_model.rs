//! JavaScript runs atomically between awaits. Enumerate all bounded external
//! transport-event orders with loom, then replay each distinct order against
//! the real TypeScript module, including paused transport writes.
use loom::sync::{Arc, Mutex};
use serde_json::json;
use std::{collections::BTreeSet, path::Path};
#[path = "../fuzz/driver.rs"]
mod driver;

fn traces(
    left: &'static [&'static str],
    right: &'static [&'static str],
) -> BTreeSet<Vec<&'static str>> {
    // This outer mutex only collects completed schedules, outside the explored
    // threads. It neither controls nor prunes their ordering.
    let output = std::sync::Arc::new(std::sync::Mutex::new(BTreeSet::new()));
    let completed = std::sync::Arc::clone(&output);
    loom::model(move || {
        // One model-only lock rank: the external transport-event delivery order.
        // No production JS state is mocked or guarded by this Rust mutex.
        let events = Arc::new(Mutex::new(Vec::new()));
        let one = Arc::clone(&events);
        let one = loom::thread::spawn(move || {
            for event in left {
                one.lock().unwrap().push(*event);
            }
        });
        let two = Arc::clone(&events);
        let two = loom::thread::spawn(move || {
            for event in right {
                two.lock().unwrap().push(*event);
            }
        });
        one.join().unwrap();
        two.join().unwrap();
        let trace = events.lock().unwrap().clone();
        completed.lock().unwrap().insert(trace);
    });
    std::sync::Arc::try_unwrap(output)
        .unwrap()
        .into_inner()
        .unwrap()
}
fn run(scenario: &str, fault: &str, left: &'static [&'static str], right: &'static [&'static str]) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    for trace in traces(left, right) {
        let mut driver = driver::Driver::configured(&root, scenario, fault);
        driver.call(json!({"action":"ready"}));
        for action in trace {
            driver.call(json!({"action":action}));
        }
        let state = driver.call(json!({"action":"release"}));
        match scenario {
            "move" => {
                assert_eq!(
                    state["settled"], true,
                    "movement must settle the pending follow"
                );
                assert_eq!(
                    state["seen"],
                    json!([0]),
                    "movement must deliver every record once"
                );
                assert_eq!(
                    state["resumes"],
                    json!([state["appliedAtMove"]]),
                    "movement must resume from the applied cursor"
                );
                assert_eq!(state["connections"], 2);
            }
            "cancel" => {
                assert_eq!(state["settled"], true);
                assert_eq!(state["active"], 0, "abort must remove every interest");
                assert_eq!(
                    state["seen"], state["seenAtAbort"],
                    "abort cannot yield a later record"
                );
            }
            "drop" => {
                assert_eq!(
                    state["outstanding"], 0,
                    "all buffered credit must be returned"
                );
                assert_eq!(state["returned"], 2);
                assert_eq!(state["active"], 0);
            }
            _ => unreachable!(),
        }
    }
}
#[test]
fn movement_and_append_are_lossless() {
    run("move", "", &["append", "release"], &["move", "release"]);
}
#[test]
#[should_panic(expected = "movement must")]
fn movement_negative_control_trusts_the_peer_resume_cursor() {
    run(
        "move",
        "resume",
        &["append", "release"],
        &["move", "release"],
    );
}
#[test]
fn cancellation_and_append_preserve_the_yield_boundary() {
    run("cancel", "", &["append"], &["abort"]);
}
#[test]
#[should_panic(expected = "abort must remove every interest")]
fn cancellation_negative_control_leaves_interest_installed() {
    run("cancel", "cancel", &["append"], &["abort"]);
}
#[test]
fn consume_and_discard_return_each_delivery_credit_once() {
    run("drop", "", &["consume"], &["drop"]);
}
#[test]
#[should_panic(expected = "all buffered credit must be returned")]
fn disposal_negative_control_discards_buffered_credit() {
    run("drop", "credit", &["consume"], &["drop"]);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        cases: 128,
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/proptest-regressions.txt")))),
        ..proptest::test_runner::Config::default()
    })]
    // Every decodable untrusted frame must preserve all fields through the
    // actual generated JavaScript decoder/encoder, with shrinking on failure.
    #[test]
    fn untrusted_frames_round_trip(input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512)) {
        static DRIVER: std::sync::OnceLock<std::sync::Mutex<driver::Driver>> = std::sync::OnceLock::new();
        let shared = DRIVER.get_or_init(|| std::sync::Mutex::new(driver::Driver::open(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."))));
        let hex: String = input.iter().map(|byte| format!("{byte:02x}")).collect();
        shared.lock().unwrap().call(json!({"action":"decode", "hex":hex}));
    }

    #[test]
    fn channel_errors_preserve_the_public_provider_codes(code in proptest::sample::select(vec![
        "not_found", "already_exists", "capacity", "invalid_path", "invalid_argument",
        "limit_exceeded", "prefix_not_retained", "out_of_range", "idempotency_mismatch",
        "access_denied", "unavailable", "hierarchy_changed", "deadline_elapsed", "unsupported", "unknown"
    ])) {
        static DRIVER: std::sync::OnceLock<std::sync::Mutex<driver::Driver>> = std::sync::OnceLock::new();
        let shared = DRIVER.get_or_init(|| std::sync::Mutex::new(driver::Driver::open(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."))));
        // SessionFrame(channel=1,generation=1,closed(code)): stable fields 1,2,18.
        let mut bytes = vec![8, 1, 16, 1, 146, 1, u8::try_from(code.len() + 2).unwrap(), 10, u8::try_from(code.len()).unwrap()];
        bytes.extend_from_slice(code.as_bytes());
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        shared.lock().unwrap().call(json!({"action":"decode", "hex":hex}));
    }
}
