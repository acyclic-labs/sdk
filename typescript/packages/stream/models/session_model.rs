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
            let state = driver.call(json!({"action":action}));
            if scenario == "reservation" && action == "abort" {
                assert_eq!(
                    state["activeReservations"], 0,
                    "cancellation must release the reservation before generator cleanup"
                );
            }
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
            "immediate" | "ack" => {
                assert_eq!(
                    state["rejected"],
                    serde_json::Value::Null,
                    "reconnect must preserve the follow after control-write failure"
                );
                assert_eq!(
                    state["settled"], true,
                    "reconnect must settle the pending next"
                );
                assert_eq!(
                    state["seen"],
                    json!([0, 1]),
                    "reconnect must resume every record once"
                );
                assert_eq!(state["connections"], 2);
            }
            "reject" => {
                assert_eq!(
                    state["settled"], true,
                    "rejected resubscription must settle the follow"
                );
                assert!(
                    state["rejected"]
                        .as_str()
                        .is_some_and(|error| error.contains("resubscription write rejected"))
                );
                assert_eq!(state["rejectedCode"], "unavailable");
                assert_eq!(state["active"], 0);
            }
            "reservation" => {
                assert_eq!(state["activeReservations"], 0);
                assert_eq!(
                    state["minimumReservations"], 0,
                    "cleanup must never release a reservation twice"
                );
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

#[test]
fn next_immediately_after_disconnect_recovers() {
    run("immediate", "", &["disconnectNext"], &["append"]);
}
#[test]
#[should_panic(expected = "reconnect must")]
fn immediate_next_negative_control_propagates_control_write_failure() {
    run("immediate", "control", &["disconnectNext"], &["append"]);
}
#[test]
fn disconnect_during_acknowledgement_recovers() {
    run(
        "ack",
        "",
        &["consume", "releaseAck"],
        &["disconnect", "append"],
    );
}
#[test]
#[should_panic(expected = "reconnect must")]
fn acknowledgement_negative_control_propagates_control_write_failure() {
    run(
        "ack",
        "control",
        &["consume", "releaseAck"],
        &["disconnect", "append"],
    );
}
#[test]
fn failed_resubscription_settles_the_reassigned_route() {
    run("reject", "", &["disconnectNext"], &["release"]);
}
#[test]
#[should_panic(expected = "rejected resubscription must settle")]
fn resubscription_negative_control_only_checks_the_old_link() {
    run("reject", "resubscribe", &["disconnectNext"], &["release"]);
}

#[test]
fn abort_and_generator_cleanup_release_one_reservation() {
    run("reservation", "", &["abort"], &["drop"]);
}
#[test]
#[should_panic(expected = "cancellation must release the reservation")]
fn reservation_negative_control_waits_for_generator_cleanup() {
    run("reservation", "reservation", &["abort"], &["drop"]);
}

fn deadline_run(fault: &str, events: &'static [&'static str]) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    for trace in traces(&["expire"], events) {
        let mut driver = driver::Driver::configured(&root, "deadline", fault);
        let ready = driver.call(json!({"action":"ready"}));
        assert_eq!(ready["active"], 2);
        let mut terminal = None;
        for action in trace {
            let state = driver.call(json!({"action":action}));
            if action == "expire" || action == "cleanup" {
                let expected = *terminal.get_or_insert(if action == "expire" { 4 } else { 1 });
                assert_eq!(
                    state["settled"], true,
                    "deadline or cleanup must settle the real raw follow: {action}"
                );
                assert_eq!(
                    state["code"], expected,
                    "the first terminal event must retain its status"
                );
                assert_eq!(state["timerActive"], false);
            }
            if terminal.is_some() {
                assert_eq!(
                    state["seen"], state["stoppedAt"],
                    "no delivery may cross the terminal boundary"
                );
            }
            if action == "reconnect" {
                assert_eq!(
                    state["connecting"], true,
                    "the model must pause the real reconnection await"
                );
            }
        }
        let state = driver.call(json!({"action":"finish"}));
        assert_eq!(state["settled"], true);
        assert_eq!(
            state["connections"],
            if events.contains(&"reconnect") { 2 } else { 1 },
            "a logical deadline must not close the shared connection"
        );
        assert_eq!(
            state["timerCreated"], 1,
            "only the logical deadline owns a timer"
        );
        assert_eq!(
            state["timerCleaned"], 1,
            "wrapper cleanup must release its timer once"
        );
        assert_eq!(
            state["active"], 1,
            "deadline must remove only its own interest"
        );
        assert_eq!(
            state["siblingSeen"], 1,
            "the shared session must survive its sibling's deadline"
        );
        assert_eq!(
            state["outstanding"], 0,
            "delivery or discard must return every frame credit"
        );
        let stopped = driver.call(json!({"action":"stop"}));
        assert_eq!(stopped["active"], 0);
        assert_eq!(stopped["outstanding"], 0);
    }
}
#[test]
fn raw_follow_deadline_races_delivery() {
    deadline_run("", &["deliver"]);
}
#[test]
fn raw_follow_deadline_races_reconnection() {
    deadline_run("", &["reconnect", "releaseReconnect"]);
}
#[test]
fn raw_follow_deadline_races_cleanup() {
    deadline_run("", &["cleanup"]);
}
#[test]
fn raw_follow_deadline_races_delivery_reconnect_and_cleanup() {
    deadline_run("", &["deliver", "reconnect", "releaseReconnect", "cleanup"]);
}
#[test]
#[should_panic(expected = "deadline or cleanup must settle the real raw follow")]
fn raw_follow_deadline_negative_control_drops_timeout_forwarding() {
    deadline_run(
        "deadline",
        &["deliver", "reconnect", "releaseReconnect", "cleanup"],
    );
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
