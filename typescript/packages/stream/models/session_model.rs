//! JavaScript runs atomically between awaits. Enumerate all bounded external
//! transport-event orders with loom, then replay each distinct order against
//! the real TypeScript module, including paused transport writes.
use serde_json::json;
use std::path::Path;
#[path = "../fuzz/driver.rs"]
mod driver;

#[path = "queue.rs"]
mod queue;
#[path = "schedules.rs"]
mod schedules;
use schedules::traces;
// Cursor, reservation, credit and movement schedules now execute the shared
// Rust engine in stream/tests/client_pool_loom.rs and stream/models. The
// browser recovery sequence is qualified in session-latest.test.ts; these
// remaining models exercise JavaScript provider selection and platform I/O.
fn child_selection(fault: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    for trace in traces(&["releaseSelection"], &["abort"]) {
        let mut driver = driver::Driver::configured(&root, "childSelect", fault);
        driver.call(json!({"action":"ready"}));
        for action in trace {
            let state = driver.call(json!({"action":action}));
            if action == "abort" {
                assert_eq!(
                    state["settled"], true,
                    "child cancellation must not wait for provider selection"
                );
                assert_eq!(state["active"], 0);
            }
        }
        let state = driver.call(json!({"action":"drop"}));
        assert_eq!(state["active"], 0);
        assert_eq!(state["outstanding"], 0);
    }
}
const BUDGET_EXHAUSTED: i32 = 70;
impl driver::Driver {
    /// Closes stdin and returns the process's exit code once it has ended.
    fn exit_code(mut self) -> Option<i32> {
        drop(self.input.take());
        self.child.wait().expect("wait for model process").code()
    }
}
fn repository() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..")
}

/// The owner side of the lifetime test, run in its own process: it parks a model command, reports the
/// acknowledgement, and waits to be killed. The model inherits this process's stderr.
#[test]
fn parked_model_owner_process() {
    let Some(fault) = std::env::var_os("STREAM_MODEL_OWNER") else {
        return;
    };
    let mut driver = driver::Driver::configured(&repository(), "move", fault.to_str().unwrap());
    driver.send(json!({"action":"park"}));
    let ack = driver
        .answer()
        .expect("model process exited before parking");
    assert_eq!(ack["ack"], "parked", "{ack}");
    std::io::Write::write_all(&mut std::io::stderr(), b"parked\n").unwrap();
    loop {
        std::thread::park();
    }
}
/// Kills a real owner process while its model command is parked. The model shares the owner's stderr pipe, so that
/// pipe reaches end of file only once the model process has exited too.
fn killed_owner_ends_its_model(fault: &str) -> Result<(), String> {
    use std::io::{BufRead, Read};
    let name = concat!(module_path!(), "::parked_model_owner_process");
    let mut owner = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            name.split_once("::").map_or(name, |(_, test)| test),
            "--nocapture",
            "--test-threads=1",
        ])
        .env("STREAM_MODEL_OWNER", fault)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut shared = std::io::BufReader::new(owner.stderr.take().unwrap());
    let mut line = String::new();
    let parked = loop {
        line.clear();
        if shared.read_line(&mut line).unwrap() == 0 {
            break false;
        }
        if line == "parked\n" {
            break true;
        }
    };
    owner.kill().unwrap();
    owner.wait().unwrap();
    if !parked {
        return Err("the owner never saw its model park".into());
    }
    // Returns only when every holder of the pipe, the parked model included, has exited.
    shared.read_to_end(&mut Vec::new()).unwrap();
    Ok(())
}
#[test]
fn a_parked_model_process_exits_when_its_owner_is_killed() {
    killed_owner_ends_its_model("").unwrap();
}
#[test]
fn a_model_that_crashes_at_startup_fails_the_lifetime_test() {
    assert!(killed_owner_ends_its_model("startupCrash").is_err());
}

/// Connects exactly `connections` times in one command against a budget of `steps`: the command itself is one step
/// and each connection one more. Returns the budget left after the command, or the exit code if the process ended.
fn connect_at_budget(steps: u64, connections: u64, fault: &str) -> Result<u64, Option<i32>> {
    let mut driver = driver::Driver::budgeted(&repository(), "move", fault, steps);
    driver.send(json!({"action":"connect", "count":connections}));
    match driver.answer() {
        Some(answer) => {
            assert!(answer.get("error").is_none(), "{answer}");
            assert_eq!(answer["result"]["connections"], connections);
            Ok(driver.remaining)
        }
        None => Err(driver.exit_code()),
    }
}
#[test]
fn every_peer_draws_from_one_process_budget_to_its_exact_boundary() {
    // 1 command + 15 connections spends exactly 16 steps; the process stays alive with nothing left.
    assert_eq!(connect_at_budget(16, 15, ""), Ok(0));
    // One more connection is the 17th step: the process ends with the budget exit code, not a crash.
    assert_eq!(connect_at_budget(16, 16, ""), Err(Some(BUDGET_EXHAUSTED)));
    // Negative control: a process that dies on its first command is not mistaken for an exhausted budget.
    assert_eq!(connect_at_budget(16, 15, "startupCrash"), Err(Some(1)));
}
#[test]
fn a_reconnecting_client_ends_at_the_process_budget() {
    let mut driver = driver::Driver::budgeted(&repository(), "move", "", 64);
    driver.send(json!({"action":"storm"}));
    assert!(driver.answer().is_none());
    assert_eq!(driver.exit_code(), Some(BUDGET_EXHAUSTED));
}
#[test]
fn fresh_validator_peers_cannot_renew_the_budget() {
    // Every decode builds a new validator peer; all of them spend the one process budget, so a stream of decodes ends
    // the process within the budget instead of running forever.
    let steps = 256;
    let mut driver = driver::Driver::budgeted(&repository(), "decode", "", steps);
    let frame = "080110019201090a07756e6b6e6f776e"; // closed(unknown) on channel 1, as in the codes property below
    let mut answered = 0;
    loop {
        driver.send(json!({"action":"decode", "hex":frame}));
        if driver.answer().is_none() {
            break;
        }
        answered += 1;
        assert!(answered < steps, "decodes outlived the process budget");
    }
    assert!(answered > 0);
    assert_eq!(driver.exit_code(), Some(BUDGET_EXHAUSTED));
}
#[test]
fn queue_items_draw_from_the_process_budget_to_its_exact_boundary() {
    let run = |steps: u64| {
        let mut driver = driver::Driver::budgeted(&repository(), "queue-work", "", steps);
        driver.send(json!({"action":"work", "capacity":8}));
        match driver.answer() {
            Some(answer) => {
                assert!(answer.get("error").is_none(), "{answer}");
                Ok(driver.remaining)
            }
            None => Err(driver.exit_code()),
        }
    };
    // 1 command + 8 dequeues.
    assert_eq!(run(9), Ok(0));
    assert_eq!(run(8), Err(Some(BUDGET_EXHAUSTED)));
}
#[test]
fn a_long_lived_driver_is_renewed_between_runs_not_reset_per_input() {
    let mut driver = driver::Driver::budgeted(&repository(), "move", "", 16);
    driver.run().call(json!({"action":"connect", "count":8}));
    assert_eq!(driver.remaining, 7);
    // Under half the budget left: the next run gets a fresh process with the whole budget.
    driver.run();
    assert_eq!(driver.remaining, 16);
    assert_eq!(
        driver.call(json!({"action":"connect", "count":1}))["connections"],
        1
    );
    assert_eq!(driver.remaining, 14);
}
#[test]
fn default_child_selection_and_cancellation_are_independent() {
    child_selection("");
}
#[test]
#[should_panic(expected = "child cancellation must not wait for provider selection")]
fn child_selection_negative_control_omits_the_selection_signal() {
    child_selection("childSelect");
}

fn rejected_subscription(fault: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    let mut driver = driver::Driver::configured(&root, "rejection", fault);
    let state = driver.call(json!({"action":"ready"}));
    assert_eq!(
        state["settled"], true,
        "a rejected resubscription must settle its real WASM cursor"
    );
    assert_eq!(state["code"], "unavailable");
    assert_eq!(driver.call(json!({"action":"drop"}))["active"], 0);
}
#[test]
fn wasm_rejected_resubscription_is_terminal() {
    rejected_subscription("");
}
#[test]
#[should_panic(expected = "a rejected resubscription must settle its real WASM cursor")]
fn negative_control_discards_the_io_rejection() {
    rejected_subscription("rejection");
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
            state["outstanding"]
                .as_u64()
                .is_some_and(|frames| frames <= 2),
            true,
            "batched aggregate credit must stay bounded by delivered frames"
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

#[test]
#[should_panic(expected = "deadline or cleanup must settle the real raw follow")]
fn raw_follow_delivery_negative_control_drops_timeout_forwarding() {
    deadline_run("deadline", &["deliver"]);
}
#[test]
#[should_panic(expected = "deadline or cleanup must settle the real raw follow")]
fn raw_follow_reconnect_negative_control_drops_timeout_forwarding() {
    deadline_run("deadline", &["reconnect", "releaseReconnect"]);
}
#[test]
#[should_panic(expected = "only the logical deadline owns a timer")]
fn raw_follow_cleanup_negative_control_drops_timeout_forwarding() {
    deadline_run("deadline", &["cleanup"]);
}

fn blocked_deadline(scenario: &str, fault: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    // All six orders of expiry, return(), and completion of the parked write.
    let mut orders = traces(&["expire"], &["return", "releaseWrite"]);
    orders.extend(traces(&["expire"], &["releaseWrite", "return"]));
    assert_eq!(orders.len(), 6);
    for trace in orders {
        let mut driver = driver::Driver::configured(&root, scenario, fault);
        assert_eq!(driver.call(json!({"action":"ready"}))["blocked"], true);
        let mut expired = false;
        let mut returning = false;
        for action in trace {
            let state = driver.call(json!({"action":action}));
            assert_eq!(
                state["siblingError"],
                serde_json::Value::Null,
                "cancelling a blocked write must preserve the sibling"
            );
            expired |= action == "expire";
            returning |= action == "return";
            if expired {
                assert_eq!(
                    state["settled"], true,
                    "deadline must settle next without releasing the blocked write"
                );
                assert_eq!(state["code"], 4);
                assert_eq!(state["timerCleaned"], 1);
                assert_eq!(state["timerActive"], false);
                if returning {
                    assert_eq!(
                        state["returned"], true,
                        "deadline must also settle a queued return"
                    );
                }
                assert_eq!(
                    state["active"], 1,
                    "no cancelled subscription may arrive late"
                );
            }
        }
        let state = driver.call(json!({"action":"finish"}));
        assert_eq!(
            state["siblingError"],
            serde_json::Value::Null,
            "cancelling a blocked write must preserve the sibling"
        );
        assert_eq!(state["siblingSeen"], 1);
        assert_eq!(state["connections"], 1);
        assert!(
            state["outstanding"]
                .as_u64()
                .is_some_and(|frames| frames <= 4)
        );
        let stopped = driver.call(json!({"action":"stop"}));
        assert_eq!(stopped["active"], 0);
        assert_eq!(stopped["outstanding"], 0);
    }
}
#[test]
fn deadline_cancels_a_blocked_subscription_write() {
    blocked_deadline("deadline-subscription", "");
}
#[test]
fn child_deadline_cancels_a_blocked_subscription_write() {
    blocked_deadline("deadline-subscription-children", "");
}
#[test]
#[should_panic(expected = "deadline must settle next without releasing the blocked write")]
fn child_deadline_negative_control_omits_timeout() {
    blocked_deadline("deadline-subscription-children", "deadline");
}
#[test]
fn child_deadline_cancels_a_blocked_acknowledgement_write() {
    blocked_deadline("deadline-ack-children", "");
}
#[test]
#[should_panic(expected = "deadline must settle next without releasing the blocked write")]
fn child_deadline_negative_control_waits_for_a_blocked_write() {
    blocked_deadline("deadline-ack-children", "write");
}
#[test]
#[should_panic(expected = "deadline must settle next without releasing the blocked write")]
fn blocked_subscription_negative_control_awaits_the_write() {
    blocked_deadline("deadline-subscription", "write");
}
#[test]
fn deadline_cancels_a_blocked_acknowledgement_write() {
    blocked_deadline("deadline-ack", "");
}
#[test]
#[should_panic(expected = "deadline must settle next without releasing the blocked write")]
fn blocked_acknowledgement_negative_control_awaits_the_write() {
    blocked_deadline("deadline-ack", "write");
}

#[test]
#[should_panic(expected = "cancelling a blocked write must preserve the sibling")]
fn blocked_subscription_negative_control_drops_the_platform_write_signal() {
    blocked_deadline("deadline-subscription", "writeSignal");
}
#[test]
#[should_panic(expected = "cancelling a blocked write must preserve the sibling")]
fn child_subscription_negative_control_drops_the_platform_write_signal() {
    blocked_deadline("deadline-subscription-children", "writeSignal");
}

fn queue_cancellation(fault: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    for trace in traces(&["abort"], &["drain"]) {
        let mut driver = driver::Driver::configured(&root, "queue", fault);
        driver.call(json!({"action":"ready"}));
        let mut aborted = false;
        for action in trace {
            let state = driver.call(json!({"action":action}));
            if action == "drain" && !aborted {
                assert_eq!(
                    state["accepted"], true,
                    "older waiting writes must precede a fresh producer"
                );
            }
            if action == "abort" {
                aborted = true;
                assert_eq!(
                    state["settled"], true,
                    "abort must settle a blocked queue producer"
                );
                assert_eq!(
                    state["waiting"],
                    if state["healthySettled"] == true || state["seen"] == json!([]) {
                        0
                    } else {
                        1
                    }
                );
            }
        }
        let state = driver.call(json!({"action":"finish"}));
        assert_eq!(
            state["accepted"], state["acceptedAtAbort"],
            "an unaccepted cancelled frame must never enter the transport"
        );
        assert_eq!(
            state["seen"],
            if state["accepted"] == true {
                json!([0, 1, 2])
            } else {
                json!([0, 2])
            },
            "queued writes must keep their wire order"
        );
        assert_eq!(state["healthySettled"], true);
        assert_eq!(state["waiting"], 0);
        assert_eq!(state["available"], 1);
    }
}
#[test]
fn queued_cancellation_races_admission_without_leaks_or_reordering() {
    queue_cancellation("");
}
#[test]
#[should_panic(expected = "abort must settle a blocked queue producer")]
fn queue_negative_control_drops_the_write_signal() {
    queue_cancellation("queue");
}

#[test]
#[should_panic(expected = "older waiting writes must precede a fresh producer")]
fn queue_negative_control_allows_fresh_writes_to_overtake_waiters() {
    queue_cancellation("order");
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
        let shared = DRIVER.get_or_init(|| std::sync::Mutex::new(driver::Driver::configured(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."), "decode", "")));
        let hex: String = input.iter().map(|byte| format!("{byte:02x}")).collect();
        shared.lock().unwrap().run().call(json!({"action":"decode", "hex":hex}));
    }

    #[test]
    fn child_frames_round_trip_and_enforce_the_direct_parent(name in "[a-z]{1,8}", nested in proptest::prelude::any::<bool>()) {
        static DRIVER: std::sync::OnceLock<std::sync::Mutex<driver::Driver>> = std::sync::OnceLock::new();
        let shared = DRIVER.get_or_init(|| std::sync::Mutex::new(driver::Driver::configured(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."), "decode", "")));
        let path = if nested { format!("parent/{name}") } else { name };
        let result = shared.lock().unwrap().run().call(json!({"action":"child", "path":path}));
        proptest::prop_assert_eq!(&result["outcome"], &json!(if nested { "unavailable" } else { "child" }));
    }

    #[test]
    fn channel_errors_preserve_the_public_provider_codes(code in proptest::sample::select(vec![
        "not_found", "already_exists", "capacity", "invalid_path", "invalid_argument",
        "limit_exceeded", "prefix_not_retained", "out_of_range", "idempotency_mismatch",
        "access_denied", "unavailable", "hierarchy_changed", "deadline_elapsed", "unsupported", "unknown"
    ])) {
        static DRIVER: std::sync::OnceLock<std::sync::Mutex<driver::Driver>> = std::sync::OnceLock::new();
        let shared = DRIVER.get_or_init(|| std::sync::Mutex::new(driver::Driver::configured(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."), "decode", "")));
        // SessionFrame(channel=1,generation=1,closed(code)): stable fields 1,2,18.
        let mut bytes = vec![8, 1, 16, 1, 146, 1, u8::try_from(code.len() + 2).unwrap(), 10, u8::try_from(code.len()).unwrap()];
        bytes.extend_from_slice(code.as_bytes());
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        shared.lock().unwrap().run().call(json!({"action":"decode", "hex":hex}));
    }
}
