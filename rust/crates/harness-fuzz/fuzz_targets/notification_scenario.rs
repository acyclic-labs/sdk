#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/e2e/matchers.rs"]
mod matchers;
#[path = "../../harness/tests/e2e/notification_history.rs"]
mod notification_history;
#[path = "../../harness/tests/e2e/notifications.rs"]
mod notifications;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    let Some(value) = fuzz_support::json_input!(fuzz_input, serde_json::Value) else {
        return;
    };
    let expected = serde_json::json!({"$json": &*value});
    let actual =
        serde_json::Value::String(fuzz_support::must_decode(std::str::from_utf8(bytes)).into());
    assert_eq!(
        matchers::ordered(
            std::slice::from_ref(&expected),
            std::slice::from_ref(&actual),
            "fuzz-json"
        )
        .is_ok(),
        matchers::subset(&expected, &actual)
    );
    let Some(case) = value.admit(notifications::Notifications::parse) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let _ = fuzz_support::round_trip(
        &case,
        notifications::Notifications::parse(serde_json::from_slice(&encoded).unwrap()),
    );
    let _ = case.verify_history();
    let _ = case.run();
});
