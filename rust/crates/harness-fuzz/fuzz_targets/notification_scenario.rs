#![no_main]

#[path = "../../harness/tests/support/scenario_matchers.rs"]
mod matchers;
#[path = "../../harness/tests/e2e/notification_history.rs"]
mod notification_history;
#[path = "../../harness/tests/e2e/notifications.rs"]
mod notifications;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(value) = serde_json::from_slice(bytes) else {
        return;
    };
    let _ = matchers::subset(
        &serde_json::json!({"$json": &value}),
        &serde_json::Value::String(std::str::from_utf8(bytes).unwrap().into()),
    );
    let Ok(case) = notifications::Notifications::parse(value) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let decoded =
        notifications::Notifications::parse(serde_json::from_slice(&encoded).unwrap()).unwrap();
    assert_eq!(case, decoded);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(case.verify_history());
});
