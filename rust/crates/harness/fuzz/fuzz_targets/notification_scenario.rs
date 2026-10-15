#![no_main]

#[path = "../../tests/e2e/matchers.rs"]
mod matchers;
#[path = "../../tests/e2e/notifications.rs"]
mod notifications;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let Ok(value) = serde_json::from_slice(bytes) else {
        return;
    };
    let Ok(case) = notifications::Notifications::parse(value) else {
        return;
    };
    let encoded = serde_json::to_vec(&case).unwrap();
    let decoded =
        notifications::Notifications::parse(serde_json::from_slice(&encoded).unwrap()).unwrap();
    assert_eq!(case, decoded);
    let _ = case.run();
});
