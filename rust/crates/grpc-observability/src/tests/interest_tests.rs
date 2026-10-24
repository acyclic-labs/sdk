//! First-use schedules run in separate processes so no unrelated dispatcher
//! can hide the single-observer registration path.
use super::Capture;
use std::process::Command;
use tracing::Dispatch;
use tracing_subscriber::prelude::*;

const CASE: &str = "ACYCLIC_CAPTURE_INTEREST_CASE";
const RELEASE: &str = "ACYCLIC_CAPTURE_RELEASE_REGISTRATION";
const TEST: &str = "tests::interest_tests::capture_owns_cold_callsite_registration";

fn emit() {
    let _span = tracing::info_span!("capture.interest.operation");
    tracing::info!(target: "capture.interest", boundary = "operation");
}

fn foreign() {
    std::thread::spawn(emit).join().unwrap();
}

fn schedule(case: &str) {
    let capture = Capture::default();
    if std::env::var_os(RELEASE).is_some() {
        capture.0.lock().unwrap()._registration = Dispatch::none();
    }
    if case == "foreign-before-install" {
        foreign();
    }
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(capture.clone()));
    if case == "observer-before-foreign" {
        tracing::dispatcher::with_default(&dispatch, emit);
    }
    foreign();
    tracing::dispatcher::with_default(&dispatch, emit);
    let expected = usize::from(case == "observer-before-foreign") + 1;
    let evidence = capture.0.lock().unwrap();
    assert_eq!(
        evidence.spans.len(),
        expected,
        "cold span callsite escaped capture: {case}"
    );
    assert!(evidence.spans.iter().all(|span| span.closed));
    assert_eq!(
        evidence.events.len(),
        expected,
        "cold event callsite escaped capture: {case}"
    );
}

#[test]
fn capture_owns_cold_callsite_registration() {
    if let Ok(case) = std::env::var(CASE) {
        schedule(&case);
        return;
    }
    for case in [
        "foreign-before-install",
        "foreign-after-install",
        "observer-before-foreign",
    ] {
        let result = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--test-threads=1", "--nocapture"])
            .env(CASE, case)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{case}: {}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
        );
    }
    // Delete the actual owned registration before installing the observer.
    // The same production callsites must then expose the old failure.
    let broken = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", TEST, "--test-threads=1", "--nocapture"])
        .env(CASE, "foreign-after-install")
        .env(RELEASE, "1")
        .output()
        .unwrap();
    assert!(
        !broken.status.success(),
        "released registration escaped the oracle"
    );
    assert!(
        String::from_utf8_lossy(&broken.stderr).contains("cold span callsite escaped capture"),
        "negative control failed for another reason: {}{}",
        String::from_utf8_lossy(&broken.stdout),
        String::from_utf8_lossy(&broken.stderr),
    );
}

#[test]
fn registration_follows_the_last_capture_owner() {
    let capture = Capture::default();
    let registration = capture.0.lock().unwrap()._registration.downgrade();
    let copy = capture.clone();
    drop(capture);
    assert!(registration.upgrade().is_some());
    drop(copy);
    assert!(registration.upgrade().is_none());
}
