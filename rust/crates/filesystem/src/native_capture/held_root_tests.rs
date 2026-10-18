use super::*;
use crate::native_host::stat_probe::{Fault, Probe, events};

/// One checkout capture: hold the root, then read its incarnation.
fn capture(path: &std::path::Path) -> Result<crate::NativeRootIncarnation, DemandError> {
    hold_root(path)?.incarnation()
}

/// What one held-root capture must do when one native query fails once.
fn expected(query: &str, errno: i32) -> Option<i32> {
    // An interrupted query is retried, and an `EPERM` open is retried without
    // `O_NOATIME`; both name the same incarnation. Every other failure keeps
    // its cause.
    (errno != libc::EINTR && !(query == "openat" && errno == libc::EPERM)).then_some(errno)
}

/// Every native query on the identity path, failed once with every
/// transient or capability errno, either names the unchanged incarnation or
/// fails with the injected cause: never a different incarnation.
fn every_fault_keeps_one_incarnation(fold: bool) -> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let baseline = capture(directory.path()).map_err(|error| error.to_string())?;
    for query in ["openat", "fstat", "handle"] {
        for errno in [libc::EINTR, libc::ENOSYS, libc::EPERM] {
            let fault = Fault::inject(query, errno);
            if fold {
                fault.fold_errors();
            }
            let held = capture(directory.path());
            if !fault.fired() {
                return Err(format!("{query} was never queried"));
            }
            match (expected(query, errno), held) {
                (None, Ok(held)) if held == baseline => {}
                (Some(errno), Err(crate::demand::DemandError::Io(error)))
                    if error.raw_os_error() == Some(errno) => {}
                (_, held) => return Err(format!("{query} errno {errno}: {held:?}")),
            }
        }
    }
    Ok(())
}

#[test]
fn every_identity_query_fault_keeps_one_incarnation_or_its_cause() {
    every_fault_keeps_one_incarnation(false).unwrap();
}

#[test]
fn negative_control_a_folded_handle_error_changes_the_incarnation() {
    assert!(every_fault_keeps_one_incarnation(true).is_err());
}

/// The physical native queries of one held-root capture.
fn capture_queries(duplicate: bool) -> Vec<&'static str> {
    let directory = tempfile::tempdir().unwrap();
    let probe = Probe::rescan(false);
    if duplicate {
        probe.duplicate_queries();
    }
    capture(directory.path()).unwrap();
    events()
}

#[test]
fn a_held_root_capture_makes_exactly_one_open_identity_and_handle_query() {
    assert_eq!(capture_queries(false), ["openat", "fstat", "handle"]);
}

#[test]
fn negative_control_a_repeated_identity_query_is_counted() {
    assert_ne!(capture_queries(true), ["openat", "fstat", "handle"]);
}
