use super::*;
use std::process::Stdio;

#[test]
fn an_unconfirmed_pipe_error_retains_the_pending_request_until_settled() -> io::Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::capture_events_tests::event_capture_child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_EVENT_CAPTURE", "quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let pipe = tree
        .child
        .as_ref()
        .and_then(|child| child.stdin.as_ref())
        .ok_or(io::ErrorKind::BrokenPipe)?
        .as_raw_handle();
    let bytes = [0; 64 * 1024];
    let mut pending = Pending::new(pipe, Some(&bytes), None)?;
    assert!(
        pending.active(),
        "the fixture must retain a real kernel request"
    );
    // Exercise the completion classifier with an unconfirmed status failure.
    // A nonblocking GetOverlappedResult on a genuinely pending request returns
    // IO_INCOMPLETE before validating its file handle, so changing that handle
    // cannot produce this error. Never modify kernel-owned OVERLAPPED storage.
    let queried = pending.accept_completion(
        Err(io::Error::from_raw_os_error(
            windows_sys::Win32::Foundation::ERROR_INVALID_HANDLE.cast_signed(),
        )),
        Ok(()),
        0,
    );
    let retained = pending.active();
    // Also settle safely under the counterfactual that incorrectly clears
    // active on any error; the negative control must not free live storage.
    pending.active = true;
    let (_, settled) = pending.complete();
    settled?;
    drop(pending);
    tree.terminate(None)?;
    assert!(queried.is_err());
    assert!(
        retained,
        "a failed status query cannot release unconfirmed kernel custody"
    );
    assert!(tree.is_reaped());
    Ok(())
}
