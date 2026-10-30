//! The job wait must sleep for member exit rather than query on a timer.
use super::*;
use std::process::Stdio;
use std::sync::atomic::Ordering;

#[test]
fn a_quiet_job_wait_has_no_periodic_queries() -> io::Result<()> {
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
    let mut tree = super::super::ProcessTree::spawn(&mut command)?;
    // This fixture has one member and no descendants. Let that member exit
    // naturally after the kill has already been issued by its previous owner.
    tree.guard.termination_sent = true;
    let release = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    assert!(tree.try_wait(None)?.is_none());
    drop(release);
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let started = std::time::Instant::now();
    tree.terminate(Some(&meter))?;
    let reads = meter.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "JOB_EXIT_WAIT reads={reads} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert!(tree.is_reaped());
    assert!(
        reads <= 20,
        "a quiet job must wait for member exit without periodic queries"
    );
    Ok(())
}
