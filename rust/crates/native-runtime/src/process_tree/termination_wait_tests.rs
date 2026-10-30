use super::super::*;
use std::process::Stdio;
use std::sync::atomic::Ordering;

#[test]
fn quiet_termination_wait_does_not_retry_observation_on_a_timer() -> io::Result<()> {
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
    let mut process = ProcessTree::spawn(&mut command)?;
    // This fixture creates no descendants. Let the leader exit naturally to
    // exercise the wait after containment has already stopped issuing kills.
    process.guard.active = false;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let started = Instant::now();
    process.terminate(Some(&meter))?;
    let reads = meter.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "TERMINATION_WAIT reads={reads} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert!(process.is_reaped());
    assert!(
        reads <= 6,
        "termination must wait for child exit without periodic observation"
    );
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[test]
fn permission_denied_group_wait_does_not_poll_live_members() -> io::Result<()> {
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
    let mut process = ProcessTree::spawn(&mut command)?;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let release = process.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    assert!(process.try_wait(None)?.is_none());
    drop(release);
    let started = Instant::now();
    // Exercise the actual EPERM fallback against a live, reserved group. The
    // single fixture exits naturally; keep its leader unreaped throughout.
    let empty = super::group_has_no_live_members(process.guard.process_group, Some(&meter))?;
    let reads = meter.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "APPLE_GROUP_WAIT empty={empty} reads={reads} elapsed_us={}",
        started.elapsed().as_micros()
    );
    process.terminate(None)?;
    assert!(empty, "the exited fixture must leave no live group members");
    assert!(
        reads <= 12,
        "the EPERM fallback must wait for member exit notifications"
    );
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[test]
fn group_member_fixture() -> io::Result<()> {
    if std::env::var_os("ACYCLIC_GROUP_MEMBER_FIXTURE").is_none() {
        return Ok(());
    }
    let mut children = Vec::new();
    for _ in 0..5 {
        // These children deliberately outlive their leader in its process
        // group; the supervising ProcessTree retains the unreaped leader.
        children.push(
            Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "process_tree::capture_events_tests::event_capture_child",
                    "--nocapture",
                ])
                .env_remove("ACYCLIC_GROUP_MEMBER_FIXTURE")
                .env("ACYCLIC_TEST_EVENT_CAPTURE", "quiet")
                .stdin(Stdio::inherit())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()?,
        );
    }
    // The supervising tree owns the complete group. Exiting here deliberately
    // transfers orphan reaping to the OS while retaining that tree's fence.
    std::process::exit(0)
}

#[cfg(target_vendor = "apple")]
#[test]
fn group_wait_observes_descendants_after_the_leader_exits() -> io::Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::platform::termination_wait_tests::group_member_fixture",
            "--nocapture",
        ])
        .env("ACYCLIC_GROUP_MEMBER_FIXTURE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut process = ProcessTree::spawn(&mut command)?;
    let group = process.guard.process_group;
    let release = process.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    let id = libc::id_t::try_from(group).map_err(io::Error::other)?;
    let status = super::await_exiting_id(id, None)?;
    assert!(
        status.success(),
        "the descendant fixture failed to spawn its children"
    );
    drop(release);
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let started = Instant::now();
    let empty = super::group_has_no_live_members(group, Some(&meter))?;
    let reads = meter.read_attempts.load(Ordering::Acquire);
    eprintln!(
        "APPLE_DESCENDANT_WAIT empty={empty} reads={reads} elapsed_us={}",
        started.elapsed().as_micros()
    );
    process.terminate(None)?;
    assert!(empty);
    assert!(reads <= 60, "group members must settle through exit events");
    assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    Ok(())
}
