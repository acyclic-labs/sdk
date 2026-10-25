use super::*;
use std::sync::atomic::Ordering;

#[test]
fn a_missing_exit_registration_settles_the_owned_child_before_returning() -> io::Result<()> {
    let mut tree = child("gated")?;
    let id = tree.owned_child_id()?;
    let mut input = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    assert!(platform::observe_exit_id(id, None)?.is_none());
    complete_registration(false, || {
        use std::io::Write as _;
        input.write_all(&[1])?;
        input.flush()?;
        assert_eq!(platform::await_exiting_id(id, None)?.code(), Some(37));
        Ok(())
    })?;
    let status = platform::observe_exit_id(id, None)?
        .ok_or_else(|| io::Error::other("registration returned before terminal status"))?;
    assert_eq!(status.code(), Some(37));
    assert!(
        !tree.is_reaped(),
        "terminal observation must retain PID custody"
    );
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[test]
fn every_exit_registration_outcome_preserves_terminal_custody() {
    for registered in [false, true] {
        for failed in [false, true] {
            let mut settled = false;
            let result = complete_registration(registered, || {
                settled = true;
                if failed {
                    Err(io::Error::from_raw_os_error(libc::EIO))
                } else {
                    Ok(())
                }
            });
            assert_eq!(settled, !registered);
            assert_eq!(result.is_err(), !registered && failed);
            if let Err(error) = result {
                assert_eq!(error.raw_os_error(), Some(libc::EIO));
            }
        }
    }
}

#[test]
fn exit_watch_prepays_and_records_its_descriptor_release() -> io::Result<()> {
    let mut tree = child("0")?;
    let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
    let watch = open(tree.owned_child_id()?, Some(&meter))?;
    assert_eq!(meter.write_attempts.load(Ordering::Acquire), 0);
    drop(watch);
    assert_eq!(
        meter.write_attempts.load(Ordering::Acquire),
        1,
        "the exit descriptor close must stay on its original ledger"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(meter.read_attempts.load(Ordering::Acquire), 1);
    #[cfg(target_vendor = "apple")]
    assert_eq!(meter.read_attempts.load(Ordering::Acquire), 2);

    let refused = crate::tests::ReadBudgetMeter::new(1, u64::MAX);
    assert!(
        open(tree.owned_child_id()?, Some(&refused)).is_err(),
        "an exit watch must reserve its close before opening"
    );
    assert_eq!(refused.used_operations.load(Ordering::Acquire), 0);
    assert_eq!(refused.read_attempts.load(Ordering::Acquire), 0);
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[test]
fn exit_wait_fixture() {
    if std::env::var_os("ACYCLIC_TEST_EXIT_WAIT").is_some() {
        if std::env::var("ACYCLIC_TEST_EXIT_STATUS").as_deref() == Ok("gated") {
            use std::io::Read as _;
            let mut release = [0];
            std::io::stdin()
                .read_exact(&mut release)
                .expect("exit release");
            assert_eq!(release, [1]);
        } else {
            std::thread::sleep(Duration::from_millis(200));
        }
        match std::env::var("ACYCLIC_TEST_EXIT_STATUS").as_deref() {
            Ok("37" | "gated") => std::process::exit(37),
            Ok("signal") => {
                // SAFETY: the isolated fixture deliberately terminates itself.
                unsafe { libc::raise(libc::SIGTERM) };
            }
            _ => {}
        }
    }
}

fn child(status: &str) -> io::Result<ProcessTree> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::exit_wait::tests::exit_wait_fixture",
        ])
        .env("ACYCLIC_TEST_EXIT_WAIT", "1")
        .env("ACYCLIC_TEST_EXIT_STATUS", status)
        .stdin(if status == "gated" {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    ProcessTree::spawn(&mut command)
}

#[test]
fn quiet_wait_observes_exit_without_periodic_wakeups_or_reaping() -> io::Result<()> {
    let mut tree = child("0")?;
    OBSERVATIONS.with(|count| count.set(0));
    let started = Instant::now();
    let status = tree.wait(Duration::from_secs(2))?;
    let observations = OBSERVATIONS.with(std::cell::Cell::get);
    eprintln!(
        "EXIT_WAIT observations={observations} elapsed_us={}",
        started.elapsed().as_micros()
    );
    assert!(status.success());
    assert!(
        observations <= 3,
        "quiet wait must use an exit notification"
    );
    assert!(
        !tree.is_reaped(),
        "exit observation must retain tree custody"
    );
    assert_eq!(tree.wait(Duration::ZERO)?, status);
    tree.terminate(None)?;
    assert!(tree.is_reaped());
    Ok(())
}

#[test]
fn exit_deadline_preserves_the_same_live_child_for_a_later_wait() -> io::Result<()> {
    let mut tree = child("0")?;
    assert_eq!(
        tree.wait(Duration::from_millis(20))
            .expect_err("child must remain live")
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert!(!tree.is_reaped());
    assert!(tree.wait(Duration::from_secs(2))?.success());
    tree.terminate(None)?;
    Ok(())
}

#[test]
fn exit_notifications_retain_nonzero_and_signal_status() -> io::Result<()> {
    use std::os::unix::process::ExitStatusExt;
    for kind in ["37", "signal"] {
        let mut tree = child(kind)?;
        let status = tree.wait(Duration::from_secs(2))?;
        if kind == "37" {
            assert_eq!(status.code(), Some(37));
        } else {
            assert_eq!(status.signal(), Some(libc::SIGTERM));
        }
        assert_eq!(tree.try_wait(None)?, Some(status));
        tree.terminate(None)?;
    }
    Ok(())
}
