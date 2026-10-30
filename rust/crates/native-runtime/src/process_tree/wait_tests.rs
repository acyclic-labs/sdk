use super::*;
use std::io::{BufRead as _, Write as _};
use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
use windows_sys::Win32::Foundation::FILETIME;
use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

const HELPER: &str = "ACYCLIC_NATIVE_WAIT_HELPER";
const READY: &str = "ACYCLIC_NATIVE_WAIT_READY";

fn thread_cpu() -> io::Result<u64> {
    let empty = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut created, mut exited, mut kernel, mut user) = (empty, empty, empty, empty);
    // SAFETY: the current-thread pseudo handle and writable outputs are valid.
    if unsafe {
        GetThreadTimes(
            GetCurrentThread(),
            &raw mut created,
            &raw mut exited,
            &raw mut kernel,
            &raw mut user,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let ticks =
        |time: FILETIME| u64::from(time.dwLowDateTime) | (u64::from(time.dwHighDateTime) << 32);
    ticks(kernel)
        .checked_add(ticks(user))
        .ok_or(io::ErrorKind::InvalidData.into())
}

fn thread_cycles() -> io::Result<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn QueryThreadCycleTime(
            thread: windows_sys::Win32::Foundation::HANDLE,
            cycles: *mut u64,
        ) -> i32;
    }
    let mut cycles = 0;
    // SAFETY: the current-thread pseudo handle and writable cycle count are valid.
    if unsafe { QueryThreadCycleTime(GetCurrentThread(), &raw mut cycles) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(cycles)
}

#[test]
fn wait_helper() -> io::Result<()> {
    if std::env::var_os(HELPER).is_none() {
        return Ok(());
    }
    println!("{READY}");
    std::io::stdout().flush()?;
    let mut released = [0];
    // EOF is a typed error if the parent releases custody without a message.
    std::io::stdin().read_exact(&mut released)?;
    assert_eq!(released, [1]);
    Ok(())
}

/// This test binary re-run as `wait_helper`, waiting on its original input pipe.
fn helper_command() -> io::Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::wait_tests::wait_helper",
            "--nocapture",
        ])
        .env_clear()
        .env(HELPER, "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    Ok(command)
}

type Reader = std::thread::JoinHandle<io::Result<()>>;

/// Forward one message per helper readiness line until its stdout closes.
fn readiness(stdout: std::process::ChildStdout) -> (std::sync::mpsc::Receiver<()>, Reader) {
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || -> io::Result<()> {
        for line in std::io::BufReader::new(stdout).lines() {
            if line?.ends_with(READY) {
                sent.send(()).map_err(io::Error::other)?;
            }
        }
        Ok(())
    });
    (received, reader)
}

/// Release the helper and close its input pipe, including on a write failure.
fn release(gate: &mut Option<std::process::ChildStdin>) -> io::Result<()> {
    gate.take()
        .ok_or(io::ErrorKind::BrokenPipe)?
        .write_all(&[1])
}

/// A precise tree wait charged to the optional read meter.
fn metered_wait(
    meter: Option<&dyn crate::NativeReadMeter>,
    tree: &mut ProcessTree,
    timeout: Duration,
) -> io::Result<ExitStatus> {
    process_control_attempt(meter, crate::NativeOperationKind::Read, || {
        tree.wait(timeout)
    })
}

#[test]
fn wait_preserves_timeout_and_child_ownership() -> io::Result<()> {
    wait_preserves_ownership(false)
}

#[test]
fn coarse_wait_preserves_timeout_and_child_ownership() -> io::Result<()> {
    wait_preserves_ownership(true)
}

#[test]
fn resume_rechecks_expired_deadlines_in_both_wait_paths() -> io::Result<()> {
    for precise in [false, true] {
        wait_across_resume(precise, Duration::from_secs(300))?;
    }
    Ok(())
}

#[test]
fn resume_preserves_unexpired_deadlines_in_both_wait_paths() -> io::Result<()> {
    for precise in [false, true] {
        wait_across_resume(precise, Duration::from_secs(60))?;
    }
    Ok(())
}

fn wait_across_resume(precise: bool, suspended: Duration) -> io::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use windows_sys::Win32::UI::WindowsAndMessaging::PBT_APMRESUMEAUTOMATIC;

    let mut tree = ProcessTree::spawn(&mut helper_command()?)?;
    let mut gate = Some(tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?);
    let (receive_ready, reader) = readiness(tree.take_stdout().ok_or(io::ErrorKind::NotFound)?);
    receive_ready.recv().map_err(io::Error::other)?;
    let epoch = Instant::now();
    let deadline = epoch + Duration::from_secs(120);
    let resumed = Arc::new(AtomicBool::new(false));
    let advanced = resumed.clone();
    let (armed, receive_armed) = std::sync::mpsc::sync_channel(1);
    let (checked, receive_checked) = std::sync::mpsc::sync_channel(1);
    let (finished, receive_finished) = std::sync::mpsc::sync_channel(1);
    let waiter = std::thread::spawn(move || -> io::Result<_> {
        let resume = platform::wait::ResumeNotification::new()?;
        let timer = if precise {
            Some(platform::wait::deadline_timer().ok_or_else(io::Error::last_os_error)?)
        } else {
            None
        };
        let mut armed = Some((armed, resume.event.as_handle().try_clone_to_owned()?));
        let mut checked = Some(checked);
        let result = platform::wait::wait_with_resume(
            tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
            deadline,
            timer,
            &resume,
            || {
                // Sample before publishing readiness: the initial native wait
                // retains its full 120-second relative timeout across the jump.
                let now = if advanced.load(Ordering::Acquire) {
                    if let Some(checked) = checked.take() {
                        let _ = checked.send(());
                    }
                    epoch + suspended
                } else {
                    epoch
                };
                if let Some((armed, event)) = armed.take() {
                    let _ = armed.send(event);
                }
                now
            },
        );
        let reported = result
            .as_ref()
            .map(|status| status.success())
            .map_err(io::Error::kind);
        finished.send(reported).map_err(io::Error::other)?;
        Ok((tree, result))
    });
    let result = (|| -> io::Result<()> {
        let wake = receive_armed.recv().map_err(io::Error::other)?;
        resumed.store(true, Ordering::Release);
        // SAFETY: this is an owned alias of the production callback's event.
        // No real host clock or power state is changed by this regression.
        unsafe {
            platform::wait::resume_notification(
                wake.as_raw_handle(),
                PBT_APMRESUMEAUTOMATIC,
                std::ptr::null(),
            );
        }
        receive_checked.recv().map_err(io::Error::other)?;
        if suspended >= Duration::from_secs(120) {
            assert_eq!(
                receive_finished.recv().map_err(io::Error::other)?,
                Err(io::ErrorKind::TimedOut)
            );
        }
        Ok(())
    })();
    // Always release the child after the resume observation finishes.
    let released = release(&mut gate);
    let (mut tree, status) = waiter
        .join()
        .map_err(|_| io::Error::other("resume waiter panicked"))??;
    let cleanup = tree.terminate(None);
    let drained = reader
        .join()
        .map_err(|_| io::Error::other("readiness reader panicked"))?;
    result.and(released).and(cleanup).and(drained)?;
    if suspended < Duration::from_secs(120) {
        assert!(status?.success());
    }
    Ok(())
}

/// Optional measurement hooks: wait for a start byte, and trace spans.
fn measurement_setup() -> io::Result<Option<tracing::subscriber::DefaultGuard>> {
    if std::env::var_os("ACYCLIC_NATIVE_WAIT_START_GATE").is_some() {
        let mut signal = [0];
        std::io::stdin().read_exact(&mut signal)?;
        assert_eq!(signal, *b"g");
    }
    Ok(std::env::var_os("ACYCLIC_NATIVE_WAIT_TRACING").map(|_| {
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_ansi(false)
                .finish(),
        )
    }))
}

/// Unbounded, already-expired and zero waits fail without reaping the child.
fn assert_immediate_waits(
    meter: Option<&dyn crate::NativeReadMeter>,
    tree: &mut ProcessTree,
) -> io::Result<()> {
    assert_eq!(
        metered_wait(meter, tree, Duration::MAX)
            .err()
            .ok_or(io::ErrorKind::Other)?
            .kind(),
        io::ErrorKind::InvalidInput,
    );
    assert_eq!(
        platform::wait_for_exit_coarse(
            tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
            Instant::now(),
        )
        .err()
        .ok_or(io::ErrorKind::Other)?
        .kind(),
        io::ErrorKind::TimedOut,
    );
    assert_eq!(
        metered_wait(meter, tree, Duration::ZERO)
            .err()
            .ok_or(io::ErrorKind::Other)?
            .kind(),
        io::ErrorKind::TimedOut
    );
    Ok(())
}

fn wait_preserves_ownership(coarse: bool) -> io::Result<()> {
    let _subscriber = measurement_setup()?;
    let accounting = std::env::var_os("ACYCLIC_NATIVE_WAIT_ACCOUNTING")
        .map(|_| crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX));
    let meter = accounting
        .as_ref()
        .map(|meter| meter as &dyn crate::NativeReadMeter);
    let mut command = helper_command()?;
    let mut custody = None;
    if let Err(error) = ProcessTree::spawn_controlled(&mut command, meter, &mut custody) {
        if let Some(tree) = custody.as_mut() {
            tree.terminate(meter)?;
        }
        return Err(error);
    }
    let mut tree = custody.ok_or(io::ErrorKind::NotFound)?;
    let mut gate = Some(tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?);
    let wait = |tree: &mut ProcessTree, timeout| metered_wait(meter, tree, timeout);
    let pending_wait = |tree: &mut ProcessTree, timeout: Duration| {
        if !coarse {
            return wait(tree, timeout);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(io::ErrorKind::InvalidInput)?;
        process_control_attempt(meter, crate::NativeOperationKind::Read, || {
            platform::wait_for_exit_coarse(
                tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                deadline,
            )
        })
    };
    let (received, reader) = readiness(tree.take_stdout().ok_or(io::ErrorKind::NotFound)?);
    let result = (|| {
        received.recv().map_err(io::Error::other)?;
        assert_immediate_waits(meter, &mut tree)?;
        let cycles = thread_cycles()?;
        let cpu = thread_cpu()?;
        let operations = || {
            accounting.as_ref().map_or(0, |meter| {
                meter
                    .used_operations
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        };
        let before = operations();
        let started = Instant::now();
        let result = pending_wait(&mut tree, Duration::from_millis(50));
        let elapsed = started.elapsed();
        let cpu = thread_cpu()? - cpu;
        let cycles = thread_cycles()? - cycles;
        let recorded = operations() - before;
        assert_eq!(recorded, u64::from(meter.is_some()));
        assert_eq!(
            result.err().ok_or(io::ErrorKind::Other)?.kind(),
            io::ErrorKind::TimedOut
        );
        assert!(elapsed >= Duration::from_millis(50));
        assert!(!tree.is_reaped());
        assert!(tree.try_wait(meter)?.is_none());
        eprintln!(
            "NATIVE_WAIT_TIMEOUT elapsed_us={} cpu_100ns={cpu} cycles={cycles} recorded_ops={recorded} coarse={coarse}",
            elapsed.as_micros()
        );
        release(&mut gate)?;
        assert!(tree.wait_exit(meter)?.success());
        assert!(!tree.is_reaped());
        assert!(wait(&mut tree, Duration::ZERO)?.success());
        assert!(tree.try_wait(meter)?.is_some());
        assert!(
            platform::wait_for_exit_coarse(
                tree.child.as_mut().ok_or(io::ErrorKind::NotFound)?,
                Instant::now(),
            )?
            .success()
        );
        Ok(())
    })();
    let cleanup = tree.terminate(meter);
    let drained = reader
        .join()
        .map_err(|_| io::Error::other("readiness reader panicked"))?;
    result.and(cleanup).and(drained)
}

#[test]
fn dropped_helper_input_releases_its_process_wait() -> io::Result<()> {
    let mut tree = ProcessTree::spawn(&mut helper_command()?)?;
    let gate = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
    let (received, reader) = readiness(tree.take_stdout().ok_or(io::ErrorKind::NotFound)?);
    received.recv().map_err(io::Error::other)?;
    // Negative control: no release byte is sent. The original stdin producer
    // disappears, so read_exact returns UnexpectedEof and the real helper exits.
    drop(gate);
    let status = tree.wait_exit(None)?;
    let cleanup = tree.terminate(None);
    let drained = reader
        .join()
        .map_err(|_| io::Error::other("readiness reader panicked"))?;
    cleanup.and(drained)?;
    assert!(!status.success());
    assert!(tree.is_reaped());
    Ok(())
}
