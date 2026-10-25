use super::*;
use std::io::Write;
use std::process::Stdio;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[test]
fn event_capture_child() -> io::Result<()> {
    let Some(mode) = std::env::var_os("ACYCLIC_TEST_EVENT_CAPTURE") else {
        return Ok(());
    };
    if mode == "backpressure" {
        let bytes = [b'x'; 128 * 1024];
        std::io::stdout().write_all(&bytes)?;
        std::io::stderr().write_all(&bytes)?;
        let mut count = 0;
        let mut expected = 0u8;
        let mut input = [0; 4096];
        loop {
            let read = std::io::stdin().read(&mut input)?;
            if read == 0 {
                break;
            }
            for byte in input.iter().take(read) {
                if *byte != expected {
                    return Err(io::Error::other(format!("incorrect input byte at {count}")));
                }
                count += 1;
                expected += 1;
                if expected == 251 {
                    expected = 0;
                }
            }
            std::io::stderr().write_all(b".")?;
            std::io::stderr().flush()?;
        }
        println!("INPUT_BYTES={count}");
    } else {
        let delay = if mode == "cancel" { 2_000 } else { 200 };
        if mode == "cancel" {
            println!("CONTROL_READY");
            std::io::stdout().flush()?;
        } else {
            println!("QUIET_BEGIN");
            std::io::stdout().flush()?;
        }
        std::thread::sleep(Duration::from_millis(delay));
        println!("EVENT_CAPTURE_DONE");
    }
    Ok(())
}

fn child(mode: &str) -> io::Result<ProcessTree> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::capture_events_tests::event_capture_child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_EVENT_CAPTURE", mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    ProcessTree::spawn(&mut command)
}

#[test]
fn a_quiet_capture_sleeps_until_readiness_instead_of_polling() -> io::Result<()> {
    let mut process = child("quiet")?;
    let mut checks = 0;
    let quiet = std::cell::Cell::<Option<Instant>>::new(None);
    let idle_checks = std::cell::Cell::new(0);
    let mut observed = Vec::new();
    let started = Instant::now();
    let output = process
        .wait_with_output(
            Duration::from_secs(2),
            16 * 1024,
            None,
            || {
                checks += 1;
                if quiet.get().is_some_and(|start| {
                    (Duration::from_millis(20)..Duration::from_millis(150))
                        .contains(&start.elapsed())
                }) {
                    idle_checks.set(idle_checks.get() + 1);
                }
                Ok(())
            },
            |chunk| {
                if let ProcessOutput::Stdout(bytes) = chunk {
                    observed.extend_from_slice(bytes);
                    if quiet.get().is_none()
                        && observed
                            .windows(b"QUIET_BEGIN".len())
                            .any(|bytes| bytes == b"QUIET_BEGIN")
                    {
                        quiet.set(Some(Instant::now()));
                    }
                }
                Ok(ProcessInput::default())
            },
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)?;
    eprintln!(
        "IDLE_CAPTURE checks={checks} idle_checks={} elapsed_us={}",
        idle_checks.get(),
        started.elapsed().as_micros()
    );
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("EVENT_CAPTURE_DONE"));
    assert!(process.is_reaped());
    assert!(
        quiet.get().is_some(),
        "child never declared its quiet interval"
    );
    assert_eq!(
        idle_checks.get(),
        0,
        "quiet capture must wait for readiness"
    );
    Ok(())
}

#[test]
fn control_notification_interrupts_a_silent_child_and_reaps_it() -> io::Result<()> {
    let mut process = child("cancel")?;
    let wake = super::wake::ProcessWake::new()?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let signal = wake.clone();
    let producer = Arc::clone(&cancelled);
    let (ready, announced) = std::sync::mpsc::sync_channel(1);
    let mut ready = Some(ready);
    let mut observed = Vec::new();
    let thread = std::thread::spawn(move || {
        announced
            .recv_timeout(Duration::from_secs(1))
            .map_err(io::Error::other)?;
        std::thread::sleep(Duration::from_millis(50));
        producer.store(true, Ordering::Release);
        signal.notify()
    });
    let started = Instant::now();
    let result = process.wait_with_output(
        Duration::from_secs(1),
        16 * 1024,
        None,
        || {
            if cancelled.load(Ordering::Acquire) {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                Ok(())
            }
        },
        |chunk| {
            if let ProcessOutput::Stdout(bytes) = chunk {
                observed.extend_from_slice(bytes);
                if observed
                    .windows(b"CONTROL_READY".len())
                    .any(|bytes| bytes == b"CONTROL_READY")
                    && let Some(ready) = ready.take()
                {
                    ready.send(()).map_err(io::Error::other)?;
                }
            }
            Ok(ProcessInput::default())
        },
        None,
        None,
        Some(std::slice::from_ref(&wake)),
    );
    thread
        .join()
        .map_err(|_| io::Error::other("wake producer panicked"))??;
    let failure = result.expect_err("control wake must stop the sleeping command");
    assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
    assert!(failure.cleanup_completed);
    assert!(process.is_reaped());
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "control wake did not interrupt the readiness wait"
    );
    Ok(())
}

#[test]
fn backpressure_waits_for_both_output_channels_and_remaining_input() -> io::Result<()> {
    let mut process = child("backpressure")?;
    let input: Vec<_> = (0u8..251).cycle().take(8 * 1024 * 1024).collect();
    let output = process
        .wait_with_output(
            Duration::from_secs(3),
            512 * 1024,
            Some(CaptureInput::Literal(&input)),
            || Ok(()),
            |_| Ok(ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)?;
    assert!(output.status.success());
    assert!(output.stderr.starts_with(&[b'x'; 128 * 1024]));
    assert!(
        output
            .stderr
            .get(128 * 1024..)
            .is_some_and(|tail| !tail.is_empty() && tail.iter().all(|byte| *byte == b'.'))
    );
    assert!(
        output
            .stdout
            .windows(128 * 1024)
            .any(|bytes| bytes == [b'x'; 128 * 1024])
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("INPUT_BYTES=8388608"));
    assert!(process.is_reaped());
    Ok(())
}
