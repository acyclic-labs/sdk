use super::*;
use std::collections::VecDeque;
use std::io::Write;
use std::process::Stdio;
use std::sync::{Arc, Mutex};

#[test]
fn child() -> io::Result<()> {
    let Some(mode) = std::env::var_os("ACYCLIC_TEST_STREAM_CAPTURE") else {
        return Ok(());
    };
    if mode == "wrapper" {
        let status = Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "process_tree::stream::tests::child",
                "--nocapture",
            ])
            .env("ACYCLIC_TEST_STREAM_CAPTURE", "exit")
            .status()?;
        std::process::exit(status.code().unwrap_or(1));
    }
    writeln!(std::io::stdout(), "CHILD={}", std::process::id())?;
    std::io::stdout().flush()?;
    std::io::stderr().write_all(&[0, 255, 7])?;
    if mode == "credit" {
        std::io::stdout().write_all(&[b'x'; 512 * 1024])?;
    }
    // The parent owns this pipe until it chooses completion. No timer or
    // readiness probe coordinates the child with its actual stream reader.
    std::io::stdin().read_exact(&mut [0])?;
    std::process::exit(7);
}

type Addresses = Arc<Mutex<VecDeque<(bool, usize, usize)>>>;
struct Handler {
    wake: ProcessWake,
    addresses: Addresses,
    took_wakes: bool,
}

impl CaptureHandler for Handler {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        assert!(
            !self.took_wakes,
            "control signals are transferred exactly once"
        );
        assert!(
            std::thread::current()
                .name()
                .unwrap()
                .starts_with("acyclic-host-io-")
        );
        self.took_wakes = true;
        Ok(Some(self.wake.clone()).into())
    }
    fn check(&mut self) -> io::Result<()> {
        assert!(self.took_wakes, "setup precedes the first physical turn");
        if self.wake.is_stopped() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        Ok(())
    }
    fn observe(&mut self, output: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        let (stderr, bytes) = match output {
            ProcessOutput::Stdout(bytes) => (false, bytes),
            ProcessOutput::Stderr(bytes) => (true, bytes),
        };
        self.addresses
            .lock()
            .unwrap()
            .push_back((stderr, bytes.as_ptr() as usize, bytes.len()));
        Ok(ProcessInput::default())
    }
}

fn start(
    mode: &str,
    schedule: impl Into<CaptureSchedule>,
) -> io::Result<(
    ProcessCaptureStream<Handler>,
    std::process::ChildStdin,
    u32,
    Addresses,
    ProcessWake,
)> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "process_tree::stream::tests::child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_STREAM_CAPTURE", mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut tree = ProcessTree::spawn(&mut command)?;
    let child = tree.child.as_mut().unwrap();
    let pid = child.id();
    let input = child.stdin.take().unwrap();
    let addresses = Arc::new(Mutex::new(VecDeque::new()));
    let wake = ProcessWake::new()?;
    let output = tree.stream_capture(
        &crate::TaskSpawner::current(),
        schedule,
        usize::MAX,
        None,
        Handler {
            wake: wake.clone(),
            addresses: addresses.clone(),
            took_wakes: false,
        },
    );
    Ok((output, input, pid, addresses, wake))
}

fn original_buffer(original: (bool, usize, usize), stderr: bool, bytes: &[u8]) -> bool {
    original == (stderr, bytes.as_ptr() as usize, bytes.len())
}

#[tokio::test]
async fn native_stream_moves_original_buffers_and_delivers_exit_after_pipes() -> io::Result<()> {
    for mode in ["exit", "wrapper"] {
        let (mut output, mut input, pid, addresses, _) = start(mode, CaptureSchedule::Unbounded)?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut released = false;
        while let Some(chunk) = output.recv().await {
            for (is_stderr, bytes, destination) in [
                (false, chunk.stdout, &mut stdout),
                (true, chunk.stderr, &mut stderr),
            ] {
                if bytes.is_empty() {
                    continue;
                }
                assert!(bytes.len() <= 64 * 1024);
                let original = addresses.lock().unwrap().pop_front().unwrap();
                assert!(original_buffer(original, is_stderr, &bytes));
                // A copying adapter fails the same pointer identity oracle.
                assert!(!original_buffer(original, is_stderr, &bytes.clone()));
                destination.extend_from_slice(&bytes);
            }
            if !released && String::from_utf8_lossy(&stdout).contains("CHILD=") {
                input.write_all(b"!")?;
                released = true;
            }
        }
        assert!(released);
        let direct = String::from_utf8_lossy(&stdout).contains(&format!("CHILD={pid}\n"));
        assert_eq!(
            direct,
            mode == "exit",
            "an extra launcher fails the direct-child oracle"
        );
        assert_eq!(stderr, [0, 255, 7]);
        let result = output.finish().await?.result.map_err(io::Error::other)?;
        assert_eq!(result.status.code(), Some(7));
        assert!(
            result.stdout.is_empty() && result.stderr.is_empty(),
            "no second output collector"
        );
    }
    Ok(())
}

#[tokio::test]
async fn unbounded_capture_finishes_when_its_original_input_producer_disappears() -> io::Result<()>
{
    let (mut output, input, _, _, _) = start("exit", CaptureSchedule::Unbounded)?;
    drop(input);
    while output.recv().await.is_some() {}
    let result = output.finish().await?.result.map_err(io::Error::other)?;
    assert!(!result.status.success());
    assert_ne!(
        result.status.code(),
        Some(7),
        "the child never received its release byte"
    );
    // The successful release in the original-buffer control above uses the
    // same child and capture owner; EOF must release that owner's wait too.
    Ok(())
}

#[tokio::test]
async fn receiver_loss_retires_quiet_and_backpressured_children() -> io::Result<()> {
    for mode in ["exit", "credit"] {
        let (mut output, _input, _, _, _) = start(mode, CaptureSchedule::Unbounded)?;
        assert!(output.recv().await.is_some());
        // Withhold all further output credit, then close it. Completion
        // requires physical containment cleanup, not the child's input EOF.
        let failure = output.finish().await?.result.unwrap_err();
        assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
        assert!(failure.cleanup_completed);
    }
    Ok(())
}

#[tokio::test]
async fn owner_stop_and_deadline_retire_without_reader_credit() -> io::Result<()> {
    for mode in ["stop", "relative", "admitted", "queued"] {
        let schedule = match mode {
            "admitted" => CaptureSchedule::until(Instant::now()),
            "queued" => {
                // Model elapsed submission/setup time without sleeping or
                // occupying the worker pool. The real worker uses wall time.
                super::super::set_capture_clock(Some(Instant::now() - Duration::from_secs(1)));
                let schedule = CaptureSchedule::new(Duration::from_millis(100));
                super::super::set_capture_clock(None);
                schedule
            }
            "relative" => CaptureSchedule::new(Duration::from_millis(100)),
            _ => CaptureSchedule::Unbounded,
        };
        let (mut output, _input, _, addresses, wake) = start("credit", schedule)?;
        // An elapsed original deadline allows no pipe read. Restarting the
        // relative budget instead delivers data, as the other real runs show.
        let expired_before_setup = matches!(mode, "admitted" | "queued");
        assert_eq!(output.recv().await.is_none(), expired_before_setup);
        if mode == "stop" {
            wake.stop()?;
        }
        // Await the ORIGINAL cleanup owner while retaining the unread receiver.
        // This catches an implementation that waits for credit before control.
        let outcome = (&mut output.completion).await??;
        let failure = outcome.result.unwrap_err();
        assert_eq!(
            failure.error.kind(),
            if mode == "stop" {
                io::ErrorKind::Interrupted
            } else {
                io::ErrorKind::TimedOut
            }
        );
        assert!(failure.cleanup_completed);
        assert!(output.receiver.len() <= 1);
        if expired_before_setup {
            assert!(addresses.lock().unwrap().is_empty());
        }
    }
    Ok(())
}
