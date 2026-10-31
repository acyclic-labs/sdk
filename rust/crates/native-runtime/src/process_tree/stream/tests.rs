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
    if mode == "phases" {
        let mut input = Vec::new();
        std::io::stdin().read_to_end(&mut input)?;
        std::io::stdout().write_all(&input)?;
        std::io::stdout().flush()?;
        std::process::exit(124);
    }
    if mode == "credit" {
        std::io::stdout().write_all(&[b'x'; 512 * 1024])?;
    }
    // The parent owns this pipe until it chooses completion. No timer or
    // readiness probe coordinates the child with its actual stream reader.
    std::io::stdin().read_exact(&mut [0])?;
    std::process::exit(7);
}

#[derive(Default)]
struct ProgressCounts {
    accepted: usize,
    written: usize,
    stdin_closed: bool,
    stdout: usize,
    stderr: usize,
    stdout_eof: bool,
    stderr_eof: bool,
    exit: Option<std::process::ExitStatus>,
    retired: bool,
}

struct ProgressHandler {
    counts: ProgressCounts,
    wake: ProcessWake,
    stop_at_admission: bool,
}

impl CaptureHandler for ProgressHandler {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        Ok(Some(self.wake.clone()).into())
    }

    fn check(&mut self) -> io::Result<()> {
        if self.wake.is_stopped() {
            Err(io::ErrorKind::Interrupted.into())
        } else {
            Ok(())
        }
    }

    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }

    fn progress(&mut self, progress: CaptureProgress) {
        assert!(!self.counts.retired);
        match progress {
            CaptureProgress::InputAccepted(bytes) => {
                self.counts.accepted += bytes;
                if self.stop_at_admission {
                    self.wake.stop().unwrap();
                }
            }
            CaptureProgress::InputWritten(bytes) => {
                assert!(!self.counts.stdin_closed);
                self.counts.written += bytes;
                assert!(self.counts.written <= self.counts.accepted);
            }
            CaptureProgress::StdinClosed => {
                assert!(!self.counts.stdin_closed);
                assert_eq!(self.counts.written, self.counts.accepted);
                self.counts.stdin_closed = true;
            }
            CaptureProgress::StdoutRead(bytes) => {
                assert!(!self.counts.stdout_eof);
                self.counts.stdout += bytes;
            }
            CaptureProgress::StderrRead(bytes) => {
                assert!(!self.counts.stderr_eof);
                self.counts.stderr += bytes;
            }
            CaptureProgress::StdoutEof => {
                assert!(!self.counts.stdout_eof);
                self.counts.stdout_eof = true;
            }
            CaptureProgress::StderrEof => {
                assert!(!self.counts.stderr_eof);
                self.counts.stderr_eof = true;
            }
            CaptureProgress::RootExited(status) => {
                assert!(self.counts.exit.replace(status).is_none());
            }
        }
    }

    fn retired(&mut self, completed: bool) {
        assert!(completed);
        assert!(!self.counts.retired);
        self.counts.retired = true;
    }
}

#[tokio::test]
async fn capture_progress_preserves_native_exit_and_refuses_admission_as_delivery() -> io::Result<()>
{
    for stop_at_admission in [false, true] {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "process_tree::stream::tests::child",
                "--nocapture",
            ])
            .env("ACYCLIC_TEST_STREAM_CAPTURE", "phases")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let tree = ProcessTree::spawn(&mut command)?;
        let input = b"ORIGINAL_INPUT";
        let handler = ProgressHandler {
            counts: ProgressCounts::default(),
            wake: ProcessWake::new()?,
            stop_at_admission,
        };
        let outcome = tree
            .capture(
                CaptureSchedule::new(Duration::from_secs(5)),
                usize::MAX,
                Some(OwnedCaptureInput::Literal(input.to_vec())),
                handler,
            )
            .await?;
        let counts = outcome.handler.counts;
        assert_eq!(counts.accepted, input.len());
        assert!(counts.retired);
        if stop_at_admission {
            let failure = outcome.result.unwrap_err();
            assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
            assert!(failure.cleanup_completed);
            assert_eq!(counts.written, 0);
            assert!(!counts.stdin_closed && !counts.stdout_eof && !counts.stderr_eof);
            assert!(
                counts.exit.is_none(),
                "retirement cannot invent a native exit observation"
            );
        } else {
            let output = outcome.result.map_err(io::Error::other)?;
            assert_eq!(output.status.code(), Some(124));
            assert_eq!(counts.exit, Some(output.status));
            assert_eq!(counts.written, input.len());
            assert!(counts.stdin_closed && counts.stdout_eof && counts.stderr_eof);
            assert_eq!(counts.stdout, output.stdout.len());
            assert_eq!(counts.stderr, output.stderr.len());
            assert!(output.stdout.ends_with(input));
            assert_eq!(output.stderr, [0, 255, 7]);
        }
    }
    Ok(())
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
        let (mut output, mut input, pid, addresses, _) = start(mode, Duration::from_secs(5))?;
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
async fn receiver_loss_retires_quiet_and_backpressured_children() -> io::Result<()> {
    for mode in ["exit", "credit"] {
        let (mut output, _input, _, _, _) = start(mode, Duration::from_secs(5))?;
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
            _ => CaptureSchedule::new(Duration::from_secs(5)),
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
