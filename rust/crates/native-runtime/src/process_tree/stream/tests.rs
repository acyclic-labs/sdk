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
}

impl CaptureHandler for Handler {
    fn wakes(&self) -> &[ProcessWake] {
        std::slice::from_ref(&self.wake)
    }
    fn check(&mut self) -> io::Result<()> {
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
    timeout: Duration,
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
        timeout,
        usize::MAX,
        None,
        Handler {
            wake: wake.clone(),
            addresses: addresses.clone(),
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
    for expired in [false, true] {
        let timeout = if expired {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(5)
        };
        let (mut output, _input, _, _, wake) = start("credit", timeout)?;
        assert!(output.recv().await.is_some());
        if !expired {
            wake.stop()?;
        }
        // Await the ORIGINAL cleanup owner while retaining the unread receiver.
        // This catches an implementation that waits for credit before control.
        let outcome = (&mut output.completion).await??;
        let failure = outcome.result.unwrap_err();
        assert_eq!(
            failure.error.kind(),
            if expired {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Interrupted
            }
        );
        assert!(failure.cleanup_completed);
        assert!(output.receiver.len() <= 1);
    }
    Ok(())
}
