use super::*;
use acyclic_native_runtime::{
    CaptureWakes, ProcessInput, ProcessOutput, ProcessTree, ProcessWake, TaskSpawner,
};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

#[test]
fn child() -> io::Result<()> {
    let Some(mode) = std::env::var_os("ACYCLIC_TEST_MACHINE_PROCESS") else {
        return Ok(());
    };
    if let Some(marker) = std::env::var_os("ACYCLIC_TEST_MACHINE_MARKER") {
        std::fs::write(
            marker,
            std::env::current_dir()?.to_string_lossy().as_bytes(),
        )?;
    }
    std::io::stdout().write_all(&[0, 255, 3])?;
    std::io::stdout().flush()?;
    std::io::stderr().write_all(&[0, 254, 4])?;
    std::io::stderr().flush()?;
    if mode == "credit" {
        std::io::stdout().write_all(&[b'x'; 512 * 1024])?;
    }
    if mode == "input" {
        let mut bytes = [0; 4];
        std::io::stdin().read_exact(&mut bytes)?;
        assert_eq!(bytes, [0, 255, 7, 0]);
    } else {
        std::io::stdin().read_exact(&mut [0])?;
    }
    std::process::exit(9);
}

#[tokio::test]
async fn local_machine_spawns_public_command_once_and_returns_original_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let cwd = std::fs::canonicalize(std::env::temp_dir())?;
    let marker = cwd.join(format!("acyclic-machine-{}", Uuid::new_v4()));
    let machine = MachineId::new();
    let local = LocalMachine::new(
        machine,
        LocalOptions {
            cwd: cwd.clone(),
            environment: BTreeMap::from([
                ("ACYCLIC_TEST_MACHINE_PROCESS".into(), "input".into()),
                (
                    "ACYCLIC_TEST_MACHINE_MARKER".into(),
                    marker.to_str().unwrap().into(),
                ),
            ]),
            inherit_environment: true,
            process_custody: None,
        },
    )?;
    let command = || {
        ExecCommand::new(
            vec![
                std::env::current_exe().unwrap().to_str().unwrap().into(),
                "--exact".into(),
                "local::tests::child".into(),
                "--nocapture".into(),
            ],
            5_000,
        )
        .unwrap()
        .with_stdin(vec![0, 255, 7, 0])
    };
    let buffers = Buffers::default();
    let handler = || Handler {
        buffers: buffers.clone(),
        wake: ProcessWake::new().unwrap(),
        lose_owner: false,
    };
    // The actual child would publish this file before doing anything else.
    // An expired admission must refuse before creating that real process.
    let deadline = || std::time::Instant::now() + Duration::from_secs(5);
    let mut refused = local
        .exec(command().with_expiry(1), handler(), deadline())
        .await;
    assert!(matches!(
        refused.next().await,
        Some(Err(ProviderError::Rejected(_)))
    ));
    assert!(Arc::ptr_eq(&refused.finish().await?.buffers, &buffers));
    assert!(!marker.exists());
    let mut execution = local.exec(command(), handler(), deadline()).await;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit = None;
    while let Some(frame) = execution.next().await {
        let (is_stderr, bytes, destination) = match frame? {
            ExecOutput::Data(bytes) => (false, bytes, &mut stdout),
            ExecOutput::Stderr(bytes) => (true, bytes, &mut stderr),
            ExecOutput::ExitCode(code) => {
                assert!(exit.replace(code).is_none());
                continue;
            }
            ExecOutput::Stopped(stop) => panic!("unexpected native stop: {stop:?}"),
        };
        assert!(exit.is_none());
        let original = buffers.lock().unwrap().pop_front().unwrap();
        assert_eq!(original, (is_stderr, bytes.as_ptr() as usize, bytes.len()));
        let copied = bytes.clone();
        assert_ne!(
            original.1,
            copied.as_ptr() as usize,
            "copying fails the same native-buffer oracle"
        );
        destination.extend(bytes);
    }
    assert_eq!(exit, Some(9));
    assert!(stdout.ends_with(&[0, 255, 3]));
    assert_eq!(stderr, [0, 254, 4]);
    let original = execution.finish().await?;
    assert!(Arc::ptr_eq(&original.buffers, &buffers));
    assert_eq!(std::fs::read(&marker)?, cwd.to_string_lossy().as_bytes());
    std::fs::remove_file(marker)?;
    let mut lost = local
        .exec(
            command(),
            Handler {
                buffers,
                wake: ProcessWake::new()?,
                lose_owner: true,
            },
            deadline(),
        )
        .await;
    assert!(
        matches!(lost.next().await, Some(Err(ProviderError::CommandIndeterminate(id))) if id == machine)
    );
    assert!(
        matches!(lost.finish().await, Err(ProviderError::CommandIndeterminate(id)) if id == machine)
    );
    Ok(())
}

type Buffers = Arc<Mutex<VecDeque<(bool, usize, usize)>>>;
struct Handler {
    buffers: Buffers,
    wake: ProcessWake,
    lose_owner: bool,
}

impl CaptureHandler for Handler {
    fn take_wakes(&mut self) -> io::Result<CaptureWakes> {
        Ok(Some(self.wake.clone()).into())
    }
    fn check(&mut self) -> io::Result<()> {
        assert!(!self.lose_owner, "injected native capture owner loss");
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
        self.buffers
            .lock()
            .unwrap()
            .push_back((stderr, bytes.as_ptr() as usize, bytes.len()));
        Ok(ProcessInput::default())
    }
}

#[tokio::test]
async fn public_output_moves_native_buffers_and_preserves_terminal_cleanup() -> io::Result<()> {
    for mode in ["exit", "stop", "owner_lost"] {
        let stop = mode == "stop";
        let lose_owner = mode == "owner_lost";
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", "local::tests::child", "--nocapture"])
            .env(
                "ACYCLIC_TEST_MACHINE_PROCESS",
                if stop { "credit" } else { "exit" },
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command)?;
        let mut input = tree.take_stdin().ok_or(io::ErrorKind::BrokenPipe)?;
        let buffers = Buffers::default();
        let wake = ProcessWake::new()?;
        let capture = tree.stream_capture(
            &TaskSpawner::current(),
            Duration::from_secs(5),
            usize::MAX,
            None,
            Handler {
                buffers: buffers.clone(),
                wake: wake.clone(),
                lose_owner,
            },
        );
        let machine = MachineId::new();
        let mut output = output(machine, capture);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut released = false;
        let mut terminal = false;
        while let Some(frame) = output.next().await {
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    assert!(lose_owner);
                    assert_eq!(error, ProviderError::CommandIndeterminate(machine));
                    assert!(!terminal);
                    terminal = true;
                    continue;
                }
            };
            let (is_stderr, bytes, destination) = match frame {
                ExecOutput::Data(bytes) => (false, bytes, &mut stdout),
                ExecOutput::Stderr(bytes) => (true, bytes, &mut stderr),
                ExecOutput::ExitCode(code) => {
                    assert!(!stop);
                    assert_eq!(code, 9);
                    terminal = true;
                    continue;
                }
                ExecOutput::Stopped(stopped) => {
                    assert!(stop);
                    assert_eq!(stopped.kind, ExecStopKind::ControlStopped);
                    assert!(stopped.cleanup_completed);
                    terminal = true;
                    continue;
                }
            };
            assert!(!terminal);
            let original = buffers.lock().unwrap().pop_front().unwrap();
            assert_eq!(original, (is_stderr, bytes.as_ptr() as usize, bytes.len()));
            let copied = bytes.clone();
            assert_ne!(
                original.1,
                copied.as_ptr() as usize,
                "copying transport fails identity"
            );
            destination.extend_from_slice(&bytes);
            if !released && stdout.windows(3).any(|bytes| bytes == [0, 255, 3]) {
                if stop {
                    wake.stop()?;
                } else {
                    input.write_all(b"!")?;
                }
                released = true;
            }
        }
        assert!(terminal);
        assert_eq!(released, !lose_owner);
        if !stop && !lose_owner {
            assert!(stdout.ends_with(&[0, 255, 3]));
            assert_eq!(stderr, [0, 254, 4]);
        }
        assert!(buffers.lock().unwrap().is_empty());
    }
    Ok(())
}
