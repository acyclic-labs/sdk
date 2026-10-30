use super::*;
use std::io::Write as _;
use std::time::Duration;

const MODE: &str = "ACYCLIC_TEST_NATIVE_LAUNCH";
const MARKER: &str = "ACYCLIC_TEST_NATIVE_LAUNCH_MARKER";

#[test]
fn child() -> io::Result<()> {
    if std::env::var_os(MODE).is_none() {
        return Ok(());
    }
    if let Some(marker) = std::env::var_os(MARKER) {
        std::fs::write(marker, b"ran")?;
    }
    let mut input = Vec::new();
    std::io::stdin().read_to_end(&mut input)?;
    let cwd = std::env::current_dir()?;
    std::io::stdout().write_all(&input)?;
    std::io::stdout().write_all(cwd.file_name().unwrap_or_default().as_encoded_bytes())?;
    std::io::stderr().write_all(b"diagnostic")?;
    std::process::exit(3);
}

/// Changes one launch so that it must be refused before spawn.
type Refusal = fn(&mut NativeCommand, &mut Recorder);

#[derive(Default)]
struct Recorder {
    refuse: bool,
    retired: Option<bool>,
}

impl CaptureHandler for Recorder {
    fn check(&mut self) -> io::Result<()> {
        if self.refuse {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        Ok(())
    }
    fn observe(&mut self, _: ProcessOutput<'_>) -> io::Result<ProcessInput> {
        Ok(ProcessInput::default())
    }
    fn retired(&mut self, cleanup_completed: bool) {
        self.retired = Some(cleanup_completed);
    }
}

fn host(root: &std::path::Path, marker: &std::path::Path) -> Arc<NativeHost> {
    Arc::new(NativeHost {
        cwd: root.into(),
        environment: BTreeMap::from([
            (MODE.into(), "1".into()),
            (MARKER.into(), marker.to_string_lossy().into_owned()),
        ]),
        // The test binary needs its platform's loader environment.
        inherit_environment: true,
        custody: None,
    })
}

fn command() -> NativeCommand {
    NativeCommand {
        argv: vec![
            std::env::current_exe().unwrap().into_os_string(),
            "--exact".into(),
            "process_tree::launch::tests::child".into(),
            "--nocapture".into(),
        ],
        cwd: Some("work".into()),
        stdin: Some(b"input:".to_vec()),
        deadline: Instant::now() + Duration::from_secs(60),
        maximum_output_bytes: 1 << 20,
    }
}

/// The checkout shell collects and a local Machine streams; the same
/// command must observe the same directory, input, output and exit.
#[tokio::test]
async fn collected_and_streamed_launches_run_a_command_identically() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("work"))?;
    let host = host(root.path(), &root.path().join("marker"));
    let Ok(collected) = command()
        .capture(Arc::clone(&host), Recorder::default())
        .await
    else {
        panic!("collected launch failed");
    };
    let collected = collected.result.map_err(io::Error::other)?;
    let Ok(mut streamed) = command().stream(host, Recorder::default()).await else {
        panic!("streamed launch failed");
    };
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    while let Some(chunk) = streamed.recv().await {
        stdout.extend_from_slice(&chunk.stdout);
        stderr.extend_from_slice(&chunk.stderr);
    }
    let streamed = streamed.finish().await?;
    assert_eq!(streamed.handler.retired, Some(true));
    let status = streamed.result.map_err(io::Error::other)?.status;
    // The test harness prints its own banner before the child's bytes.
    assert!(
        collected.stdout.ends_with(b"input:work"),
        "stdin or the joined cwd was lost: {collected:?}"
    );
    assert!(collected.stderr.ends_with(b"diagnostic"));
    assert_eq!(collected.status.code(), Some(3));
    assert_eq!((stdout, stderr), (collected.stdout, collected.stderr));
    assert_eq!(status.code(), collected.status.code());
    Ok(())
}

/// Every refusal returns the handler before anything runs, so a caller can
/// report it as definite. The unchanged command is the negative control: it
/// runs, so the absent marker proves each refusal preceded the spawn.
#[tokio::test]
async fn refusals_precede_spawn_and_return_the_original_handler() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("work"))?;
    let marker = root.path().join("marker");
    let host = host(root.path(), &marker);
    let refusals: [(Refusal, io::ErrorKind); 4] = [
        (
            |command, _| command.argv.clear(),
            io::ErrorKind::InvalidInput,
        ),
        (
            |command, _| command.argv.push("nul\0".into()),
            io::ErrorKind::InvalidInput,
        ),
        (
            |command, _| command.deadline = Instant::now(),
            io::ErrorKind::TimedOut,
        ),
        (
            |_, handler| handler.refuse = true,
            io::ErrorKind::PermissionDenied,
        ),
    ];
    for (refuse, kind) in refusals {
        let (mut command, mut handler) = (command(), Recorder::default());
        refuse(&mut command, &mut handler);
        let Err(LaunchFailure::Refused { failure, handler }) =
            command.capture(Arc::clone(&host), handler).await
        else {
            panic!("{kind:?} was not refused before spawn");
        };
        assert_eq!(failure.error.kind(), kind);
        assert!(failure.cleanup_completed && failure.status.is_none());
        assert_eq!(handler.retired, Some(true));
        assert!(!marker.exists(), "{kind:?} reached the process");
    }
    assert!(command().capture(host, Recorder::default()).await.is_ok());
    assert!(marker.exists(), "the negative control never ran");
    Ok(())
}

/// A failed spawn may have started a process, so it is never a refusal.
#[tokio::test]
async fn a_failed_spawn_is_indeterminate_with_its_handler() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let mut command = command();
    command.argv = vec![root.path().join("absent").into_os_string()];
    command.cwd = None;
    let Err(LaunchFailure::Indeterminate {
        handler: Some(handler),
    }) = command
        .stream(
            host(root.path(), &root.path().join("marker")),
            Recorder::default(),
        )
        .await
    else {
        panic!("a failed spawn was reported as definite");
    };
    assert_eq!(handler.retired, Some(false));
    Ok(())
}
