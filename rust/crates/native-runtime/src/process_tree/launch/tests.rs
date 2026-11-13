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
    #[cfg(unix)]
    assert_eq!(std::env::args_os().next().unwrap(), "native-launch-child");
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
type Refusal = fn(&mut CaptureRequest, &mut Recorder);

#[derive(Default)]
struct Recorder {
    refuse: bool,
    panic: bool,
    retired: Option<bool>,
}

impl CaptureHandler for Recorder {
    fn check(&mut self) -> io::Result<()> {
        assert!(!self.panic, "planted owner callback failure");
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

fn command(root: &std::path::Path) -> CaptureRequest {
    command_at(root, std::env::current_exe().unwrap())
}

fn command_at(root: &std::path::Path, program: impl AsRef<std::ffi::OsStr>) -> CaptureRequest {
    let mut command = ProcessCommand::new(program);
    // Libtest parses UTF-8 argv even when the OS launched a native-byte path.
    // Preserve that path and give this test child an explicit, valid argv[0].
    #[cfg(unix)]
    command.arg0("native-launch-child");
    command
        .args([
            "--exact",
            "process_tree::launch::tests::child",
            "--nocapture",
        ])
        .current_dir(root.join("work"))
        .env(MODE, "1")
        .env(MARKER, root.join("marker"));
    CaptureRequest {
        command,
        custody: None,
        execution_grant: None,
        input: Some(OwnedCaptureInput::Literal(b"input:".to_vec())),
        schedule: CaptureSchedule::new(Duration::from_secs(60)),
        maximum_output_bytes: Some(1 << 20),
    }
}

/// The checkout shell collects and a local Machine streams; the same
/// command must observe the same directory, input, output and exit.
#[tokio::test]
async fn collected_and_streamed_launches_run_a_command_identically() -> io::Result<()> {
    crate::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("work"))?;
    let Ok(collected) = command(root.path()).capture(Recorder::default()).await else {
        panic!("collected launch failed");
    };
    let collected = collected.await?;
    let collected = collected.result.map_err(io::Error::other)?;
    let executable = std::env::current_exe()?;
    #[cfg(unix)]
    let executable = {
        // APFS requires valid UTF-8 names; other Unix filesystems also exercise
        // the native-byte path that cannot be represented as UTF-8.
        #[cfg(target_os = "macos")]
        let name = std::ffi::OsStr::new("child-\u{ff}");
        #[cfg(not(target_os = "macos"))]
        let name = {
            use std::os::unix::ffi::OsStrExt;
            std::ffi::OsStr::from_bytes(b"child-\xff")
        };
        let native = root.path().join(name);
        std::os::unix::fs::symlink(executable, &native)?;
        native
    };
    // The collected command is the ordinary-path control. Streaming must also
    // preserve the native executable alias accepted by the filesystem.
    let Ok(mut streamed) = command_at(root.path(), executable)
        .stream(Recorder::default())
        .await
    else {
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
    crate::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("work"))?;
    let marker = root.path().join("marker");
    let refusals: [(Refusal, io::ErrorKind); 5] = [
        (
            |request, _| request.command = ProcessCommand::new(""),
            io::ErrorKind::InvalidInput,
        ),
        (
            |request, _| {
                request.command.arg("nul\0");
            },
            io::ErrorKind::InvalidInput,
        ),
        (
            |request, _| request.schedule = CaptureSchedule::until(Instant::now()),
            io::ErrorKind::TimedOut,
        ),
        (
            |_, handler| handler.refuse = true,
            io::ErrorKind::PermissionDenied,
        ),
        (
            |request, _| request.schedule = CaptureSchedule::Overflow,
            io::ErrorKind::InvalidInput,
        ),
    ];
    for (refuse, kind) in refusals {
        let (mut command, mut handler) = (command(root.path()), Recorder::default());
        refuse(&mut command, &mut handler);
        let handler = match command.capture(handler).await {
            Err(LaunchFailure::Refused { failure, handler }) => {
                assert_eq!(failure.error.kind(), kind);
                assert!(failure.cleanup_completed && failure.status.is_none());
                handler
            }
            Err(LaunchFailure::Control { error, handler }) => {
                assert_eq!(kind, io::ErrorKind::PermissionDenied);
                assert_eq!(error.kind(), kind);
                handler
            }
            _ => panic!("{kind:?} was not refused before spawn"),
        };
        assert_eq!(handler.retired, Some(true));
        assert!(!marker.exists(), "{kind:?} reached the process");
    }
    let foreign = tempfile::tempdir()?;
    let mut request = command(root.path());
    request.command.env_clear();
    request.execution_grant = Some(crate::execution::ExecutionGrant::new(
        foreign.path(),
        crate::execution::ExecutionAccess::ReadOnly,
        Vec::new(),
        crate::execution::ExecutionNetwork::Off,
    )?);
    let Err(LaunchFailure::Refused { failure, handler }) =
        request.capture(Recorder::default()).await
    else {
        panic!("foreign working directory was not refused before spawn");
    };
    assert_eq!(failure.error.kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(
        failure.error.to_string(),
        "execution working directory escapes the granted workspace"
    );
    assert!(failure.cleanup_completed && failure.status.is_none());
    assert_eq!(handler.retired, Some(true));
    assert!(!marker.exists(), "grant refusal reached the process");
    let Ok(capture) = command(root.path()).capture(Recorder::default()).await else {
        panic!("negative control was refused");
    };
    assert!(capture.await?.result.is_ok());
    assert!(marker.exists(), "the negative control never ran");
    Ok(())
}

/// A failed spawn may have started a process, so it is never a refusal.
#[tokio::test]
async fn a_failed_spawn_is_indeterminate_with_its_handler() -> io::Result<()> {
    crate::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let mut request = command(root.path());
    request.command = ProcessCommand::new(root.path().join("absent"));
    let Err(LaunchFailure::Indeterminate {
        error,
        handler: Some(handler),
    }) = request.stream(Recorder::default()).await
    else {
        panic!("a failed spawn was reported as definite");
    };
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    assert_eq!(handler.retired, Some(false));
    let Err(LaunchFailure::Indeterminate {
        error,
        handler: None,
    }) = command(root.path())
        .stream(Recorder {
            panic: true,
            ..Recorder::default()
        })
        .await
    else {
        panic!("lost owner callback did not end in producer loss");
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    Ok(())
}

/// Launches return the process to its open-descriptor count. The raised
/// descriptor limit is headroom for real work, never cover for a leak.
#[cfg(unix)]
#[tokio::test]
async fn repeated_launches_return_every_descriptor() -> io::Result<()> {
    if crate::tests::passed_in_exclusive_process(
        "process_tree::launch::tests::repeated_launches_return_every_descriptor",
    )? {
        return Ok(());
    }
    async fn launch(root: &std::path::Path) -> io::Result<()> {
        let Ok(launched) = command(root).capture(Recorder::default()).await else {
            panic!("launch failed");
        };
        launched.await?.result.map_err(io::Error::other)?;
        Ok(())
    }
    fn open_descriptors() -> io::Result<usize> {
        Ok(std::fs::read_dir("/dev/fd")?.count())
    }
    crate::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("work"))?;
    // The first launch starts the shared services, which keep theirs.
    launch(root.path()).await?;
    let baseline = open_descriptors()?;
    for _ in 0..8 {
        launch(root.path()).await?;
    }
    assert_eq!(open_descriptors()?, baseline, "launches leaked descriptors");
    Ok(())
}
