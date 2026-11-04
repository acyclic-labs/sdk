use super::*;
use std::path::Path;

/// Independent instances start with a complete frame, then move a partial
/// band in both directions. Each final row is distinct, including the fixed
/// header/footer; an omitted scroll cannot match by repainting exposed rows.
fn fixtures() -> Vec<(Vec<u8>, Vec<String>)> {
    let size = Size::new(WIDTH, HEIGHT);
    let band = Rect::new(0, 1, WIDTH, HEIGHT - 2);
    let scene = |top: usize| {
        let mut rows = vec![Line::raw("fixed header")];
        rows.extend((top..top + usize::from(band.height)).map(|n| Line::raw(format!("row{n:04}"))));
        rows.push(Line::raw("fixed footer"));
        (rows, Some((2, HEIGHT - 1)))
    };
    [(0, 1), (0, 2), (4, 3), (4, 2)]
        .into_iter()
        .map(|(from, to)| {
            let last = scene(to);
            let expected = last
                .0
                .iter()
                .map(|line| {
                    let text: String = line
                        .spans
                        .iter()
                        .map(|span| span.content.as_ref())
                        .collect();
                    format!("{text:<width$}", width = usize::from(WIDTH))
                })
                .collect();
            let mut initial = scene(from);
            *initial.0.last_mut().unwrap() = Line::raw("initial footer");
            let frames = scrolled(
                &[
                    (initial, None),
                    (
                        last,
                        Some(RegionScroll {
                            region: band,
                            rows: to as isize - from as isize,
                        }),
                    ),
                ],
                size,
            );
            (frames.concat(), expected)
        })
        .collect()
}

/// Shared production bytes for local Windows ReadConsoleOutput and the native
/// Linux terminal oracles. This exports fixtures only; it makes no OS claim.
/// Without the script's directory it exports into, and removes, its own.
#[test]
#[ignore = "job=stress-terminal-screen; fixture=script:scripts/test-terminal-region-screen.ps1"]
fn export_windows_screen_fixtures() {
    let requested = std::env::var_os("ACYCLIC_REGION_SCREEN_FIXTURES");
    let owned = requested
        .is_none()
        .then(tempfile::tempdir)
        .transpose()
        .unwrap();
    let root = requested
        .as_deref()
        .map_or_else(|| owned.as_ref().unwrap().path(), Path::new);
    if requested.is_some() {
        std::fs::create_dir_all(root).unwrap();
    }
    for (index, (bytes, expected)) in fixtures().into_iter().enumerate() {
        std::fs::write(root.join(format!("{index}.bin")), &bytes).unwrap();
        std::fs::write(
            root.join(format!("{index}-control.bin")),
            without_scrolling(&bytes),
        )
        .unwrap();
        std::fs::write(
            root.join(format!("{index}.json")),
            serde_json::to_vec(&expected).unwrap(),
        )
        .unwrap();
    }
}

#[cfg(target_os = "linux")]
pub(super) mod linux {
    use super::*;
    use std::process::Command;
    use std::time::{Duration, Instant};

    use rustix::event::{PollFd, PollFlags, Timespec, poll};
    use std::io::{BufRead, Read};
    use std::os::fd::{AsFd, OwnedFd};
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::process::{Child, Stdio};

    const CHILD_ROOT: &str = "ACYCLIC_REGION_EMULATOR_CHILD";

    struct Session {
        child: Child,
        errors: io::PipeReader,
        lifetime: OwnedFd,
    }
    impl Drop for Session {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
    impl Session {
        fn new(mut child: Child, errors: io::PipeReader) -> io::Result<Self> {
            use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
            use rustix::process::{Pid, PidfdFlags, pidfd_open};
            let admission = (|| {
                fcntl_setfl(&errors, fcntl_getfl(&errors)? | OFlags::NONBLOCK)?;
                pidfd_open(Pid::from_child(&child), PidfdFlags::empty())
            })();
            let lifetime = match admission {
                Ok(lifetime) => lifetime,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(io::Error::new(
                        io::Error::from(error).kind(),
                        format!(
                            "emulator event admission failed: {error}; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture"
                        ),
                    ));
                }
            };
            Ok(Self {
                child,
                errors,
                lifetime,
            })
        }
        fn wait(&self, source: &impl AsFd, deadline: Instant) -> io::Result<()> {
            let stderr = &self.errors;
            let mut fds = [
                PollFd::new(&self.lifetime, PollFlags::IN),
                PollFd::new(source, PollFlags::IN),
            ];
            loop {
                let timeout =
                    Timespec::try_from(deadline.saturating_duration_since(Instant::now()))
                        .map_err(|_| io::ErrorKind::InvalidInput)?;
                match poll(&mut fds, Some(&timeout)) {
                    Err(rustix::io::Errno::INTR) => continue,
                    result => {
                        if result? == 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "emulator consumption deadline; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
                            ));
                        }
                    }
                }
                if !fds[0].revents().is_empty() {
                    let mut bytes = [0; 4096];
                    // A descendant may still own the diagnostic writer after
                    // the original process exits. Diagnostics cannot delay loss.
                    let diagnostic = match rustix::io::read(stderr, &mut bytes) {
                        Ok(count) => String::from_utf8_lossy(&bytes[..count]),
                        Err(rustix::io::Errno::AGAIN) => std::borrow::Cow::Borrowed(""),
                        Err(error) => {
                            std::borrow::Cow::Owned(format!("diagnostic read failed: {error}"))
                        }
                    };
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!(
                            "emulator producer closed or reported an error: {}; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
                            diagnostic
                        ),
                    ));
                }
                if fds[1]
                    .revents()
                    .intersects(PollFlags::ERR | PollFlags::NVAL)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "emulator event source failed; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
                    ));
                }
                return Ok(());
            }
        }
    }
    fn checked(command: &mut Command) -> Vec<u8> {
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{command:?}: {}; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }
    fn write_frame(
        control: &mut UnixStream,
        output: &mut impl Write,
        length: u64,
    ) -> io::Result<()> {
        let copied = io::copy(&mut Read::take(control, length), output)?;
        if copied != length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "original frame producer closed before its declared bytes; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
            ));
        }
        output.write_all(b"\x1b[6n")?;
        output.flush()
    }
    // Runs inside the original emulator's PTY. A DSR reply proves the emulator
    // consumed all preceding frame bytes; process exit alone cannot prove that.
    fn frame_child(root: &Path, tool: &str) -> io::Result<()> {
        use rustix::termios::{OptionalActions, tcgetattr, tcsetattr};
        let stdin = io::stdin();
        let mut raw = tcgetattr(&stdin)?;
        raw.make_raw();
        tcsetattr(&stdin, OptionalActions::Now, &raw)?;
        let mut control = UnixStream::connect(root.join("control"))?;
        if tool == "screen" {
            let mut resized = [0];
            stdin.lock().read_exact(&mut resized)?;
            assert_eq!(resized, *b"G", "screen resize command completed");
        }
        let mut output = io::stdout().lock();
        let mut reply = Vec::new();
        loop {
            let mut header = [0; 9];
            // The socket owns the session lifetime. EOF between frames ends it;
            // EOF inside a declared frame remains an error in write_frame.
            if control.read(&mut header[..1])? == 0 {
                return Ok(());
            }
            control.read_exact(&mut header[1..])?;
            let begin = header[0];
            let length = u64::from_le_bytes(header[1..].try_into().unwrap());
            // Each capture starts at the same blank/default baseline as a new
            // terminal, including margins left by a negative-control frame.
            output.write_all(b"\x1b[0m\x1b[r\x1b[2J\x1b[H")?;
            write_frame(&mut control, &mut output, length)?;
            reply.clear();
            stdin.lock().read_until(b'R', &mut reply)?;
            if !reply.starts_with(b"\x1b[") || !reply.ends_with(b"R") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "emulator DSR reply missing; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
                ));
            }
            control.write_all(b"R")?;
            if begin == b'S' {
                let mut copied = [0];
                stdin.lock().read_exact(&mut copied)?;
                assert_eq!(copied, *b"H", "screen hardcopy command completed");
                control.write_all(b"H")?;
            }
        }
    }
    pub(crate) struct Capture {
        control: UnixStream,
        session: Session,
        directory: tempfile::TempDir,
        tool: String,
        name: String,
    }
    impl Capture {
        pub(crate) fn new(tool: &str) -> Self {
            let dir = tempfile::Builder::new()
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir()
                .unwrap();
            let root = dir.path();
            let config = root.join("tmux.conf");
            let socket = root.join("tmux.sock");
            let listener = UnixListener::bind(root.join("control")).unwrap();
            let name = format!(
                "acyclic-region-{}-{}",
                std::process::id(),
                root.file_name().unwrap().to_string_lossy()
            );
            let test = format!(
                "terminal_regions::native_profiles::linux::{}",
                if tool == "tmux" {
                    "tmux_capture_pane_agrees_with_the_rendered_screen"
                } else {
                    "screen_hardcopy_agrees_with_the_rendered_screen"
                }
            );
            let binary = std::env::current_exe().unwrap();
            let (errors, error_writer) = io::pipe().unwrap();
            let mut command = Command::new(tool);
            command.env("TERM", "xterm-256color").env("SCREENDIR", root);
            command.stdout(if tool == "screen" {
                Stdio::from(error_writer.try_clone().unwrap())
            } else {
                Stdio::null()
            });
            let watch = if tool == "tmux" {
                std::fs::write(&config, "set -g status off\n").unwrap();
                let watch =
                    rustix::fs::inotify::init(rustix::fs::inotify::CreateFlags::CLOEXEC).unwrap();
                rustix::fs::inotify::add_watch(
                    &watch,
                    root,
                    rustix::fs::inotify::WatchFlags::CREATE,
                )
                .unwrap();
                command.args([
                    "-D",
                    "-S",
                    socket.to_str().unwrap(),
                    "-f",
                    config.to_str().unwrap(),
                ]);
                Some(watch)
            } else {
                command.args(["-c", "/dev/null", "-DmS", &name]);
                command.arg(&binary).args(["--exact", &test, "--nocapture"]);
                None
            };
            let child = command
                .env(CHILD_ROOT, root)
                .stdin(Stdio::null())
                .stderr(error_writer)
                .spawn()
                .unwrap();
            // Explicitly release the parent's diagnostic write ends before waiting.
            drop(command);
            let session = Session::new(child, errors).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            if let Some(watch) = watch {
                let mut buffer = [std::mem::MaybeUninit::uninit(); 4096];
                let mut events = rustix::fs::inotify::Reader::new(&watch, &mut buffer);
                while !socket.exists() {
                    session.wait(&watch, deadline).unwrap();
                    let event = events.next().unwrap();
                    assert!(
                        !event.events().intersects(
                            rustix::fs::inotify::ReadFlags::IGNORED
                                | rustix::fs::inotify::ReadFlags::QUEUE_OVERFLOW
                        ),
                        "emulator socket watch lost; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture"
                    );
                }
                let shell = format!(
                    "exec {} --exact {} --nocapture",
                    shell_words::quote(binary.to_str().unwrap()),
                    test
                );
                checked(Command::new(tool).env("SCREENDIR", root).args([
                    "-S",
                    socket.to_str().unwrap(),
                    "new-session",
                    "-d",
                    "-x",
                    "24",
                    "-y",
                    "8",
                    &shell,
                ]));
            }
            session.wait(&listener, deadline).unwrap();
            let (control, _) = listener.accept().unwrap();
            if tool == "screen" {
                checked(Command::new(tool).env("SCREENDIR", root).args([
                    "-S",
                    &name,
                    "-X",
                    "eval",
                    "width 24 8",
                    "stuff G",
                ]));
            }
            Self {
                control,
                session,
                directory: dir,
                tool: tool.into(),
                name,
            }
        }
        pub(crate) fn capture(&mut self, bytes: &[u8]) -> Vec<String> {
            let root = self.directory.path();
            let socket = root.join("tmux.sock");
            let dump = root.join("screen.txt");
            let tool = self.tool.as_str();
            let name = &self.name;
            let session = &self.session;
            let control = &mut self.control;
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut header = [0; 9];
            header[0] = if tool == "screen" { b'S' } else { b'G' };
            header[1..].copy_from_slice(&u64::try_from(bytes.len()).unwrap().to_le_bytes());
            control.write_all(&header).unwrap();
            control.write_all(bytes).unwrap();
            session.wait(&control, deadline).unwrap();
            let mut consumed = [0];
            control.read_exact(&mut consumed).unwrap();
            assert_eq!(
                consumed, *b"R",
                "original emulator consumption acknowledgement"
            );
            let text = if tool == "tmux" {
                String::from_utf8(checked(Command::new(tool).env("SCREENDIR", root).args([
                    "-S",
                    socket.to_str().unwrap(),
                    "capture-pane",
                    "-p",
                    "-t",
                    "0",
                ])))
                .unwrap()
            } else {
                let hardcopy = format!("hardcopy {}", shell_words::quote(dump.to_str().unwrap()));
                checked(
                    Command::new(tool)
                        .env("SCREENDIR", root)
                        .args(["-S", &name, "-X", "eval", &hardcopy, "stuff H"]),
                );
                session.wait(&control, deadline).unwrap();
                control.read_exact(&mut consumed).unwrap();
                assert_eq!(
                    consumed, *b"H",
                    "hardcopy completed before its one file read"
                );
                std::fs::read_to_string(&dump).unwrap()
            };
            text.lines()
                .take(8)
                .map(|row| format!("{row:<24}"))
                .collect()
        }
    }
    #[test]
    fn incomplete_frame_cannot_request_consumption() {
        let (bytes, _) = fixtures().into_iter().next().unwrap();
        let partial = &bytes[..bytes.len() - 1];
        let (mut source, mut sender) = UnixStream::pair().unwrap();
        sender.write_all(partial).unwrap();
        drop(sender);
        let (mut terminal, mut emitted) = UnixStream::pair().unwrap();
        assert_eq!(
            write_frame(&mut source, &mut terminal, bytes.len().try_into().unwrap())
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
        drop(terminal);
        let mut actual = Vec::new();
        emitted.read_to_end(&mut actual).unwrap();
        assert_eq!(
            actual, partial,
            "incomplete bytes cannot issue a DSR request"
        );
    }
    #[test]
    fn emulator_loss_wins_over_a_ready_consumer() {
        let (errors, writer) = io::pipe().unwrap();
        // Retain the same write endpoint as an emulator descendant can do.
        // Its empty, open pipe must not become another producer-loss wait.
        let retained_writer = writer.try_clone().unwrap();
        let child = Command::new("sh")
            .args(["-c", "read hold"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(writer)
            .spawn()
            .unwrap();
        let mut session = Session::new(child, errors).unwrap();
        let (reader, mut writer) = UnixStream::pair().unwrap();
        writer.write_all(b"R").unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        session.wait(&reader, deadline).unwrap();
        session.child.kill().unwrap();
        session.child.wait().unwrap();
        assert_eq!(
            session.wait(&reader, deadline).unwrap_err().kind(),
            io::ErrorKind::BrokenPipe,
            "a queued consumer byte cannot mask original producer loss"
        );
        drop(retained_writer);
    }
    fn oracle(tool: &str) {
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            frame_child(Path::new(&root), tool).unwrap();
            return;
        }
        let mut terminal = Capture::new(tool);
        assert_eq!(
            terminal.capture(b""),
            vec![" ".repeat(usize::from(WIDTH)); usize::from(HEIGHT)],
            "the original harness preamble cannot become the renderer baseline"
        );
        for (bytes, expected) in fixtures() {
            assert_eq!(terminal.capture(&bytes), expected, "{tool} region screen");
            assert_ne!(
                terminal.capture(&without_scrolling(&bytes)),
                expected,
                "removing production scroll sequences must fail the same screen oracle"
            );
        }
    }
    #[test]
    fn tmux_capture_pane_agrees_with_the_rendered_screen() {
        oracle("tmux");
    }
    #[test]
    fn screen_hardcopy_agrees_with_the_rendered_screen() {
        oracle("screen");
    }
}
