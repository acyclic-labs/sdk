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
#[test]
#[ignore = "invoked by scripts/test-terminal-region-screen.ps1"]
fn export_windows_screen_fixtures() {
    let root = std::env::var_os("ACYCLIC_REGION_SCREEN_FIXTURES").expect("fixture directory");
    let root = Path::new(&root);
    std::fs::create_dir_all(root).unwrap();
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
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::process::{Child, Stdio};

    const CHILD_ROOT: &str = "ACYCLIC_REGION_EMULATOR_CHILD";

    struct Session {
        child: Child,
    }
    impl Drop for Session {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
    impl Session {
        fn wait(&self, source: &impl AsFd, deadline: Instant) -> io::Result<()> {
            let stderr = self
                .child
                .stderr
                .as_ref()
                .ok_or(io::ErrorKind::BrokenPipe)?;
            let mut fds = [
                PollFd::new(stderr, PollFlags::IN),
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
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "emulator producer closed or reported an error; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
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
    // Runs inside the original emulator's PTY. A DSR reply proves the emulator
    // consumed all preceding frame bytes; process exit alone cannot prove that.
    fn frame_child(root: &Path) -> io::Result<()> {
        use rustix::termios::{OptionalActions, tcgetattr, tcsetattr};
        let stdin = io::stdin();
        let mut raw = tcgetattr(&stdin)?;
        raw.make_raw();
        tcsetattr(&stdin, OptionalActions::Now, &raw)?;
        let mut control = UnixStream::connect(root.join("control"))?;
        let mut begin = [0];
        control.read_exact(&mut begin)?;
        let mut output = io::stdout().lock();
        output.write_all(&std::fs::read(root.join("frame.bin"))?)?;
        output.write_all(b"\x1b[6n")?;
        output.flush()?;
        let mut reply = Vec::new();
        stdin.lock().read_until(b'R', &mut reply)?;
        if !reply.starts_with(b"\x1b[") || !reply.ends_with(b"R") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "emulator DSR reply missing; recovery: cargo test -p acyclic-harness-tui --features tui --test main terminal_regions::native_profiles::linux -- --nocapture",
            ));
        }
        control.write_all(b"R")?;
        // Hold the screen until its one snapshot has been taken. Dropping the
        // original parent socket releases this wait, including failure paths.
        assert_eq!(control.read(&mut begin)?, 0);
        Ok(())
    }
    pub(crate) fn capture(tool: &str, bytes: &[u8]) -> Vec<String> {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let dump = root.join("screen.txt");
        let config = root.join("tmux.conf");
        std::fs::write(root.join("frame.bin"), bytes).unwrap();
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
        let mut command = Command::new(tool);
        if tool == "tmux" {
            let shell = format!(
                "exec {} --exact {} --nocapture",
                shell_words::quote(binary.to_str().unwrap()),
                test
            );
            std::fs::write(
                &config,
                format!(
                    "set -g status off\nnew-session -d -x 24 -y 8 {}\n",
                    shell_words::quote(&shell)
                ),
            )
            .unwrap();
            // The original foreground server owns stderr for the whole wait.
            command.args(["-D", "-L", &name, "-f", config.to_str().unwrap()]);
        } else {
            command.args(["-c", "/dev/null", "-DmS", &name]);
            command.arg(&binary).args(["--exact", &test, "--nocapture"]);
        }
        let session = Session {
            child: command
                .env(CHILD_ROOT, root)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        };
        let deadline = Instant::now() + Duration::from_secs(2);
        session.wait(&listener, deadline).unwrap();
        let (mut control, _) = listener.accept().unwrap();
        if tool == "screen" {
            checked(Command::new(tool).args(["-S", &name, "-X", "width", "24", "8"]));
        }
        control.write_all(b"G").unwrap();
        session.wait(&control, deadline).unwrap();
        let mut consumed = [0];
        control.read_exact(&mut consumed).unwrap();
        assert_eq!(
            consumed, *b"R",
            "original emulator consumption acknowledgement"
        );
        let text = if tool == "tmux" {
            String::from_utf8(checked(Command::new(tool).args([
                "-L",
                &name,
                "capture-pane",
                "-p",
                "-t",
                "0",
            ])))
            .unwrap()
        } else {
            checked(Command::new(tool).args([
                "-S",
                &name,
                "-X",
                "hardcopy",
                dump.to_str().unwrap(),
            ]));
            std::fs::read_to_string(&dump).unwrap()
        };
        text.lines()
            .take(8)
            .map(|row| format!("{row:<24}"))
            .collect()
    }
    #[test]
    fn emulator_loss_wins_over_a_ready_consumer() {
        let mut session = Session {
            child: Command::new("sh")
                .args(["-c", "read hold"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        };
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
    }
    fn oracle(tool: &str) {
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            frame_child(Path::new(&root)).unwrap();
            return;
        }
        for (bytes, expected) in fixtures() {
            assert_eq!(capture(tool, &bytes), expected, "{tool} region screen");
            assert_ne!(
                capture(tool, &without_scrolling(&bytes)),
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
