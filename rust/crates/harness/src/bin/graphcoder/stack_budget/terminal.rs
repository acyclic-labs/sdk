//! Drives the TUI case on a real pseudo-terminal.
//!
//! The case process is the terminal's foreground program, as `graphcoder`
//! is when a person runs it: it opens the terminal, negotiates, renders, and
//! reads keys from it. The driver types the prompt once the TUI has drawn its
//! composer, and quits once the scripted provider has served its last turn
//! and the TUI shows the session idle.
use super::{CASE, Outcome, SETTLED};
use std::{
    io::Write as _,
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// A hang is a failure, never a wait: the whole TUI turn takes seconds.
const DEADLINE: Duration = Duration::from_secs(90);

/// The harness crate, whose ancestors hold the repository's scripts.
#[cfg(windows)]
const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

/// The terminal host: script(1) on Unix, the repository's console
/// pseudo-terminal host on Windows.
fn host(executable: &Path, scratch: &Path) -> Outcome<Command> {
    #[cfg(target_os = "linux")]
    let command = {
        let mut command = Command::new("script");
        command.args(["-q", "-e", "-f", "-c"]).arg(format!(
            "stty rows 24 cols 80; ulimit -s 1024; exec '{}'",
            executable.display()
        ));
        command.arg("/dev/null");
        command
    };
    #[cfg(all(unix, not(target_os = "linux")))]
    let command = {
        let mut command = Command::new("script");
        command
            .args(["-q", "/dev/null", "/bin/sh", "-c"])
            .arg(format!(
                "stty rows 24 cols 80; ulimit -s 1024; exec '{}'",
                executable.display()
            ));
        command
    };
    #[cfg(windows)]
    let command = {
        let scripts = Path::new(MANIFEST_DIR)
            .ancestors()
            .nth(6)
            .ok_or("the repository root")?
            .join("scripts");
        let catalog = scratch.join("unused-catalog.json");
        std::fs::write(&catalog, b"{}")?;
        let mut command = Command::new("powershell.exe");
        command
            .args(["-NoProfile", "-NonInteractive", "-File"])
            .arg(scripts.join("graphcoder-conpty.ps1"))
            .arg("-Binary")
            .arg(executable)
            .arg("-Workspace")
            .arg(scratch)
            .arg("-Catalog")
            .arg(catalog)
            .arg("-SettingsRoot")
            .arg(scratch)
            .args(["-DeadlineMilliseconds", "90000"]);
        command
    };
    #[cfg(unix)]
    let _ = scratch;
    Ok(command)
}

/// The visible text of terminal output: escape sequences removed.
fn visible(output: &[u8]) -> String {
    let text = String::from_utf8_lossy(output);
    let mut visible = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            visible.push(c);
            continue;
        }
        match chars.next() {
            // CSI: parameters and intermediates up to a final byte.
            Some('[') => {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            // OSC, DCS and the other strings: up to BEL or ST.
            Some(']' | 'P' | '_' | '^' | 'X') => {
                while let Some(c) = chars.next() {
                    if c == '\u{7}' || (c == '\u{1b}' && chars.peek() == Some(&'\\')) {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    visible
}

struct Session {
    child: Child,
    output: Arc<Mutex<Vec<u8>>>,
    started: Instant,
}

impl Session {
    /// Applies `read` to the output so far.
    fn read<T>(&self, read: impl FnOnce(&[u8]) -> T) -> Outcome<T> {
        let output = self
            .output
            .lock()
            .map_err(|_| "the terminal output is poisoned")?;
        Ok(read(&output))
    }

    /// Waits until `ready` holds for the output so far, failing at the deadline.
    fn until(&mut self, what: &str, ready: impl Fn(&[u8]) -> bool) -> Outcome {
        loop {
            if self.read(&ready)? {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait()? {
                let screen = self.screen();
                return Err(format!("the TUI exited ({status}) before {what}: {screen}").into());
            }
            if self.started.elapsed() > DEADLINE {
                let _ = self.child.kill();
                let screen = self.screen();
                return Err(format!("the TUI did not reach {what}: {screen}").into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The last of the visible output, for a failure report.
    fn screen(&self) -> String {
        let text = self.read(visible).unwrap_or_default();
        let skipped = text.chars().count().saturating_sub(2000);
        text.chars().skip(skipped).collect()
    }

    fn send(&mut self, keys: &[u8]) -> Outcome {
        let stdin = self
            .child
            .stdin
            .as_mut()
            .ok_or("the terminal host's input")?;
        stdin.write_all(keys)?;
        stdin.flush()?;
        Ok(())
    }

    /// Waits for the TUI to exit, failing at the deadline.
    fn exit(&mut self) -> Outcome<std::process::ExitStatus> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if self.started.elapsed() > DEADLINE {
                let _ = self.child.kill();
                return Err(format!("the TUI did not quit: {}", self.screen()).into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Copies the host's output into `output` until it closes.
fn copy_output(
    mut stdout: impl std::io::Read + Send + 'static,
    output: Arc<Mutex<Vec<u8>>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut bytes = [0; 4096];
        while let Ok(count @ 1..) = stdout.read(&mut bytes) {
            let (Some(read), Ok(mut output)) = (bytes.get(..count), output.lock()) else {
                return;
            };
            output.extend_from_slice(read);
        }
    })
}

/// Runs the TUI case on a real terminal and requires it to pass its budget.
pub(super) fn run(executable: &Path) -> Outcome {
    let scratch = tempfile::tempdir()?;
    let settled = scratch.path().join("settled");
    let mut child = host(executable, scratch.path())?
        .env(CASE, "tui")
        .env(SETTLED, &settled)
        .env("TERM", "xterm-256color")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let output = Arc::new(Mutex::new(Vec::new()));
    let stdout = child.stdout.take().ok_or("the terminal host's output")?;
    let reader = copy_output(stdout, Arc::clone(&output));
    let mut session = Session {
        child,
        output,
        started: Instant::now(),
    };
    // The composer is drawn and the cursor shown: the TUI reads keys now.
    session.until("its composer", |output| {
        visible(output).contains("GraphCoder") && output.windows(6).any(|w| w == b"\x1b[?25h")
    })?;
    session.send(b"\x1b[200~go\x1b[201~\r")?;
    session.until("the scripted turn's end", |_| settled.exists())?;
    let served = session.read(<[u8]>::len)?;
    // The session shows idle again after the last turn.
    session.until("an idle session", |output| {
        visible(output.get(served..).unwrap_or_default()).contains('\u{25cb}')
    })?;
    session.send(b"\x04")?;
    let status = session.exit()?;
    reader
        .join()
        .map_err(|_| "the terminal output reader panicked")?;
    // The case reports its depth on the terminal it ran on.
    let report = session.read(visible)?;
    if let Some(line) = report
        .lines()
        .find(|line| line.contains("GraphCoder turn:"))
    {
        eprintln!("tui: {}", line.trim());
    }
    assert!(
        status.success(),
        "main-thread tui ({status}): {}",
        session.screen()
    );
    Ok(())
}
