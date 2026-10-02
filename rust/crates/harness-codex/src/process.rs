//! Running one `codex exec` process (task A6).
//!
//! Codex runs in its own process group with a closed stdin (0.155.1 blocks on
//! an open one), a cleared environment, and stdout read line by line. Past the
//! deadline the whole group gets SIGTERM, then SIGKILL after [`GRACE`].

use acyclic_harness::{Error, Result};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader, Lines},
    process::{Child, ChildStdout, Command},
    task::JoinHandle,
    time::Instant,
};

/// How long Codex gets to exit after SIGTERM before SIGKILL.
pub const GRACE: Duration = Duration::from_secs(10);

/// How much of stderr is kept for error messages.
const STDERR_TAIL: usize = 64 * 1024;

/// Environment variables passed through from the parent when set.
const INHERITED: [&str; 6] = [
    "PATH",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

/// What to run.
pub(crate) struct Launch<'a> {
    pub binary: &'a Path,
    pub args: Vec<OsString>,
    pub env: Vec<(String, String)>,
    pub workspace: &'a Path,
}

/// The next thing a running Codex produced.
pub(crate) enum Next {
    Line(String),
    Eof,
    Deadline,
}

/// A running `codex exec`.
pub(crate) struct CodexProcess {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    stderr: JoinHandle<Vec<u8>>,
}

impl CodexProcess {
    pub(crate) fn spawn(launch: Launch<'_>) -> Result<Self> {
        let mut command = Command::new(launch.binary);
        command
            .args(&launch.args)
            .current_dir(launch.workspace)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        for name in INHERITED {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        if std::env::var_os("PATH").is_none() {
            command.env("PATH", "/usr/local/bin:/usr/bin:/bin");
        }
        command.envs(launch.env);
        let mut child = command.spawn().map_err(|error| {
            Error::Unsupported(format!(
                "cannot start codex at {}: {error}",
                launch.binary.display()
            ))
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::Storage("codex stdout was not captured".into()))?;
        let stderr = child.stderr.take();
        let stderr = tokio::spawn(async move {
            let mut tail = Vec::new();
            if let Some(mut stderr) = stderr {
                let mut chunk = [0_u8; 8192];
                while let Ok(read) = stderr.read(&mut chunk).await {
                    if read == 0 {
                        break;
                    }
                    tail.extend_from_slice(chunk.get(..read).unwrap_or_default());
                    if tail.len() > STDERR_TAIL {
                        tail.drain(..tail.len() - STDERR_TAIL);
                    }
                }
            }
            tail
        });
        Ok(Self {
            child,
            lines: BufReader::new(stdout).lines(),
            stderr,
        })
    }

    /// Waits for the next stdout line, end of output, or the deadline.
    pub(crate) async fn next(&mut self, deadline: Option<Instant>) -> Next {
        let line = self.lines.next_line();
        let result = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, line).await {
                Ok(result) => result,
                Err(_) => return Next::Deadline,
            },
            None => line.await,
        };
        match result {
            Ok(Some(line)) => Next::Line(line),
            Ok(None) | Err(_) => Next::Eof,
        }
    }

    /// Waits for exit after stdout closed, within the deadline.
    pub(crate) async fn wait(&mut self, deadline: Option<Instant>) -> Option<ExitStatus> {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, self.child.wait())
                .await
                .ok()
                .and_then(std::result::Result::ok),
            None => self.child.wait().await.ok(),
        }
    }

    /// SIGTERM to the whole process group, then SIGKILL after [`GRACE`].
    pub(crate) async fn terminate(&mut self) {
        signal_group(&self.child, rustix::process::Signal::TERM);
        if tokio::time::timeout(GRACE, self.child.wait())
            .await
            .is_err()
        {
            signal_group(&self.child, rustix::process::Signal::KILL);
            let _ = self.child.wait().await;
        }
    }

    /// The last 64 KB of stderr, once the process is gone.
    pub(crate) async fn stderr_tail(&mut self) -> String {
        let bytes = (&mut self.stderr).await.unwrap_or_default();
        String::from_utf8_lossy(&bytes).trim().to_owned()
    }
}

fn signal_group(child: &Child, signal: rustix::process::Signal) {
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw);
    if let Some(pid) = pid {
        let _ = rustix::process::kill_process_group(pid, signal);
    }
}

/// Runs `<binary> --version` and checks it is the pinned release.
pub(crate) async fn check_version(binary: &Path) -> Result<()> {
    let output = Command::new(binary)
        .arg("--version")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|error| {
            Error::Unsupported(format!(
                "cannot run {} --version: {error}",
                binary.display()
            ))
        })?;
    let version = String::from_utf8_lossy(&output.stdout);
    if version
        .split_whitespace()
        .any(|word| word == crate::CODEX_VERSION)
    {
        Ok(())
    } else {
        Err(Error::Unsupported(format!(
            "{} reports {:?}; this executor is qualified only for codex {}",
            binary.display(),
            version.trim(),
            crate::CODEX_VERSION
        )))
    }
}

/// Per-operation directories under the configured state dir.
pub(crate) struct Dirs {
    pub codex_home: PathBuf,
    pub home: PathBuf,
}

impl Dirs {
    pub(crate) fn create(state_dir: &Path, operation: &str) -> Result<Self> {
        let root = state_dir.join(operation);
        let dirs = Self {
            codex_home: root.join("codex"),
            home: root.join("home"),
        };
        for dir in [&dirs.codex_home, &dirs.home] {
            std::fs::create_dir_all(dir).map_err(|error| {
                Error::Storage(format!("cannot create {}: {error}", dir.display()))
            })?;
        }
        Ok(dirs)
    }
}
