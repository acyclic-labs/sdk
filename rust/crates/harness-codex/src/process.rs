//! Running one `codex exec` process (task A6).
//!
//! Codex runs in its own process group with a closed stdin (0.155.1 blocks on
//! an open one), a cleared environment, and stdout read line by line. Past the
//! deadline the whole group gets SIGTERM, then SIGKILL after [`GRACE`].

use acyclic_harness::{Error, OperationId, Result};
use futures::future::BoxFuture;
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader},
    process::{ChildStderr, ChildStdout},
    task::JoinHandle,
    time::Instant,
};

/// How long Codex gets to exit after SIGTERM before SIGKILL.
pub const GRACE: Duration = Duration::from_secs(10);

/// How long to wait for stderr to close after Codex exits.
const STDERR_GRACE: Duration = Duration::from_secs(2);

/// How much of stderr is kept for error messages.
const STDERR_TAIL: usize = 64 * 1024;

/// Maximum bytes in one JSON event, independently of transcript policy.
const MAX_EVENT_BYTES: usize = 8 * 1024 * 1024;

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
pub struct Launch<'a> {
    /// Approved native executable path.
    pub binary: &'a Path,
    /// Arguments for this original command.
    pub args: Vec<OsString>,
    /// Explicit environment additions after clearing the ambient environment.
    pub env: Vec<(String, String)>,
    /// The admitted command's working directory.
    pub workspace: &'a Path,
}

/// The next thing a running Codex produced.
pub enum Next {
    /// One complete UTF-8 stdout line from the original command.
    Line(String),
    /// That command's stdout closed; physical exit still requires `wait`.
    Eof,
    /// The observation deadline elapsed, without claiming physical completion.
    Deadline,
    /// The original lifecycle requested cancellation or revoked dispatch.
    Cancelled,
    /// Reading the original command's control stream failed.
    Failed(std::io::Error),
}

/// An owned Codex process, local or provider-backed. An elapsed deadline is
/// pending work: `wait` returns `None` until actual exit and descendant closure.
pub trait RunningProcess: Send {
    /// Reads one genuine stdout observation without restarting the command.
    fn next(&mut self, deadline: Option<Instant>) -> BoxFuture<'_, Next>;
    /// Observes exit and full owned-process closure; never infers it from time.
    fn wait(&mut self, deadline: Option<Instant>) -> BoxFuture<'_, Result<Option<String>>>;
    /// Stops and drains the same owned process, including its descendants.
    fn terminate(&mut self) -> BoxFuture<'_, Result<()>>;
    /// Returns the retained stderr suffix of that original process.
    fn stderr_tail(&mut self) -> BoxFuture<'_, String>;
}

/// Consumer-owned physical process transport. Protected consumers must supply
/// an implementation backed by their held original slot and durable command
/// association; this interface does not itself authorize any external effect.
pub trait ProcessHost: Send + Sync {
    /// Immutable implementation identity included in the durable turn binding.
    fn identity(&self) -> &acyclic_harness::registry::ComponentIdentity;
    /// Verifies the pinned version in the actual selected execution environment.
    fn check_version<'a>(&'a self, context: &'a acyclic_harness::runtime::TaskContext, binary: &'a Path) -> BoxFuture<'a, Result<()>>;
    /// Resolves the accepted host-only tools for the exact upstream protocol.
    fn builtin_bindings(&self, context: &acyclic_harness::runtime::TaskContext) -> Result<Vec<crate::exec_server::BuiltinToolBinding>>;
    /// Starts or reconciles the original command. A lost command ACK must not
    /// dispatch a second mutation. Endpoint URLs in `home` must be made
    /// reachable from the owned process without exposing their bearer tokens.
    fn launch<'a>(
        &'a self,
        operation: OperationId,
        config: &'a crate::executor::CodexConfig,
        home: crate::config::HomeConfig,
        args: Vec<OsString>,
        env: Vec<(String, String)>,
        context: acyclic_harness::runtime::TaskContext,
        builtins: crate::exec_server::BuiltinMediator,
    ) -> BoxFuture<'a, Result<Box<dyn RunningProcess>>>;
}


/// A running `codex exec`.
pub struct CodexProcess {
    child: Option<acyclic_native_runtime::ProcessTree>,
    status: Option<ExitStatus>,
    stdout: BufReader<ChildStdout>,
    line: Vec<u8>,
    stderr: JoinHandle<()>,
    stderr_tail: Arc<Mutex<Vec<u8>>>,
}

impl CodexProcess {
    /// Launches the native command in an owned process tree. Consumers must
    /// establish their authority and mediated tool transport before calling.
    pub fn spawn(launch: Launch<'_>) -> Result<Self> {
        let mut command = Command::new(launch.binary);
        command
            .args(&launch.args)
            .current_dir(launch.workspace)
            .env_clear()
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for name in INHERITED {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        if std::env::var_os("PATH").is_none() {
            command.env("PATH", "/usr/local/bin:/usr/bin:/bin");
        }
        command.envs(launch.env);
        let mut child =
            acyclic_native_runtime::spawn_process_tree(&mut command).map_err(|error| {
                Error::Unsupported(format!(
                    "cannot start codex at {}: {error}",
                    launch.binary.display()
                ))
            })?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| Error::Storage("codex stdout was not captured".into()))?;
        let stdout =
            ChildStdout::from_std(stdout).map_err(|error| Error::Storage(error.to_string()))?;
        let stderr = child
            .take_stderr()
            .map(ChildStderr::from_std)
            .transpose()
            .map_err(|error| Error::Storage(error.to_string()))?;
        let stderr_tail = Arc::new(Mutex::new(Vec::new()));
        let tail = stderr_tail.clone();
        let stderr = tokio::spawn(async move {
            let Some(mut stderr) = stderr else { return };
            let mut chunk = [0_u8; 8192];
            while let Ok(read) = stderr.read(&mut chunk).await {
                if read == 0 {
                    break;
                }
                let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
                tail.extend_from_slice(chunk.get(..read).unwrap_or_default());
                if tail.len() > STDERR_TAIL {
                    let excess = tail.len() - STDERR_TAIL;
                    tail.drain(..excess);
                }
            }
        });
        Ok(Self {
            child: Some(child),
            status: None,
            stdout: BufReader::new(stdout),
            line: Vec::new(),
            stderr,
            stderr_tail,
        })
    }

    /// Waits for the next stdout line, end of output, or the deadline.
    pub(crate) async fn next(&mut self, deadline: Option<Instant>) -> Next {
        self.line.clear();
        let mut reader = (&mut self.stdout).take((MAX_EVENT_BYTES + 1) as u64);
        let line = reader.read_until(b'\n', &mut self.line);
        let result = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, line).await {
                Ok(result) => result,
                Err(_) => return Next::Deadline,
            },
            None => line.await,
        };
        match result {
            Err(error) => Next::Failed(error),
            Ok(_) if self.line.len() > MAX_EVENT_BYTES => Next::Failed(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "codex event exceeded its byte limit",
            )),
            Ok(0) => Next::Eof,
            Ok(_) => {
                if self.line.last() == Some(&b'\n') {
                    self.line.pop();
                    if self.line.last() == Some(&b'\r') {
                        self.line.pop();
                    }
                }
                match std::str::from_utf8(&self.line) {
                    Ok(line) => Next::Line(line.to_owned()),
                    Err(error) => {
                        Next::Failed(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
                    }
                }
            }
        }
    }

    /// Observes exit and cleans containment; only an elapsed deadline returns None.
    pub(crate) async fn wait(&mut self, deadline: Option<Instant>) -> Result<Option<ExitStatus>> {
        if self.status.is_some() {
            return Ok(self.status);
        }
        loop {
            let tree = self
                .child
                .as_mut()
                .ok_or_else(|| Error::Storage("codex process ownership absent".into()))?;
            if let Some(status) = tree
                .try_wait()
                .map_err(|error| Error::Storage(error.to_string()))?
            {
                let mut tree = self
                    .child
                    .take()
                    .ok_or_else(|| Error::Storage("codex process ownership absent".into()))?;
                let (tree, result) = acyclic_native_runtime::run_blocking_io(move || {
                    let result = tree.terminate();
                    (tree, result)
                })
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
                self.child = Some(tree);
                result.map_err(|error| Error::Storage(error.to_string()))?;
                self.status = Some(status);
                return Ok(self.status);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Requests graceful exit, then cleans the owned tree even if its leader
    /// exits before descendants. The existing native pool owns admitted cleanup.
    pub(crate) async fn terminate(&mut self) -> Result<()> {
        let Some(mut tree) = self.child.take() else {
            return Ok(());
        };
        let (tree, result) = acyclic_native_runtime::run_blocking_io(move || {
            let result = tree.terminate_after(GRACE);
            (tree, result)
        })
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        self.child = Some(tree);
        self.status = Some(result.map_err(|error| Error::Storage(error.to_string()))?);
        Ok(())
    }

    /// The last 64 KB of stderr, once the process is gone. A command Codex
    /// left running in the background can hold the pipe open forever, so
    /// this waits [`STDERR_GRACE`] at most and keeps what it has.
    pub(crate) async fn stderr_tail(&mut self) -> String {
        if tokio::time::timeout(STDERR_GRACE, &mut self.stderr)
            .await
            .is_err()
        {
            self.stderr.abort();
        }
        let bytes = self
            .stderr_tail
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        String::from_utf8_lossy(&bytes).trim().to_owned()
    }
}

impl RunningProcess for CodexProcess {
    fn next(&mut self, deadline: Option<Instant>) -> BoxFuture<'_, Next> {
        Box::pin(CodexProcess::next(self, deadline))
    }
    fn wait(&mut self, deadline: Option<Instant>) -> BoxFuture<'_, Result<Option<String>>> {
        Box::pin(async move {
            CodexProcess::wait(self, deadline).await.map(|status| status.map(|value| value.to_string()))
        })
    }
    fn terminate(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(CodexProcess::terminate(self))
    }
    fn stderr_tail(&mut self) -> BoxFuture<'_, String> {
        Box::pin(CodexProcess::stderr_tail(self))
    }
}

impl Drop for CodexProcess {
    fn drop(&mut self) {
        self.stderr.abort();
    }
}

/// Runs `<binary> --version` and checks it is the pinned release.
pub async fn check_version(binary: &Path) -> Result<()> {
    let mut command = Command::new(binary);
    command.arg("--version");
    let output = acyclic_native_runtime::run_blocking_io(move || {
        acyclic_native_runtime::process_output(&mut command, Duration::from_secs(30), STDERR_TAIL)
    })
    .await
    .map_err(|error| Error::Storage(error.to_string()))?
    .map_err(|error| {
        Error::Unsupported(format!(
            "cannot run {} --version: {error}",
            binary.display(),
        ))
    })?;
    if !output.status.success() {
        return Err(Error::Unsupported(format!(
            "{} --version exited with {}: {}",
            binary.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
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
pub struct Dirs {
    /// Native Codex configuration directory.
    pub codex_home: PathBuf,
    /// Native engine home, separate from external tool workspaces.
    pub home: PathBuf,
}

impl Dirs {
    /// Reopens the same operation's engine directories, never a new run ID.
    pub fn create(state_dir: &Path, operation: &str) -> Result<Self> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_version_probe_cannot_qualify_expected_version_text() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let binary = directory.path().join("failed-version");
        std::fs::write(&binary, "#!/bin/sh\nprintf 'codex 0.155.1\\n'\nexit 7\n")
            .map_err(|error| Error::Storage(error.to_string()))?;
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert!(check_version(&binary).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn streaming_rejects_oversized_and_invalid_records() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        for script in ["head -c 8388609 /dev/zero", "printf '\\377\\n'"] {
            let mut process = CodexProcess::spawn(Launch {
                binary: Path::new("/bin/sh"),
                args: vec!["-c".into(), script.into()],
                env: Vec::new(),
                workspace: directory.path(),
            })?;
            let next = process
                .next(Some(Instant::now() + Duration::from_secs(10)))
                .await;
            assert!(matches!(next, Next::Failed(_)));
            process.terminate().await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn observed_exit_cleans_background_descendants_before_success() -> Result<()> {
        let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let marker = directory.path().join("escaped");
        let mut process = CodexProcess::spawn(Launch {
            binary: Path::new("/bin/sh"),
            args: vec![
                "-c".into(),
                "(sleep 1; touch \"$1\") >/dev/null 2>&1 & exit 7".into(),
                "fixture".into(),
                marker.as_os_str().into(),
            ],
            env: Vec::new(),
            workspace: directory.path(),
        })?;
        assert_eq!(
            process
                .wait(Some(Instant::now() + Duration::from_secs(10)))
                .await?
                .and_then(|status| status.code()),
            Some(7)
        );
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(!marker.exists());
        Ok(())
    }
}
