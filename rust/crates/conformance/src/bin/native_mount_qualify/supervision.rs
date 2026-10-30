use super::*;

pub(super) fn stop_failed_mount(mut failure: acyclic_fs::MountLifecycleError) -> Failure {
    if let acyclic_fs::MountLifecycleError::Native(acyclic_fs::NativeMountError::StartupFailed(
        startup,
    )) = &mut failure
        && let Err(cleanup) = startup.owner_mut().stop()
    {
        return format!("{failure}; failed startup cleanup also failed: {cleanup}").into();
    }
    failure.into()
}

pub(super) fn qualification_case_worker(case: &str) -> Result<(), Failure> {
    match case {
        "real-mount-mutation-matrix" => {
            let kind = probe_native_mount()
                .kind
                .map(kind_name)
                .ok_or("backend kind absent")?;
            tokio::runtime::Runtime::new()?.block_on(mutation_matrix(kind))
        }
        "crash-detach-recovery" => crash_recovery(),
        "rename-before-hydration" => {
            tokio::runtime::Runtime::new()?.block_on(rename_hydration_case())
        }
        _ => Err(format!("unknown qualification worker case: {case}").into()),
    }
}

pub(super) fn run_supervised_case(case: &str) -> Result<(), Failure> {
    let output = capture_child(
        Command::new(std::env::current_exe()?)
            .arg("--qualification-case-worker")
            .arg(case),
    )?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "qualification worker {case} failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

struct CapturedOutput;

impl acyclic_native_runtime::CaptureHandler for CapturedOutput {
    fn check(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn observe(
        &mut self,
        _chunk: acyclic_native_runtime::ProcessOutput<'_>,
    ) -> std::io::Result<acyclic_native_runtime::ProcessInput> {
        Ok(acyclic_native_runtime::ProcessInput::default())
    }
}

/// Publication and exit share the original native pipe/capture owner.
pub(super) struct CapturedChild {
    output: Option<acyclic_native_runtime::ProcessCaptureStream<CapturedOutput>>,
    stdin: Option<std::process::ChildStdin>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    status: Option<std::process::ExitStatus>,
}

impl CapturedChild {
    pub(super) fn spawn(command: &mut Command) -> Result<Self, Failure> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut process = ProcessTree::spawn(command)?;
        let stdin = process.take_stdin();
        let output = process.stream_capture(
            &acyclic_native_runtime::TaskSpawner::current(),
            acyclic_native_runtime::CaptureSchedule::Unbounded,
            usize::MAX,
            None,
            CapturedOutput,
        );
        Ok(Self {
            output: Some(output),
            stdin,
            stdout: Vec::new(),
            stderr: Vec::new(),
            status: None,
        })
    }

    async fn receive(&mut self) -> std::io::Result<bool> {
        let Some(output) = self.output.as_mut() else {
            return Ok(false);
        };
        if let Some(chunk) = output.recv().await {
            self.stdout.extend(chunk.stdout);
            self.stderr.extend(chunk.stderr);
            return Ok(true);
        }
        let completed = self
            .output
            .take()
            .ok_or_else(|| std::io::Error::other("child capture owner disappeared"))?
            .finish()
            .await?;
        self.status = Some(completed.result.map_err(std::io::Error::other)?.status);
        Ok(false)
    }

    pub(super) async fn receipt(&mut self, expected: &[u8]) -> Result<(), Failure> {
        if expected.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "empty child receipt",
            )
            .into());
        }
        let mut scanned = 0;
        loop {
            if self.stdout[scanned..]
                .windows(expected.len())
                .any(|bytes| bytes == expected)
            {
                return Ok(());
            }
            scanned = self.stdout.len().saturating_sub(expected.len() - 1);
            if !self.receive().await? {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "captured child exited {:?} before publication: {}{}",
                        self.status,
                        String::from_utf8_lossy(&self.stdout),
                        String::from_utf8_lossy(&self.stderr),
                    ),
                )
                .into());
            }
        }
    }

    pub(super) fn release(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        self.stdin
            .take()
            .ok_or("captured child input was already closed")?
            .write_all(bytes)?;
        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<std::process::Output, Failure> {
        self.stdin.take();
        while self.receive().await? {}
        Ok(std::process::Output {
            status: self.status.ok_or("child capture omitted its exit status")?,
            stdout: self.stdout,
            stderr: self.stderr,
        })
    }

    pub(super) async fn stop(mut self) -> Result<(), Failure> {
        // Keep input open until forced cleanup joins, so cancellation cannot
        // turn an intended crash into the child's orderly EOF teardown.
        let _stdin = self.stdin.take();
        if let Some(output) = self.output.take() {
            let outcome = output.finish().await?;
            if let Err(failure) = outcome.result
                && !failure.cleanup_completed
            {
                return Err(failure.into());
            }
        }
        Ok(())
    }
}

pub(super) fn capture_child(command: &mut Command) -> Result<std::process::Output, Failure> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    ProcessTree::spawn(command)?
        .wait_with_output(
            acyclic_native_runtime::CaptureSchedule::Unbounded,
            usize::MAX,
            None,
            || Ok(()),
            |_| Ok(acyclic_native_runtime::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| {
            format!(
                "child capture failed: {}; cleanup_completed={}; stdout={} stderr={}",
                failure.error,
                failure.cleanup_completed,
                String::from_utf8_lossy(&failure.stdout),
                String::from_utf8_lossy(&failure.stderr)
            )
            .into()
        })
}

fn append_child_failure(
    failures: &mut Vec<String>,
    label: &str,
    result: Result<std::process::Output, Failure>,
) {
    match result {
        Ok(output) if output.status.success() => {}
        Ok(output) => failures.push(format!(
            "{label} failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )),
        Err(error) => failures.push(format!("{label} failed: {error}")),
    }
}

fn verify_empty_mount_directory(mount: &Path) -> Result<(), Failure> {
    if !mount.is_dir() || fs::read_dir(mount)?.next().is_some() {
        return Err("orderly detach did not restore the empty mount directory".into());
    }
    Ok(())
}

pub(super) fn finish_mounted_child(
    label: &str,
    child: Result<std::process::Output, Failure>,
    observed: Result<(), Failure>,
    synced: Result<(), Failure>,
    detached: Result<(), Failure>,
    mount: &Path,
) -> Result<(), Failure> {
    let mut failures = Vec::new();
    append_child_failure(&mut failures, label, child);
    if let Err(error) = observed {
        failures.push(format!("watch verification failed: {error}"));
    }
    if let Err(error) = synced {
        failures.push(format!("mount sync failed: {error}"));
    }
    if let Err(error) = detached {
        failures.push(format!("mount detach failed: {error}"));
    }
    if let Err(error) = verify_empty_mount_directory(mount) {
        failures.push(error.to_string());
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; ").into())
    }
}

pub(super) fn release_identity(executable: &Path) -> Result<(String, String), Failure> {
    let output = tool_output(Command::new(executable).arg("--version"))?;
    if !output.status.success() {
        return Err("release executable did not report its version".into());
    }
    let version_output = String::from_utf8(output.stdout)?;
    let mut fields = version_output.split_whitespace();
    if fields.next() != Some("acyclic") {
        return Err("release executable reported an unexpected product name".into());
    }
    let version = fields
        .next()
        .filter(|value| !value.is_empty())
        .ok_or("release executable did not report a version")?;
    if fields.next().is_some() {
        return Err("release executable reported an invalid version response".into());
    }
    Ok((version.to_owned(), file_blake3(executable)?))
}

pub(super) fn file_blake3(path: &Path) -> Result<String, Failure> {
    let mut file = File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let bytes = buffer
            .get(..count)
            .ok_or("release executable digest read exceeded its buffer")?;
        hasher.update(bytes);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
