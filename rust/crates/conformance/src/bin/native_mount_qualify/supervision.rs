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
        "crash-detach-recovery" => tokio::runtime::Runtime::new()?.block_on(crash_recovery()),
        "rename-before-hydration" => {
            tokio::runtime::Runtime::new()?.block_on(rename_hydration_case())
        }
        _ => Err(format!("unknown qualification worker case: {case}").into()),
    }
}

pub(super) fn run_supervised_case(case: &str, timeout: Duration) -> Result<(), Failure> {
    let output = capture_child(
        Command::new(std::env::current_exe()?)
            .arg("--qualification-case-worker")
            .arg(case),
        timeout,
    )?;
    if output.status.success() {
        Ok(())
    } else {
        let status = output.status;
        output.write_diagnostics(&format!("qualification worker {case}"))?;
        Err(format!("qualification worker {case} failed ({status})").into())
    }
}

pub(super) struct CapturedChild {
    pub(super) process: ProcessTree,
    pub(super) stdout: File,
    pub(super) stderr: File,
}

pub(super) fn spawn_captured_child(command: &mut Command) -> Result<CapturedChild, Failure> {
    let stdout = tempfile::tempfile()?;
    let stderr = tempfile::tempfile()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    Ok(CapturedChild {
        process: ProcessTree::spawn(command)?,
        stdout,
        stderr,
    })
}

impl CapturedChild {
    pub(super) fn write_diagnostics(&mut self, label: &str) -> Result<(), Failure> {
        write_child_diagnostics(label, &mut self.stdout, &mut self.stderr)
    }
}

/// Waits for the actual child's atomically published ready record. File hints
/// only schedule another observation; they never certify readiness themselves.
pub(super) async fn captured_child_ready(
    child: &mut CapturedChild,
    marker: &Path,
    deadline: Instant,
) -> Result<(), Failure> {
    use acyclic_fs::{NativeMaterializationWork, NativeWatch, NativeWatchOptions, WatchBatch};

    let original = NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)?;
    let mut watch = NativeWatch::open_with_profile_with_native_work(
        marker.parent().ok_or("ready marker has no parent")?,
        if cfg!(windows) {
            acyclic_fs::model::FilesystemProfile::Windows
        } else {
            acyclic_fs::model::FilesystemProfile::Posix
        },
        NativeWatchOptions {
            limits: acyclic_fs::model::VolumeLimits::default(),
            recursive: false,
        },
        &original,
    )?;
    if let WatchBatch::RescanRequired { reason, .. } =
        watch.accept_lazy_baseline_with_native_work(&original)?
    {
        return Err(format!("ready marker watch cannot establish continuity: {reason:?}").into());
    }
    let observation = async {
        loop {
            // Both subscriptions precede the marker and process observations.
            let changed = watch.changed_with_native_work(&original);
            let exited = child.process.readiness()?;
            if observe_ready_marker(marker)? {
                return Ok(());
            }
            if let Some(status) = child.process.try_wait(None)? {
                return Err(format!("child exited before readiness ({status})").into());
            }
            tokio::select! {
                result = changed => {
                    result?;
                    // A page is work already delivered by the native producer.
                    // Unconsumed pages keep readiness set; there is no total cap.
                    let batch = watch.poll_with_native_work_guarded(256, &original, |_| Ok(()))?;
                    if let WatchBatch::RescanRequired { reason, .. } = &batch.0 {
                        return Err(format!("ready marker watch lost continuity: {reason:?}").into());
                    }
                }
                () = exited => {}
            }
        }
    };
    tokio::time::timeout_at(deadline.into(), observation)
        .await
        .map_err(|_| -> Failure { "child did not publish readiness before its deadline".into() })?
}

/// The fixture record is the literal `ready`, rather than arbitrary payload.
/// Check the complete fixed schema with stack storage, including trailing data.
fn observe_ready_marker(marker: &Path) -> Result<bool, Failure> {
    let mut file = match File::open(marker) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let mut record = [0; 6];
    let mut end = 0;
    loop {
        if end == record.len() {
            return Err("child published a malformed ready marker".into());
        }
        match file.read(&mut record[end..]) {
            Ok(0) if &record[..end] == b"ready" => return Ok(true),
            Ok(0) => return Err("child published a malformed ready marker".into()),
            Ok(count) => end += count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) struct CapturedOutput {
    pub(super) status: std::process::ExitStatus,
    stdout: File,
    stderr: File,
}

impl CapturedOutput {
    /// Streams complete diagnostics from the spool without retaining the output.
    pub(super) fn write_diagnostics(mut self, label: &str) -> Result<(), Failure> {
        write_child_diagnostics(
            &format!("{label} ({})", self.status),
            &mut self.stdout,
            &mut self.stderr,
        )
    }
}

fn write_child_diagnostics(
    label: &str,
    stdout: &mut File,
    stderr: &mut File,
) -> Result<(), Failure> {
    let mut destination = std::io::stderr().lock();
    writeln!(destination, "{label}:")?;
    stdout.seek(SeekFrom::Start(0))?;
    std::io::copy(stdout, &mut destination)?;
    stderr.seek(SeekFrom::Start(0))?;
    std::io::copy(stderr, &mut destination)?;
    Ok(())
}

pub(super) fn capture_child(
    command: &mut Command,
    timeout: Duration,
) -> Result<CapturedOutput, Failure> {
    let mut child = spawn_captured_child(command)?;
    match child.process.wait(timeout) {
        Ok(status) => captured_child_output(status, child),
        Err(error) => {
            let cleanup = child.process.terminate(None);
            let diagnostics = if cleanup.is_ok() {
                write_child_diagnostics(
                    "child capture failed",
                    &mut child.stdout,
                    &mut child.stderr,
                )
            } else {
                Ok(())
            };
            Err(format!(
                "child capture failed: {error}; cleanup={cleanup:?}; diagnostics={diagnostics:?}"
            )
            .into())
        }
    }
}

pub(super) fn captured_child_output(
    status: std::process::ExitStatus,
    mut child: CapturedChild,
) -> Result<CapturedOutput, Failure> {
    // Reap all descendants before reading the spool, including descendants that
    // outlived a successful direct child. Retain containment until cleanup ends.
    child.process.terminate(None)?;
    Ok(CapturedOutput {
        status,
        stdout: child.stdout,
        stderr: child.stderr,
    })
}

fn append_child_failure(
    failures: &mut Vec<String>,
    label: &str,
    result: Result<CapturedOutput, Failure>,
) {
    match result {
        Ok(output) if output.status.success() => {}
        Ok(output) => {
            let status = output.status;
            if let Err(error) = output.write_diagnostics(label) {
                failures.push(format!("{label} diagnostics failed: {error}"));
            }
            failures.push(format!("{label} failed ({status})"));
        }
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
    child: Result<CapturedOutput, Failure>,
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
