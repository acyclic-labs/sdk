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
        Err(format!(
            "qualification worker {case} failed ({}): {}{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
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

pub(super) fn capture_child(
    command: &mut Command,
    timeout: Duration,
) -> Result<std::process::Output, Failure> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    ProcessTree::spawn(command)?
        .wait_with_output(
            timeout,
            usize::try_from(MAX_CHILD_OUTPUT_BYTES)?,
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

pub(super) fn captured_child_output(
    status: std::process::ExitStatus,
    stdout: &mut File,
    stderr: &mut File,
) -> Result<std::process::Output, Failure> {
    if stdout
        .metadata()?
        .len()
        .saturating_add(stderr.metadata()?.len())
        > MAX_CHILD_OUTPUT_BYTES
    {
        return Err(format!("child output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes").into());
    }
    fn read(file: &mut File) -> Result<Vec<u8>, Failure> {
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.take(MAX_CHILD_OUTPUT_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_CHILD_OUTPUT_BYTES {
            return Err(format!("child output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes").into());
        }
        Ok(bytes)
    }
    Ok(std::process::Output {
        status,
        stdout: read(stdout)?,
        stderr: read(stderr)?,
    })
}

pub(super) fn captured_child_output_exceeds_limit(child: &CapturedChild) -> Result<bool, Failure> {
    Ok(child
        .stdout
        .metadata()?
        .len()
        .saturating_add(child.stderr.metadata()?.len())
        > MAX_CHILD_OUTPUT_BYTES)
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
