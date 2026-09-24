//! Real-kernel native-mount qualification with a machine-readable receipt.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::model::{Lifecycle, VolumeConfig};
#[cfg(windows)]
use acyclic_fs::recover_native_mount_destination_preserving_residue;
use acyclic_fs::{
    Fs, LocalOptions, MountOptions, NativeMountKind, TransactionCommit, probe_native_mount,
    recover_native_mount_destination,
};
use acyclic_native_runtime::{ProcessTree, spawn_process_tree};
use bytes::Bytes;
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};

type Failure = Box<dyn std::error::Error + Send + Sync>;
type LocalTransaction =
    acyclic_fs::Transaction<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;
type LocalWorkspace =
    acyclic_fs::Workspace<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

#[derive(Serialize)]
struct Case {
    name: &'static str,
    status: &'static str,
    elapsed_ms: u128,
    reason: Option<String>,
}

#[derive(Serialize)]
struct Capability {
    kind: Option<&'static str>,
    available: bool,
    writable: bool,
    provider_process_io_observable: bool,
    session_isolation: String,
    unavailable_reason: Option<String>,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    os: &'static str,
    arch: &'static str,
    coverage: &'static [&'static str],
    capability: Capability,
    required_kind: Option<String>,
    release_version: Option<String>,
    executable_blake3: Option<String>,
    passed: bool,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct ReceiptCase {
    name: String,
    status: String,
}

#[derive(Deserialize)]
struct ReceiptCapability {
    kind: Option<String>,
    available: bool,
    writable: bool,
    provider_process_io_observable: bool,
    session_isolation: String,
    unavailable_reason: Option<String>,
}

#[derive(Deserialize)]
struct ReceiptReport {
    schema: String,
    os: String,
    arch: String,
    coverage: Vec<String>,
    capability: ReceiptCapability,
    required_kind: Option<String>,
    release_version: Option<String>,
    executable_blake3: Option<String>,
    passed: bool,
    cases: Vec<ReceiptCase>,
}

const COVERAGE: &[&str] = &[
    "create-read-write",
    "atomic-save",
    "rename-delete",
    "rename-before-hydration",
    "nested-paths",
    "large-directory-paging",
    "concurrent-handles",
    "watchers",
    "crash-detach-recovery",
    "mount-restoration",
    "hard-links",
    "symbolic-links-reparse-points",
    "metadata",
    "case-behavior",
    "escape-attempts",
    "root-checkout-untouched",
    "git-administration-untouched",
];
const MACOS_NFS_PARALLEL_CLIENTS: usize = 4;
const MACOS_NFS_LARGE_TREE_ENTRIES: usize = 512;
#[cfg(windows)]
const QUALIFICATION_MODIFIED_NS: i64 = 1_700_000_000_000_000_000;
const MAX_CHILD_OUTPUT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Eq, PartialEq)]
struct CheckoutSnapshot {
    head: Vec<u8>,
    status: Vec<u8>,
    checkout: blake3::Hash,
    git_admin: blake3::Hash,
}

enum QualificationWatcher {
    Native(notify::RecommendedWatcher),
    Poll(notify::PollWatcher),
}

impl QualificationWatcher {
    fn watch(&mut self, path: &Path) -> notify::Result<()> {
        match self {
            Self::Native(watcher) => watcher.watch(path, RecursiveMode::Recursive),
            Self::Poll(watcher) => watcher.watch(path, RecursiveMode::Recursive),
        }
    }
}

fn main() {
    if let Err(error) = dispatch() {
        eprintln!("native mount qualification failed: {error}");
        std::process::exit(1);
    }
}

fn dispatch() -> Result<(), Failure> {
    let args = std::env::args_os().collect::<Vec<_>>();
    match args.get(1).and_then(|value| value.to_str()) {
        Some("--io-child") => io_child(&required_path(&args, 2, "mount path")?),
        Some("--rename-hydration-child") => {
            rename_hydration_child(&required_path(&args, 2, "mount path")?)
        }
        Some("--parallel-io-child") => parallel_io_child(
            &required_path(&args, 2, "mount path")?,
            required_usize(&args, 3, "client index")?,
            required_usize(&args, 4, "client count")?,
        ),
        Some("--crash-child") => crash_child(
            required_path(&args, 2, "crash root")?,
            required_path(&args, 3, "mount path")?,
            required_path(&args, 4, "ready path")?,
        ),
        Some("--qualification-case-worker") => {
            let case = args
                .get(2)
                .and_then(|value| value.to_str())
                .ok_or("--qualification-case-worker requires a case name")?;
            run_on_qualification_thread(|| qualification_case_worker(case))
        }
        Some("--verify-receipt") => verify_receipt(args.get(1..).unwrap_or_default()),
        _ => run_on_qualification_thread(|| qualify(args.get(1..).unwrap_or_default())),
    }
}

fn run_on_qualification_thread(
    action: impl FnOnce() -> Result<(), Failure> + Send,
) -> Result<(), Failure> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("native-mount-qualify".into())
            .stack_size(32 * 1024 * 1024)
            .spawn_scoped(scope, action)
            .map_err(Failure::from)?
            .join()
            .map_err(|_| -> Failure { "qualification worker panicked".into() })?
    })
}

fn verify_receipt(args: &[std::ffi::OsString]) -> Result<(), Failure> {
    let receipt_path = option(args, "--verify-receipt")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires a path")?;
    let executable = option(args, "--release-executable")
        .map(PathBuf::from)
        .ok_or("--verify-receipt requires --release-executable")?;
    let required_kind =
        option(args, "--require-kind").ok_or("--verify-receipt requires --require-kind")?;
    let release_version =
        option(args, "--release-version").ok_or("--verify-receipt requires --release-version")?;
    let (expected_os, provider_process_io_observable) = match required_kind.as_str() {
        "linux-fuse" => ("linux", true),
        "macos-nfs" => ("macos", true),
        "windows-projfs" => ("windows", false),
        _ => return Err(format!("unsupported receipt backend: {required_kind}").into()),
    };
    let expected_arch = std::env::consts::ARCH;
    let report: ReceiptReport = serde_json::from_slice(&fs::read(receipt_path)?)?;
    let digest = file_blake3(&executable)?;
    if report.schema != "acyclic-native-mount-qualification-v2"
        || report.os != expected_os
        || report.arch != expected_arch
        || !report
            .coverage
            .iter()
            .map(String::as_str)
            .eq(COVERAGE.iter().copied())
        || report.required_kind.as_deref() != Some(required_kind.as_str())
        || report.release_version.as_deref() != Some(release_version.as_str())
        || report.executable_blake3.as_deref() != Some(digest.as_str())
        || !report.passed
        || report.capability.kind.as_deref() != Some(required_kind.as_str())
        || !report.capability.available
        || !report.capability.writable
        || report.capability.provider_process_io_observable != provider_process_io_observable
        || report.capability.session_isolation != "SharedProcess"
        || report.capability.unavailable_reason.is_some()
        || report.cases.len() != 3
        || !report
            .cases
            .iter()
            .zip([
                "real-mount-mutation-matrix",
                "crash-detach-recovery",
                "checkout-and-git-untouched",
            ])
            .all(|(case, name)| case.name == name && case.status == "passed")
    {
        return Err("native mount receipt does not match the release artifact".into());
    }
    Ok(())
}

fn required_path(
    args: &[std::ffi::OsString],
    index: usize,
    name: &str,
) -> Result<PathBuf, Failure> {
    args.get(index)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {name}").into())
}

fn required_usize(args: &[std::ffi::OsString], index: usize, name: &str) -> Result<usize, Failure> {
    args.get(index)
        .and_then(|value| value.to_str())
        .ok_or_else(|| -> Failure { format!("missing {name}").into() })?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}").into())
}

fn kind_name(kind: NativeMountKind) -> &'static str {
    match kind {
        NativeMountKind::LinuxFuse => "linux-fuse",
        NativeMountKind::MacOsNfs => "macos-nfs",
        NativeMountKind::WindowsProjFs => "windows-projfs",
    }
}

fn option(args: &[std::ffi::OsString], name: &str) -> Option<String> {
    args.windows(2).find_map(|pair| {
        let [key, value] = pair else {
            return None;
        };
        (key == OsStr::new(name)).then(|| value.to_string_lossy().into_owned())
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "qualification admission, case selection, and receipt emission form one CLI transaction"
)]
fn qualify(args: &[std::ffi::OsString]) -> Result<(), Failure> {
    let output = option(args, "--output").map(PathBuf::from);
    let checkout_root = option(args, "--checkout-root").map(PathBuf::from);
    let only_case = option(args, "--only-case");
    reject_output_inside_checkout(output.as_deref(), checkout_root.as_deref())?;
    let required_kind = option(args, "--require-kind");
    let release_executable = option(args, "--release-executable").map(PathBuf::from);
    if required_kind.is_some() && release_executable.is_none() {
        return Err("--require-kind requires --release-executable".into());
    }
    let (release_version, executable_blake3) = release_executable
        .as_deref()
        .map(release_identity)
        .transpose()?
        .map_or((None, None), |(version, digest)| {
            (Some(version), Some(digest))
        });
    let allow_unsupported = args.iter().any(|arg| arg == "--allow-unsupported");
    let native = probe_native_mount();
    let observed_kind = native.kind.map(kind_name);
    let capability = Capability {
        kind: observed_kind,
        available: native.available,
        writable: native.writable,
        provider_process_io_observable: native.provider_process_io_observable,
        session_isolation: format!("{:?}", native.session_isolation),
        unavailable_reason: native.unavailable_reason.clone(),
    };
    let mut report = Report {
        schema: "acyclic-native-mount-qualification-v2",
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        coverage: COVERAGE,
        capability,
        required_kind: required_kind.clone(),
        release_version,
        executable_blake3,
        passed: true,
        cases: Vec::new(),
    };

    let unsupported = if !native.available {
        Some(
            native
                .unavailable_reason
                .unwrap_or_else(|| "native mount unavailable".into()),
        )
    } else if !native.writable {
        Some("native mount capability is read-only".into())
    } else if let Some(required) = &required_kind {
        (observed_kind != Some(required.as_str())).then(|| {
            format!(
                "required backend {required}, observed {}",
                observed_kind.unwrap_or("none")
            )
        })
    } else {
        None
    };
    if let Some(reason) = unsupported {
        let allowed = allow_unsupported && required_kind.is_none();
        report.cases.push(Case {
            name: "capability",
            status: if allowed { "skipped" } else { "failed" },
            elapsed_ms: 0,
            reason: Some(reason),
        });
        report.passed = allowed;
        emit_report(&report, output.as_deref())?;
        return if allowed {
            Ok(())
        } else {
            Err("required native mount capability is absent".into())
        };
    }

    let snapshot = checkout_root
        .as_deref()
        .map(snapshot_checkout)
        .transpose()?;
    if only_case
        .as_deref()
        .is_none_or(|case| case == "real-mount-mutation-matrix")
    {
        run_case(&mut report, "real-mount-mutation-matrix", || {
            // The matrix includes a 10-second I/O-child deadline followed by
            // a 30-second parallel-client deadline and mount teardown.
            run_supervised_case("real-mount-mutation-matrix", Duration::from_secs(45))
        });
    }
    if only_case
        .as_deref()
        .is_none_or(|case| case == "crash-detach-recovery")
    {
        run_case(&mut report, "crash-detach-recovery", || {
            // The recovery path itself permits a 30-second detach; the outer
            // supervisor must not kill it before that bound can report why.
            run_supervised_case("crash-detach-recovery", Duration::from_secs(35))
        });
    }
    if only_case
        .as_deref()
        .is_none_or(|case| case == "checkout-and-git-untouched")
    {
        run_case(&mut report, "checkout-and-git-untouched", || {
            if let (Some(root), Some(before)) = (checkout_root.as_deref(), snapshot.as_ref()) {
                let after = snapshot_checkout(root)?;
                if &after != before {
                    return Err("root checkout or Git administrative state changed".into());
                }
            }
            Ok(())
        });
    }
    if only_case.as_deref() == Some("rename-before-hydration") {
        run_case(&mut report, "rename-before-hydration", || {
            run_supervised_case("rename-before-hydration", Duration::from_secs(15))
        });
    }
    if report.cases.is_empty() {
        return Err(format!(
            "unknown --only-case value: {}",
            only_case.as_deref().unwrap_or_default()
        )
        .into());
    }
    report.passed = report.cases.iter().all(|case| case.status == "passed");
    emit_report(&report, output.as_deref())?;
    if report.passed {
        Ok(())
    } else {
        Err("one or more qualification cases failed".into())
    }
}

fn qualification_case_worker(case: &str) -> Result<(), Failure> {
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

fn run_supervised_case(case: &str, timeout: Duration) -> Result<(), Failure> {
    let child = spawn_captured_child(
        Command::new(std::env::current_exe()?)
            .arg("--qualification-case-worker")
            .arg(case),
    )?;
    let output = wait_for_child(child, timeout)?;
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

async fn rename_hydration_case() -> Result<(), Failure> {
    const RENAME_REPETITIONS: usize = 16;
    let root = tempfile::tempdir()?;
    let mount_path = root.path().join("mount");
    fs::create_dir(&mount_path)?;
    let engine = Fs::local(LocalOptions::new(root.path().join("store"))).await?;
    let workspace = engine
        .create_workspace_with_config("rename-hydration", VolumeConfig::native(Lifecycle::Durable))
        .await?;
    let mut fixture = workspace
        .begin_transaction(acyclic_fs::IdempotencyKey::new())
        .await?;
    for index in 0..RENAME_REPETITIONS {
        fixture
            .create_file(
                &format!("/before-{index}.txt"),
                Bytes::from(format!("renamed-before-hydration-{index}")),
                FileMetadata::default(),
            )
            .await?;
    }
    match fixture.commit().await? {
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
        _ => return Err("rename hydration fixture transaction was not committed".into()),
    }
    let mount = workspace
        .mount(&mount_path, MountOptions::read_write())
        .await?;
    let child = spawn_captured_child(
        Command::new(std::env::current_exe()?)
            .arg("--rename-hydration-child")
            .arg(&mount_path),
    )?;
    let child_result = wait_for_child(child, Duration::from_secs(10));
    let synced = mount.sync().await.map_err(Failure::from);
    let unmounted = mount.unmount().await.map_err(Failure::from);
    finish_mounted_child(
        "rename hydration child",
        child_result,
        Ok(()),
        synced,
        unmounted,
        &mount_path,
    )?;
    for index in 0..RENAME_REPETITIONS {
        let renamed = format!("/after-{index}.txt");
        let replacement = format!("/replacement-{index}.txt");
        if workspace.read(&renamed, 64).await?.as_ref()
            != format!("renamed-before-hydration-{index}").as_bytes()
        {
            return Err(format!("renamed content {index} was not published").into());
        }
        if workspace.read(&replacement, 64).await?.as_ref()
            != format!("replacement-content-{index}").as_bytes()
        {
            return Err(format!("replacement content {index} was not published").into());
        }
        #[cfg(windows)]
        if !matches!(
            workspace.stat(&renamed).await?.metadata.windows_attributes,
            Some(attributes) if attributes & 1 != 0
        ) {
            return Err(format!("renamed metadata {index} was not published").into());
        }
    }
    Ok(())
}

struct CapturedChild {
    process: ProcessTree,
    stdout: File,
    stderr: File,
}

fn spawn_captured_child(command: &mut Command) -> Result<CapturedChild, Failure> {
    let stdout = tempfile::tempfile()?;
    let stderr = tempfile::tempfile()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout.try_clone()?))
        .stderr(Stdio::from(stderr.try_clone()?));
    Ok(CapturedChild {
        process: spawn_process_tree(command)?,
        stdout,
        stderr,
    })
}

fn wait_for_child(
    mut child: CapturedChild,
    timeout: Duration,
) -> Result<std::process::Output, Failure> {
    let deadline = Instant::now() + timeout;
    loop {
        if captured_child_output_exceeds_limit(&child)? {
            child.process.terminate_descendants()?;
            let _ = child.process.wait();
            return Err(format!("child output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes").into());
        }
        if let Some(status) = child.process.try_wait()? {
            return captured_child_output(status, &mut child.stdout, &mut child.stderr);
        }
        if Instant::now() >= deadline {
            child.process.terminate_descendants()?;
            let status = child.process.wait()?;
            let output = captured_child_output(status, &mut child.stdout, &mut child.stderr)?;
            return Err(format!(
                "child process exceeded {} ms: stdout={} stderr={}",
                timeout.as_millis(),
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn captured_child_output(
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

fn captured_child_output_exceeds_limit(child: &CapturedChild) -> Result<bool, Failure> {
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

fn finish_mounted_child(
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

fn rename_hydration_child(mount: &Path) -> Result<(), Failure> {
    for index in 0..16 {
        let source = mount.join(format!("before-{index}.txt"));
        let renamed = mount.join(format!("after-{index}.txt"));
        let replacement = mount.join(format!("replacement-{index}.txt"));
        fs::rename(&source, &renamed)?;
        #[cfg(windows)]
        mutate_default_metadata(&renamed)?;
        fs::write(&source, format!("replacement-content-{index}"))?;
        fs::rename(&source, &replacement)?;
        if fs::read(&renamed)? != format!("renamed-before-hydration-{index}").as_bytes() {
            return Err(
                format!("renamed placeholder {index} returned stale or missing content").into(),
            );
        }
        if fs::read(&replacement)? != format!("replacement-content-{index}").as_bytes() {
            return Err(
                format!("replacement path {index} returned stale or missing content").into(),
            );
        }
    }
    Ok(())
}

fn release_identity(executable: &Path) -> Result<(String, String), Failure> {
    let output = Command::new(executable).arg("--version").output()?;
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

fn file_blake3(path: &Path) -> Result<String, Failure> {
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

fn run_case(report: &mut Report, name: &'static str, action: impl FnOnce() -> Result<(), Failure>) {
    let started = Instant::now();
    let result = action();
    report.cases.push(Case {
        name,
        status: if result.is_ok() { "passed" } else { "failed" },
        elapsed_ms: started.elapsed().as_millis(),
        reason: result.err().map(|error| error.to_string()),
    });
}

fn emit_report(report: &Report, output: Option<&Path>) -> Result<(), Failure> {
    let json = serde_json::to_vec_pretty(report)?;
    println!("{}", String::from_utf8_lossy(&json));
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, json)?;
    }
    Ok(())
}

fn reject_output_inside_checkout(
    output: Option<&Path>,
    checkout_root: Option<&Path>,
) -> Result<(), Failure> {
    let (Some(output), Some(checkout_root)) = (output, checkout_root) else {
        return Ok(());
    };
    let checkout_root = checkout_root.canonicalize()?;
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()?.join(output)
    };
    let output = resolve_existing_ancestor(&output)?;
    if output.starts_with(&checkout_root) {
        return Err("qualification receipt must be written outside the checkout".into());
    }
    let git_dir = resolve_git_dir(&checkout_root)?;
    if output.starts_with(git_dir) {
        return Err(
            "qualification receipt must be written outside Git administrative state".into(),
        );
    }
    Ok(())
}

fn resolve_existing_ancestor(path: &Path) -> Result<PathBuf, Failure> {
    let mut cursor = path;
    let mut missing = Vec::new();
    while !cursor.exists() {
        let name = cursor
            .file_name()
            .ok_or("output path has no existing ancestor")?;
        missing.push(name.to_owned());
        cursor = cursor
            .parent()
            .ok_or("output path has no existing ancestor")?;
    }
    let mut resolved = cursor.canonicalize()?;
    for component in missing.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

#[allow(clippy::too_many_lines)]
async fn mutation_matrix(kind: &'static str) -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let store = root.path().join("store");
    let mount_path = root.path().join("mount");
    fs::create_dir(&mount_path)?;
    fs::write(root.path().join("outside-guard"), b"outside-safe")?;
    let engine = Fs::local(LocalOptions::new(&store)).await?;
    let workspace = engine
        .create_workspace_with_config("native-matrix", VolumeConfig::native(Lifecycle::Durable))
        .await?;
    let mut transaction = workspace
        .begin_transaction(acyclic_fs::IdempotencyKey::new())
        .await?;
    transaction.create_dir_all("/seed/nested").await?;
    transaction
        .create_file(
            "/seed/nested/original.txt",
            Bytes::from_static(b"seed"),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .create_file(
            "/seed/nested/default-metadata.txt",
            Bytes::new(),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .create_file(
            "/seed/nested/default-read.txt",
            Bytes::from_static(b"read-only-observation"),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .create_file(
            "/seed/nested/rename-before-read.txt",
            Bytes::from_static(b"renamed-before-hydration"),
            FileMetadata::default(),
        )
        .await?;
    transaction
        .hard_link(
            "/seed/nested/original.txt",
            "/seed/nested/original-hard.txt",
        )
        .await?;
    transaction
        .create_symbolic_link(
            "/seed/nested/original-link.txt",
            native_link_bytes("original.txt"),
        )
        .await?;
    add_large_windows_fixture(kind, &mut transaction).await?;
    if kind == "macos-nfs" {
        transaction.create_dir_all("/large-tree").await?;
        for index in 0..MACOS_NFS_LARGE_TREE_ENTRIES {
            transaction
                .create_file(
                    &format!("/large-tree/entry-{index:04}.txt"),
                    Bytes::from_static(b"large-tree"),
                    FileMetadata::default(),
                )
                .await?;
        }
    }
    match transaction.commit().await? {
        TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_) => {}
        _ => return Err("fixture transaction was not committed".into()),
    }
    #[cfg(windows)]
    let default_read_metadata = workspace
        .stat("/seed/nested/default-read.txt")
        .await?
        .metadata;

    let (watch_tx, watch_rx) = mpsc::channel();
    let mut watcher = if kind == "macos-nfs" {
        QualificationWatcher::Poll(notify::PollWatcher::new(
            move |event| {
                let _ = watch_tx.send(event);
            },
            notify::Config::default().with_poll_interval(Duration::from_millis(100)),
        )?)
    } else {
        QualificationWatcher::Native(notify::recommended_watcher(move |event| {
            let _ = watch_tx.send(event);
        })?)
    };
    let mount = workspace
        .mount(&mount_path, MountOptions::read_write())
        .await?;
    watcher.watch(&mount_path)?;
    let child = spawn_captured_child(
        Command::new(std::env::current_exe()?)
            .arg("--io-child")
            .arg(&mount_path)
            .env("ACYCLIC_EXPECTED_NATIVE_KIND", kind),
    )?;
    let child_result = wait_for_child(child, Duration::from_secs(10));
    let watched = wait_for_watch_after_child(
        &watch_rx,
        &mount_path.join("nested/watch-sentinel.txt"),
        &child_result,
    );
    eprintln!("qualification phase: I/O child finished; parallel clients starting");
    let parallel = if kind == "macos-nfs" {
        run_parallel_io_children(&mount_path, MACOS_NFS_PARALLEL_CLIENTS)
    } else {
        Ok(())
    };
    eprintln!("qualification phase: parallel clients finished; mount sync starting");
    let synced = mount.sync().await.map_err(Failure::from);
    eprintln!("qualification phase: mount sync finished; unmount starting");
    let detached = mount.unmount().await.map_err(Failure::from);
    eprintln!("qualification phase: unmount finished");
    finish_mounted_child(
        "I/O child",
        child_result,
        watched,
        synced,
        detached,
        &mount_path,
    )?;
    parallel?;
    verify_published_mutations(&workspace, root.path()).await?;
    let metadata = workspace.stat("/nested/metadata.txt").await?.metadata;
    #[cfg(windows)]
    {
        if metadata
            .windows_attributes
            .is_none_or(|attributes| attributes & 1 == 0)
        {
            return Err("Windows read-only metadata was not published".into());
        }
        let timestamps = workspace.stat("/nested/timestamps.txt").await?.metadata;
        if timestamps.modified_ns != Some(QUALIFICATION_MODIFIED_NS) {
            return Err(format!(
                "modified timestamp was not published: {:?}",
                timestamps.modified_ns
            )
            .into());
        }
        let defaulted = workspace
            .stat("/seed/nested/default-metadata.txt")
            .await?
            .metadata;
        if defaulted
            .windows_attributes
            .is_none_or(|attributes| attributes & 1 == 0)
            || defaulted.modified_ns != Some(QUALIFICATION_MODIFIED_NS)
        {
            return Err("metadata on a default-metadata projection was not published".into());
        }
        let observed = workspace
            .stat("/seed/nested/default-read.txt")
            .await?
            .metadata;
        if observed != default_read_metadata {
            return Err(format!(
                "plain read published metadata for a default-metadata projection: before {default_read_metadata:?}, after {observed:?}"
            )
            .into());
        }
    }
    #[cfg(unix)]
    if metadata.posix_mode.is_none_or(|mode| mode & 0o777 != 0o640) {
        return Err("POSIX mode metadata was not published".into());
    }
    if kind == "macos-nfs" {
        macos_nfs_handoff_and_mount_loss(&workspace, root.path()).await?;
    }
    Ok(())
}

async fn verify_published_mutations(
    workspace: &LocalWorkspace,
    root: &Path,
) -> Result<(), Failure> {
    if fs::read(root.join("outside-guard"))? != b"outside-safe" {
        return Err("escape probe changed content outside the mount".into());
    }
    let escaped = root.join("escape-attempt.txt");
    if fs::read(&escaped)? != b"host-only" {
        return Err("traversal mutation did not remain in the temporary host parent".into());
    }
    if workspace.stat("/escape-attempt.txt").await.is_ok() {
        return Err("traversal mutation escaped into workspace state".into());
    }
    fs::remove_file(escaped)?;
    if workspace.read("/nested/created.txt", 64).await?.as_ref() != b"created-v2" {
        return Err("created content was not published".into());
    }
    if workspace.read("/nested/atomic.txt", 64).await?.as_ref() != b"atomic-new" {
        return Err("atomic save was not published".into());
    }
    if workspace.read("/nested/renamed.txt", 64).await?.as_ref() != b"rename-me" {
        return Err("rename was not published".into());
    }
    if workspace.stat("/nested/delete-me.txt").await.is_ok() {
        return Err("delete was not published".into());
    }
    let original = workspace.stat("/nested/hard-source.txt").await?;
    let linked = workspace.stat("/nested/hard-link.txt").await?;
    if original.file_id != linked.file_id || original.link_count < 2 {
        return Err("hard-link identity was not preserved".into());
    }
    if workspace.read_symbolic_link("/nested/symbolic.txt").await?
        != native_link_bytes("created.txt")
    {
        return Err("symbolic-link target was not published".into());
    }
    Ok(())
}

async fn add_large_windows_fixture(
    kind: &str,
    transaction: &mut LocalTransaction,
) -> Result<(), Failure> {
    if kind != "windows-projfs" {
        return Ok(());
    }
    transaction.create_dir_all("/large").await?;
    for ordinal in 0..300_u16 {
        let extension = if ordinal % 10 == 0 { "rs" } else { "txt" };
        transaction
            .create_file(
                &format!("/large/entry-{ordinal:04}.{extension}"),
                Bytes::from(format!("entry-{ordinal}")),
                FileMetadata::default(),
            )
            .await?;
    }
    Ok(())
}

#[allow(clippy::match_same_arms)]
fn wait_for_watch(
    receiver: &mpsc::Receiver<notify::Result<notify::Event>>,
    sentinel: &Path,
) -> Result<(), Failure> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = None;
    while Instant::now() < deadline {
        match receiver.recv_timeout(Duration::from_millis(250)) {
            Ok(Ok(event)) if event.paths.iter().any(|path| path == sentinel) => {
                return Ok(());
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => last_error = Some(error),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => return Err(format!("watcher channel failed: {error}").into()),
        }
    }
    Err(last_error
        .map_or_else(
            || "no sentinel watcher event observed within 15 seconds".to_owned(),
            |error| format!("no sentinel watcher event observed; last watcher error: {error}"),
        )
        .into())
}

fn wait_for_watch_after_child(
    receiver: &mpsc::Receiver<notify::Result<notify::Event>>,
    sentinel: &Path,
    child: &Result<std::process::Output, Failure>,
) -> Result<(), Failure> {
    match child {
        Ok(output) if output.status.success() => wait_for_watch(receiver, sentinel),
        _ => Ok(()),
    }
}

fn verify_large_windows_directory(mount: &Path) -> Result<(), Failure> {
    let large = mount.join("large");
    if fs::read_dir(&large)?.count() != 300
        || fs::read(large.join("entry-0000.rs"))? != b"entry-0"
        || fs::read(large.join("entry-0299.txt"))? != b"entry-299"
    {
        return Err("large projected directory was incomplete or corrupt".into());
    }
    let wildcard = Command::new("cmd")
        .current_dir(&large)
        .args(["/D", "/Q", "/C", "dir", "/B", "/A:-D", "/ON", "*.rs"])
        .output()?;
    let wildcard_output = String::from_utf8_lossy(&wildcard.stdout);
    let wildcard_count = wildcard_output.lines().count();
    if !wildcard.status.success() || wildcard_count != 30 {
        return Err(format!(
            "wildcard enumeration failed (status {}, count {wildcard_count}): stdout={wildcard_output:?}, stderr={:?}",
            wildcard.status,
            String::from_utf8_lossy(&wildcard.stderr)
        )
        .into());
    }
    Ok(())
}

fn io_child(mount: &Path) -> Result<(), Failure> {
    verify_projected_seed(mount)?;
    fs::create_dir_all(mount.join("nested/deeper"))?;
    let created = mount.join("nested/created.txt");
    fs::write(&created, b"created-v1")?;
    fs::write(&created, b"created-v2")?;
    if fs::read(&created)? != b"created-v2" {
        return Err("create/read/write mismatch".into());
    }

    let atomic = mount.join("nested/atomic.txt");
    fs::write(&atomic, b"atomic-old")?;
    let temporary = mount.join("nested/.atomic.txt.save");
    let mut saved = File::create(&temporary)?;
    saved.write_all(b"atomic-new")?;
    saved.sync_all()?;
    drop(saved);
    replace(&temporary, &atomic)?;

    let rename_source = mount.join("nested/rename-source.txt");
    fs::write(&rename_source, b"rename-me")?;
    fs::rename(&rename_source, mount.join("nested/renamed.txt"))?;
    let deleted = mount.join("nested/delete-me.txt");
    fs::write(&deleted, b"delete-me")?;
    fs::remove_file(&deleted)?;

    let mut first = OpenOptions::new().read(true).write(true).open(&created)?;
    let mut second = OpenOptions::new().read(true).open(&created)?;
    first.seek(SeekFrom::End(0))?;
    first.write_all(b"-concurrent")?;
    first.sync_all()?;
    let mut observed = Vec::new();
    second.seek(SeekFrom::Start(0))?;
    second.read_to_end(&mut observed)?;
    if observed != b"created-v2-concurrent" {
        return Err("an already-open handle did not observe the concurrent write".into());
    }
    drop(first);
    drop(second);
    fs::write(&created, b"created-v2")?;

    let hard_source = mount.join("nested/hard-source.txt");
    fs::write(&hard_source, b"linked")?;
    fs::hard_link(&hard_source, mount.join("nested/hard-link.txt"))?;
    create_symlink(Path::new("created.txt"), &mount.join("nested/symbolic.txt"))?;
    let metadata = mount.join("nested/metadata.txt");
    create_metadata_file(&metadata)?;
    if std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND").as_deref() == Ok("macos-nfs") {
        macos_nfs_xattrs_and_toolchain(mount, &metadata)?;
        let entries = fs::read_dir(mount.join("large-tree"))?.collect::<Result<Vec<_>, _>>()?;
        if entries.len() != MACOS_NFS_LARGE_TREE_ENTRIES {
            return Err(format!(
                "large-tree readdir returned {} of {} entries",
                entries.len(),
                MACOS_NFS_LARGE_TREE_ENTRIES
            )
            .into());
        }
        for index in [
            0,
            MACOS_NFS_LARGE_TREE_ENTRIES / 2,
            MACOS_NFS_LARGE_TREE_ENTRIES - 1,
        ] {
            let path = mount.join(format!("large-tree/entry-{index:04}.txt"));
            if fs::metadata(&path)?.len() != 10 || fs::read(path)? != b"large-tree" {
                return Err(format!("large-tree entry {index} was incoherent").into());
            }
        }
    }
    #[cfg(windows)]
    create_timestamp_file(&mount.join("nested/timestamps.txt"))?;
    case_behavior(mount)?;

    let outside = mount
        .parent()
        .ok_or("mount has no parent")?
        .join("outside-guard");
    if fs::read(&outside)? != b"outside-safe" {
        return Err("escape guard changed".into());
    }
    let traversal = mount.join("nested/../../outside-guard");
    if fs::read(traversal)? != b"outside-safe" {
        return Err("normalized traversal did not remain host-controlled".into());
    }
    let traversal_write = mount.join("nested/../../escape-attempt.txt");
    fs::write(traversal_write, b"host-only")?;
    fs::write(mount.join("nested/watch-sentinel.txt"), b"watch-me")?;
    println!("native mount I/O matrix passed");
    Ok(())
}

fn verify_projected_seed(mount: &Path) -> Result<(), Failure> {
    let renamed_before_read = mount.join("seed/nested/rename-before-read.txt");
    let renamed_before_read_destination = mount.join("seed/nested/renamed-before-read.txt");
    fs::rename(&renamed_before_read, &renamed_before_read_destination)?;
    if fs::read(&renamed_before_read_destination)? != b"renamed-before-hydration" {
        return Err("rename-before-hydration returned stale or missing content".into());
    }
    if fs::read(mount.join("seed/nested/original.txt"))? != b"seed" {
        return Err("projected regular file differs through real mount".into());
    }
    if fs::read(mount.join("seed/nested/original-hard.txt"))? != b"seed" {
        return Err("projected hard link differs through real mount".into());
    }
    if std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND")? == "windows-projfs" {
        verify_large_windows_directory(mount)?;
    }
    let projected_link = fs::read_link(mount.join("seed/nested/original-link.txt"))?;
    if projected_link != Path::new("original.txt") {
        return Err(format!("projected symbolic link target differs: {projected_link:?}").into());
    }
    #[cfg(windows)]
    mutate_default_metadata(&mount.join("seed/nested/default-metadata.txt"))?;
    #[cfg(windows)]
    if fs::read(mount.join("seed/nested/default-read.txt"))? != b"read-only-observation" {
        return Err("default-metadata read fixture differs".into());
    }
    Ok(())
}

fn run_parallel_io_children(mount: &Path, count: usize) -> Result<(), Failure> {
    let executable = std::env::current_exe()?;
    let mut children = Vec::with_capacity(count);
    for index in 0..count {
        let child = spawn_captured_child(
            Command::new(&executable)
                .arg("--parallel-io-child")
                .arg(mount)
                .arg(index.to_string())
                .arg(count.to_string()),
        );
        match child {
            Ok(child) => children.push(Some(child)),
            Err(error) => {
                let cleanup = terminate_captured_children(&mut children);
                return Err(
                    format!("parallel I/O child {index} did not start: {error}{cleanup}").into(),
                );
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut remaining = count;
    while remaining != 0 {
        for index in 0..children.len() {
            let Some(child) = children.get_mut(index).and_then(Option::as_mut) else {
                continue;
            };
            if captured_child_output_exceeds_limit(child)? {
                let cleanup = terminate_captured_children(&mut children);
                return Err(format!(
                    "parallel I/O child {index} output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes{cleanup}"
                )
                .into());
            }
            let Some(status) = child.process.try_wait()? else {
                continue;
            };
            let mut child = children
                .get_mut(index)
                .and_then(Option::take)
                .ok_or("parallel child disappeared")?;
            let output = captured_child_output(status, &mut child.stdout, &mut child.stderr)?;
            remaining -= 1;
            if !output.status.success() {
                let cleanup = terminate_captured_children(&mut children);
                return Err(format!(
                    "parallel I/O child {index} failed ({}): {}{}{cleanup}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
                .into());
            }
        }
        if remaining != 0 && Instant::now() >= deadline {
            let cleanup = terminate_captured_children(&mut children);
            return Err(format!("parallel I/O children exceeded 30 seconds{cleanup}").into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn terminate_captured_children(children: &mut [Option<CapturedChild>]) -> String {
    let mut failures = Vec::new();
    for (index, child) in children.iter_mut().enumerate() {
        let Some(mut child) = child.take() else {
            continue;
        };
        if let Err(error) = child.process.terminate() {
            failures.push(format!("child {index}: {error}"));
        }
    }
    if failures.is_empty() {
        String::new()
    } else {
        format!("; cleanup failed: {}", failures.join(", "))
    }
}

fn parallel_io_child(mount: &Path, index: usize, count: usize) -> Result<(), Failure> {
    if count == 0 || index >= count {
        return Err("invalid parallel client identity".into());
    }
    let directory = mount.join("parallel-clients");
    let staging = mount.join("parallel-staging");
    fs::create_dir_all(&directory)
        .map_err(|error| format!("parallel client {index} create directory: {error}"))?;
    fs::create_dir_all(&staging)
        .map_err(|error| format!("parallel client {index} create staging: {error}"))?;
    let body = format!("client-{index}");
    let staged = staging.join(format!("client-{index}.tmp"));
    fs::write(&staged, body.as_bytes())
        .map_err(|error| format!("parallel client {index} write own file: {error}"))?;
    fs::rename(&staged, directory.join(format!("client-{index}.txt")))
        .map_err(|error| format!("parallel client {index} publish own file: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let entries = retry_nfs_directory_listing(deadline, || {
            fs::read_dir(&directory).and_then(Iterator::collect::<Result<Vec<_>, _>>)
        })
        .map_err(|error| format!("parallel client {index} list directory: {error}"))?;
        if entries.len() == count {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "parallel client {index} observed {} of {count} peers",
                entries.len()
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    for peer in 0..count {
        let path = directory.join(format!("client-{peer}.txt"));
        let expected = format!("client-{peer}");
        if fs::metadata(&path)
            .map_err(|error| format!("parallel client {index} stat peer {peer}: {error}"))?
            .len()
            != u64::try_from(expected.len())?
            || fs::read(&path)
                .map_err(|error| format!("parallel client {index} read peer {peer}: {error}"))?
                != expected.as_bytes()
        {
            return Err(format!("parallel client {index} saw incoherent peer {peer}").into());
        }
    }
    Ok(())
}

fn retry_nfs_directory_listing<T>(
    deadline: Instant,
    mut list: impl FnMut() -> std::io::Result<T>,
) -> std::io::Result<T> {
    loop {
        match list() {
            Ok(entries) => return Ok(entries),
            Err(error) if retryable_nfs_directory_error(&error) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => return Err(error),
        }
    }
}

fn retryable_nfs_directory_error(error: &std::io::Error) -> bool {
    const MACOS_EIO: i32 = 5;
    error.kind() == std::io::ErrorKind::StaleNetworkFileHandle
        // macOS NFS also reports a concurrently invalidated directory page as EIO.
        || (cfg!(target_os = "macos") && error.raw_os_error() == Some(MACOS_EIO))
}

fn macos_nfs_xattrs_and_toolchain(mount: &Path, metadata: &Path) -> Result<(), Failure> {
    #[cfg(target_os = "macos")]
    {
        let write = Command::new("/usr/bin/xattr")
            .args(["-w", "com.acyclic.qualifier", "xattr-value"])
            .arg(metadata)
            .output()?;
        if !write.status.success() {
            return Err(format!(
                "xattr write failed: {}",
                String::from_utf8_lossy(&write.stderr)
            )
            .into());
        }
        let read = Command::new("/usr/bin/xattr")
            .args(["-px", "com.acyclic.qualifier"])
            .arg(metadata)
            .output()?;
        let encoded = String::from_utf8_lossy(&read.stdout)
            .replace([' ', '\n'], "")
            .to_ascii_lowercase();
        if !read.status.success() || encoded != "78617474722d76616c7565" {
            return Err(format!(
                "xattr round trip through NFS was not exact: status={} hex={encoded:?} stderr={}",
                read.status,
                String::from_utf8_lossy(&read.stderr)
            )
            .into());
        }
        let resource = Command::new("/usr/bin/xattr")
            .args([
                "-wx",
                "com.apple.ResourceFork",
                "7265736f757263652d666f726b",
            ])
            .arg(metadata)
            .output()?;
        if !resource.status.success() {
            return Err(format!(
                "resource fork write failed: {}",
                String::from_utf8_lossy(&resource.stderr)
            )
            .into());
        }
        let read = Command::new("/usr/bin/xattr")
            .args(["-px", "com.apple.ResourceFork"])
            .arg(metadata)
            .output()?;
        let encoded = String::from_utf8_lossy(&read.stdout)
            .replace([' ', '\n'], "")
            .to_ascii_lowercase();
        if !read.status.success() || encoded != "7265736f757263652d666f726b" {
            let listed = Command::new("/usr/bin/xattr")
                .args(["-l"])
                .arg(metadata)
                .output()?;
            let sidecar = metadata.with_file_name("._metadata.txt");
            let sidecar_bytes = fs::read(&sidecar).ok();
            let sidecar_payload_offset = sidecar_bytes.as_ref().and_then(|bytes| {
                bytes
                    .windows(b"resource-fork".len())
                    .position(|window| window == b"resource-fork")
            });
            return Err(format!(
                "resource fork round trip through NFS was not exact: status={} hex={encoded:?} stderr={} listed={} sidecar_bytes={:?} sidecar_header={:?} sidecar_payload_offset={sidecar_payload_offset:?}",
                read.status,
                String::from_utf8_lossy(&read.stderr),
                String::from_utf8_lossy(&listed.stdout),
                sidecar_bytes.as_ref().map(Vec::len),
                sidecar_bytes.as_ref().map(|bytes| &bytes[..bytes.len().min(16)])
            )
            .into());
        }
        let source = mount.join("nested/toolchain.c");
        let binary = mount.join("nested/toolchain");
        fs::write(&source, b"int main(void) { return 0; }\n")?;
        let compiled = Command::new("/usr/bin/clang")
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .output()?;
        if !compiled.status.success() {
            return Err(format!(
                "clang over NFS failed: {}",
                String::from_utf8_lossy(&compiled.stderr)
            )
            .into());
        }
        if !Command::new(&binary).status()?.success() {
            return Err("compiled NFS subprocess failed".into());
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (mount, metadata);
        Err("macOS NFS qualification ran on a non-macOS target".into())
    }
    #[cfg(target_os = "macos")]
    Ok(())
}

async fn macos_nfs_handoff_and_mount_loss<A, O>(
    workspace: &acyclic_fs::Workspace<A, O>,
    root: &Path,
) -> Result<(), Failure>
where
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    let destination = root.join("handoff-mount");
    fs::create_dir(&destination)?;
    let remount = workspace
        .mount(&destination, MountOptions::read_write())
        .await?;
    if fs::read(destination.join("nested/created.txt"))? != b"created-v2" {
        return Err("service handoff remount did not expose the published generation".into());
    }
    #[cfg(target_os = "macos")]
    {
        let detached = Command::new("/sbin/umount")
            .arg("-f")
            .arg(&destination)
            .output()?;
        if !detached.status.success() {
            return Err(format!(
                "forced mount-loss detach failed: {}",
                String::from_utf8_lossy(&detached.stderr)
            )
            .into());
        }
    }
    remount.unmount().await?;
    if !destination.is_dir() || fs::read_dir(&destination)?.next().is_some() {
        return Err("mount-loss cleanup did not restore an empty directory".into());
    }
    Ok(())
}

#[cfg(windows)]
fn create_timestamp_file(path: &Path) -> Result<(), Failure> {
    fs::write(path, b"timestamp")?;
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let modified = std::time::UNIX_EPOCH
        .checked_add(Duration::from_nanos(u64::try_from(
            QUALIFICATION_MODIFIED_NS,
        )?))
        .ok_or("qualification timestamp overflowed")?;
    file.set_times(fs::FileTimes::new().set_modified(modified))?;
    Ok(())
}

#[cfg(windows)]
fn mutate_default_metadata(path: &Path) -> Result<(), Failure> {
    let editor = OpenOptions::new().read(true).write(true).open(path)?;
    let observer = OpenOptions::new().read(true).open(path)?;
    let modified = std::time::UNIX_EPOCH
        .checked_add(Duration::from_nanos(u64::try_from(
            QUALIFICATION_MODIFIED_NS,
        )?))
        .ok_or("qualification timestamp overflowed")?;
    editor.set_times(fs::FileTimes::new().set_modified(modified))?;
    // Close the earlier, modified handle first. This exercises per-handle
    // metadata baselines when another handle to the same path remains open.
    drop(editor);
    drop(observer);
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn replace(source: &Path, destination: &Path) -> Result<(), Failure> {
    acyclic_native_runtime::durable_rename(
        source,
        destination,
        acyclic_native_runtime::RenameMode::Replace,
    )?;
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<(), Failure> {
    std::os::unix::fs::symlink(target, link)?;
    Ok(())
}

#[cfg(windows)]
fn create_symlink(target: &Path, link: &Path) -> Result<(), Failure> {
    std::os::windows::fs::symlink_file(target, link)?;
    Ok(())
}

#[cfg(unix)]
fn create_metadata_file(path: &Path) -> Result<(), Failure> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::write(path, b"metadata")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o640))?;
    if fs::metadata(path)?.permissions().mode() & 0o777 != 0o640 {
        return Err("POSIX mode did not round-trip".into());
    }
    Ok(())
}

#[cfg(windows)]
fn create_metadata_file(path: &Path) -> Result<(), Failure> {
    fs::write(path, b"metadata")?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(path, permissions)?;
    if !fs::metadata(path)?.permissions().readonly() {
        return Err("Windows read-only attribute did not round-trip".into());
    }
    Ok(())
}

fn case_behavior(mount: &Path) -> Result<(), Failure> {
    let upper = mount.join("nested/CaseProbe.txt");
    let lower = mount.join("nested/caseprobe.txt");
    fs::write(&upper, b"upper")?;
    let expected = std::env::var("ACYCLIC_EXPECTED_NATIVE_KIND")?;
    if expected == "windows-projfs" {
        if fs::read(&lower)? != b"upper" {
            return Err("Windows mount did not preserve case-insensitive lookup".into());
        }
    } else {
        fs::write(&lower, b"lower")?;
        if fs::read(&upper)? != b"upper" || fs::read(&lower)? != b"lower" {
            return Err("case-distinct names were not preserved".into());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn native_link_bytes(target: &str) -> Bytes {
    Bytes::copy_from_slice(target.as_bytes())
}

#[cfg(windows)]
fn native_link_bytes(target: &str) -> Bytes {
    Bytes::from(
        target
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>(),
    )
}

fn crash_recovery() -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let crash_root = root.path().join("child");
    let mount = root.path().join("mount");
    let ready = root.path().join("ready");
    fs::create_dir(&mount)?;
    let qualification = crash_recovery_inner(&crash_root, &mount, &ready);
    #[cfg(windows)]
    // The recovered path becomes an ordinary directory after the resumed
    // provider stops. The preserved residue lives under the same temporary
    // parent and is removed only when this isolated fixture is dropped.
    let cleanup = fs::remove_dir(&mount).map_err(Failure::from);
    #[cfg(not(windows))]
    let cleanup = recover_with_retry(&mount);
    match (qualification, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Err(cleanup_error)) => {
            Err(format!("{error}; final mount cleanup also failed: {cleanup_error}").into())
        }
    }
}

fn crash_recovery_inner(crash_root: &Path, mount: &Path, ready: &Path) -> Result<(), Failure> {
    let mut child = spawn_captured_child(
        Command::new(std::env::current_exe()?)
            .arg("--crash-child")
            .arg(crash_root)
            .arg(mount)
            .arg(ready),
    )?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() && Instant::now() < deadline {
        if captured_child_output_exceeds_limit(&child)? {
            child.process.terminate()?;
            return Err(
                format!("crash child output exceeded {MAX_CHILD_OUTPUT_BYTES} bytes").into(),
            );
        }
        if let Some(status) = child.process.try_wait()? {
            let output = captured_child_output(status, &mut child.stdout, &mut child.stderr)?;
            return Err(format!(
                "crash child exited before readiness ({}): {}{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !ready.exists() {
        child.process.terminate()?;
        return Err("crash child did not mount within 30 seconds".into());
    }
    eprintln!("qualification phase: crash child ready; termination starting");
    child.process.terminate()?;
    eprintln!("qualification phase: crash child terminated; recovery starting");
    #[cfg(windows)]
    {
        // Generic disposable recovery must still reject a stale writable
        // ProjFS cache. The explicit preserving operation moves it aside.
        if recover_native_mount_destination(mount).is_ok() {
            return Err("generic recovery discarded a stale ProjFS root".into());
        }
        let preserved = recover_native_mount_destination_preserving_residue(mount)?
            .ok_or("crash recovery failed to preserve the stale ProjFS root")?;
        if !preserved.is_dir() || mount.exists() {
            return Err("crash recovery did not move the stale root aside".into());
        }
        if fs::read(preserved.join("unpublished.txt"))? != b"unpublished" {
            return Err("crash recovery lost unpublished authored residue".into());
        }
        if recover_native_mount_destination_preserving_residue(mount)?.is_some() {
            return Err("crash recovery was not idempotent".into());
        }
        fs::create_dir(mount)?;
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            eprintln!("qualification phase: reopening durable crash workspace");
            let engine = Fs::local(LocalOptions::new(crash_root.join("store"))).await?;
            let workspace = engine.open_workspace("crash-owner").await?;
            if workspace.read("/alive.txt", 16).await?.as_ref() != b"alive" {
                return Err::<(), Failure>("durable workspace lost crash fixture".into());
            }
            eprintln!("qualification phase: remounting the original ProjFS destination");
            let resumed = workspace.mount(mount, MountOptions::read_write()).await?;
            eprintln!("qualification phase: reading durable ProjFS content");
            let contents = fs::read(mount.join("alive.txt"))?;
            resumed.unmount().await?;
            if contents != b"alive" {
                return Err::<(), Failure>("recovered mount lost the durable workspace".into());
            }
            Ok::<(), Failure>(())
        })?;
        if !preserved.is_dir() {
            return Err("recovered mount discarded the preserved residue".into());
        }
        eprintln!("qualification phase: preserved stale root and resumed at original path");
        Ok(())
    }
    #[cfg(not(windows))]
    {
        recover_with_retry(mount)?;
        eprintln!("qualification phase: crash recovery finished");
        if mount.exists() && (!mount.is_dir() || fs::read_dir(mount)?.next().is_some()) {
            return Err("crash recovery did not restore an empty ordinary directory".into());
        }
        fs::create_dir_all(mount)?;
        fs::write(mount.join("restored.txt"), b"ordinary")?;
        fs::remove_file(mount.join("restored.txt"))?;
        Ok(())
    }
}

#[cfg(not(windows))]
fn recover_with_retry(mount: &Path) -> Result<(), Failure> {
    let mut recovery_error = None;
    for attempt in 0..2 {
        match recover_native_mount_destination(mount) {
            Ok(()) => return Ok(()),
            Err(error) => {
                recovery_error = Some(error);
                if attempt == 0 {
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }
    Err(recovery_error.map_or_else(|| "native mount recovery failed".into(), Failure::from))
}

fn crash_child(root: PathBuf, mount: PathBuf, ready: PathBuf) -> Result<(), Failure> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        fs::create_dir_all(&root)?;
        let engine = Fs::local(LocalOptions::new(root.join("store"))).await?;
        let workspace = engine
            .create_workspace_with_config("crash-owner", VolumeConfig::native(Lifecycle::Durable))
            .await?;
        workspace.write_text("/alive.txt", "alive").await?;
        let _mount = workspace.mount(&mount, MountOptions::read_write()).await?;
        #[cfg(windows)]
        fs::write(mount.join("unpublished.txt"), b"unpublished")?;
        #[cfg(not(windows))]
        {
            if fs::read(mount.join("alive.txt"))? != b"alive" {
                return Err("crash fixture did not hydrate projected content".into());
            }
            let _open_hydrated_file = File::open(mount.join("alive.txt"))?;
        }
        fs::write(ready, b"ready")?;
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), Failure>(())
    })
}

fn snapshot_checkout(root: &Path) -> Result<CheckoutSnapshot, Failure> {
    let root = root.canonicalize()?;
    let head = git_output(&root, &["rev-parse", "HEAD"])?;
    let status = git_output(
        &root,
        &["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    let git_dir = resolve_git_dir(&root)?;
    Ok(CheckoutSnapshot {
        head,
        status,
        checkout: tree_digest(&root, &[OsStr::new(".git"), OsStr::new("target")])?,
        git_admin: tree_digest(&git_dir, &[])?,
    })
}

fn resolve_git_dir(root: &Path) -> Result<PathBuf, Failure> {
    let git_dir =
        PathBuf::from(String::from_utf8(git_output(root, &["rev-parse", "--git-dir"])?)?.trim());
    let git_dir = if git_dir.is_absolute() {
        git_dir
    } else {
        root.join(git_dir)
    };
    Ok(git_dir.canonicalize()?)
}

fn git_output(root: &Path, args: &[&str]) -> Result<Vec<u8>, Failure> {
    let output = Command::new("git").args(args).current_dir(root).output()?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}

fn tree_digest(root: &Path, excluded_root_names: &[&OsStr]) -> Result<blake3::Hash, Failure> {
    let mut paths = Vec::new();
    collect_paths(root, root, excluded_root_names, &mut paths)?;
    paths.sort();
    let mut digest = blake3::Hasher::new();
    for relative in paths {
        let path = root.join(&relative);
        let metadata = fs::symlink_metadata(&path)?;
        digest.update(relative.to_string_lossy().as_bytes());
        update_stable_metadata(&mut digest, &metadata);
        if metadata.file_type().is_symlink() {
            digest.update(b"L");
            digest.update(fs::read_link(path)?.to_string_lossy().as_bytes());
        } else if metadata.is_dir() {
            digest.update(b"D");
        } else if metadata.is_file() {
            digest.update(b"F");
            digest.update(&fs::read(path)?);
        }
    }
    Ok(digest.finalize())
}

#[cfg(unix)]
fn update_stable_metadata(digest: &mut blake3::Hasher, metadata: &fs::Metadata) {
    use std::os::unix::fs::MetadataExt as _;
    digest.update(&metadata.mode().to_le_bytes());
    digest.update(&metadata.uid().to_le_bytes());
    digest.update(&metadata.gid().to_le_bytes());
    digest.update(&metadata.mtime().to_le_bytes());
    digest.update(&metadata.mtime_nsec().to_le_bytes());
    digest.update(&metadata.ctime().to_le_bytes());
    digest.update(&metadata.ctime_nsec().to_le_bytes());
}

#[cfg(windows)]
fn update_stable_metadata(digest: &mut blake3::Hasher, metadata: &fs::Metadata) {
    use std::os::windows::fs::MetadataExt as _;
    digest.update(&metadata.file_attributes().to_le_bytes());
    digest.update(&metadata.creation_time().to_le_bytes());
    digest.update(&metadata.last_write_time().to_le_bytes());
}

fn collect_paths(
    root: &Path,
    directory: &Path,
    excluded_root_names: &[&OsStr],
    paths: &mut Vec<PathBuf>,
) -> Result<(), Failure> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if directory == root
            && excluded_root_names
                .iter()
                .any(|excluded| *excluded == entry.file_name().as_os_str())
        {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(root)?.to_path_buf();
        paths.push(relative);
        if entry.file_type()?.is_dir() {
            collect_paths(root, &path, excluded_root_names, paths)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod parallel_io_tests {
    use super::{retry_nfs_directory_listing, retryable_nfs_directory_error};
    use std::io::{Error, ErrorKind};
    use std::time::{Duration, Instant};

    #[test]
    fn retries_only_transient_nfs_directory_errors() {
        assert!(retryable_nfs_directory_error(&Error::from(
            ErrorKind::StaleNetworkFileHandle
        )));
        assert!(!retryable_nfs_directory_error(&Error::from(
            ErrorKind::PermissionDenied
        )));
        assert_eq!(
            retryable_nfs_directory_error(&Error::from_raw_os_error(5)),
            cfg!(target_os = "macos")
        );
    }

    #[test]
    fn retries_a_transient_listing_then_uses_the_complete_result() {
        let mut attempts = 0;
        let entries = retry_nfs_directory_listing(Instant::now() + Duration::from_secs(1), || {
            attempts += 1;
            if attempts == 1 {
                Err(Error::from(ErrorKind::StaleNetworkFileHandle))
            } else {
                Ok(vec!["peer-0", "peer-1"])
            }
        });
        assert_eq!(attempts, 2);
        assert_eq!(entries.ok(), Some(vec!["peer-0", "peer-1"]));
    }

    #[test]
    fn stops_retrying_after_the_deadline() {
        let mut attempts = 0;
        let result = retry_nfs_directory_listing::<()>(Instant::now(), || {
            attempts += 1;
            Err(Error::from(ErrorKind::StaleNetworkFileHandle))
        });
        assert_eq!(attempts, 1);
        assert!(matches!(result, Err(error) if error.kind() == ErrorKind::StaleNetworkFileHandle));
    }

    #[test]
    fn fails_immediately_on_an_unrelated_error() {
        let mut attempts = 0;
        let result =
            retry_nfs_directory_listing::<()>(Instant::now() + Duration::from_secs(1), || {
                attempts += 1;
                Err(Error::from(ErrorKind::PermissionDenied))
            });
        assert_eq!(attempts, 1);
        assert!(matches!(result, Err(error) if error.kind() == ErrorKind::PermissionDenied));
    }
}
