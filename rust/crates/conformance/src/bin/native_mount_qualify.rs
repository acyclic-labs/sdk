//! Real-kernel native-mount qualification with a machine-readable receipt.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::{
    Fs, LocalOptions, MountOptions, NativeMountKind, TransactionCommit, probe_native_mount,
    recover_native_mount_destination,
};
use bytes::Bytes;
use notify::{RecursiveMode, Watcher as _};
use serde::{Deserialize, Serialize};

type Failure = Box<dyn std::error::Error + Send + Sync>;

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
    "nested-paths",
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
#[cfg(windows)]
const QUALIFICATION_MODIFIED_NS: i64 = 1_700_000_000_000_000_000;

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
        Some("--crash-child") => crash_child(
            required_path(&args, 2, "crash root")?,
            required_path(&args, 3, "mount path")?,
            required_path(&args, 4, "ready path")?,
        ),
        Some("--verify-receipt") => verify_receipt(args.get(1..).unwrap_or_default()),
        _ => std::thread::scope(|scope| {
            let qualification_args = args.get(1..).unwrap_or_default();
            std::thread::Builder::new()
                .name("native-mount-qualify".into())
                .stack_size(32 * 1024 * 1024)
                .spawn_scoped(scope, || qualify(qualification_args))
                .map_err(Failure::from)?
                .join()
                .map_err(|_| -> Failure { "qualification worker panicked".into() })?
        }),
    }
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
    let (expected_os, expected_arch) = match required_kind.as_str() {
        "linux-fuse" => ("linux", "x86_64"),
        "macos-nfs" => ("macos", "aarch64"),
        "windows-projfs" => ("windows", "x86_64"),
        _ => return Err(format!("unsupported receipt backend: {required_kind}").into()),
    };
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

fn qualify(args: &[std::ffi::OsString]) -> Result<(), Failure> {
    let output = option(args, "--output").map(PathBuf::from);
    let checkout_root = option(args, "--checkout-root").map(PathBuf::from);
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
    run_case(&mut report, "real-mount-mutation-matrix", || {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(mutation_matrix(observed_kind.ok_or("backend kind absent")?))
    });
    run_case(&mut report, "crash-detach-recovery", crash_recovery);
    run_case(&mut report, "checkout-and-git-untouched", || {
        if let (Some(root), Some(before)) = (checkout_root.as_deref(), snapshot.as_ref()) {
            let after = snapshot_checkout(root)?;
            if &after != before {
                return Err("root checkout or Git administrative state changed".into());
            }
        }
        Ok(())
    });
    report.passed = report.cases.iter().all(|case| case.status == "passed");
    emit_report(&report, output.as_deref())?;
    if report.passed {
        Ok(())
    } else {
        Err("one or more qualification cases failed".into())
    }
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
    let workspace = engine.create_workspace("native-matrix").await?;
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
    let child = Command::new(std::env::current_exe()?)
        .arg("--io-child")
        .arg(&mount_path)
        .env("ACYCLIC_EXPECTED_NATIVE_KIND", kind)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;
    let child_error = (!child.status.success()).then(|| {
        format!(
            "I/O child failed ({}): {}{}",
            child.status,
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        )
    });
    let watched = wait_for_watch(&watch_rx, &mount_path.join("nested/watch-sentinel.txt"));
    let synced = mount.sync().await.map_err(Failure::from);
    let detached = mount.unmount().await.map_err(Failure::from);
    if let Some(error) = child_error {
        return Err(error.into());
    }
    watched?;
    synced?;
    detached?;
    if !mount_path.is_dir() || fs::read_dir(&mount_path)?.next().is_some() {
        return Err("orderly detach did not restore the empty mount directory".into());
    }
    if fs::read(root.path().join("outside-guard"))? != b"outside-safe" {
        return Err("escape probe changed content outside the mount".into());
    }
    let escaped = root.path().join("escape-attempt.txt");
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

fn io_child(mount: &Path) -> Result<(), Failure> {
    if fs::read(mount.join("seed/nested/original.txt"))? != b"seed" {
        return Err("projected regular file differs through real mount".into());
    }
    if fs::read(mount.join("seed/nested/original-hard.txt"))? != b"seed" {
        return Err("projected hard link differs through real mount".into());
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
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--crash-child")
        .arg(crash_root)
        .arg(mount)
        .arg(ready)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait()? {
            return Err(format!("crash child exited before readiness: {status}").into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !ready.exists() {
        child.kill()?;
        let _ = child.wait()?;
        return Err("crash child did not mount within 30 seconds".into());
    }
    child.kill()?;
    let _ = child.wait()?;
    recover_with_retry(mount)?;
    if mount.exists() && (!mount.is_dir() || fs::read_dir(mount)?.next().is_some()) {
        return Err("crash recovery did not restore an empty ordinary directory".into());
    }
    fs::create_dir_all(mount)?;
    fs::write(mount.join("restored.txt"), b"ordinary")?;
    fs::remove_file(mount.join("restored.txt"))?;
    Ok(())
}

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
        let workspace = engine.create_workspace("crash-owner").await?;
        workspace.write_text("/alive.txt", "alive").await?;
        let _mount = workspace.mount(&mount, MountOptions::read_write()).await?;
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
        checkout: tree_digest(&root, Some(OsStr::new(".git")))?,
        git_admin: tree_digest(&git_dir, None)?,
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

fn tree_digest(root: &Path, excluded_root_name: Option<&OsStr>) -> Result<blake3::Hash, Failure> {
    let mut paths = Vec::new();
    collect_paths(root, root, excluded_root_name, &mut paths)?;
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
    excluded_root_name: Option<&OsStr>,
    paths: &mut Vec<PathBuf>,
) -> Result<(), Failure> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if directory == root && excluded_root_name == Some(entry.file_name().as_os_str()) {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(root)?.to_path_buf();
        paths.push(relative);
        if entry.file_type()?.is_dir() {
            collect_paths(root, &path, excluded_root_name, paths)?;
        }
    }
    Ok(())
}
