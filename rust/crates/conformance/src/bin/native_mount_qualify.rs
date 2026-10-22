//! Real-kernel native-mount qualification with a machine-readable receipt.

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::model::{Lifecycle, VolumeConfig};
use acyclic_fs::{
    Fs, LocalOptions, MountOptions, NativeMountKind, TransactionCommit,
    native_mount::NativeMountRecoveryOwner, probe_native_mount,
};
use acyclic_native_runtime::ProcessTree;
use bytes::Bytes;
use notify::{RecursiveMode, Watcher};
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
    reason: serde_json::Value,
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

#[path = "native_mount_qualify/receipt.rs"]
mod receipt;
use receipt::*;
#[path = "native_mount_qualify/supervision.rs"]
mod supervision;
use supervision::*;
#[path = "native_mount_qualify/hydration.rs"]
mod hydration;
use hydration::*;
#[path = "native_mount_qualify/mutation.rs"]
mod mutation;
use mutation::*;
#[path = "native_mount_qualify/io.rs"]
mod io;
use io::*;
#[path = "native_mount_qualify/macos.rs"]
mod macos;
use macos::*;
#[path = "native_mount_qualify/filesystem.rs"]
mod filesystem;
use filesystem::*;
#[path = "native_mount_qualify/recovery.rs"]
mod recovery;
use recovery::*;
#[cfg(test)]
#[path = "native_mount_qualify/parallel_io_tests.rs"]
mod parallel_io_tests;
#[cfg(test)]
#[path = "native_mount_qualify/receipt_tests.rs"]
mod receipt_tests;

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
        available: native.available(),
        writable: native.available(),
        provider_process_io_observable: native.provider_process_io_observable(),
        session_isolation: if native.kind.is_some() {
            "SharedProcess"
        } else {
            "ProcessIsolated"
        }
        .to_owned(),
        unavailable_reason: native.unavailable_reason.map(str::to_owned),
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

    let unsupported = if !native.available() {
        Some(
            native
                .unavailable_reason
                .unwrap_or("native mount unavailable")
                .to_owned(),
        )
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

/// Qualification tool policy; process ownership and capture live in native-runtime.
fn tool_output(command: &mut Command) -> std::io::Result<std::process::Output> {
    acyclic_native_runtime::process_output(command, Duration::from_secs(120), 8 * 1024 * 1024)
}
