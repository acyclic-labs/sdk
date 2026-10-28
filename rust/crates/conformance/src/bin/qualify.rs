//! One bounded, machine-readable local qualification entry point.

#[path = "qualify/tools/mod.rs"]
#[allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::panic,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "the imported qualification fixture tool retains its existing bounded diagnostic behavior"
)]
mod tools;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::native_mount::{MountOptions, NativeMountKind, probe_native_mount};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{CancellationToken, Fs, LocalOptions};
use acyclic_objects::local::LocalObjectsLimits;
use acyclic_objects::{Blobs, ByteRange, Digest, LocalObjects, Store};
use acyclic_stream::{AppendOutcome, IdempotencyKey, LocalStream, LocalStreamLimits};
use bytes::Bytes;
use futures::StreamExt as _;
use serde::Serialize;

#[derive(Serialize)]
struct Case {
    name: &'static str,
    elapsed_ms: u128,
    passed: bool,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    supported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsupported_reason: Option<String>,
}

#[derive(Serialize)]
struct Report {
    schema: u32,
    os: &'static str,
    arch: &'static str,
    seed: String,
    budget_seconds: u64,
    elapsed_ms: u128,
    passed: bool,
    cases: Vec<Case>,
}

async fn run_case<F>(name: &'static str, started: Instant, budget: Duration, future: F) -> Case
where
    F: Future<Output = Result<(), String>>,
{
    let case_started = Instant::now();
    let result = match budget.checked_sub(started.elapsed()) {
        Some(remaining) => tokio::time::timeout(remaining, future)
            .await
            .unwrap_or_else(|_| Err("qualification time budget exceeded".into())),
        None => Err("qualification time budget exceeded".into()),
    };
    Case {
        name,
        elapsed_ms: case_started.elapsed().as_millis(),
        passed: result.is_ok(),
        error: result.err(),
        supported: None,
        unsupported_reason: None,
    }
}

async fn qualify_stream_local(root: PathBuf) -> Result<(), String> {
    let provider = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    acyclic_conformance::stream(&acyclic_stream::conformance::Fixture::new(provider)).await
}

async fn qualify_objects_local(root: PathBuf) -> Result<(), String> {
    let store = Store::new(
        acyclic_objects::LocalMedium::open(root, LocalObjectsLimits::default(), None)
            .await
            .map_err(|error| error.to_string())?,
    );
    let first = store.account(b"first").map_err(|error| error.to_string())?;
    let second = store
        .account(b"second")
        .map_err(|error| error.to_string())?;
    acyclic_objects::conformance::verify(&first, &second, "conformance")
        .await
        .map_err(|error| error.to_string())
}

async fn qualify_filesystem_local(root: PathBuf) -> Result<(), String> {
    let provider = Fs::local(LocalOptions::new(root))
        .await
        .map_err(|error| error.to_string())?;
    acyclic_conformance::filesystem_smoke(&provider).await
}

async fn qualify_stream_objects(
    root: &Path,
    started: Instant,
    budget: Duration,
    seed: u64,
) -> Vec<Case> {
    let mut cases = Vec::new();
    cases.push(
        run_case(
            "stream/local",
            started,
            budget,
            Box::pin(qualify_stream_local(root.join("stream"))),
        )
        .await,
    );
    cases.push(
        run_case(
            "objects/local",
            started,
            budget,
            Box::pin(qualify_objects_local(root.join("objects"))),
        )
        .await,
    );
    let sparse = root.join("objects-sparse");
    cases.push(
        run_case(
            "objects/sparse-first-put",
            started,
            budget,
            objects_sparse_first_put(&sparse),
        )
        .await,
    );
    if cases.last().is_some_and(|case| case.passed) {
        cases.push(
            run_case(
                "objects/sparse-reopen-gc",
                started,
                budget,
                objects_sparse_reopen_gc(&sparse),
            )
            .await,
        );
    }
    cases.push(
        run_case(
            "stream/model-recovery",
            started,
            budget,
            Box::pin(stream_history(&root.join("stream-history"), seed)),
        )
        .await,
    );
    cases.push(
        run_case(
            "objects/model-recovery",
            started,
            budget,
            Box::pin(objects_history(&root.join("objects-history"), seed)),
        )
        .await,
    );
    let chunks = root.join("objects-many-chunks");
    cases.push(
        run_case(
            "objects/reopen-many-chunks/setup",
            started,
            budget,
            objects_many_chunks_setup(&chunks),
        )
        .await,
    );
    if cases.last().is_some_and(|case| case.passed) {
        cases.push(
            run_case(
                "objects/reopen-many-chunks/startup",
                started,
                budget,
                objects_many_chunks_startup(&chunks),
            )
            .await,
        );
    }
    cases
}

async fn qualify_filesystem_cases(
    root: &Path,
    started: Instant,
    budget: Duration,
    seed: u64,
) -> Vec<Case> {
    vec![
        run_case(
            "filesystem/local-smoke",
            started,
            budget,
            Box::pin(qualify_filesystem_local(root.join("filesystem"))),
        )
        .await,
        run_case(
            "filesystem/model-recovery",
            started,
            budget,
            Box::pin(filesystem_history(root.join("filesystem-history"), seed)),
        )
        .await,
        run_case(
            "filesystem/demand-local",
            started,
            budget,
            Box::pin(filesystem_demand(root.join("filesystem-demand"))),
        )
        .await,
    ]
}

async fn qualify_native_case(root: &Path, started: Instant, budget: Duration) -> Case {
    let capability = probe_native_mount();
    if capability.available() && capability.kind != Some(NativeMountKind::MacOsNfs) {
        let mut case = run_case(
            "filesystem/sqlite-wal-mount",
            started,
            budget,
            filesystem_sqlite_wal(root.join("sqlite-wal")),
        )
        .await;
        case.supported = Some(true);
        case
    } else {
        Case {
            name: "filesystem/sqlite-wal-mount",
            elapsed_ms: 0,
            passed: true,
            error: None,
            supported: Some(false),
            unsupported_reason: Some(if capability.kind == Some(NativeMountKind::MacOsNfs) {
                "SQLite WAL declined on the macOS loopback NFS mount; the shared-memory WAL consumer is unsupported".into()
            } else {
                capability
                    .unavailable_reason
                    .unwrap_or("native mount is unavailable or read-only on this host")
                    .to_owned()
            }),
        }
    }
}

#[path = "qualify/stream_cases.rs"]
mod stream_cases;
use stream_cases::*;

#[path = "qualify/object_cases.rs"]
mod object_cases;
use object_cases::*;

#[path = "qualify/filesystem_cases.rs"]
mod filesystem_cases;
use filesystem_cases::*;

fn main() {
    if let Some(argument) = std::env::args().nth(1) {
        if tools::is_command(&argument) {
            tools::run();
            return;
        }
        if !argument.starts_with("--") {
            eprintln!("unknown qualification command: {argument}");
            std::process::exit(2);
        }
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cannot start qualification runtime: {error}");
            std::process::exit(2);
        }
    };
    qualify_main(&runtime);
}

#[allow(
    clippy::too_many_lines,
    reason = "one linear bounded qualification sequence"
)]
fn qualify_main(runtime: &tokio::runtime::Runtime) {
    let mut arguments = std::env::args_os();
    let child_mode = arguments.nth(1);
    if child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-wal-child"))
        || child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-verify-child"))
    {
        let result = arguments
            .next()
            .ok_or_else(|| "missing SQLite child database path".to_owned())
            .and_then(|path| {
                if child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-wal-child")) {
                    sqlite_wal_child(Path::new(&path))
                } else {
                    sqlite_verify_child(Path::new(&path))
                }
            });
        if let Err(error) = result {
            eprintln!("{error}");
            std::process::exit(1);
        }
        return;
    }
    let budget_seconds = std::env::args()
        .skip(1)
        .find_map(|argument| argument.strip_prefix("--max-seconds=").map(str::to_owned))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);
    let budget = Duration::from_secs(budget_seconds);
    let seed = std::env::var("ACYCLIC_QUAL_SEED").unwrap_or_else(|_| "0".into());
    let numeric_seed = seed.parse::<u64>().unwrap_or(0);
    let started = Instant::now();
    let mut cases = Vec::new();

    let root = match tempfile::tempdir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("cannot create qualification root: {error}");
            std::process::exit(2);
        }
    };
    cases.extend(runtime.block_on(qualify_stream_objects(
        root.path(),
        started,
        budget,
        numeric_seed,
    )));
    cases.extend(runtime.block_on(qualify_filesystem_cases(
        root.path(),
        started,
        budget,
        numeric_seed,
    )));
    cases.push(runtime.block_on(Box::pin(qualify_native_case(root.path(), started, budget))));
    let report = Report {
        schema: 1,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        seed,
        budget_seconds,
        elapsed_ms: started.elapsed().as_millis(),
        passed: cases.iter().all(|case| case.passed),
        cases,
    };
    match serde_json::to_string(&report) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("cannot encode qualification report: {error}");
            std::process::exit(2);
        }
    }
    if !report.passed {
        std::process::exit(1);
    }
}
