//! Public Filesystem consumer fixtures and diagnostics.
//!
//! Subcommands:
//!   fixture <dir> [--with-fifo]   build a small fixture repo exercising the tricky cases
//!   roundtrip <src> <work>        capture <src> into a fresh store under <work>, commit,
//!                                 materialize into <work>/restore, compare content+mode
//!
//! Exit code 0 = round-trip verified identical; 1 = mismatches or engine failure.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Instant;

use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector, Lifecycle,
    MutationMode, VolumeConfig,
};
use acyclic_fs::{
    CancellationToken, GenerationId, LivePublicationObservation, LocalFs, LocalOptions,
    OperationId, VolumeId, WorkCounters,
};
use acyclic_fs::{
    CaptureOptions, MaterializeOptions, capture_baseline, capture_root_identity,
    materialize_checkout,
};
use acyclic_objects::LocalDurability as ObjectsDurability;
use acyclic_stream::LocalDurability as StreamDurability;

pub(super) fn is_command(command: &str) -> bool {
    matches!(
        command,
        "fixture"
            | "roundtrip"
            | "corpus"
            | "bench"
            | "restore-gen"
            | "mount-smoke"
            | "mount-smoke2"
            | "mount-hold"
            | "source-probe"
            | "mount-bench"
            | "mount-bench-writer"
            | "kernel-bench"
            | "par-bench"
            | "mount-run"
    )
}

pub(super) fn run() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("fixture") => fixture(&args[1..]),
        Some("roundtrip") => roundtrip(&args[1..]),
        Some("corpus") => corpus(&args[1..]),
        Some("bench") => bench(&args[1..]),
        Some("restore-gen") => restore_gen(&args[1..]),
        Some("mount-smoke") => mount_smoke(&args[1..]),
        Some("mount-smoke2") => mount_smoke2(&args[1..]),
        Some("mount-hold") => mount_hold(&args[1..]),
        Some("source-probe") => source_probe(&args[1..]),
        Some("mount-bench") => mount_bench(&args[1..]),
        Some("mount-bench-writer") => mount_bench_writer(&args[1..]),
        Some("kernel-bench") => kernel_bench(&args[1..]),
        Some("par-bench") => par_bench(&args[1..]),
        Some("mount-run") => mount_run(&args[1..]),
        _ => Err(
            "usage: qualify fixture <dir> [--with-fifo] | roundtrip <src> <work> \
             | corpus <dir> <files> <mb> | bench <src> <work> [rounds] \
             | mount-bench <work> [files] [file-bytes] [threads] \
             | kernel-bench <work> [files] [file-bytes] \
             | par-bench <work> [writers] [files] [lazy|workspace]              | mount-run <src> <work> <runs> <program> [args...]"
                .into(),
        ),
    };
    if let Err(message) = result {
        eprintln!("FAIL: {message}");
        std::process::exit(1);
    }
}

type Failure = Box<dyn std::error::Error + Send + Sync>;

fn local_options(root: impl Into<PathBuf>) -> Result<LocalOptions, Failure> {
    let mut options = LocalOptions::new(root);
    match std::env::var("QUAL_DURABILITY").as_deref() {
        Err(std::env::VarError::NotPresent) if cfg!(target_vendor = "apple") => {
            options.stream.durability = StreamDurability::Barrier;
            options.objects.durability = ObjectsDurability::Barrier;
        }
        Err(std::env::VarError::NotPresent) | Ok("full-flush") => {
            options.stream.durability = StreamDurability::FullFlush;
            options.objects.durability = ObjectsDurability::FullFlush;
        }
        Ok("barrier") => {
            options.stream.durability = StreamDurability::Barrier;
            options.objects.durability = ObjectsDurability::Barrier;
        }
        _ => return Err("QUAL_DURABILITY must be barrier or full-flush".into()),
    }
    Ok(options)
}

fn engine_err<E: std::fmt::Debug>(context: &str) -> impl FnOnce(E) -> Failure + '_ {
    move |error| format!("{context}: {error:?}").into()
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

mod fixtures;
use fixtures::*;

mod capture;
use capture::*;

mod mounting;
use mounting::*;

mod benchmark;
mod benchmark_access;
use benchmark::*;

mod kernel_bench;
use kernel_bench::*;
