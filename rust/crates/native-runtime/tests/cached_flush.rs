//! Original cached lease custody through the deployed library entry points.
#![cfg(all(not(loom), any(windows, target_os = "linux", target_vendor = "apple")))]

use acyclic_native_runtime::{self as native, CachedFile};
use native::obs::{DurabilitySyscall, Metrics, Operations, Sample, Sink};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Observe each mandatory file stage, allowing the production coalescer to
/// share a device drain with another process. Never force a private registry.
#[derive(Clone)]
struct Stages(Metrics);

impl Sink for Stages {
    fn record(&self, sample: &Sample<'_>) {
        if sample.durability.is_some_and(|attempt| {
            attempt.syscall != Some(DurabilitySyscall::FullFsync)
                || sample.outcome != native::obs::Outcome::Ok
        }) {
            self.0.record(sample);
        }
    }
}

fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

const CONTROL: &str = "public_leases_keep_unfinished_flushes_until_physical_close";
const DRAIN_CHILD: &str = "ACYCLIC_NATIVE_CACHED_FLUSH_CHILD";
const COMPLETED: &str = "ORIGINAL_CACHED_FLUSH_COMPLETED";

/// A child acknowledges only after its original native operation succeeds.
/// Missing execution, refusal or process loss cannot supply that completion.
fn invoke(command: &mut native::ProcessCommand) -> io::Result<()> {
    let output =
        native::process_output(command, native::CaptureSchedule::Unbounded, 8 * 1024 * 1024)?;
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains(COMPLETED),
        "original native cache child did not complete:\n{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// The public lease's original descriptor owns unflushed writes through every
/// callback exit. Only a successful covering flush can omit its closing flush.
#[test]
fn public_leases_keep_unfinished_flushes_until_physical_close() -> io::Result<()> {
    use tracing_subscriber::layer::SubscriberExt as _;

    if let Some(path) = std::env::var_os(DRAIN_CHILD) {
        let path = Path::new(&path);
        let cached = CachedFile::new(create(path)?, path, true, true)?;
        cached.lease()?.flush(|file| file.flush_data())?;
        println!("{COMPLETED}");
        return Ok(());
    }

    // Configure the real cache before its first admission in a fresh process.
    // The native process completion event owns this wait.
    if std::env::var_os(native::CACHED_DESCRIPTORS_VARIABLE).as_deref()
        != Some(std::ffi::OsStr::new("1"))
    {
        return invoke(
            native::ProcessCommand::new(std::env::current_exe()?)
                .args(["--exact", CONTROL, "--nocapture"])
                .env(native::CACHED_DESCRIPTORS_VARIABLE, "1"),
        );
    }
    assert_eq!(native::cached_descriptor_capacity(), 1);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Exit {
        Error,
        Unwind,
        NoFlush,
        ForeignFlush,
        Barrier,
        Success,
        WriteAfterFlush,
        WriteAcrossFlush,
        ReopenedBarrier,
        ProcessCoveredBarrier,
        CoveredBarrier,
        WriteAfterCoveredBarrier,
        WriteAcrossBarrier,
    }
    let directory = tempfile::tempdir()?;
    let metrics = Metrics::default();
    let attempts = metrics.query("acyclic.runtime.durability");
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(Stages(metrics))));
    let full_attempts = 1;
    tracing::dispatcher::with_default(&dispatch, || -> io::Result<()> {
        for exit in [
            Exit::Error,
            Exit::Unwind,
            Exit::NoFlush,
            Exit::ForeignFlush,
            Exit::Barrier,
            Exit::Success,
            Exit::WriteAfterFlush,
            Exit::WriteAcrossFlush,
            Exit::ReopenedBarrier,
            Exit::ProcessCoveredBarrier,
            Exit::CoveredBarrier,
            Exit::WriteAfterCoveredBarrier,
            Exit::WriteAcrossBarrier,
        ] {
            let path = directory.path().join(format!("{exit:?}"));
            let cached = CachedFile::new(create(&path)?, &path, true, true)?;
            let lease = if exit == Exit::ReopenedBarrier {
                cached.lease()?.flush(|file| file.flush_data())?;
                // The original entry survives a physical eviction. Pin the
                // other file so its descriptor cannot be the eviction victim.
                let evictor_path = directory.path().join("evictor");
                let evictor = CachedFile::new(create(&evictor_path)?, &evictor_path, true, true)?;
                let pinned = evictor.lease()?;
                let mut reopens = 0;
                let lease = cached.lease_charged(&mut |cost| {
                    if cost == native::DescriptorCost::Reopen {
                        reopens += 1;
                    }
                    Ok(())
                })?;
                assert_eq!(
                    reopens, 1,
                    "original public entry did not physically reopen"
                );
                drop(pinned);
                drop(evictor);
                lease
            } else {
                cached.lease()?
            };
            // Resolve through the original public full-flush entry point.
            // A barrier itself must never classify the descriptor.
            let covering = matches!(
                exit,
                Exit::CoveredBarrier
                    | Exit::ProcessCoveredBarrier
                    | Exit::WriteAfterCoveredBarrier
                    | Exit::WriteAcrossBarrier
            );
            if covering {
                lease.flush(|file| file.flush_data())?;
            }
            lease.write(|file| native::write_all_at(file, 0, b"original", None))?;
            let before = attempts.snapshot();
            let mut barrier_completed = None;
            match exit {
                Exit::Error => {
                    lease
                        .flush(|_| Err::<(), _>(io::Error::other("original flush refused")))
                        .expect_err("failed callback must retain its error");
                }
                Exit::Unwind => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        lease.flush(|_| -> io::Result<()> {
                            std::panic::panic_any("original flush unwound");
                        })
                    }));
                    assert!(result.is_err(), "flush swallowed the original unwind");
                }
                Exit::NoFlush => lease.flush(|_| Ok(()))?,
                Exit::ForeignFlush => lease.flush(|_| {
                    let foreign_path = directory.path().join("foreign");
                    let foreign =
                        CachedFile::new(create(&foreign_path)?, &foreign_path, true, true)?;
                    foreign.lease()?.flush(|file| file.flush_data())
                })?,
                Exit::Barrier => {
                    barrier_completed =
                        Some(lease.flush(|file| file.sync_data(native::Durability::Barrier))?);
                    #[cfg(not(target_vendor = "apple"))]
                    assert_eq!(
                        barrier_completed,
                        Some(native::Durability::Full),
                        "native full barrier fallback lost its completed guarantee"
                    );
                }
                Exit::Success => lease.flush(|file| file.flush_data())?,
                Exit::WriteAfterFlush => lease.flush(|file| {
                    file.flush_data()?;
                    lease.write(|file| native::write_all_at(file, 0, b"new-write", None))
                })?,
                Exit::WriteAcrossFlush => lease.write(|file| {
                    // The callback holds the original descriptor throughout.
                    // Its physical write completes after the intervening flush.
                    lease.flush(|file| file.flush_data())?;
                    native::write_all_at(file, 0, b"later-write", None)
                })?,
                Exit::ReopenedBarrier
                | Exit::ProcessCoveredBarrier
                | Exit::CoveredBarrier
                | Exit::WriteAfterCoveredBarrier
                | Exit::WriteAcrossBarrier => {
                    lease.flush(|file| {
                        barrier_completed = Some(file.sync_data(native::Durability::Barrier)?);
                        // A write completed inside the same callback must
                        // override the ordered cut when that callback exits.
                        if exit == Exit::WriteAcrossBarrier {
                            lease.write(|file| {
                                native::write_all_at(file, 0, b"later-write", None)
                            })?;
                        }
                        Ok(())
                    })?;
                    let foreign_path = directory.path().join("covering");
                    if exit == Exit::ProcessCoveredBarrier {
                        let own = tempfile::tempdir()?;
                        invoke(
                            native::ProcessCommand::new(std::env::current_exe()?)
                                .args(["--exact", CONTROL, "--nocapture"])
                                .env(DRAIN_CHILD, &foreign_path)
                                .env("TMPDIR", own.path()),
                        )?;
                    } else {
                        let foreign =
                            CachedFile::new(create(&foreign_path)?, &foreign_path, true, true)?;
                        foreign.lease()?.flush(|file| file.flush_data())?;
                    }
                    if exit == Exit::WriteAfterCoveredBarrier {
                        lease.write(|file| native::write_all_at(file, 0, b"new-write", None))?;
                    }
                }
            }
            let before_close = attempts.snapshot();
            let completed = matches!(
                exit,
                Exit::ForeignFlush | Exit::Success | Exit::WriteAfterFlush | Exit::WriteAcrossFlush
            );
            assert_eq!(
                before_close.ok - before.ok,
                if covering || exit == Exit::ReopenedBarrier {
                    u64::from(exit != Exit::ProcessCoveredBarrier) * full_attempts
                        + if barrier_completed == Some(native::Durability::Full) {
                            full_attempts
                        } else {
                            1
                        }
                } else if completed || barrier_completed == Some(native::Durability::Full) {
                    full_attempts
                } else if barrier_completed.is_some() {
                    1
                } else {
                    0
                },
                "callback changed the original native flush count: {exit:?}"
            );
            drop(cached);
            drop(lease);
            let after_close = attempts.snapshot();
            assert_eq!(
                after_close.ok - before_close.ok,
                if exit == Exit::Success
                    || matches!(exit, Exit::CoveredBarrier | Exit::ProcessCoveredBarrier)
                    || (barrier_completed == Some(native::Durability::Full)
                        && !matches!(
                            exit,
                            Exit::WriteAfterCoveredBarrier | Exit::WriteAcrossBarrier
                        ))
                {
                    0
                } else {
                    full_attempts
                },
                "unfinished original flush lost its physical closing flush: {exit:?}"
            );
            assert_eq!(after_close.err, before_close.err);
            assert_eq!(after_close.incomplete, before_close.incomplete);
        }
        Ok(())
    })?;
    println!("{COMPLETED}");
    Ok(())
}
