//! The production descriptor cache under every interleaving of use, eviction
//! and close, on simulated files whose flushes can fail.

use super::cache::{Access, Cache, Entry, Files};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct World {
    /// The file identity each name holds.
    names: HashMap<PathBuf, u64>,
    identities: u64,
    descriptors: u64,
    open: usize,
    /// Writes made through each open descriptor and not yet flushed.
    unflushed: HashMap<u64, u64>,
    /// Writes whose descriptor closed without a flush.
    dropped: u64,
    /// Writes whose flush failed as their descriptor closed.
    failed: u64,
    fail_flushes: bool,
    flushes: u64,
    discarded: u64,
    flushed: u64,
    /// The one descriptor whose next flush fails, when only one does.
    failing: Option<u64>,
}

/// Simulated files. The world lock is never held across a Loom operation,
/// so a model thread never waits on it.
#[derive(Clone, Default)]
struct Sim(Arc<Mutex<World>>);

impl Sim {
    fn world(&self) -> std::sync::MutexGuard<'_, World> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn create(&self, path: &str) -> SimFile {
        let mut world = self.world();
        world.identities += 1;
        let identity = world.identities;
        world.names.insert(PathBuf::from(path), identity);
        drop(world);
        self.descriptor(identity)
    }

    fn descriptor(&self, identity: u64) -> SimFile {
        let mut world = self.world();
        world.descriptors += 1;
        world.open += 1;
        let id = world.descriptors;
        world.unflushed.insert(id, 0);
        SimFile {
            id,
            identity,
            world: self.clone(),
        }
    }
}

struct SimFile {
    id: u64,
    identity: u64,
    world: Sim,
}

impl SimFile {
    fn write(&self) {
        *self.world.world().unflushed.entry(self.id).or_default() += 1;
    }

    fn discard(&self) {
        let mut world = self.world.world();
        world.discarded += world.unflushed.insert(self.id, 0).unwrap_or_default();
    }
}

impl Drop for SimFile {
    fn drop(&mut self) {
        let mut world = self.world.world();
        world.open -= 1;
        let unflushed = world.unflushed.remove(&self.id).unwrap_or_default();
        world.dropped += unflushed;
    }
}

impl Files for Sim {
    type File = SimFile;
    type Identity = u64;

    fn reopen(&self, path: &Path, _: Access) -> io::Result<SimFile> {
        let identity = self
            .world()
            .names
            .get(path)
            .copied()
            .ok_or(io::ErrorKind::NotFound)?;
        Ok(self.descriptor(identity))
    }

    fn identity(&self, file: &SimFile) -> io::Result<u64> {
        Ok(file.identity)
    }

    fn flush(&self, file: &SimFile) -> io::Result<()> {
        let mut world = self.world();
        world.flushes += 1;
        let unflushed = world.unflushed.insert(file.id, 0).unwrap_or_default();
        // A failing descriptor reports its failure once, as a Linux
        // descriptor does; a later flush of it succeeds.
        let fails_once = world.failing == Some(file.id);
        if fails_once {
            world.failing = None;
        }
        if world.fail_flushes || fails_once {
            world.failed += unflushed;
            return Err(io::ErrorKind::StorageFull.into());
        }
        world.flushed += unflushed;
        Ok(())
    }
}

type Findings = Arc<Mutex<Option<String>>>;

/// Runs `model` under every interleaving. A violated property is reported
/// after the run: a model thread that panics cannot unwind through Loom.
fn explore(model: impl Fn() -> Result<(), String> + Send + Sync + 'static) {
    let findings = Findings::default();
    let reported = Arc::clone(&findings);
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        if let Err(finding) = model() {
            reported
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(finding);
        }
    });
    let finding = findings
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    assert_eq!(finding, None, "a model run violated a property");
}

fn failed(error: impl std::fmt::Debug) -> String {
    format!("model step failed: {error:?}")
}

fn check(holds: bool, property: &str) -> Result<(), String> {
    if holds {
        Ok(())
    } else {
        Err(property.to_owned())
    }
}

fn cache(sim: &Sim, capacity: usize) -> Arc<Cache<Sim>> {
    Arc::new(Cache::new(sim.clone(), capacity))
}

fn entry(cache: &Arc<Cache<Sim>>, sim: &Sim, path: &str) -> Result<Arc<Entry<Sim>>, String> {
    let file = sim.create(path);
    Entry::new(
        cache,
        file,
        Path::new(path),
        Access::Write,
        true,
        &mut |_| Ok(()),
    )
    .map_err(failed)
}

/// Writes once through `entry`, as a store appends.
fn append(entry: &Arc<Entry<Sim>>) -> io::Result<()> {
    entry.lease(&mut |_| Ok(()))?.write(|file| {
        file.write();
        Ok(())
    })
}

fn join<T>(thread: loom::thread::JoinHandle<T>) -> Result<T, String> {
    thread
        .join()
        .map_err(|_| "a model thread panicked".to_owned())
}

/// Two files' leases race at capacity 1. An eviction skips a file in use,
/// so whichever finds the other leased leaves the cache over capacity, and
/// that lease's release must bring it back.
#[test]
fn released_leases_return_the_cache_to_capacity() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let first = entry(&cache, &sim, "/a")?;
        let second = entry(&cache, &sim, "/b")?;
        let racing = {
            let first = Arc::clone(&first);
            loom::thread::spawn(move || first.lease(&mut |_| Ok(())).map(drop))
        };
        second.lease(&mut |_| Ok(())).map(drop).map_err(failed)?;
        join(racing)?.map_err(failed)?;
        check(
            cache.cached() <= 1,
            "released leases left the cache over capacity",
        )
    });
}

/// A write and an eviction of its descriptor race: the descriptor flushes
/// the write before it closes, whichever side closes it.
#[test]
fn a_write_racing_eviction_is_flushed_before_its_descriptor_closes() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let written = entry(&cache, &sim, "/a")?;
        let other = entry(&cache, &sim, "/b")?;
        let writer = {
            let written = Arc::clone(&written);
            loom::thread::spawn(move || append(&written))
        };
        let evicting = loom::thread::spawn(move || other.lease(&mut |_| Ok(())).map(drop));
        join(writer)?.map_err(failed)?;
        join(evicting)?.map_err(failed)?;
        drop(written);
        drop(cache);
        check(
            sim.world().dropped == 0,
            "a write closed with its descriptor unflushed",
        )
    });
}

/// The flush a closing descriptor makes fails: the file's next use reports
/// it, since a descriptor opened later would never see that failure.
#[test]
fn a_flush_that_fails_as_a_descriptor_closes_fails_the_next_use() {
    explore(|| {
        let sim = Sim::default();
        sim.world().fail_flushes = true;
        let cache = cache(&sim, 1);
        let written = entry(&cache, &sim, "/a")?;
        let other = entry(&cache, &sim, "/b")?;
        let writer = {
            let written = Arc::clone(&written);
            loom::thread::spawn(move || append(&written))
        };
        let evicting = loom::thread::spawn(move || other.lease(&mut |_| Ok(())).map(drop));
        join(writer)?.map_err(failed)?;
        let _ = join(evicting)?;
        let failed = sim.world().failed;
        let next = written.lease(&mut |_| Ok(()));
        check(
            failed == 0 || next.is_err(),
            "a write lost as its descriptor closed was never reported",
        )?;
        check(sim.world().dropped == 0, "a write closed unflushed")
    });
}

/// Files are used concurrently: idle descriptors return within the
/// capacity at the next admission.
#[test]
fn idle_descriptors_stay_within_capacity() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let mut files = Vec::new();
        for path in ["/a", "/b", "/c"] {
            files.push(entry(&cache, &sim, path)?);
        }
        let threads: Vec<_> = files
            .iter()
            .take(2)
            .map(|file| {
                let file = Arc::clone(file);
                loom::thread::spawn(move || append(&file))
            })
            .collect();
        for thread in threads {
            join(thread)?.map_err(failed)?;
        }
        // A file in use when another was admitted may be spared.
        check(
            sim.world().open <= 1 + 2,
            "descriptors exceed capacity and use",
        )?;
        let last = entry(&cache, &sim, "/d")?;
        check(
            sim.world().open <= 1,
            "idle descriptors exceed the capacity",
        )?;
        drop((files, last));
        check(sim.world().dropped == 0, "a write closed unflushed")
    });
}

/// Another file takes the name of a closed one: the reopen refuses it.
#[test]
fn a_file_replaced_while_closed_is_refused() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let replaced = entry(&cache, &sim, "/a")?;
        let other = entry(&cache, &sim, "/b")?;
        other.lease(&mut |_| Ok(())).map_err(failed)?;
        drop(sim.create("/a"));
        check(
            replaced.lease(&mut |_| Ok(())).is_err(),
            "another file under the cached name was used",
        )
    });
}

/// An idle descriptor's closing flush fails while the file is used again:
/// a flush through any descriptor of the file that reports success came
/// after that failure was kept, so it reports the failure instead.
#[test]
fn a_flush_never_succeeds_past_a_failed_close() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let written = entry(&cache, &sim, "/a")?;
        sim.world().failing = Some(1);
        append(&written).map_err(failed)?;
        let evicting = {
            let cache = Arc::clone(&cache);
            let sim = sim.clone();
            loom::thread::spawn(move || entry(&cache, &sim, "/b").map(drop))
        };
        let flushed = (|| {
            let lease = written.lease(&mut |_| Ok(()))?;
            lease.write(|file| {
                file.write();
                Ok(())
            })?;
            lease.flush(|file| sim.flush(file))
        })();
        join(evicting)??;
        check(
            flushed.is_err() || sim.world().failed == 0,
            "a flush succeeded though a write was lost as a descriptor closed",
        )
    });
}

/// The replacement is already durable. Discarding its old log retires only
/// those bytes; the former write-and-truncate path still incurs a close flush.
#[test]
fn an_exclusive_discard_omits_only_the_obsolete_log_flush() {
    for former_write in [false, true] {
        explore(move || {
            let sim = Sim::default();
            let cache = cache(&sim, 1);
            let log = entry(&cache, &sim, "/log")?;
            let mut lease = log.lease(&mut |_| Ok(())).map_err(failed)?;
            lease
                .write(|file| {
                    file.write();
                    Ok(())
                })
                .map_err(failed)?;
            if former_write {
                lease
                    .write(|file| {
                        file.discard();
                        Ok(())
                    })
                    .map_err(failed)?;
            } else {
                lease
                    .discard(|file| {
                        file.discard();
                        Ok(())
                    })
                    .map_err(failed)?;
            }
            drop(lease);
            drop(log);
            drop(cache);
            let world = sim.world();
            check(
                world.discarded == 1 && world.dropped == 0,
                "discard lost a live write",
            )?;
            check(
                world.flushes == u64::from(former_write),
                "discard retained the obsolete close flush",
            )
        });
    }
}

/// A competing lease refuses without changing its live writes.
#[test]
fn a_refused_discard_preserves_live_writes() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let log = entry(&cache, &sim, "/log")?;
        let mut lease = log.lease(&mut |_| Ok(())).map_err(failed)?;
        lease
            .write(|file| {
                file.write();
                Ok(())
            })
            .map_err(failed)?;
        let writer = log.lease(&mut |_| Ok(())).map_err(failed)?;
        let refused = lease.discard(|file| {
            file.discard();
            Ok(())
        });
        check(
            refused.map_err(|error| error.kind()) == Err(io::ErrorKind::WouldBlock),
            "discard accepted a competing lease",
        )?;
        check(sim.world().discarded == 0, "refused discard changed bytes")?;
        drop(writer);
        drop(lease);
        drop(log);
        drop(cache);
        let world = sim.world();
        check(
            world.flushed == 1 && world.dropped == 0,
            "refused discard lost the live write",
        )
    });
}

/// Starting clean, a partial failed discard must mark dirty before its
/// callback and preserve that marker until the actual descriptor closes.
#[test]
fn a_failed_discard_keeps_its_close_flush_from_a_clean_descriptor() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let log = entry(&cache, &sim, "/log")?;
        let mut lease = log.lease(&mut |_| Ok(())).map_err(failed)?;
        check(sim.world().flushes == 0, "clean descriptor already flushed")?;
        let refused = lease.discard(|file| {
            file.write();
            Err(io::ErrorKind::StorageFull.into())
        });
        check(
            refused.map_err(|error| error.kind()) == Err(io::ErrorKind::StorageFull),
            "discard hid the truncation failure",
        )?;
        drop(lease);
        drop(log);
        drop(cache);
        let world = sim.world();
        check(
            world.flushes == 1 && world.flushed == 1 && world.dropped == 0,
            "failed discard cleared or never set its dirty marker",
        )
    });
}
/// The same production lease gate protects every ordering of a later append
/// and retirement. Every non-discarded write is flushed before close.
#[test]
fn a_discard_racing_an_append_never_clears_the_append_dirty_marker() {
    explore(|| {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let log = entry(&cache, &sim, "/log")?;
        append(&log).map_err(failed)?;
        let mut lease = log.lease(&mut |_| Ok(())).map_err(failed)?;
        let writer = {
            let log = Arc::clone(&log);
            loom::thread::spawn(move || append(&log))
        };
        if let Err(error) = lease.discard(|file| {
            file.discard();
            Ok(())
        }) {
            check(
                error.kind() == io::ErrorKind::WouldBlock,
                "a racing discard changed its refusal",
            )?;
        }
        drop(lease);
        join(writer)?.map_err(failed)?;
        drop(log);
        drop(cache);
        let world = sim.world();
        check(
            world.dropped == 0 && world.discarded + world.flushed == 2,
            "discard cleared the dirty marker of a live append",
        )
    });
}

/// A real eviction can retain a descriptor while its lease is reacquired.
/// Once that close loses a write, discard must preserve its original error.
#[test]
fn a_discard_refuses_the_original_loss_after_an_eviction_race() {
    let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = Arc::clone(&reached);
    explore(move || {
        let sim = Sim::default();
        let cache = cache(&sim, 1);
        let log = entry(&cache, &sim, "/log")?;
        append(&log).map_err(failed)?;
        sim.world().failing = Some(1);
        let evicting = {
            let cache = Arc::clone(&cache);
            let sim = sim.clone();
            loom::thread::spawn(move || entry(&cache, &sim, "/other").map(drop))
        };
        let acquired = log.lease(&mut |_| Ok(()));
        join(evicting)??;
        match acquired {
            Ok(mut lease) if sim.world().failed != 0 => {
                observed.store(true, std::sync::atomic::Ordering::Relaxed);
                let original = log
                    .lease(&mut |_| Ok(()))
                    .err()
                    .ok_or_else(|| "lease forgot the original close failure".to_owned())?;
                let mut called = false;
                let refused = lease
                    .discard(|file| {
                        called = true;
                        file.discard();
                        Ok(())
                    })
                    .err()
                    .ok_or_else(|| "discard erased a retained close failure".to_owned())?;
                check(
                    !called
                        && refused.kind() == original.kind()
                        && refused.to_string() == original.to_string()
                        && refused.kind() == io::ErrorKind::StorageFull,
                    "discard replaced the retained error or changed bytes",
                )?;
                check(
                    sim.world().discarded == 0,
                    "discard callback changed bytes after a retained loss",
                )
            }
            Ok(_) => Ok(()),
            Err(error) => check(
                error.kind() == io::ErrorKind::StorageFull,
                "eviction changed the original close failure",
            ),
        }
    });
    assert!(
        reached.load(std::sync::atomic::Ordering::Relaxed),
        "no real eviction loss reached an already acquired discard lease"
    );
}
