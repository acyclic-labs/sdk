//! The production descriptor cache under every interleaving of use, eviction
//! and close, on simulated files whose flushes can fail.

use super::cache::{Cache, Entry, Files};
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

    fn reopen(&self, path: &Path, _: bool) -> io::Result<SimFile> {
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
    Entry::new(cache, file, Path::new(path), true, true, &mut |_| Ok(())).map_err(failed)
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
