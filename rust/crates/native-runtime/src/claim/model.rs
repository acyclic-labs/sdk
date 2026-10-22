//! The production claim protocol under every interleaving of acquire, release
//! and process death, on a simulated machine whose processes, files, locks and
//! probes can fail as real ones do.

use super::protocol::{self, Claim, Host, Liveness, Table};
use super::record::Process;
use crate::sync::Mutex;
use loom::sync::Arc;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// Process lock paths that a newline-separated record could not tell apart
/// from more fields.
const LOCKS: [&[u8]; 3] = [
    b"/run/a\nacyclic-claim-v1\n1\n\n/0.lock",
    b"/run/a\nacyclic-claim-v1\n1\n\n/1.lock",
    b"/run/\n\n/2.lock",
];

#[derive(Default)]
struct Inode {
    bytes: Vec<u8>,
    /// The handle holding the file's lock.
    holder: Option<u64>,
    /// Open handles, which keep the inode from reuse.
    open: usize,
}

#[derive(Default)]
struct World {
    names: HashMap<PathBuf, usize>,
    inodes: Vec<Inode>,
    handles: u64,
    /// The process of each open handle, whose locks its death releases.
    handle_process: HashMap<u64, usize>,
    alive: Vec<bool>,
    /// Liveness probes answer `Unknown` for every process.
    unproven: bool,
    /// The first violated property, reported after the model run: a model
    /// thread that panics while holding simulated files cannot unwind.
    findings: Findings,
    /// Processes whose reads of claim files fail.
    unreadable: Vec<bool>,
    tokens: u64,
    /// The live processes that believe they own each file.
    owners: HashMap<usize, Vec<usize>>,
}

impl World {
    fn new(processes: usize, findings: &Findings) -> Self {
        Self {
            findings: findings.clone(),
            alive: vec![true; processes],
            unreadable: vec![false; processes],
            ..Self::default()
        }
    }

    fn inode(&mut self, index: usize) -> io::Result<&mut Inode> {
        self.inodes
            .get_mut(index)
            .ok_or_else(|| io::Error::other("no such inode"))
    }
}

type Shared = Arc<Mutex<World>>;

fn enter(world: &Shared) -> loom::sync::MutexGuard<'_, World> {
    world
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One open file of one simulated process. Closing it releases its lock.
struct SimFile {
    world: Shared,
    inode: usize,
    handle: u64,
}

impl Drop for SimFile {
    fn drop(&mut self) {
        let mut world = enter(&self.world);
        world.handle_process.remove(&self.handle);
        if let Ok(inode) = world.inode(self.inode) {
            inode.open = inode.open.saturating_sub(1);
            if inode.holder == Some(self.handle) {
                inode.holder = None;
            }
        }
    }
}

/// One simulated process, as the protocol sees its machine.
#[derive(Clone)]
struct Sim {
    world: Shared,
    pid: usize,
    process: Option<Process>,
    table: Arc<Table<usize>>,
}

impl Sim {
    fn new(world: &Shared, pid: usize) -> Self {
        Self {
            world: world.clone(),
            pid,
            process: Some(Process {
                pid: u32::try_from(pid).unwrap_or(u32::MAX),
                start: b"boot:1".to_vec(),
                lock: LOCKS.get(pid).copied().unwrap_or_default().to_vec(),
                lock_identity: vec![u8::try_from(pid).unwrap_or(u8::MAX)],
            }),
            table: Arc::new(Table::new()),
        }
    }

    /// The world, while this process still runs: a dead process takes no
    /// further step.
    fn running(&self) -> io::Result<loom::sync::MutexGuard<'_, World>> {
        let world = enter(&self.world);
        if world.alive.get(self.pid).copied().unwrap_or(false) {
            Ok(world)
        } else {
            Err(io::Error::other("process died"))
        }
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut world = self.running()?;
        let inode = world
            .names
            .remove(Path::new(from))
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        world.names.insert(PathBuf::from(to), inode);
        Ok(())
    }

    /// Removes `from`, and creates `to` on its freed inode, as a filesystem
    /// that reuses inodes does once nothing holds the old file open.
    fn reuse(&self, from: &str, to: &str) -> io::Result<()> {
        let mut world = self.running()?;
        let index = world
            .names
            .remove(Path::new(from))
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        let inode = world.inode(index)?;
        if inode.open != 0 {
            return Err(io::Error::other("an open inode is never reused"));
        }
        *inode = Inode::default();
        world.names.insert(PathBuf::from(to), index);
        Ok(())
    }

    /// Records that this process owns the file `claim` holds, checking that no
    /// other live process believes it does too.
    fn own(&self, claim: &Claim<Self>) {
        self.own_key(*claim.key());
    }

    fn disown(&self, claim: &Claim<Self>) {
        if let Some(owners) = enter(&self.world).owners.get_mut(claim.key()) {
            owners.retain(|owner| *owner != self.pid);
        }
    }

    /// Claims `path`, holds it across a scheduling point, and releases it.
    fn hold(&self, path: &str) -> bool {
        let Ok(claim) = protocol::acquire(self.clone(), Path::new(path)) else {
            return false;
        };
        self.own(&claim);
        loom::thread::yield_now();
        self.disown(&claim);
        drop(claim);
        true
    }
}

fn kill(world: &Shared, pid: usize) {
    let mut world = enter(world);
    if let Some(alive) = world.alive.get_mut(pid) {
        *alive = false;
    }
    // The OS releases a dead process's locks.
    let handles = world
        .handle_process
        .iter()
        .filter(|(_, process)| **process == pid)
        .map(|(handle, _)| *handle)
        .collect::<Vec<_>>();
    for inode in &mut world.inodes {
        if inode.holder.is_some_and(|holder| handles.contains(&holder)) {
            inode.holder = None;
        }
    }
}

impl Host for Sim {
    type File = SimFile;
    type Key = usize;

    fn open(&self, path: &Path, create: bool) -> io::Result<SimFile> {
        let mut world = self.running()?;
        let inode = match world.names.get(path) {
            Some(inode) => *inode,
            None if create => {
                world.inodes.push(Inode::default());
                let inode = world.inodes.len() - 1;
                world.names.insert(path.to_path_buf(), inode);
                inode
            }
            None => return Err(io::Error::from(io::ErrorKind::NotFound)),
        };
        world.handles += 1;
        let handle = world.handles;
        world.handle_process.insert(handle, self.pid);
        world.inode(inode)?.open += 1;
        Ok(SimFile {
            world: self.world.clone(),
            inode,
            handle,
        })
    }

    fn identity(&self, file: &SimFile, _: &Path) -> io::Result<usize> {
        self.running()?;
        Ok(file.inode)
    }

    fn try_lock(&self, file: &SimFile) -> io::Result<bool> {
        let mut world = self.running()?;
        let inode = world.inode(file.inode)?;
        match inode.holder {
            Some(holder) if holder != file.handle => Ok(false),
            _ => {
                inode.holder = Some(file.handle);
                Ok(true)
            }
        }
    }

    fn lock(&self, file: &SimFile) -> io::Result<()> {
        while !self.try_lock(file)? {
            loom::thread::yield_now();
        }
        Ok(())
    }

    fn unlock(&self, file: &SimFile) -> io::Result<()> {
        let mut world = self.running()?;
        let inode = world.inode(file.inode)?;
        if inode.holder == Some(file.handle) {
            inode.holder = None;
        }
        Ok(())
    }

    fn read(&self, file: &SimFile) -> io::Result<Vec<u8>> {
        let mut world = self.running()?;
        if world.unreadable.get(self.pid).copied().unwrap_or(false) {
            return Err(io::Error::other("read failed"));
        }
        Ok(world.inode(file.inode)?.bytes.clone())
    }

    /// Truncates, then writes in two steps, so a death between them leaves a
    /// torn record.
    fn write(&self, file: &SimFile, bytes: &[u8]) -> io::Result<()> {
        let half = bytes.len() / 2;
        self.running()?.inode(file.inode)?.bytes = bytes.get(..half).unwrap_or_default().to_vec();
        if half < bytes.len() {
            loom::thread::yield_now();
            self.running()?.inode(file.inode)?.bytes = bytes.to_vec();
        }
        Ok(())
    }

    fn process(&self) -> Option<&Process> {
        self.process.as_ref()
    }

    fn established(&self) -> Option<&Process> {
        self.process.as_ref()
    }

    fn liveness(&self, process: &Process, probes: &mut u64) -> Liveness {
        *probes += 1;
        let world = enter(&self.world);
        if world.unproven {
            return Liveness::Unknown;
        }
        let alive = usize::try_from(process.pid)
            .ok()
            .and_then(|pid| world.alive.get(pid).copied());
        match alive {
            Some(true) => Liveness::Alive,
            Some(false) => Liveness::Dead,
            None => Liveness::Unknown,
        }
    }

    fn token(&self) -> u64 {
        let mut world = enter(&self.world);
        world.tokens += 1;
        world.tokens
    }

    fn table(&self) -> &Table<usize> {
        &self.table
    }
}

/// A host that fails open, as the first claim protocol did: an unreadable
/// claim file reads as empty, and a probe that cannot answer reads as dead.
#[derive(Clone)]
struct FailOpen(Sim);

impl Host for FailOpen {
    type File = SimFile;
    type Key = usize;

    fn open(&self, path: &Path, create: bool) -> io::Result<SimFile> {
        self.0.open(path, create)
    }
    fn identity(&self, file: &SimFile, path: &Path) -> io::Result<usize> {
        self.0.identity(file, path)
    }
    fn try_lock(&self, file: &SimFile) -> io::Result<bool> {
        self.0.try_lock(file)
    }
    fn lock(&self, file: &SimFile) -> io::Result<()> {
        self.0.lock(file)
    }
    fn unlock(&self, file: &SimFile) -> io::Result<()> {
        self.0.unlock(file)
    }
    fn read(&self, file: &SimFile) -> io::Result<Vec<u8>> {
        Ok(self.0.read(file).unwrap_or_default())
    }
    fn write(&self, file: &SimFile, bytes: &[u8]) -> io::Result<()> {
        self.0.write(file, bytes)
    }
    fn process(&self) -> Option<&Process> {
        self.0.process()
    }
    fn established(&self) -> Option<&Process> {
        self.0.established()
    }
    fn liveness(&self, process: &Process, probes: &mut u64) -> Liveness {
        match self.0.liveness(process, probes) {
            Liveness::Unknown => Liveness::Dead,
            known => known,
        }
    }
    fn token(&self) -> u64 {
        self.0.token()
    }
    fn table(&self) -> &Table<usize> {
        self.0.table()
    }
}

type Findings = std::sync::Arc<std::sync::Mutex<Option<String>>>;

fn report(findings: &Findings, finding: String) {
    findings
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get_or_insert(finding);
}

/// Runs `model` under every interleaving within the bound, then fails with
/// the first property any of them violated.
/// A model step that failed outright, as a finding.
fn failed(error: impl std::fmt::Debug) -> String {
    format!("model step failed: {error:?}")
}

fn explore(model: impl Fn(&Findings) -> Result<(), String> + Send + Sync + 'static) {
    let findings = Findings::default();
    let reported = findings.clone();
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        if let Err(finding) = model(&reported) {
            report(&reported, finding);
        }
    });
    let finding = findings
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    assert_eq!(finding, None, "a model run violated a property");
}

/// Two processes race for one store while a third kills the first at any
/// step, including between the halves of its record write. At most one live
/// owner ever holds it, and afterwards a new process can claim it: a dead
/// owner's record, whole or torn, never locks the store out.
#[test]
fn racing_claims_and_a_crash_admit_one_live_owner() {
    explore(|findings| {
        let world = Arc::new(Mutex::new(World::new(3, findings)));
        let first = Sim::new(&world, 0);
        let second = Sim::new(&world, 1);
        let killer = world.clone();
        let threads = [
            loom::thread::spawn(move || {
                first.hold("store");
            }),
            loom::thread::spawn(move || {
                second.hold("store");
            }),
            loom::thread::spawn(move || kill(&killer, 0)),
        ];
        for thread in threads {
            thread.join().map_err(failed)?;
        }
        if !Sim::new(&world, 2).hold("store") {
            report(findings, "a dead owner locked the store out".to_owned());
        }
        Ok(())
    });
}

/// The owner holds the store while the other process's reads of the claim
/// file fail, or its liveness probes cannot answer: the other must refuse.
fn uncertain(fail_open: bool, unreadable: bool) {
    explore(move |findings| {
        let world = Arc::new(Mutex::new(World::new(2, findings)));
        {
            let mut world = enter(&world);
            world.unproven = !unreadable;
            if let Some(reads) = world.unreadable.get_mut(1) {
                *reads = unreadable;
            }
        }
        let owner = Sim::new(&world, 0);
        let other = Sim::new(&world, 1);
        let held = protocol::acquire(owner.clone(), Path::new("store")).map_err(failed)?;
        owner.own(&held);
        let probe = other.clone();
        let contender = loom::thread::spawn(move || {
            let path = Path::new("store");
            if fail_open {
                protocol::acquire(FailOpen(probe), path)
                    .map(|claim| *claim.key())
                    .ok()
            } else {
                protocol::acquire(probe, path)
                    .map(|claim| *claim.key())
                    .ok()
            }
        });
        // Checked here, so a breach is this model's panic, not a thread's.
        if let Some(key) = contender.join().map_err(failed)? {
            other.own_key(key);
        }
        owner.disown(&held);
        Ok(())
    });
}

impl Sim {
    /// Records that this live process owns file `key`, reporting a breach if
    /// another live process believes it does too.
    fn own_key(&self, key: usize) {
        let mut world = enter(&self.world);
        if !world.alive.get(self.pid).copied().unwrap_or(false) {
            return;
        }
        let alive = world.alive.clone();
        let findings = world.findings.clone();
        let owners = world.owners.entry(key).or_default();
        if owners
            .iter()
            .any(|other| alive.get(*other).copied().unwrap_or(false))
        {
            let breach = format!("two live owners of one store: {owners:?} and {}", self.pid);
            report(&findings, breach);
        }
        owners.push(self.pid);
    }
}

#[test]
fn an_unreadable_claim_file_refuses() {
    uncertain(false, true);
}

#[test]
fn an_owner_of_unknown_liveness_refuses() {
    uncertain(false, false);
}

#[test]
#[should_panic(expected = "two live owners")]
fn reading_an_unreadable_claim_as_empty_admits_two_owners() {
    uncertain(true, true);
}

#[test]
#[should_panic(expected = "two live owners")]
fn treating_unknown_liveness_as_death_admits_two_owners() {
    uncertain(true, false);
}

/// One process claims a store, its claim file is renamed away, and another of
/// its stores claims a new file at that path before the first claim releases.
/// That release must leave the second claim's record, or another process,
/// racing that release, is admitted beside the second owner.
#[test]
fn a_release_never_clears_another_claims_record() {
    explore(|findings| {
        let world = Arc::new(Mutex::new(World::new(2, findings)));
        let owner = Sim::new(&world, 0);
        let other = Sim::new(&world, 1);
        let first = protocol::acquire(owner.clone(), Path::new("a")).map_err(failed)?;
        owner.rename("a", "b").map_err(failed)?;
        let second = protocol::acquire(owner.clone(), Path::new("a")).map_err(failed)?;
        owner.own(&second);
        // The other process races the first claim's release from here on.
        let contender = loom::thread::spawn(move || {
            other.hold("a");
        });
        drop(first);
        loom::thread::yield_now();
        owner.disown(&second);
        drop(second);
        contender.join().map_err(failed)?;
        Ok(())
    });
}

/// A claim file longer than any record refuses, whatever whole record it
/// begins with; and a process whose record would be that long writes none,
/// so no reader ever takes part of a record for another.
#[test]
fn a_record_is_never_read_in_part() {
    explore(|findings| {
        let world = Arc::new(Mutex::new(World::new(2, findings)));
        let dead = Sim::new(&world, 0);
        let mut bytes = super::record::Record {
            process: dead.process.ok_or("no process")?,
            token: 1,
        }
        .encode();
        bytes.resize(super::record::RECORD_LIMIT + 1, 0);
        kill(&world, 0);
        {
            let mut world = enter(&world);
            world.inodes.push(Inode {
                bytes,
                ..Inode::default()
            });
            world.names.insert(PathBuf::from("store"), 0);
        }
        let mut long = Sim::new(&world, 1);
        let reader = long.clone();
        let refused =
            loom::thread::spawn(move || protocol::acquire(reader, Path::new("store")).is_err());
        if !refused.join().map_err(failed)? {
            report(
                findings,
                "a claim file longer than a record admitted".to_owned(),
            );
        }
        if let Some(process) = long.process.as_mut() {
            process.lock = vec![b'/'; super::record::RECORD_LIMIT];
        }
        if protocol::acquire(long, Path::new("other")).is_ok() {
            report(
                findings,
                "a record longer than the bound was claimed".to_owned(),
            );
        }
        if !enter(&world)
            .inodes
            .get(1)
            .is_some_and(|inode| inode.bytes.is_empty())
        {
            report(
                findings,
                "a record longer than the bound was written".to_owned(),
            );
        }
        Ok(())
    });
}

/// A claimed store is removed while its claim lives, and a new store's file
/// reuses its inode. The stale claim must not refuse the new store, and its
/// release must not free the new store's claim, while another process races
/// for the new store throughout.
#[test]
fn a_removed_claim_file_whose_identity_is_reused_is_a_new_store() {
    explore(|findings| {
        let world = Arc::new(Mutex::new(World::new(2, findings)));
        let owner = Sim::new(&world, 0);
        let other = Sim::new(&world, 1);
        let stale = protocol::acquire(owner.clone(), Path::new("a")).map_err(failed)?;
        owner.reuse("a", "b").map_err(failed)?;
        let contender = loom::thread::spawn(move || {
            other.hold("b");
        });
        let releasing = loom::thread::spawn(move || drop(stale));
        match protocol::acquire(owner.clone(), Path::new("b")) {
            Ok(fresh) => {
                owner.own(&fresh);
                releasing.join().map_err(failed)?;
                if protocol::acquire(owner.clone(), Path::new("b")).is_ok() {
                    report(
                        findings,
                        "a stale claim's release freed a live claim".to_owned(),
                    );
                }
                owner.disown(&fresh);
            }
            Err(error) => {
                releasing.join().map_err(failed)?;
                // Refused only by the other process, never by the stale claim.
                if error.to_string().contains("in this process") {
                    report(
                        findings,
                        "a removed file's claim refused the file that reused it".to_owned(),
                    );
                }
            }
        }
        contender.join().map_err(failed)?;
        Ok(())
    });
}

/// Every handle on a claimed store drops while the store's owner still holds
/// the claim for its last work, as a store does while a detached write and
/// its close-time flush finish. A reopen in the same process then waits for
/// that release and takes the store, never reporting it open, while another
/// process races for the store throughout.
#[test]
fn a_reopen_after_the_last_handle_takes_over_the_closing_claim() {
    explore(|findings| {
        let world = Arc::new(Mutex::new(World::new(2, findings)));
        let owner = Sim::new(&world, 0);
        let other = Sim::new(&world, 1);
        let closing = protocol::acquire(owner.clone(), Path::new("store")).map_err(failed)?;
        owner.own(&closing);
        closing.handles().issue();
        closing.handles().retire();
        let contender = loom::thread::spawn(move || {
            other.hold("store");
        });
        let draining = {
            let owner = owner.clone();
            loom::thread::spawn(move || {
                loom::thread::yield_now();
                owner.disown(&closing);
                drop(closing);
            })
        };
        match protocol::acquire(owner.clone(), Path::new("store")) {
            Ok(reopened) => {
                owner.own(&reopened);
                owner.disown(&reopened);
            }
            // Only the other process may hold the store at this point.
            Err(error) if error.to_string().contains("in this process") => report(
                findings,
                "a reopen after the last handle found the store open".to_owned(),
            ),
            Err(_) => {}
        }
        draining.join().map_err(failed)?;
        contender.join().map_err(failed)?;
        Ok(())
    });
}
