//! The claim protocol, written once over [`Host`] so that native runs and the
//! model run the same code.

use super::record::{Contents, Process, RECORD_LIMIT, Record};
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::path::{Path, PathBuf};

/// What a probe proved about a recorded owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Liveness {
    Alive,
    /// Positive proof that the owner died.
    Dead,
    /// Nothing proven either way. Refuses, as `Alive` does: a false refusal
    /// is an error the caller sees, a false admission is two owners.
    Unknown,
}

/// The operations the protocol needs from its platform.
pub(super) trait Host {
    type File;
    type Key: Clone + Eq + Hash + std::fmt::Debug;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File>;
    /// The file's identity, the same for every name of it; `path` is the
    /// name `file` was opened by.
    fn identity(&self, file: &Self::File, path: &Path) -> io::Result<Self::Key>;
    /// Takes the file's exclusive lock without waiting: `Ok(false)` when
    /// another holds it.
    fn try_lock(&self, file: &Self::File) -> io::Result<bool>;
    /// Takes the file's exclusive lock, waiting for its holder.
    fn lock(&self, file: &Self::File) -> io::Result<()>;
    fn unlock(&self, file: &Self::File) -> io::Result<()>;
    /// The whole file, up to [`super::record::RECORD_LIMIT`] bytes.
    fn read(&self, file: &Self::File) -> io::Result<Vec<u8>>;
    /// Replaces the whole file with `bytes`.
    fn write(&self, file: &Self::File, bytes: &[u8]) -> io::Result<()>;
    /// This process, when it holds a process lock that claims can name,
    /// establishing that lock on first use.
    fn process(&self) -> Option<&Process>;
    /// [`Host::process`], only if already established. A record can name
    /// this process only after its lock is established.
    fn established(&self) -> Option<&Process>;
    /// What can be proven about `process`, counting each probe in `probes`.
    fn liveness(&self, process: &Process, probes: &mut u64) -> Liveness;
    /// A token no other claim of this process has used.
    fn token(&self) -> u64;
    fn table(&self) -> &Table<Self::Key>;
}

pub(super) fn contention(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, reason)
}

#[cfg(not(acyclic_native_runtime_loom))]
const SHARDS: usize = 64;
/// Models keep the table small; sharding is not what they check.
#[cfg(acyclic_native_runtime_loom)]
const SHARDS: usize = 2;

/// Claimed files of one process, sharded so unrelated claims never contend.
/// Each entry is the claim's token, and whether the claim has closed the
/// file. A closed file can be removed and its identity reused by another
/// file, so a closed claim's entry binds only while the file still carries
/// that claim's record.
pub(super) struct Table<Key> {
    shards: [Shard<Key>; SHARDS],
}

struct Shard<Key> {
    entries: crate::sync::Mutex<HashMap<Key, Entry>>,
    /// Signalled whenever an entry is removed, so that an acquire waiting
    /// on a closing claim takes the store over as that claim's release ends.
    removed: crate::sync::Condvar,
}

struct Entry {
    token: u64,
    /// The claim closed the file after recording itself in it.
    recorded: bool,
    handles: crate::sync::Arc<Handles>,
}

/// The handles a claim's owner gave out. Once it gave some and every one
/// has dropped, the claim is closing: its owner's last work drains and its
/// release follows without fail, so an acquire in this process waits for
/// that release rather than reporting the store open.
pub(super) struct Handles {
    issued: AtomicBool,
    live: AtomicUsize,
}

impl Handles {
    fn new() -> Self {
        Self {
            issued: AtomicBool::new(false),
            live: AtomicUsize::new(0),
        }
    }

    pub(super) fn issue(&self) {
        self.live.fetch_add(1, Ordering::SeqCst);
        self.issued.store(true, Ordering::SeqCst);
    }

    pub(super) fn retire(&self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }

    fn closing(&self) -> bool {
        self.issued.load(Ordering::SeqCst) && self.live.load(Ordering::SeqCst) == 0
    }
}

impl<Key: Clone + Eq + Hash> Table<Key> {
    pub(super) fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Shard {
                entries: crate::sync::Mutex::new(HashMap::new()),
                removed: crate::sync::Condvar::new(),
            }),
        }
    }

    fn shard(&self, key: &Key) -> io::Result<&Shard<Key>> {
        use std::hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher};
        let index = BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % SHARDS as u64;
        usize::try_from(index)
            .ok()
            .and_then(|index| self.shards.get(index))
            .ok_or_else(|| io::Error::other("claim shard out of range"))
    }

    fn entries(&self, key: &Key) -> io::Result<crate::sync::MutexGuard<'_, HashMap<Key, Entry>>> {
        Ok(self
            .shard(key)?
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Waits while a claim of this process on the file `key` is closing:
    /// its handles are gone and only its release remains.
    fn await_closing(&self, key: &Key) -> io::Result<()> {
        let shard = self.shard(key)?;
        let mut entries = shard
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while entries
            .get(key)
            .is_some_and(|entry| entry.handles.closing())
        {
            entries = shard
                .removed
                .wait(entries)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        Ok(())
    }

    /// Enters claim `token` for the file `key`, whose lock the caller holds
    /// and which holds `contents`. Refuses while another claim of this
    /// process holds that file: one that still has it open, or one whose
    /// record it still carries. Any other entry is of a file since removed,
    /// whose identity this file reuses.
    fn insert(
        &self,
        key: &Key,
        token: u64,
        handles: &crate::sync::Arc<Handles>,
        contents: &Contents,
        own: Option<&Process>,
    ) -> io::Result<()> {
        let mut entries = self.entries(key)?;
        if let Some(entry) = entries.get(key) {
            let holds = !entry.recorded
                || matches!(contents, Contents::Record(record)
                    if own == Some(&record.process) && record.token == entry.token);
            if holds {
                return Err(contention("store already claimed in this process"));
            }
        }
        entries.insert(
            key.to_owned(),
            Entry {
                token,
                recorded: false,
                handles: crate::sync::Arc::clone(handles),
            },
        );
        Ok(())
    }

    /// Notes that claim `token` recorded itself and is closing the file.
    fn recorded(&self, key: &Key, token: u64) -> io::Result<()> {
        if let Some(entry) = self.entries(key)?.get_mut(key)
            && entry.token == token
        {
            entry.recorded = true;
        }
        Ok(())
    }

    /// Removes claim `token`'s entry, never one that replaced it.
    fn remove(&self, key: &Key, token: u64) {
        let Ok(shard) = self.shard(key) else {
            return;
        };
        let mut entries = shard
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.get(key).is_some_and(|entry| entry.token == token) {
            entries.remove(key);
            drop(entries);
            shard.removed.notify_all();
        }
    }
}

/// Whether the recorded owner of a claim file excludes a new one.
fn excludes<H: Host>(
    host: &H,
    own: Option<&Process>,
    contents: &Contents,
    probes: &mut u64,
) -> Option<io::Error> {
    let record = match contents {
        Contents::Empty | Contents::Torn => return None,
        // Not a record any claim wrote, nor provably the remains of one.
        Contents::Oversized => return Some(contention("store claim file exceeds a record")),
        Contents::Record(record) => record,
    };
    // A record of this process that its table does not hold is a release
    // that could not clear it: no owner.
    if own == Some(&record.process) {
        return None;
    }
    match host.liveness(&record.process, probes) {
        Liveness::Dead => None,
        Liveness::Alive => Some(contention("store claimed by a live process")),
        Liveness::Unknown => Some(contention("store claimed by a process of unknown liveness")),
    }
}

/// One held claim.
pub(super) struct Claim<H: Host> {
    host: H,
    path: PathBuf,
    key: H::Key,
    /// This claim's table entry, and unless `held` the record it wrote,
    /// which release alone may clear.
    token: u64,
    /// The claim file, kept locked as the claim itself when this process has
    /// no process lock to name.
    held: Option<H::File>,
    handles: crate::sync::Arc<Handles>,
}

impl<H: Host> std::fmt::Debug for Claim<H> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreClaim")
            .field("path", &self.path)
            .field("key", &self.key)
            .field("token", &self.token)
            .field("held", &self.held.is_some())
            .finish()
    }
}

/// Removes a table entry unless the claim that owns it is completed.
struct Pending<'a, H: Host> {
    host: &'a H,
    entry: Option<(H::Key, u64)>,
}

impl<H: Host> Drop for Pending<'_, H> {
    fn drop(&mut self) {
        if let Some((key, token)) = self.entry.take() {
            self.host.table().remove(&key, token);
        }
    }
}

pub(super) fn acquire<H: Host>(host: H, path: &Path) -> io::Result<Claim<H>> {
    let file = host.open(path, true)?;
    let key = host.identity(&file, path)?;
    let token = host.token();
    // A closing claim of this process hands the store over once released.
    host.table().await_closing(&key)?;
    let handles = crate::sync::Arc::new(Handles::new());
    if !host.try_lock(&file)? {
        return Err(contention(
            "store claim is being exchanged by another owner",
        ));
    }
    let mut pending = Pending {
        host: &host,
        entry: None,
    };
    let exchanged = (|| {
        let contents = Contents::decode(&host.read(&file)?);
        let own = host.process();
        host.table().insert(&key, token, &handles, &contents, own)?;
        pending.entry = Some((key.clone(), token));
        if let Some(refusal) = excludes(&host, own, &contents, &mut 0) {
            return Err(refusal);
        }
        let Some(process) = own else {
            // Without a process lock, the claim file's own lock is the claim:
            // the OS releases it when this process dies.
            if contents != Contents::Empty {
                host.write(&file, &[])?;
            }
            return Ok(false);
        };
        let record = Record {
            process: process.clone(),
            token,
        };
        let encoded = record.encode();
        // A record that cannot be read back whole would read as another.
        if encoded.len() > RECORD_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "claim record exceeds its bound",
            ));
        }
        host.write(&file, &encoded)?;
        // Before the file closes: until then its identity cannot be reused.
        host.table().recorded(&key, token)?;
        Ok(true)
    })();
    let recorded = match exchanged {
        Ok(recorded) => recorded,
        Err(error) => {
            let _ = host.unlock(&file);
            return Err(error);
        }
    };
    let held = if recorded {
        // Closing the file releases its lock even if unlocking fails.
        let _ = host.unlock(&file);
        None
    } else {
        Some(file)
    };
    pending.entry = None;
    drop(pending);
    Ok(Claim {
        host,
        path: path.to_path_buf(),
        key,
        token,
        held,
        handles,
    })
}

/// Whether the claim file `file`, whose lock the caller holds, names an
/// owner that excludes others: a live process, one of unknown liveness, or
/// this one. Also returns the reads it made: the record, then each probe.
pub(super) fn is_claimed<H: Host>(host: &H, file: &H::File) -> io::Result<(bool, u64)> {
    let contents = Contents::decode(&host.read(file)?);
    let mut reads = 1;
    let own = host.established();
    let claimed = match &contents {
        Contents::Record(record) if own == Some(&record.process) => true,
        _ => excludes(host, own, &contents, &mut reads).is_some(),
    };
    Ok((claimed, reads))
}

impl<H: Host> Claim<H> {
    #[cfg(all(test, unix))]
    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// The handles this claim's owner gives out.
    pub(super) fn handles(&self) -> &crate::sync::Arc<Handles> {
        &self.handles
    }

    /// The claimed file's identity.
    #[cfg(all(test, acyclic_native_runtime_loom))]
    pub(super) fn key(&self) -> &H::Key {
        &self.key
    }

    /// Clears this claim's own record: only in the very file it claimed, and
    /// only while the record still carries this claim's token.
    fn release(&mut self) -> io::Result<()> {
        if let Some(held) = self.held.take() {
            return self.host.unlock(&held);
        }
        let token = self.token;
        let file = match self.host.open(&self.path, false) {
            Ok(file) => file,
            // The file is gone, and the record with it.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        // The path may now name another store's file; this claim's record,
        // if it survives, is in the file it claimed and names this live
        // process, which keeps others out until it exits.
        if self.host.identity(&file, &self.path)? != self.key {
            return Ok(());
        }
        // Waits out an exchange in progress: other holders keep this lock for
        // a few syscalls, and none can own the claim while this record
        // stands. Giving up instead would leave a record naming this live
        // process, which locks every other process out until it exits.
        self.host.lock(&file)?;
        let cleared = (|| match Contents::decode(&self.host.read(&file)?) {
            Contents::Record(record)
                if Some(&record.process) == self.host.process() && record.token == token =>
            {
                self.host.write(&file, &[])
            }
            _ => Ok(()),
        })();
        let unlocked = self.host.unlock(&file);
        cleared.and(unlocked)
    }
}

impl<H: Host> Drop for Claim<H> {
    fn drop(&mut self) {
        let _ = self.release();
        self.host.table().remove(&self.key, self.token);
    }
}
