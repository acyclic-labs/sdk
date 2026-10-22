//! The descriptor cache, written once over [`Files`] so that native runs and
//! the model run the same code.

use crate::sync::Mutex;
use crate::sync::atomic::{AtomicBool, Ordering, Ordering::SeqCst, fence};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, PoisonError, Weak};

/// The operations the cache needs from its platform.
pub(super) trait Files: Send + Sync + Sized + 'static {
    type File: Send + Sync;
    type Identity: Copy + Eq + std::fmt::Debug + Send + Sync;

    /// Opens the existing file at `path`, never creating or truncating it.
    fn reopen(&self, path: &Path, write: bool) -> io::Result<Self::File>;
    fn identity(&self, file: &Self::File) -> io::Result<Self::Identity>;
    /// Makes every write through `file` durable before it closes.
    fn flush(&self, file: &Self::File) -> io::Result<()>;
}

/// Work a lease does for the cache, charged before the work is done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cost {
    /// Reopening a file by path, then reading its identity.
    Reopen,
    /// One allocation of this many bytes, owned by the cache.
    Allocation(usize),
    /// Flushing another file's writes before its idle descriptor closes.
    Flush,
}

/// Charges a [`Cost`] before its work; an error refuses that work.
pub(super) type Charge<'a> = &'a mut dyn FnMut(Cost) -> io::Result<()>;

/// The most descriptors one eviction pass takes, so that evicting never
/// allocates.
const BATCH: usize = 4;

/// Files whose idle descriptor an eviction pass took, still open.
type Victims<F> = [Option<(Arc<Entry<F>>, crate::sync::Arc<Handle<F>>)>; BATCH];

/// Open descriptors of one process, at most `capacity` of them idle.
pub(super) struct Cache<F: Files> {
    files: F,
    capacity: usize,
    ring: Mutex<Ring<F>>,
    /// The ring's length, written under its lock, so that a released lease
    /// can tell without the lock whether the cache is over capacity.
    listed: crate::sync::atomic::AtomicUsize,
}

/// Files holding a cached descriptor, swept by a clock hand.
struct Ring<F: Files> {
    entries: Vec<Weak<Entry<F>>>,
    hand: usize,
    /// Slots reserved for files being admitted, so that listing them never
    /// allocates.
    reserved: usize,
}

impl<F: Files> Cache<F> {
    pub(super) fn new(files: F, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        // Sized for the capacity and the file being listed past it, so that
        // the ring grows only while more files are in use than the capacity.
        // Grown on demand instead, its growth would fall to whichever file
        // first passed a size every file in the process had reached.
        let mut entries = Vec::new();
        let _ = entries.try_reserve_exact(capacity.saturating_add(1));
        Self {
            files,
            capacity,
            ring: Mutex::new(Ring {
                entries,
                hand: 0,
                reserved: 0,
            }),
            listed: crate::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }

    fn ring(&self) -> crate::sync::MutexGuard<'_, Ring<F>> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Publishes the ring's length after it changed, under its lock.
    fn note(&self, ring: &Ring<F>) {
        self.listed.store(ring.entries.len(), SeqCst);
    }

    /// Whether more files hold a descriptor than the capacity allows.
    fn over(&self) -> bool {
        self.listed.load(SeqCst) > self.capacity()
    }

    /// Files whose descriptor the cache holds.
    /// The files the ring lists, wanted or not.
    #[cfg(all(test, not(acyclic_native_runtime_loom)))]
    pub(super) fn listed(&self) -> usize {
        self.listed.load(SeqCst)
    }

    pub(super) fn cached(&self) -> usize {
        self.ring()
            .entries
            .iter()
            .filter(|entry| entry.strong_count() != 0)
            .count()
    }

    /// Reserves the ring slot of one file about to be listed, charging the
    /// ring's growth when it must grow.
    fn reserve(&self, charge: Charge<'_>) -> io::Result<()> {
        let mut ring = self.ring();
        let needed = ring.entries.len() + ring.reserved + 1;
        if needed > ring.entries.capacity() {
            let grown = needed.max(ring.entries.capacity().saturating_mul(2));
            charge(Cost::Allocation(
                grown.saturating_mul(std::mem::size_of::<Weak<Entry<F>>>()),
            ))?;
            let additional = grown - ring.entries.len();
            ring.entries
                .try_reserve_exact(additional)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        }
        ring.reserved += 1;
        Ok(())
    }

    fn unreserve(&self) {
        self.ring().reserved -= 1;
    }

    /// Lists `entry` in the slot reserved for it.
    fn list(&self, entry: &Arc<Entry<F>>) {
        let mut ring = self.ring();
        ring.reserved -= 1;
        entry.slot.store(ring.entries.len(), Relaxed);
        ring.entries.push(Arc::downgrade(entry));
        self.note(&ring);
    }

    /// Unlists the file at `index`, moving the last file into its slot.
    fn unlist(&self, ring: &mut Ring<F>, index: usize) {
        ring.entries.swap_remove(index);
        if let Some(moved) = ring.entries.get(index).and_then(Weak::upgrade) {
            moved.slot.store(index, Relaxed);
        }
        self.note(ring);
    }

    /// Closes the idle descriptors of files that went unused longest until
    /// the cache is within its capacity, or every other file is in use.
    fn shrink(&self, charge: Charge<'_>) {
        loop {
            let victims = self.evict(charge);
            if victims.iter().all(Option::is_none) {
                return;
            }
            for (entry, handle) in victims.into_iter().flatten() {
                entry.close(&handle);
            }
        }
    }

    /// Takes up to [`BATCH`] idle descriptors from the ring. Each stays its
    /// file's descriptor until [`Entry::close`] has flushed it, so that a
    /// use of the file meanwhile finds it rather than reopening the file.
    fn evict(&self, charge: Charge<'_>) -> Victims<F> {
        let mut victims: Victims<F> = std::array::from_fn(|_| None);
        let mut taken = 0;
        let mut ring = self.ring();
        // Pairs with the fence a released lease runs before it reads
        // `listed`: either that release is seen below, so its descriptor is
        // idle here, or the release sees this ring over capacity and closes
        // its descriptor itself. A file lock held here is released after
        // this fence, so its holder's release sees the ring too.
        fence(SeqCst);
        let capacity = self.capacity();
        // A file's reference bit clears on the hand's first pass, so the
        // second finds every idle victim; a sweep that finds none leaves the
        // ring over capacity only while every other file is in use.
        let mut budget = ring.entries.len().saturating_mul(3);
        while ring.entries.len() > capacity && budget > 0 && taken < BATCH {
            budget -= 1;
            if ring.hand >= ring.entries.len() {
                ring.hand = 0;
            }
            let hand = ring.hand;
            let Some(candidate) = ring.entries.get(hand).and_then(Weak::upgrade) else {
                // A file that was dropped closed its own descriptor.
                self.unlist(&mut ring, hand);
                continue;
            };
            // Never waits while holding the ring: a file whose lock is held
            // is in use, which is a reference.
            let Ok(mut state) = candidate.state.try_lock() else {
                ring.hand += 1;
                continue;
            };
            if std::mem::take(&mut state.referenced) {
                ring.hand += 1;
                continue;
            }
            let victim = match &state.open {
                // A leased descriptor is in use.
                Some(open) if crate::sync::Arc::strong_count(open) > 1 => {
                    ring.hand += 1;
                    continue;
                }
                // Closing a written descriptor flushes it, which the user
                // that evicts it pays for, or the file stays.
                Some(open) if open.dirty() && charge(Cost::Flush).is_err() => {
                    ring.hand += 1;
                    continue;
                }
                open => open.clone(),
            };
            state.cached = false;
            drop(state);
            self.unlist(&mut ring, hand);
            if let Some(handle) = victim
                && let Some(slot) = victims.get_mut(taken)
            {
                *slot = Some((candidate, handle));
                taken += 1;
            }
        }
        victims
    }
}

/// A leased descriptor, open while the lease lives.
pub(super) struct Lease<F: Files> {
    /// Declared first, so that it is released before `_settle` runs.
    handle: crate::sync::Arc<Handle<F>>,
    _settle: Settle<F>,
}

impl<F: Files> std::ops::Deref for Lease<F> {
    type Target = Handle<F>;

    fn deref(&self) -> &Handle<F> {
        &self.handle
    }
}

/// Closes the released descriptor when the cache is over capacity. An
/// eviction skips leased descriptors, so one that ran while this lease was
/// held may have left the cache over capacity; once released, this lease's
/// descriptor is the excess, and closing it takes no sweep of the cache.
struct Settle<F: Files>(Arc<Entry<F>>);

impl<F: Files> Drop for Settle<F> {
    fn drop(&mut self) {
        // Pairs with the fence in `Cache::evict`.
        fence(SeqCst);
        self.0.settle();
    }
}

/// One cached file.
pub(super) struct Entry<F: Files> {
    cache: Arc<Cache<F>>,
    path: PathBuf,
    write: bool,
    /// Whether closing a written descriptor flushes it.
    durable: bool,
    state: Mutex<State<F>>,
    /// The file's index in the cache's ring while listed, kept under the
    /// ring's lock.
    slot: AtomicUsize,
    /// A write that no flush could make durable before its descriptor
    /// closed. Kept apart from `state`, since a descriptor may close while
    /// `state` is held.
    lost: Arc<Mutex<Option<io::ErrorKind>>>,
}

struct State<F: Files> {
    identity: F::Identity,
    open: Option<crate::sync::Arc<Handle<F>>>,
    /// Used since the clock hand last passed.
    referenced: bool,
    /// Listed in the cache's ring.
    cached: bool,
}

/// One open descriptor. It flushes the writes made through it before it
/// closes, whoever closes it last.
pub(super) struct Handle<F: Files> {
    file: F::File,
    cache: Arc<Cache<F>>,
    /// Written since the last flush.
    unsynced: AtomicBool,
    /// Whether closing must flush.
    durable: bool,
    lost: Arc<Mutex<Option<io::ErrorKind>>>,
    /// Held across each flush, so that a flush which reports success ran
    /// after any earlier flush of this descriptor had kept its failure.
    flushing: Mutex<()>,
}

/// Fails when a write to the file was lost as a descriptor closed.
fn check_lost(lost: &Mutex<Option<io::ErrorKind>>) -> io::Result<()> {
    match *lost.lock().unwrap_or_else(PoisonError::into_inner) {
        Some(kind) => Err(io::Error::new(
            kind,
            "a write was lost when its file's descriptor closed",
        )),
        None => Ok(()),
    }
}

impl<F: Files> Drop for Handle<F> {
    fn drop(&mut self) {
        self.close_flush();
    }
}

impl<F: Files> Handle<F> {
    pub(super) fn file(&self) -> &F::File {
        &self.file
    }

    /// Whether closing this descriptor must flush it.
    fn dirty(&self) -> bool {
        self.durable && self.unsynced.load(Ordering::Acquire)
    }

    /// Flushes the writes made through this descriptor before it closes.
    /// On Linux a descriptor opened later never sees a writeback error of
    /// this one, and a later flush of this one may no longer report it, so a
    /// failure here is kept for the file's every later use and flush.
    fn close_flush(&self) {
        let _flushing = self.flushing.lock().unwrap_or_else(PoisonError::into_inner);
        if self.durable
            && self.unsynced.swap(false, Ordering::AcqRel)
            && let Err(error) = self.cache.files.flush(&self.file)
        {
            self.lost
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(error.kind());
        }
    }

    /// Marks the writes `write` makes, before it makes them.
    pub(super) fn write<R>(&self, write: impl FnOnce(&F::File) -> io::Result<R>) -> io::Result<R> {
        self.unsynced.store(true, Ordering::Release);
        write(&self.file)
    }

    /// Runs `flush`, which makes every earlier write durable. It fails when
    /// a write to the file was lost as a descriptor closed, even if `flush`
    /// itself succeeds.
    pub(super) fn flush<R>(&self, flush: impl FnOnce(&F::File) -> io::Result<R>) -> io::Result<R> {
        let _flushing = self.flushing.lock().unwrap_or_else(PoisonError::into_inner);
        // Cleared first, so a write racing the flush stays marked.
        let unsynced = self.unsynced.swap(false, Ordering::AcqRel);
        let flushed = flush(&self.file).inspect_err(|_| {
            if unsynced {
                self.unsynced.store(true, Ordering::Release);
            }
        })?;
        check_lost(&self.lost)?;
        Ok(flushed)
    }
}

impl<F: Files> Entry<F> {
    /// Caches `file`, opened at `path`. `durable` files flush writes before
    /// their descriptor closes.
    pub(super) fn new(
        cache: &Arc<Cache<F>>,
        file: F::File,
        path: &Path,
        write: bool,
        durable: bool,
        charge: Charge<'_>,
    ) -> io::Result<Arc<Self>> {
        // The entry, its failure slot and its descriptor, each with two
        // reference counts, and its path.
        let counts = 2 * std::mem::size_of::<usize>();
        charge(Cost::Allocation(std::mem::size_of::<Self>() + counts))?;
        charge(Cost::Allocation(
            std::mem::size_of::<Mutex<Option<io::ErrorKind>>>() + counts,
        ))?;
        charge(Cost::Allocation(std::mem::size_of::<Handle<F>>() + counts))?;
        charge(Cost::Allocation(path.as_os_str().len()))?;
        let absolute = std::path::absolute(path)?;
        // A relative path also takes the working directory.
        if let Some(extra) = absolute
            .as_os_str()
            .len()
            .checked_sub(path.as_os_str().len())
            .filter(|extra| *extra != 0)
        {
            charge(Cost::Allocation(extra))?;
        }
        let identity = cache.files.identity(&file)?;
        let lost = Arc::new(Mutex::new(None));
        let entry = Arc::new(Self {
            cache: Arc::clone(cache),
            path: absolute,
            write,
            durable,
            state: Mutex::new(State {
                identity,
                open: None,
                referenced: true,
                cached: false,
            }),
            slot: AtomicUsize::new(0),
            lost,
        });
        entry.install(file, charge)?;
        Ok(entry)
    }

    fn handle(&self, file: F::File) -> crate::sync::Arc<Handle<F>> {
        crate::sync::Arc::new(Handle {
            file,
            cache: Arc::clone(&self.cache),
            unsynced: AtomicBool::new(false),
            durable: self.durable,
            lost: Arc::clone(&self.lost),
            flushing: Mutex::new(()),
        })
    }

    /// Reopens the file by path, charging the reopen and its descriptor
    /// first. It must be the very file cached, never another that took its
    /// name.
    fn reopen(
        &self,
        identity: F::Identity,
        charge: Charge<'_>,
    ) -> io::Result<crate::sync::Arc<Handle<F>>> {
        charge(Cost::Reopen)?;
        // The descriptor and its two reference counts.
        charge(Cost::Allocation(
            std::mem::size_of::<Handle<F>>() + 2 * std::mem::size_of::<usize>(),
        ))?;
        let file = self.cache.files.reopen(&self.path, self.write)?;
        if self.cache.files.identity(&file)? != identity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a cached file was replaced while its descriptor was closed",
            ));
        }
        Ok(self.handle(file))
    }

    /// The file's descriptor, reopened by path when the cache closed it. The
    /// work the cache does for it (a reopen, the allocations it makes and
    /// the flushes of other files it evicts) is charged before it is done.
    pub(super) fn lease(self: &Arc<Self>, charge: Charge<'_>) -> io::Result<Lease<F>> {
        let (handle, listed) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            // Checked under the lock: an evicted descriptor leaves `open`
            // only once its closing flush has kept any failure, so no reopen
            // can pass this check before an earlier close reports.
            check_lost(&self.lost)?;
            state.referenced = true;
            let listing = !state.cached;
            if listing {
                self.cache.reserve(charge)?;
            }
            let opened = match &state.open {
                Some(open) => Ok(crate::sync::Arc::clone(open)),
                None => self.reopen(state.identity, charge),
            };
            let handle = match opened {
                Ok(handle) => handle,
                Err(error) => {
                    if listing {
                        self.cache.unreserve();
                    }
                    return Err(error);
                }
            };
            state.open = Some(crate::sync::Arc::clone(&handle));
            if listing {
                state.cached = true;
                self.cache.list(self);
            }
            (handle, listing)
        };
        if listed {
            self.cache.shrink(charge);
        }
        Ok(Lease {
            handle,
            _settle: Settle(Arc::clone(self)),
        })
    }

    /// Closes this file's descriptor, just released, if the cache is over
    /// capacity and nothing else uses it. A release has no meter to charge,
    /// so a descriptor with writes to flush stays for a lease that lists a
    /// file, which pays to evict it.
    fn settle(&self) {
        if !self.cache.over() {
            return;
        }
        // Takes the ring, then the file, as an eviction does. A file whose
        // lock is held is in use by a lease that settles as it is released.
        let mut ring = self.cache.ring();
        let slot = self.slot.load(Relaxed);
        if !ring
            .entries
            .get(slot)
            .is_some_and(|listed| std::ptr::eq(listed.as_ptr(), self))
        {
            return;
        }
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        match &state.open {
            Some(open) if crate::sync::Arc::strong_count(open) == 1 && !open.dirty() => {}
            _ => return,
        }
        state.cached = false;
        let handle = state.open.take();
        drop(state);
        self.cache.unlist(&mut ring, slot);
        drop(ring);
        drop(handle);
    }

    /// Unlists this file as its owner drops it, so that the ring holds only
    /// files that are still wanted. Not in `Drop`: an eviction can drop the
    /// last reference to a file while it holds the ring.
    pub(super) fn forget(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !std::mem::take(&mut state.cached) {
            return;
        }
        let mut ring = self.cache.ring();
        let slot = self.slot.load(Relaxed);
        if ring
            .entries
            .get(slot)
            .is_some_and(|listed| std::ptr::eq(listed.as_ptr(), self))
        {
            self.cache.unlist(&mut ring, slot);
        }
    }

    /// Closes `handle`, an idle descriptor of this file that an eviction
    /// took: it flushes, then leaves `open` unless the file was used since.
    fn close(&self, handle: &crate::sync::Arc<Handle<F>>) {
        handle.close_flush();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.cached
            && state
                .open
                .as_ref()
                .is_some_and(|open| crate::sync::Arc::ptr_eq(open, handle))
            && crate::sync::Arc::strong_count(handle) == 2
        {
            state.open = None;
        }
        drop(state);
    }

    fn install(self: &Arc<Self>, file: F::File, charge: Charge<'_>) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        self.cache.reserve(charge)?;
        state.open = Some(self.handle(file));
        state.referenced = true;
        state.cached = true;
        self.cache.list(self);
        drop(state);
        self.cache.shrink(charge);
        Ok(())
    }
}
