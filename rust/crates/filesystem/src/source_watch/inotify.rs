//! Linux: one inotify instance watching exactly the directories the source
//! reads, each before it is read.
//!
//! inotify has no recursive watch, so the source admits every directory on
//! the way to what it reads ([`PlatformWatch::admit`]). A fact about a path
//! is read after its parent (and every ancestor) is watched, so every later
//! change to the entries it depends on is reported: the kernel queues an
//! event before the system call that caused it returns.
//!
//! Watch descriptors name inodes, not paths, so this keeps the paths it
//! admitted each directory under. A directory moved, removed, or replaced
//! beneath a watched parent is reported by that parent, which invalidates
//! everything beneath the old name, and its paths are forgotten, so the
//! next read beneath the new name admits it again. A read that raced such a
//! change sampled its stamp before the change was recorded, so its fact is
//! invalidated too. A directory reached through a symbolic link is one inode
//! under two paths; each event on it is reported under both.
//!
//! The source fails closed once its root's path stops naming the held root,
//! so the watch also watches every ancestor of the root for the one name
//! on the way to it: once that name is removed, replaced, or moved, the
//! watch becomes inexact.
//!
//! Writes through a shared memory mapping are not reported by inotify.

#![allow(unsafe_code)]

use super::{Delivery, HostChange};
use std::collections::{BTreeMap, HashMap};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

/// Every change to a directory's entries or to the directory itself.
const WATCHED: u32 = libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MODIFY
    | libc::IN_ATTRIB
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO
    | libc::IN_DELETE_SELF
    | libc::IN_MOVE_SELF
    | libc::IN_ONLYDIR
    | libc::IN_EXCL_UNLINK;
/// Events that change which entry a name binds.
const REBINDING: u32 = libc::IN_CREATE | libc::IN_DELETE | libc::IN_MOVED_FROM | libc::IN_MOVED_TO;
/// Every change to the names an ancestor of the root holds, or to itself.
const ANCESTRAL: u32 = REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_ONLYDIR;
/// Room for many events per read; the kernel never splits one.
const READ_BUFFER_BYTES: usize = 64 * 1024;
/// `struct inotify_event` before its name.
const EVENT_HEADER_BYTES: usize = 16;

pub(super) struct PlatformWatch {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

struct Shared {
    inotify: OwnedFd,
    /// Wakes the reader thread to stop.
    wake: OwnedFd,
    /// The source root, which every watched path is relative to.
    root: OwnedFd,
    delivery: Arc<Delivery>,
    /// Each ancestor of the root, by the name it holds on the way to it.
    ancestors: HashMap<i32, std::ffi::OsString>,
    /// Held while reading the queue and delivering what was read, so a
    /// fence returns only after everything queued before it is delivered.
    reading: Mutex<Vec<u8>>,
    watches: Mutex<Watches>,
    stopping: AtomicBool,
}

/// Which paths each watched directory was admitted under.
#[derive(Default)]
struct Watches {
    by_path: BTreeMap<PathBuf, i32>,
    by_descriptor: HashMap<i32, Vec<PathBuf>>,
}

impl Watches {
    fn insert(&mut self, descriptor: i32, path: PathBuf) {
        let aliases = self.by_descriptor.entry(descriptor).or_default();
        if !aliases.contains(&path) {
            aliases.push(path.clone());
        }
        self.by_path.insert(path, descriptor);
    }

    /// Forgets `path` and every path beneath it; returns the descriptors
    /// left with no path at all.
    fn forget_beneath(&mut self, path: &Path) -> Vec<i32> {
        let forgotten = self
            .by_path
            .range(path.to_path_buf()..)
            .take_while(|(candidate, _)| candidate.starts_with(path))
            .map(|(candidate, descriptor)| (candidate.clone(), *descriptor))
            .collect::<Vec<_>>();
        let mut orphaned = Vec::new();
        for (candidate, descriptor) in forgotten {
            self.by_path.remove(&candidate);
            if let Some(aliases) = self.by_descriptor.get_mut(&descriptor) {
                aliases.retain(|alias| *alias != candidate);
                if aliases.is_empty() {
                    self.by_descriptor.remove(&descriptor);
                    orphaned.push(descriptor);
                }
            }
        }
        orphaned
    }

    /// The kernel dropped `descriptor`: its directory is gone.
    fn drop_descriptor(&mut self, descriptor: i32) {
        for alias in self.by_descriptor.remove(&descriptor).unwrap_or_default() {
            if self.by_path.get(&alias) == Some(&descriptor) {
                self.by_path.remove(&alias);
            }
        }
    }
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

/// Watches every ancestor of the held root for the name on the way to it.
fn watch_ancestors(
    inotify: &OwnedFd,
    root: &OwnedFd,
) -> io::Result<HashMap<i32, std::ffi::OsString>> {
    let path = std::fs::read_link(format!("/proc/self/fd/{}", root.as_raw_fd()))?;
    let mut ancestors = HashMap::new();
    let mut child = path.as_path();
    while let (Some(parent), Some(name)) = (child.parent(), child.file_name()) {
        let parent_path = CString::new(parent.as_os_str().as_bytes())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: a NUL-terminated path that outlives the call.
        let descriptor = unsafe {
            libc::inotify_add_watch(inotify.as_raw_fd(), parent_path.as_ptr(), ANCESTRAL)
        };
        if descriptor < 0 {
            return Err(last_error());
        }
        ancestors.insert(descriptor, name.to_os_string());
        child = parent;
    }
    Ok(ancestors)
}

fn owned(descriptor: i32) -> io::Result<OwnedFd> {
    if descriptor < 0 {
        return Err(last_error());
    }
    // SAFETY: a non-negative descriptor a system call just returned is ours.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

impl PlatformWatch {
    pub(super) fn start(
        directory: &crate::native_host::HostRoot,
        delivery: Arc<Delivery>,
    ) -> io::Result<Self> {
        // SAFETY: plain descriptor-creating calls with valid flags.
        let inotify = owned(unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) })?;
        // SAFETY: as above.
        let wake = owned(unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) })?;
        // SAFETY: duplicating a live descriptor the root holds.
        let root =
            owned(unsafe { libc::fcntl(directory.directory_fd(), libc::F_DUPFD_CLOEXEC, 0) })?;
        let ancestors = watch_ancestors(&inotify, &root)?;
        let shared = Arc::new(Shared {
            inotify,
            wake,
            root,
            delivery,
            ancestors,
            reading: Mutex::new(vec![0; READ_BUFFER_BYTES]),
            watches: Mutex::new(Watches::default()),
            stopping: AtomicBool::new(false),
        });
        let root_watch = shared.add_watch(Path::new(""))?;
        shared.lock_watches().insert(root_watch, PathBuf::new());
        let reader = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("acyclic-source-watch".to_owned())
            .spawn(move || reader.run())?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    pub(super) fn admit(&self, directory: &Path) -> bool {
        self.shared.admit(directory)
    }

    pub(super) fn fence(&self) -> io::Result<()> {
        self.shared.read_and_deliver()
    }

    pub(super) fn delivery(&self) -> &Delivery {
        &self.shared.delivery
    }

    /// Holds delivery back while the returned guard lives.
    #[cfg(test)]
    pub(super) fn paused(&self) -> impl Sized + '_ {
        self.shared
            .reading
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for PlatformWatch {
    fn drop(&mut self) {
        self.shared.stopping.store(true, Ordering::Release);
        let one = 1_u64.to_ne_bytes();
        // SAFETY: writes eight bytes from a live buffer to our eventfd.
        let _ =
            unsafe { libc::write(self.shared.wake.as_raw_fd(), one.as_ptr().cast(), one.len()) };
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Shared {
    fn lock_watches(&self) -> std::sync::MutexGuard<'_, Watches> {
        self.watches.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn add_watch(&self, relative: &Path) -> io::Result<i32> {
        let mut path = format!("/proc/self/fd/{}", self.root.as_raw_fd()).into_bytes();
        if !relative.as_os_str().is_empty() {
            path.push(b'/');
            path.extend_from_slice(relative.as_os_str().as_bytes());
        }
        let path = CString::new(path).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: a NUL-terminated path that outlives the call.
        let descriptor =
            unsafe { libc::inotify_add_watch(self.inotify.as_raw_fd(), path.as_ptr(), WATCHED) };
        if descriptor < 0 {
            return Err(last_error());
        }
        Ok(descriptor)
    }

    fn admit(&self, directory: &Path) -> bool {
        if !self.delivery.is_exact() {
            return false;
        }
        let mut watches = self.lock_watches();
        let mut prefix = PathBuf::new();
        let mut added = false;
        for component in directory.components() {
            prefix.push(component);
            added = false;
            if watches.by_path.contains_key(&prefix) {
                continue;
            }
            match self.add_watch(&prefix) {
                Ok(descriptor) => {
                    watches.insert(descriptor, prefix.clone());
                    added = true;
                }
                // Nothing exists to read beneath here; the deepest existing
                // ancestor is watched and reports its creation.
                Err(error)
                    if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) =>
                {
                    return false;
                }
                // A directory that cannot be watched (a watch limit, no read
                // permission) cannot report its changes.
                Err(_) => {
                    drop(watches);
                    self.delivery.abandon();
                    return false;
                }
            }
        }
        added
    }

    fn run(&self) {
        let mut descriptors = [
            libc::pollfd {
                fd: self.inotify.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        while !self.stopping.load(Ordering::Acquire) {
            // SAFETY: two initialized pollfd records in a live array.
            let ready = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, -1) };
            if ready < 0 {
                if last_error().kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                self.delivery.abandon();
                return;
            }
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            if self.read_and_deliver().is_err() {
                self.delivery.abandon();
                return;
            }
        }
    }

    /// Reads everything queued and delivers it, holding the read lock
    /// throughout.
    fn read_and_deliver(&self) -> io::Result<()> {
        let mut buffer = self.reading.lock().unwrap_or_else(PoisonError::into_inner);
        let mut changes = Vec::new();
        loop {
            // SAFETY: reads at most the buffer's length into it.
            let read = unsafe {
                libc::read(
                    self.inotify.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if read < 0 {
                let error = last_error();
                match error.kind() {
                    io::ErrorKind::WouldBlock => break,
                    io::ErrorKind::Interrupted => continue,
                    _ => return Err(error),
                }
            }
            let read = usize::try_from(read).unwrap_or(0);
            self.translate(buffer.get(..read).unwrap_or_default(), &mut changes);
        }
        self.delivery.deliver(&changes);
        Ok(())
    }

    /// Turns raw events into changes, keeping the admitted paths current.
    fn translate(&self, mut events: &[u8], changes: &mut Vec<HostChange>) {
        let mut watches = self.lock_watches();
        let mut orphaned = Vec::new();
        while let Some(header) = events.get(..EVENT_HEADER_BYTES) {
            let field = |offset: usize| {
                header
                    .get(offset..offset + 4)
                    .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                    .map_or(0, u32::from_ne_bytes)
            };
            let descriptor = i32::from_ne_bytes(field(0).to_ne_bytes());
            let mask = field(4);
            let length = usize::try_from(field(12)).unwrap_or(usize::MAX);
            let Some(name) =
                events.get(EVENT_HEADER_BYTES..EVENT_HEADER_BYTES.saturating_add(length))
            else {
                changes.push(HostChange::Everything);
                return;
            };
            events = events
                .get(EVENT_HEADER_BYTES + length..)
                .unwrap_or_default();
            let name = OsStr::from_bytes(name.split(|byte| *byte == 0).next().unwrap_or_default());
            if mask & (libc::IN_Q_OVERFLOW | libc::IN_UNMOUNT) != 0 {
                changes.push(HostChange::Everything);
                continue;
            }
            if let Some(on_the_way) = self.ancestors.get(&descriptor) {
                // The root's path no longer names the held root, or can no
                // longer be watched.
                if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF | libc::IN_IGNORED) != 0
                    || mask & REBINDING != 0 && name == on_the_way.as_os_str()
                {
                    drop(watches);
                    self.delivery.abandon();
                    return;
                }
                continue;
            }
            if mask & libc::IN_IGNORED != 0 {
                watches.drop_descriptor(descriptor);
                continue;
            }
            let aliases = watches
                .by_descriptor
                .get(&descriptor)
                .cloned()
                .unwrap_or_default();
            for alias in aliases {
                if name.is_empty() {
                    // The directory itself: its attributes, or its own move
                    // or removal, which its parent reports too. The root is
                    // held open, so it answers wherever it moves, but once
                    // removed nothing reports changes to it.
                    if alias.as_os_str().is_empty() && mask & libc::IN_DELETE_SELF != 0 {
                        drop(watches);
                        self.delivery.abandon();
                        return;
                    }
                    if mask & (libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) == 0 {
                        changes.push(HostChange::Altered(alias));
                    } else {
                        if !alias.as_os_str().is_empty() {
                            orphaned.extend(watches.forget_beneath(&alias));
                        }
                        changes.push(HostChange::Rebound(alias));
                    }
                } else {
                    let path = alias.join(name);
                    if mask & REBINDING == 0 {
                        changes.push(HostChange::Altered(path));
                    } else {
                        orphaned.extend(watches.forget_beneath(&path));
                        changes.push(HostChange::Rebound(path));
                    }
                }
            }
        }
        for descriptor in orphaned {
            // SAFETY: removing a watch this instance added; failure means the
            // kernel already dropped it.
            unsafe { libc::inotify_rm_watch(self.inotify.as_raw_fd(), descriptor) };
        }
    }
}
