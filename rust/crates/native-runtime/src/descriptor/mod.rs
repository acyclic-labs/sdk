//! Files that hold no descriptor while idle.
//!
//! A [`CachedFile`] names an open file by path and identity. The process
//! keeps the descriptors of recently used files open, up to a capacity
//! derived from its descriptor limit; any other file is reopened by path when
//! next used, and is refused unless it is still the very file cached. A
//! descriptor that closes with writes not yet flushed flushes them first, and
//! a flush that fails there fails the file's next use, since on Linux a
//! descriptor opened later never sees an earlier one's writeback error.
//! Descriptors therefore scale with the files in use, not the files open.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

mod cache;
#[cfg(all(test, acyclic_native_runtime_loom))]
mod model;
#[cfg(all(test, not(acyclic_native_runtime_loom)))]
mod tests;

use cache::{Cache, Cost, Entry, Files, Lease};

/// The cache on this machine's files.
struct Native;

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
type Identity = crate::FileIdentity;
/// Without a file identity a reopen cannot be checked, so descriptors stay
/// open for the file's life.
#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
type Identity = ();

impl Files for Native {
    type File = File;
    type Identity = Identity;

    fn reopen(&self, path: &Path, write: bool) -> io::Result<File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(write)
            .open(path)
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    fn identity(&self, file: &File) -> io::Result<Identity> {
        crate::file_identity(file)
    }

    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    fn identity(&self, _: &File) -> io::Result<Identity> {
        Ok(())
    }

    fn flush(&self, file: &File) -> io::Result<()> {
        crate::flush_file_data(file)
    }
}

/// The environment variable that bounds the idle descriptors a process
/// keeps, read once as the process first caches a file.
pub const CACHED_DESCRIPTORS_VARIABLE: &str = "ACYCLIC_CACHED_DESCRIPTORS";

/// The idle descriptors this process keeps: [`CACHED_DESCRIPTORS_VARIABLE`]
/// when it is a positive integer, or otherwise a quarter of the process's
/// descriptor limit, leaving the rest to sockets, pipes, directories and
/// files in use. It is fixed for the process's life, so that no part of it
/// changes another's cost.
fn configured_capacity() -> usize {
    std::env::var_os(CACHED_DESCRIPTORS_VARIABLE)
        .and_then(|value| value.to_str()?.parse::<usize>().ok())
        .filter(|capacity| *capacity > 0)
        .unwrap_or_else(default_capacity)
}

fn default_capacity() -> usize {
    const FLOOR: usize = 16;
    const CEILING: usize = 1 << 16;
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    let limit = match descriptor_limit() {
        0 => 1024,
        limit => limit,
    };
    // Windows bounds handles by memory, not by a per-process limit.
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    let limit: u64 = 16_384;
    usize::try_from(limit / 4)
        .unwrap_or(CEILING)
        .clamp(FLOOR, CEILING)
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
fn descriptor_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes only the struct it is given.
    #[allow(unsafe_code, reason = "FFI: getrlimit")]
    let status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut limit) };
    if status == 0 { limit.rlim_cur } else { 0 }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn cache() -> &'static Arc<Cache<Native>> {
    static CACHE: OnceLock<Arc<Cache<Native>>> = OnceLock::new();
    CACHE.get_or_init(|| Arc::new(Cache::new(Native, configured_capacity())))
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
fn cache() -> &'static Arc<Cache<Native>> {
    static CACHE: OnceLock<Arc<Cache<Native>>> = OnceLock::new();
    CACHE.get_or_init(|| Arc::new(Cache::new(Native, usize::MAX)))
}

/// How many [`CachedFile`]s hold a descriptor now.
pub fn cached_descriptors() -> usize {
    cache().cached()
}

/// The idle descriptors this process keeps; files in use keep theirs
/// regardless. See [`CACHED_DESCRIPTORS_VARIABLE`].
pub fn cached_descriptor_capacity() -> usize {
    cache().capacity()
}

/// An open file that holds a descriptor only while recently used.
pub struct CachedFile {
    entry: Arc<Entry<Native>>,
}

impl Drop for CachedFile {
    fn drop(&mut self) {
        self.entry.forget();
    }
}

impl std::fmt::Debug for CachedFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CachedFile").finish_non_exhaustive()
    }
}

impl CachedFile {
    /// Caches `file`, opened at `path` for reading, and for writing when
    /// `write`. A reopen opens `path` the same way, without creating or
    /// truncating it. A `durable` file flushes its writes before a descriptor
    /// with unflushed writes closes.
    pub fn new(file: File, path: &Path, write: bool, durable: bool) -> io::Result<Self> {
        Self::new_metered(file, path, write, durable, None)
    }

    /// [`CachedFile::new`], charging `meter` the allocations caching the
    /// file makes, and the flushes of other files it evicts, before it
    /// makes them.
    pub fn new_metered(
        file: File,
        path: &Path,
        write: bool,
        durable: bool,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        Entry::new(cache(), file, path, write, durable, &mut charge(meter))
            .map(|entry| Self { entry })
    }

    /// The file's descriptor, held open until the lease drops.
    ///
    /// Fails when the file was replaced or removed while its descriptor was
    /// closed, or when a write was lost as a descriptor closed.
    pub fn lease(&self) -> io::Result<FileLease> {
        self.lease_metered(None)
    }

    /// [`CachedFile::lease`], charging `meter` the work the cache does for
    /// the lease before doing it: when the lease reopens the file, two reads
    /// (the open and the identity check) and the descriptor's allocation; a
    /// growth of the cache's ring; and a durability operation for each
    /// written file whose idle descriptor it evicts. A file whose flush the
    /// meter refuses keeps its descriptor.
    pub fn lease_metered(
        &self,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<FileLease> {
        self.entry
            .lease(&mut charge(meter))
            .map(|handle| FileLease { handle })
    }
}

/// Charges the cache's work to `meter`, when there is one.
fn charge(meter: Option<&dyn crate::NativeReadMeter>) -> impl FnMut(Cost) -> io::Result<()> {
    move |cost| {
        let Some(meter) = meter else {
            return Ok(());
        };
        meter.check()?;
        match cost {
            Cost::Reopen => {
                meter.admit_read(0)?;
                meter.admit_read(0)
            }
            Cost::Allocation(bytes) => {
                let bytes = bytes as u64;
                meter.allocate(bytes)?;
                // The cache keeps it, not the caller.
                meter.release(bytes);
                Ok(())
            }
            Cost::Flush => meter.admit_durability(),
        }
    }
}

/// A [`CachedFile`]'s descriptor, open while the lease lives.
///
/// Writes go through [`FileLease::write`], so that a descriptor never closes
/// with writes it has not flushed; [`FileLease::file`] is for reading.
pub struct FileLease {
    handle: Lease<Native>,
}

impl std::fmt::Debug for FileLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("FileLease").finish_non_exhaustive()
    }
}

impl FileLease {
    /// The descriptor, for reading.
    pub fn file(&self) -> &File {
        self.handle.file()
    }

    /// Runs `write`, which writes through the descriptor.
    pub fn write<R>(&self, write: impl FnOnce(&File) -> io::Result<R>) -> io::Result<R> {
        self.handle.write(write)
    }

    /// Runs `flush`, which makes every earlier write durable.
    pub fn flush<R>(&self, flush: impl FnOnce(&File) -> io::Result<R>) -> io::Result<R> {
        self.handle.flush(flush)
    }
}

/// Writes through a file at an explicit offset that advances with each
/// write. A reopened descriptor keeps no position, so writers of a
/// [`CachedFile`] name theirs.
#[derive(Debug)]
pub struct WriteAt<'a> {
    file: &'a File,
    offset: u64,
}

impl<'a> WriteAt<'a> {
    /// Writes to `file` starting at `offset`.
    pub fn new(file: &'a File, offset: u64) -> Self {
        Self { file, offset }
    }
}

impl io::Write for WriteAt<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        #[cfg(unix)]
        let written = std::os::unix::fs::FileExt::write_at(self.file, bytes, self.offset)?;
        #[cfg(windows)]
        let written = std::os::windows::fs::FileExt::seek_write(self.file, bytes, self.offset)?;
        #[cfg(not(any(unix, windows)))]
        let written = {
            use std::io::Seek as _;
            let mut file = self.file;
            file.seek(io::SeekFrom::Start(self.offset))?;
            io::Write::write(&mut file, bytes)?
        };
        self.offset = self
            .offset
            .checked_add(written as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
