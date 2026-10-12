/// A byte range admitted for one native read: at most one MiB long and
/// addressable without overflow.
#[derive(Clone, Copy)]
struct ReadRange {
    offset: u64,
    length: u64,
}

impl ReadRange {
    fn new(offset: u64, length: u64) -> Result<Self, DemandError> {
        if length > 1024 * 1024 || offset.checked_add(length).is_none() {
            return Err(DemandError::InvalidRequest);
        }
        Ok(Self { offset, length })
    }
}

/// A regular source file held open after its version was proven against
/// the opened file itself. The type cannot exist without that proof.
///
/// Reads use the held file and then prove, exactly as a path-based read does, that
/// the file is unmodified, that its source path still names it, and that
/// the source root and reference are still current.
#[derive(Clone)]
struct NativeDemandFile {
    provider: NativeDemandSource,
    source: SourceReference,
    relative: PathBuf,
    expected: SourceVersion,
    logical_bytes: u64,
    #[cfg(windows)]
    file: Arc<std::sync::RwLock<Arc<SourceFile>>>,
    #[cfg(not(windows))]
    file: Arc<SourceFile>,
}

/// A source file held for reading. On Windows it gives way to whoever
/// renames or deletes it, as an editor saving over it does, instead of
/// making that fail while it is read.
#[cfg(windows)]
type SourceFile = crate::native_host::YieldingFile;
#[cfg(not(windows))]
type SourceFile = std::fs::File;

/// How long a read of a file under a root that is not local may take
/// before its caller is answered, well inside a native callback's own
/// deadline: a callback thread runs the read and cannot be interrupted.
const REMOTE_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);
/// Most reads of files under roots that are not local that may be
/// blocked at once: a hung share holds one thread per read until the host
/// gives up, and beyond this every such read fails at once.
const MAXIMUM_BLOCKED_REMOTE_READS: usize = 16;
static BLOCKED_REMOTE_READS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl NativeDemandFile {
    fn open(
        provider: NativeDemandSource,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> Result<Self, DemandError> {
        provider.check(source, cancellation)?;
        provider.observe_parent(path)?;
        let relative = provider.relative(path)?;
        #[cfg(windows)]
        let file = provider.inner.root.open_file_yielding(&relative);
        #[cfg(not(windows))]
        let file = provider
            .inner
            .root
            .open_file(&relative)
            .map(cap_std::fs::File::into_std);
        let file = match file {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DemandError::Absent);
            }
            Err(error) => return Err(error.into()),
        };
        let opened = stat_held(&provider, &file)?;
        if !opened.file_type().is_file() {
            return Err(DemandError::NotRegularFile);
        }
        if version(&opened) != expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(Self {
            provider,
            source,
            relative,
            expected,
            logical_bytes: opened.len(),
            #[cfg(windows)]
            file: Arc::new(std::sync::RwLock::new(Arc::new(file))),
            #[cfg(not(windows))]
            file: Arc::new(file),
        })
    }

    fn read(
        &self,
        range: ReadRange,
        cancellation: &CancellationToken,
        work: &mut WorkCounters,
    ) -> Result<Bytes, DemandError> {
        #[cfg(windows)]
        let file = self
            .file
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        #[cfg(not(windows))]
        let file = Arc::clone(&self.file);
        let result = self.read_once(&file, range, cancellation, work);
        #[cfg(windows)]
        if matches!(result, Err(DemandError::StaleVersion)) && file.was_yielded() {
            // A benign opener can break a handle oplock too. Reopen only
            // at the exact expected version; a save that replaced the
            // file still reports stale to the caller.
            let reopened = Arc::new(self.reopen(cancellation)?);
            let current = {
                let mut held = self
                    .file
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if Arc::ptr_eq(&*held, &file) {
                    *held = reopened;
                }
                Arc::clone(&*held)
            };
            return self.read_once(&current, range, cancellation, work);
        }
        result
    }

    #[cfg(windows)]
    fn reopen(&self, cancellation: &CancellationToken) -> Result<SourceFile, DemandError> {
        self.provider.check(self.source, cancellation)?;
        let file = match self.provider.inner.root.open_file_yielding(&self.relative) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DemandError::StaleVersion);
            }
            Err(error) => return Err(error.into()),
        };
        let opened = stat_held(&self.provider, &file)?;
        if version(&opened) != self.expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(file)
    }

    fn read_once(
        &self,
        file: &SourceFile,
        ReadRange { offset, length }: ReadRange,
        cancellation: &CancellationToken,
        work: &mut WorkCounters,
    ) -> Result<Bytes, DemandError> {
        let length = length.min(self.logical_bytes.saturating_sub(offset));
        let mut bytes = vec![0; usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?];
        let mut filled = 0;
        while let Some(unfilled) = bytes.get_mut(filled..).filter(|rest| !rest.is_empty()) {
            match read_at(file, offset + filled as u64, unfilled) {
                Ok(0) => break,
                Ok(count) => filled += count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error.into()),
            }
        }
        bytes.truncate(filled);
        work.source_bytes_read = work
            .source_bytes_read
            .checked_add(filled as u64)
            .ok_or(DemandError::InvalidRequest)?;
        self.prove_current(file, cancellation)?;
        work.output_bytes = filled as u64;
        Ok(Bytes::from(bytes))
    }

    /// What the file's name holds now. It is only compared with the file
    /// held beneath the root, which nothing a name resolves elsewhere can
    /// match, so one lookup through the root's path serves, where a walk
    /// held beneath the root costs several.
    #[cfg(not(windows))]
    fn named_stat(&self) -> std::io::Result<crate::native_host::HostStat> {
        std::fs::symlink_metadata(self.provider.inner.path.join(&self.relative))
            .map(cap_std::fs::Metadata::from_just_metadata)
    }

    /// What the file's name holds now, by one query against the held root.
    #[cfg(windows)]
    fn named_stat(&self) -> std::io::Result<crate::native_host::HostStat> {
        self.provider.inner.root.stat(&self.relative)
    }

    /// Proves the bytes just read belong to the opened version: the held
    /// file is unmodified (in-place writes), the source path still names
    /// it (replacement by rename), and the root and reference are current.
    fn prove_current(
        &self,
        file: &SourceFile,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        let held = stat_held(&self.provider, file)?;
        let named = self.named_stat();
        if version(&held) != self.expected
            || named.as_ref().map(version).ok() != Some(self.expected)
        {
            // A replaced root renames every file under it: report that.
            self.provider.check(self.source, cancellation)?;
            return Err(DemandError::StaleVersion);
        }
        self.provider.check(self.source, cancellation)
    }
}

impl DemandFile for NativeDemandFile {
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        let range = ReadRange::new(offset, length).map_err(OperationFailure::before_work)?;
        // A local root answers promptly, so its read runs in place; any
        // other runs on a thread of its own, bounded by a deadline.
        if self.provider.inner.inline {
            return measured(|work| self.read(range, cancellation, work));
        }
        self.read_remote(range, cancellation)
    }
}

impl NativeDemandFile {
    fn read_remote(
        &self,
        range: ReadRange,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        use std::sync::atomic::Ordering;

        let unavailable = || OperationFailure::before_work(DemandError::SourceUnavailable);
        if BLOCKED_REMOTE_READS.fetch_add(1, Ordering::AcqRel) >= MAXIMUM_BLOCKED_REMOTE_READS {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
            return Err(unavailable());
        }
        let (file, cancellation) = (self.clone(), cancellation.clone());
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        let spawned = std::thread::Builder::new()
            .name("acyclic-remote-read".to_owned())
            .spawn(move || {
                let read = measured(|work| file.read(range, &cancellation, work));
                BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
                let _ = answer.send(read);
            });
        if spawned.is_err() {
            BLOCKED_REMOTE_READS.fetch_sub(1, Ordering::AcqRel);
            return Err(unavailable());
        }
        answered
            .recv_timeout(REMOTE_READ_DEADLINE)
            .unwrap_or_else(|_| Err(unavailable()))
    }
}

#[cfg(unix)]
fn read_at(file: &std::fs::File, offset: u64, destination: &mut [u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, destination, offset)
}

#[cfg(windows)]
fn read_at(file: &SourceFile, offset: u64, destination: &mut [u8]) -> std::io::Result<usize> {
    file.read_at(offset, destination)
}

/// Stats the held `file` as its source root stats a name.
#[cfg(windows)]
fn stat_held(
    provider: &NativeDemandSource,
    file: &SourceFile,
) -> std::io::Result<crate::native_host::HostStat> {
    file.with(|file| provider.inner.root.stat_file(file))
}

/// Stats the held `file` as its source root stats a name.
#[cfg(not(windows))]
fn stat_held(
    provider: &NativeDemandSource,
    file: &SourceFile,
) -> std::io::Result<crate::native_host::HostStat> {
    provider.inner.root.stat_file(file)
}
