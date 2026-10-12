use super::*;

pub(super) struct FileHandle {
    pub(super) file_id: FileId,
    /// Bound to the source file on first use: an open that is only closed,
    /// as a read served from the kernel's cache is, never opens the source.
    pub(super) file: Option<Arc<dyn MountOpenFile>>,
    pub(super) observation: Arc<Mutex<FileObservation>>,
    /// Ledger sequence of the latest mutation through this handle; zero
    /// until one completes.
    pub(super) written: Arc<AtomicU64>,
}

pub(super) struct FileObservation {
    pub(super) stamp: Option<CacheStamp>,
    pub(super) lookup: MountLookup,
}

/// When this mount sampled the source's view, before observing facts that
/// a later callback may reuse: the source's stamp, and how many changes the
/// source did not record the mount had been told of by then (see
/// [`DarwinMountContext::forget_view`]). Ordered by age.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CacheStamp {
    pub(super) forgotten: u64,
    pub(super) view: ViewStamp,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct CacheEpochs {
    pub(super) view: CacheStamp,
    pub(super) binding: u64,
}

#[derive(Clone)]
pub(super) struct DirectoryHandle {
    pub(super) path: MountPath,
    pub(super) binding_epoch: Option<u64>,
    pub(super) epochs: Option<CacheEpochs>,
    pub(super) cursor: Option<Vec<u8>>,
    pub(super) entries: VecDeque<MountDirectoryEntry>,
    pub(super) exhausted: bool,
    pub(super) emitted: i64,
    pub(super) revision: Option<u64>,
}

pub(super) struct DirectoryCheckpoint {
    pub(super) revision: u64,
    pub(super) handle: DirectoryHandle,
}

impl DirectoryHandle {
    pub(in crate::native_mount) fn new(path: MountPath, binding_epoch: Option<u64>, epochs: Option<CacheEpochs>) -> Self {
        Self {
            path,
            binding_epoch,
            epochs,
            cursor: None,
            entries: VecDeque::new(),
            exhausted: false,
            emitted: 0,
            revision: None,
        }
    }

    pub(in crate::native_mount) fn rewind(&mut self) {
        self.cursor = None;
        self.entries.clear();
        self.exhausted = false;
        self.emitted = 0;
        self.revision = None;
    }

    /// Whether buffered pages still describe the directory: its binding is
    /// the same and nothing has changed its listing since they were read.
    pub(in crate::native_mount) fn is_current(&self, context: &DarwinMountContext) -> bool {
        match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable()
                    && context.source.binding_epoch() == Some(epochs.binding)
                    && context.unchanged_since(&self.path, None, epochs.view)
            }
            None => context.cache_epochs().is_none(),
        }
    }

    pub(super) const fn can_reuse_pages(&self) -> bool {
        self.epochs.is_some()
    }

    /// Keeps the listing in the current view, or fails it with `ESTALE`. A
    /// listing that has delivered nothing but `.` and `..` (which no view
    /// changes) starts over in the view current now, as if just opened: a
    /// change to the source while a directory is being opened or first read
    /// never fails the listing. Only one already delivering entries from an
    /// older view fails, rather than mixing two views in one listing.
    pub(in crate::native_mount) fn keep_current(&mut self, context: &DarwinMountContext) -> Result<(), i32> {
        for _ in 0..MAXIMUM_LISTING_RESTARTS {
            if !self.can_reuse_pages() || self.is_current(context) {
                return Ok(());
            }
            if self.emitted > 2 || context.source.binding_epoch() != self.binding_epoch {
                return Err(libc::ESTALE);
            }
            self.epochs = context.cache_epochs();
            self.cursor = None;
            self.entries.clear();
            self.exhausted = false;
            self.revision = None;
        }
        Err(libc::ESTALE)
    }
}

pub(super) struct FileTarget {
    pub(super) file: Arc<dyn MountOpenFile>,
    pub(super) written: Option<Arc<AtomicU64>>,
}

#[derive(Clone, Copy)]
pub(super) enum InitialHandleState {
    Clean,
    /// Created by this callback: unpublished until a sync covers it.
    Written,
}

