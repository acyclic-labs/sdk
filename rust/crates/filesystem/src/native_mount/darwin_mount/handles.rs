use super::*;

pub(super) struct FileHandle {
    pub(super) file_id: FileId,
    /// Bound to the source file on first use: an open that is only closed, as a read served from the kernel's cache is, never opens the source.
    pub(super) file: Option<(
        Arc<dyn MountOpenFile>,
        Option<crate::demand::NativeMountAllocationCustody>,
    )>,
    pub(super) observation: Arc<DarwinOwned<Mutex<FileObservation>>>,
    /// Ledger sequence of the latest mutation through this handle; zero until one completes.
    pub(super) written: Arc<DarwinOwned<AtomicU64>>,
}

pub(super) struct FileObservation {
    pub(super) stamp: Option<CacheStamp>,
    pub(super) lookup: MountLookup,
}

// Sampled before source observations, including locally forgotten views.
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

pub(super) struct FileTarget {
    pub(super) file: (
        Arc<dyn MountOpenFile>,
        Option<crate::demand::NativeMountAllocationCustody>,
    ),
    pub(super) written: Option<Arc<DarwinOwned<AtomicU64>>>,
}
