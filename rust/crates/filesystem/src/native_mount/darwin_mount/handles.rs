use super::*;

pub(super) struct FileHandle {
    pub(super) file_id: FileId,
    pub(super) file: Option<(
        Arc<dyn MountOpenFile>,
        Option<crate::demand::NativeMountAllocationCustody>,
    )>,
    pub(super) written: Arc<DarwinOwned<AtomicU64>>,
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
