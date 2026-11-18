use acyclic_fs::kernel::{Extent, ExtentKind};

pub(crate) fn hole(offset: u64, length: u64) -> Extent {
    Extent {
        offset,
        length,
        kind: ExtentKind::Hole,
    }
}

pub(crate) fn allocated(offset: u64, length: u64) -> Extent {
    Extent {
        offset,
        length,
        kind: ExtentKind::AllocatedZero,
    }
}
