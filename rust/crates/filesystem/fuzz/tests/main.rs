//! Filesystem fuzz oracles share one executable.

#[cfg(loom)]
use acyclic_fs::{
    CancellationToken, Digest, FsError, ObjectId, ObjectKind, OperationClaims, VolumeId, WorkError,
    kernel,
};

// Included production sources resolve the filesystem facade from the crate root.
#[cfg(loom)]
mod sync {
    pub(crate) use loom::sync::{Mutex as InlineMutex, *};
}

#[cfg(loom)]
mod model;

#[cfg(not(loom))]
mod window_state;
