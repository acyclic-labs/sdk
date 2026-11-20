//! Integration tests for this package.

#[cfg(all(loom, feature = "model-checking"))]
use acyclic_fs::{CancellationToken, OperationClaims};
#[cfg(all(loom, feature = "model-checking"))]
use acyclic_fs_format::{
    identity::{Digest, VolumeId},
    object_reference::{ObjectId, ObjectKind},
};
#[cfg(all(loom, feature = "model-checking"))]
mod sync {
    pub(crate) use loom::sync::{Mutex as InlineMutex, *};
}

#[cfg(feature = "model-checking")]
#[path = "model.rs"]
mod model;
#[path = "window_state.rs"]
mod window_state;
