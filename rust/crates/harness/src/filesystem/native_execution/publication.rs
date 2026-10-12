use super::super::native_process::NativeProcessPublishedVolume;
use crate::{Error, conversation::FileRef};

/// Publication failure retaining the SDK acknowledgement and completed prefix.
#[derive(Debug)]
pub struct NativeViewPublicationError {
    /// Original SDK failure, including acknowledgement and accounting.
    pub cause: acyclic_fs::native_mount::NativeMountPublicationFailure<Error>,
    /// Snapshots returned before this failure.
    pub published: Vec<NativeProcessPublishedVolume>,
    /// Output retained before publication.
    pub diagnostic: Option<FileRef>,
    /// Original allocation ownership.
    pub custody: Option<acyclic_fs::NativeAllocation>,
}
impl std::fmt::Display for NativeViewPublicationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(formatter)
    }
}
impl std::error::Error for NativeViewPublicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

pub(super) fn failure(
    cause: Error,
) -> acyclic_fs::native_mount::NativeMountPublicationFailure<Error> {
    acyclic_fs::native_mount::NativeMountPublicationFailure {
        cause,
        accounting_failure: None,
        source_acknowledged: false,
        publication: None,
    }
}
