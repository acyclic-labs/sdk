//! Whether a failed callback invalidates the invocation retained by its session.
//!
//! A reply status cannot answer this: an ordinary policy refusal and a revoked
//! installed callback guard both report access denied, but only the latter
//! invalidates the session. Post-effect notification custody is separate: an
//! acknowledged physical mutation must never be discarded as a reply refusal.
use super::{CallbackFailure, MountSourceError, NativeMountError};

pub(crate) fn callback_is_terminal(error: &NativeMountError) -> bool {
    match error {
        NativeMountError::Source(error) => source_is_terminal(error),
        NativeMountError::Demand(error) => demand_is_terminal(error),
        NativeMountError::Materialization(error) => materialization_is_terminal(error),
        NativeMountError::Io(_) => false,
        _ => true,
    }
}

pub(crate) fn source_is_terminal(mut error: &MountSourceError) -> bool {
    // Walk the primary chain without recursion or a replacement allocation.
    // Its reply status does not erase any failed completion accounting.
    while let MountSourceError::MutationFinalization {
        primary, cleanup, ..
    } = error
    {
        if cleanup.is_some() {
            return true;
        }
        let Some(primary) = primary.as_ref() else {
            return true;
        };
        error = primary;
    }
    match error {
        MountSourceError::NativeWork(error) => materialization_is_terminal(error),
        MountSourceError::WorkspaceCause(error) => workspace_is_terminal(error),
        MountSourceError::Filesystem(error) => filesystem_is_terminal(error),
        MountSourceError::NamespacePath(error) => matches!(
            error,
            crate::kernel::NamespacePathError::Work(_)
                | crate::kernel::NamespacePathError::Cancelled
                | crate::kernel::NamespacePathError::AllocationFailed
        ),
        MountSourceError::AccessDenied
        | MountSourceError::ReadOnly
        | MountSourceError::NotFound
        | MountSourceError::AlreadyExists
        | MountSourceError::NotDirectory
        | MountSourceError::IsDirectory
        | MountSourceError::DirectoryNotEmpty
        | MountSourceError::Invalid(_)
        | MountSourceError::Unsupported(_)
        | MountSourceError::NativeUnsupported
        | MountSourceError::Io(_, _)
        | MountSourceError::Stale => false,
        _ => true,
    }
}

fn materialization_is_terminal(error: &crate::NativeTreeMaterializationError) -> bool {
    use crate::NativeTreeMaterializationError as E;
    matches!(
        error,
        E::Work(_)
            | E::Cancelled
            | E::OriginalWorkPoisoned
            | E::MissingCustody
            | E::AllocationFailed(_)
            | E::ExternalMutation
            | E::AllocationChanged { .. }
    )
}

fn filesystem_is_terminal(error: &crate::FsError) -> bool {
    if let crate::FsError::NativeKernel(error) = error {
        return materialization_is_terminal(error);
    }
    matches!(
        super::filesystem_failure(error),
        CallbackFailure::Io | CallbackFailure::OutOfMemory | CallbackFailure::Cancelled
    )
}

fn workspace_is_terminal(error: &crate::WorkspaceError) -> bool {
    if let crate::WorkspaceError::Filesystem(error) = error {
        return filesystem_is_terminal(error);
    }
    matches!(
        super::workspace_failure(error),
        CallbackFailure::Io | CallbackFailure::OutOfMemory | CallbackFailure::Cancelled
    )
}

fn demand_is_terminal(error: &crate::DemandError) -> bool {
    use crate::DemandError as E;
    match error {
        E::Mount(error) => source_is_terminal(error),
        E::Io(_)
        | E::StaleSource
        | E::StaleVersion
        | E::StaleCursor
        | E::CursorBusy
        | E::Absent
        | E::NotRegularFile
        | E::NotDirectory
        | E::InvalidRequest => false,
        _ => true,
    }
}
