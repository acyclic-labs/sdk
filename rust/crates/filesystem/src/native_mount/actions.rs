use crate::access::Action;
#[cfg(any(windows, test))]
use crate::access::PathTrie;

/// Source and destination requirements shared by native mutation adapters.
#[derive(Clone, Copy)]
pub(crate) enum Mutation {
    Create,
    Delete,
    Rename,
    HardLink,
}

impl Mutation {
    pub(crate) const fn actions(self) -> (Action, Action) {
        match self {
            Self::Create => (Action::Create, Action::Create),
            Self::Delete => (Action::Delete, Action::Delete),
            Self::Rename => (Action::Rename, Action::Rename),
            Self::HardLink => (Action::Admin, Action::Admin),
        }
    }
}

#[cfg(any(windows, test))]
pub(crate) fn require_projfs_mode(
    view: &PathTrie,
    root: &crate::kernel::NamespacePath,
    writable: bool,
    now: u64,
    work: &crate::NativeMaterializationWork,
) -> Result<(), super::NativeMountError> {
    if !writable {
        return Err(super::NativeMountError::Lifecycle(
            "ProjFS cannot prevent host mutations on read-only mounts",
        ));
    }
    if view.valid_until().is_some()
        || view.generation_sensitive()
        || !view.allows_native_subtree(
            root,
            &[Action::Create, Action::Write, Action::Append],
            now,
            work,
        )?
    {
        return Err(super::NativeMountError::Lifecycle(
            "ProjFS cannot enforce restrictive writable policies before host effects",
        ));
    }
    Ok(())
}
