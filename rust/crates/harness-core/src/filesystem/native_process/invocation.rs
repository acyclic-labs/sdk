use super::{Error, Result};
use acyclic_native_runtime::{NativeCommand, NativeHost};
use std::path::Path;

/// One shell command, composed for the shared native launch core.
#[doc(hidden)]
pub struct Invocation {
    /// The command's host configuration, owned by this one command.
    pub host: NativeHost,
    /// The composed command line, input and bounds.
    pub command: NativeCommand,
}

impl Invocation {
    /// Keeps the command's directory inside the task view and its
    /// executable resolved; the host checks its own directory and environment.
    pub(super) fn validate(&self, root: &Path) -> Result<()> {
        let cwd = &self.host.cwd;
        if self.host.validate().is_err()
            || !root.is_absolute()
            || !cwd.starts_with(root)
            || cwd
                .components()
                .chain(root.components())
                .any(|part| matches!(part, std::path::Component::ParentDir))
            || !self
                .command
                .argv
                .first()
                .is_some_and(|program| Path::new(program).is_absolute())
        {
            return Err(Error::Invalid(
                "invalid or changed native process invocation".into(),
            ));
        }
        Ok(())
    }
}
