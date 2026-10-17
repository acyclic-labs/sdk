use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

/// A command in the calling task's view; workspace selection is host-owned.
/// Release 1 uses no OS sandbox: commands can reach the host filesystem.
/// Writes outside the task view are not captured. The cwd must remain in the view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BashInput {
    /// Bash source, interpreted without a login shell, inheriting the host environment.
    #[schemars(length(min = 1))]
    pub command: String,
    /// Optional directory relative to the task view. Parent traversal is forbidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Positive timeout in seconds. Omission uses 300 seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 4294967))]
    pub timeout: Option<u32>,
}

impl BashInput {
    /// Validates the command and relative directory before any native I/O.
    pub fn validate(&self) -> Result<()> {
        let cwd = Path::new(self.cwd.as_deref().unwrap_or(""));
        if self.command.is_empty()
            || self.command.contains('\0')
            || self.cwd.as_ref().is_some_and(|cwd| cwd.contains('\0'))
            || cwd
                .components()
                .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::Invalid(
                "invalid bash command, cwd or timeout".into(),
            ));
        }
        self.timeout_ms()?;
        Ok(())
    }

    pub(crate) fn timeout_ms(&self) -> Result<u32> {
        self.timeout
            .unwrap_or(300)
            .checked_mul(1000)
            .filter(|timeout| *timeout > 0)
            .ok_or_else(|| Error::Invalid("bash timeout is outside the native clock range".into()))
    }
}

#[cfg(test)]
mod tests;
