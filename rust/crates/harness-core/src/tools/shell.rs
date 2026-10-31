//! Platform shell contracts shared by model tools, policy routing and execution.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub use acyclic_host_platform::shell::{PLATFORM, ShellSpec};

/// Prepares the bounded model presentation before a backend commits its output.
/// A streaming backend uses the resulting omission count to retain a complete
/// spill even when the wire encoding, rather than the raw tail, needs truncation.
pub fn bounded_result(
    result: crate::tool::ToolResult,
    maximum: u64,
) -> Result<crate::tool::ToolResult> {
    super::bounds::result(PLATFORM.name, result, maximum)
}

pub(crate) struct ShellProjection;

impl crate::tool::ToolProjection for ShellProjection {
    fn project(
        &self,
        _invocation: &crate::tool::ToolInvocation,
        result: &crate::tool::ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(serde_json::Value, crate::NativeCustody)> {
        let text = result
            .value
            .get("text")
            .or_else(|| result.value.get("error"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Error::Invalid(
                    "shell presentation is absent; recovery: bind the shared workspace executor"
                        .into(),
                )
            })?;
        crate::tool::schema::project_text(text, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining()?;
        }
        Ok(())
    }
}

pub(super) fn presentation(fields: &serde_json::Map<String, serde_json::Value>) -> String {
    use std::fmt::Write;
    if let Some(error) = fields.get("error").and_then(serde_json::Value::as_str) {
        return format!("Tool failed: {error}");
    }
    let exit = fields
        .get("exit_code")
        .and_then(serde_json::Value::as_i64)
        .map_or_else(|| "unavailable".into(), |code| code.to_string());
    let stdout = fields
        .get("stdout")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let stderr = fields
        .get("stderr")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let mut text = format!("Exit code: {exit}\n{stdout}{stderr}");
    if let Some(message) = fields
        .get("stop")
        .and_then(|stop| stop.get("message"))
        .and_then(serde_json::Value::as_str)
    {
        let _ = write!(text, "\n{message}");
    }
    if let Some(omitted) = fields
        .get("omitted_bytes")
        .and_then(serde_json::Value::as_u64)
        .filter(|omitted| *omitted > 0)
    {
        let _ = write!(text, "\n[{omitted} bytes omitted; showing the tail");
        if let Some(spill) = fields.get("spill").and_then(serde_json::Value::as_str) {
            let _ = write!(text, "; complete output: {spill}");
        }
        text.push(']');
    }
    text
}

/// A command in the calling task's view; workspace selection is host-owned.
/// Release 1 uses no OS sandbox: commands can reach the host filesystem.
/// Writes outside the task view are not captured. The cwd must remain in the view.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct ShellInput {
    /// Source for the platform shell. Only explicitly supplied ordinary process
    /// variables and volume discovery hints are available; host settings are not inherited.
    #[schemars(length(min = 1))]
    pub command: String,
    /// Optional directory relative to the task view. Parent traversal is forbidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Timeout in milliseconds, from 1 to 300000. Omission uses 300000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 300000))]
    pub timeout_ms: Option<u32>,
}

impl ShellInput {
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
                "invalid shell command, cwd or timeout".into(),
            ));
        }
        self.timeout_ms()?;
        Ok(())
    }

    pub(crate) fn timeout_ms(&self) -> Result<u32> {
        Some(self.timeout_ms.unwrap_or(300_000))
            .filter(|timeout| (1..=300_000).contains(timeout))
            .ok_or_else(|| Error::Invalid("shell timeout_ms must be in 1..=300000; recovery: choose a timeout_ms in that range".into()))
    }
}

#[cfg(test)]
mod tests;
