//! Platform shell contracts shared by model tools, policy routing and execution.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub use acyclic_host_platform::shell::{PLATFORM, ShellSpec};

/// Source or literal argv, executed by the same target-owned process engine.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(untagged)]
pub enum ShellCommand {
    /// Source in the selected target's command language.
    Source(String),
    /// Executable followed by literal arguments.
    Argv(Vec<String>),
}

/// A stdout line window applied to original chunks before tail retention.
/// This selects presentation only; it never stops or restarts the process.
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct OutputLines {
    /// Complete stdout lines to skip, independent of stderr.
    pub offset: u64,
    /// Lines to retain; omission selects through the original EOF.
    pub limit: Option<u64>,
}

impl OutputLines {
    /// Advances this window over one original stdout chunk without allocation.
    /// Newlines complete lines, so a split line uses no extra retained buffer.
    pub fn select<'a>(&mut self, mut bytes: &'a [u8]) -> &'a [u8] {
        if self.limit == Some(0) {
            return &bytes[..0];
        }
        while self.offset > 0 {
            let Some(end) = bytes.iter().position(|byte| *byte == b'\n') else {
                return &bytes[bytes.len()..];
            };
            bytes = &bytes[end + 1..];
            self.offset -= 1;
        }
        if let Some(remaining) = &mut self.limit {
            for (index, byte) in bytes.iter().enumerate() {
                if *byte == b'\n' {
                    *remaining -= 1;
                    if *remaining == 0 {
                        return &bytes[..index + 1];
                    }
                }
            }
        }
        bytes
    }
}

impl From<String> for ShellCommand {
    fn from(source: String) -> Self {
        Self::Source(source)
    }
}
impl From<&str> for ShellCommand {
    fn from(source: &str) -> Self {
        Self::Source(source.into())
    }
}
impl ShellCommand {
    /// Prepares one process invocation. Literal arguments never pass through a
    /// command language, including on Windows PowerShell 5.1.
    pub fn prepare(
        self,
        shell: &ShellSpec,
    ) -> Result<(Option<String>, Vec<String>, Option<Vec<u8>>)> {
        if !self.valid() {
            return Err(Error::Invalid(
                "shell command is empty or contains NUL".into(),
            ));
        }
        Ok(match self {
            Self::Source(source) => {
                let (arguments, stdin) = shell.prepare(source);
                (None, arguments.collect(), stdin)
            }
            Self::Argv(mut argv) => {
                let program = argv.remove(0);
                (Some(program), argv, None)
            }
        })
    }
    fn valid(&self) -> bool {
        match self {
            Self::Source(source) => !source.is_empty() && !source.contains('\0'),
            Self::Argv(argv) => {
                argv.first().is_some_and(|program| !program.is_empty())
                    && argv.iter().all(|argument| !argument.contains('\0'))
            }
        }
    }
}

/// Shared command capability description. Native profiles supply their lab's
/// model-visible name and schema; execution keeps the selected target's
/// command language and path style from the conversation's roster.
pub const DESCRIPTION: &str = "Run a command on an execution target, in that target's listed shell language and path style. `target` names a target from the roster (the latest targets.json announced in the conversation), or user:<owner> for that user's home device; omitted, the command runs on your home target. The command runs in the calling task's composed workspace view; writes outside it are not captured. No OS sandbox in release 1";
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
    /// Shell source or literal argv. Only explicitly supplied ordinary process
    /// variables and volume discovery hints are available; host settings are not inherited.
    pub command: ShellCommand,
    /// Optional directory relative to the task view. Parent traversal is forbidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Optional positive lifetime in milliseconds. Omission is unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub timeout_ms: Option<u32>,
    /// Target from the roster, or user:<owner> for that user's home device.
    /// Omission runs on the agent's home target; there is no fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub target: Option<String>,
    /// Internal query presentation selection over the original stdout stream.
    /// Native declarations supply their own argument names and schemas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(skip)]
    pub output_lines: Option<OutputLines>,
}

impl ShellInput {
    /// Validates the command and relative directory before any native I/O.
    pub fn validate(&self) -> Result<()> {
        let cwd = Path::new(self.cwd.as_deref().unwrap_or(""));
        if !self.command.valid()
            || self.cwd.as_ref().is_some_and(|cwd| cwd.contains('\0'))
            || cwd
                .components()
                .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::Invalid(
                "invalid shell command, cwd, target or timeout".into(),
            ));
        }
        self.timeout_ms()?;
        Ok(())
    }

    pub(crate) fn timeout_ms(&self) -> Result<Option<u32>> {
        if self.timeout_ms == Some(0) {
            return Err(Error::Invalid("shell lifetime must be positive; recovery: omit timeout_ms for an unbounded command or supply a positive duration".into()));
        }
        Ok(self.timeout_ms)
    }
}

#[cfg(test)]
mod tests;
