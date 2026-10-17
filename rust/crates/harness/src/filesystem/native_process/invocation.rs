use super::{Error, NativeProcessRequest, Result};
use std::{collections::BTreeMap, path::Path};

pub(super) fn validate(
    executable: &Path,
    cwd: &Path,
    root: &Path,
    environment: &BTreeMap<String, String>,
    argv: &[String],
) -> Result<()> {
    if !executable.is_absolute()
        || !cwd.is_absolute()
        || !root.is_absolute()
        || !cwd.starts_with(root)
        || cwd
            .components()
            .chain(root.components())
            .any(|part| matches!(part, std::path::Component::ParentDir))
        || environment
            .iter()
            .any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
        || argv.iter().any(|arg| arg.contains('\0'))
    {
        return Err(Error::Invalid(
            "invalid or changed native process invocation".into(),
        ));
    }
    #[cfg(windows)]
    if environment
        .keys()
        .map(|key| key.to_uppercase())
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != environment.len()
    {
        return Err(Error::Invalid(
            "duplicate native environment key under Windows case folding".into(),
        ));
    }
    Ok(())
}

/// Owned process input, shared by approved effects and task workspace commands.
pub(crate) struct Invocation {
    pub executable: std::path::PathBuf,
    pub argv: Vec<String>,
    pub cwd: std::path::PathBuf,
    pub environment: std::collections::BTreeMap<String, String>,
    pub inherit_environment: bool,
    pub timeout_ms: u32,
    pub control_timeout_ms: u32,
    pub cancellation_poll_ms: u32,
    pub maximum_output_bytes: usize,
    pub mcp_stdio: Option<crate::mcp::stdio::McpStdioRequest>,
    pub standard_input: Option<Vec<u8>>,
}

impl From<&NativeProcessRequest> for Invocation {
    fn from(request: &NativeProcessRequest) -> Self {
        Self {
            executable: request.executable.clone(),
            argv: request.argv.clone(),
            cwd: request.cwd.clone(),
            environment: request.environment.clone(),
            inherit_environment: false,
            timeout_ms: request.timeout_ms,
            control_timeout_ms: request.control_timeout_ms,
            cancellation_poll_ms: request.cancellation_poll_ms,
            maximum_output_bytes: request.maximum_output_bytes as usize,
            mcp_stdio: request.mcp_stdio.clone(),
            standard_input: request.standard_input.clone(),
        }
    }
}
