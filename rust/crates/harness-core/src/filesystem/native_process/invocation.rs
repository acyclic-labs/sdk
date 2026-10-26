use super::{Error, Result};
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

/// Owned process input of one shell command.
#[doc(hidden)]
pub struct Invocation {
    pub executable: std::path::PathBuf,
    pub argv: Vec<String>,
    pub cwd: std::path::PathBuf,
    pub environment: std::collections::BTreeMap<String, String>,
    pub inherit_environment: bool,
    pub timeout_ms: u32,
    pub control_timeout_ms: u32,
    pub maximum_output_bytes: usize,
    pub standard_input: Option<Vec<u8>>,
    /// Owner custody that ties the process tree to this host and fences it
    /// on reopen; `None` spawns an ordinary owned tree.
    pub process_custody: Option<std::sync::Arc<acyclic_native_runtime::ProcessCustody>>,
}
