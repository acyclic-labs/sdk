//! Shared workspace admission for machine adapters.

use crate::ProviderError;

/// Refuses snapshot operations until the transport has an atomic boundary
/// covering every writer, or an admitted immutable filesystem generation.
pub fn snapshot_unavailable() -> ProviderError {
    ProviderError::Unsupported(
        "atomic workspace snapshot is unavailable; recovery: use a backend with a provider snapshot boundary or an admitted immutable filesystem generation".into(),
    )
}

/// Validates a normalized absolute workspace directory below the filesystem root.
pub fn validate_workspace(workspace: &str) -> Result<(), ProviderError> {
    if !workspace.starts_with('/')
        || workspace == "/"
        || workspace.contains('\0')
        || workspace
            .split('/')
            .skip(1)
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(ProviderError::Invalid("workspace admission failed; recovery: configure a normalized absolute directory below /".into()));
    }
    Ok(())
}

/// POSIX single-quoted shell word.
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_root_must_be_a_normalized_directory() {
        for path in ["/", ".", "/a/..", "/a/../b", "//a", "/a/", "/a\0b"] {
            assert!(validate_workspace(path).is_err());
        }
        assert!(validate_workspace("/home/user/workspace").is_ok());
    }
}
