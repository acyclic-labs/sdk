//! Shared workspace admission for machine adapters.

use super::{ConsoleHistory, ExecSession, SandboxInstance, collect_success, owned_exec};
use crate::ProviderError;
use crate::{ExecCommand, MachineId};
use sha2::Digest;

fn owned_file_command(
    instance: &SandboxInstance,
    command: &ExecCommand,
    session: ExecSession,
) -> Result<crate::ExecStream, ProviderError> {
    owned_exec(
        MachineId::from_digest(sha2::Sha256::digest(instance.id.as_bytes()).into()),
        command,
        None,
        ConsoleHistory::default(),
        session,
    )
}

/// File download for transports exposing only exec. It has the same owned
/// cancellation, expiry and exit-code checks as a user command.
pub fn exec_read_file(
    instance: &SandboxInstance,
    path: &str,
    session: impl FnOnce(&ExecCommand, Option<Vec<u8>>) -> Result<ExecSession, ProviderError> + Send,
) -> impl Future<Output = Result<Vec<u8>, ProviderError>> + Send {
    let prepared = (|| {
        let command = ExecCommand::new(vec!["cat".into(), "--".into(), path.into()], 30_000)?;
        let session = session(&command, None)?;
        owned_file_command(instance, &command, session)
    })();
    async move { collect_success(prepared?, "guest file download").await }
}

/// File upload through exec, with binary stdin and restrictive creation mode.
pub fn exec_write_file(
    instance: &SandboxInstance,
    path: &str,
    data: &[u8],
    session: impl FnOnce(&ExecCommand, Option<Vec<u8>>) -> Result<ExecSession, ProviderError> + Send,
) -> impl Future<Output = Result<(), ProviderError>> + Send {
    let prepared = (|| {
        let command = ExecCommand::new(
            vec![
                "sh".into(),
                "-c".into(),
                format!("umask 077; cat > {}", shell_quote(path)),
            ],
            30_000,
        )?;
        let session = session(&command, Some(data.to_vec()))?;
        owned_file_command(instance, &command, session)
    })();
    async move {
        collect_success(prepared?, "guest file upload")
            .await
            .map(|_| ())
    }
}

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
