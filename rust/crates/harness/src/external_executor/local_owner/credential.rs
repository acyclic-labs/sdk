//! Workspace selection is authenticated independently of task request contents.
use super::*;

pub(super) fn workspace(path: &Path) -> Result<(String, PathBuf)> {
    let canonical = std::fs::canonicalize(path).map_err(storage)?;
    let bytes = canonical.as_os_str().as_encoded_bytes();
    Ok((blake3::hash(bytes).to_hex().to_string(), canonical))
}

pub(super) fn credential(secret: &[u8; 32], workspace: &str) -> String {
    format!(
        "{workspace}.{}",
        blake3::keyed_hash(secret, workspace.as_bytes()).to_hex()
    )
}

pub(super) fn verify(secret: &[u8; 32], credential: &str) -> Result<String> {
    let (workspace, digest) = credential
        .split_once('.')
        .ok_or_else(|| Error::Unauthorized("missing local owner scope".into()))?;
    if workspace != "owner" && super::super::handoff::workspace_key(workspace).is_err() {
        return Err(Error::Unauthorized("invalid local owner scope".into()));
    }
    let actual = blake3::Hash::from_hex(digest).map_err(storage)?;
    if actual != blake3::keyed_hash(secret, workspace.as_bytes()) {
        return Err(Error::Unauthorized("local owner credential differs".into()));
    }
    Ok(workspace.into())
}
