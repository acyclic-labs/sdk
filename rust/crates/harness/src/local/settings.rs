use super::*;
use crate::native_settings::{NativeSettings, SettingsConfig};

pub(super) async fn load(
    project: &LocalProject,
    configured: Option<SettingsConfig>,
) -> Result<NativeSettings> {
    let mut configured = match configured {
        Some(configured) => configured,
        None => SettingsConfig::new(
            project.root(),
            dirs::home_dir().ok_or_else(|| {
                Error::Unsupported("user settings directory is unavailable".into())
            })?,
        ),
    };
    configured.load_mcp = cfg!(any(feature = "mcp-stdio", feature = "mcp-http"));
    NativeSettings::load(configured).await
}

#[cfg(feature = "hooks")]
pub(super) async fn capture_hooks(
    hooks: &crate::hooks::Hooks,
    publisher: &dyn ContentPublisher,
    limits: Limits,
) -> Result<crate::conversation::FileRef> {
    let prefix = "inputs/config/hooks/";
    if prefix.len() + 64 + ".json".len() > limits.path_bytes {
        return Err(Error::Invalid(
            "hook configuration path exceeds path bound".into(),
        ));
    }
    let bytes = hooks.encode(limits.file_bytes)?;
    let digest = blake3::hash(&bytes);
    let operation = OperationId::from_bytes(digest.as_bytes()[..16].try_into().unwrap());
    let reference = publisher
        .stage_once(
            operation,
            &format!("{prefix}{digest}.json"),
            &bytes,
            crate::hooks::Hooks::MEDIA_TYPE,
            "hooks.json",
            None,
        )?
        .await?
        .0;
    Ok(reference)
}
