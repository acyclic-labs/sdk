//! Home settings modules written through the harness's own file grants.

use crate::{
    IdempotencyKey, LocalHarness, Result,
    conversation::{ContentAccess, ContentGrant, VolumeOperation},
};

/// Writes a home settings module, returning its version.
pub(in crate::local) async fn write(
    harness: &LocalHarness,
    path: &str,
    source: &str,
) -> Result<String> {
    let grant = ContentGrant::verify(
        &harness.verifier,
        &harness.signed,
        &harness.scratch,
        VolumeOperation::Write,
        &mut None,
    )?;
    let file = harness
        .files
        .put_content(
            &harness.scratch,
            &grant,
            path,
            source.as_bytes(),
            "text/typescript",
            path.rsplit('/').next().unwrap_or(path),
            None,
            &IdempotencyKey::new(format!(
                "settings:{path}:{}",
                blake3::hash(source.as_bytes())
            ))?,
            None,
            ContentAccess::default(),
        )
        .await?;
    Ok(file.version().to_owned())
}
