use super::*;

pub(super) fn staging(directory: &Path) -> Result<PathBuf> {
    let path = directory.join(format!("project-initializing-{}", uuid::Uuid::new_v4()));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    // Exclusive creation establishes this attempt's custody. Never remove or
    // reuse another attempt, including an abandoned pre-publication directory.
    builder.create(&path).map_err(storage)?;
    Ok(path)
}

// The publish callback is the platform's atomic no-replace rename. Models
// substitute that one atomic filesystem primitive, not a second algorithm.
pub(super) async fn initialize(
    prepare: impl Future<Output = Result<()>>,
    publish: impl FnOnce() -> std::io::Result<()>,
    discard: impl FnOnce() -> Result<()>,
) -> Result<()> {
    prepare.await?;
    match publish() {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => discard(),
        Err(error) => Err(storage(error)),
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod loom_tests;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
