use super::*;

pub(super) fn staging(directory: &Path) -> Result<PathBuf> {
    let path = directory.join(format!(
        "project-initializing-{}",
        crate::identity::fresh_uuid()
    ));
    // Exclusive creation establishes this attempt's custody.
    acyclic_native_runtime::private_file::create_directory(&path).map_err(storage)?;
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

#[cfg(test)]
#[cfg(loom)]
mod loom_tests;
#[cfg(test)]
#[cfg(not(loom))]
mod tests;
