use super::*;

/// Tool output directory on an agent's private volume.
pub const TOOL_OUTPUT: &str = "tools";

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// The `fs:/` resource of [`TOOL_OUTPUT`] on `volume`.
    ///
    /// # Errors
    /// When the volume cannot be opened.
    pub async fn tool_resource(&self, volume: &VolumeRef) -> Result<String> {
        let reference = super::bounded_io::content_workspace(&self.provider, volume, None)?;
        let workspace = self.open(&reference, None).await?;
        acyclic_fs::session::resource(workspace.id().volume_id(), TOOL_OUTPUT)
            .map_err(|error| Error::Invalid(format!("tool output resource: {error}")))
    }
}
