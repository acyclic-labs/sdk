use super::*;

/// Tool output directory on an agent's private volume.
pub const TOOL_OUTPUT: &str = "tools";

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemHost<A, O> {
    /// Routes change follows over this filesystem's original Streams owner.
    /// Backends without a synchronous policy fence refuse resource follows;
    /// ordinary session commands remain available.
    ///
    /// # Errors
    /// When `streams` belongs to another backend owner.
    pub fn change_provider<P: crate::store::JournalProvider + ?Sized>(
        &self,
        streams: Arc<P>,
    ) -> Result<acyclic_fs::session::ChangeProvider<P, A, O>> {
        acyclic_fs::session::ChangeProvider::new(self.filesystem.clone(), streams)
            .map_err(|error| Error::Unauthorized(format!("tool output changes: {error}")))
    }

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
