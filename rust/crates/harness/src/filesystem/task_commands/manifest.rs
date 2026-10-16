use super::*;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds model context and tool visibility to one immutable volume file.
    /// Resolve after `with_model(..., None)` and before dispatching native work.
    pub async fn with_context_manifest(
        mut self,
        reference: FileRef,
        reader: &dyn crate::conversation::ContentResidencyVerifier,
        limits: crate::conversation::Limits,
    ) -> Result<Self> {
        let binding = self.model.as_mut().ok_or_else(|| {
            Error::Unsupported("context manifest requires a model binding".into())
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        if binding.native_custody.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "load the context manifest before native configuration",
            });
        }
        if binding.context.contains_stage("context-manifest") {
            return Err(Error::Conflict(
                "command model already has a context manifest".into(),
            ));
        }
        let (tools, stage) = crate::context::ContextManifest::bind(
            reference,
            reader,
            self.runtime.tool_registry().clone(),
            limits,
        )
        .await?;
        stage.validate(None)?;
        binding.context = std::mem::take(&mut binding.context).with(stage);
        binding.tools = Some(tools);
        Ok(self)
    }
}
