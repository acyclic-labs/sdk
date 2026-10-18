use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub(crate) fn with_task_files(
        mut self,
        files: Arc<crate::task_volume::TaskFiles<dyn StreamProvider>>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if self._native_configuration_custody.is_some() {
            return Err(Error::Unsupported(
                "task files require ordinary runtime construction".into(),
            ));
        }
        Arc::get_mut(&mut self.harness)
            .ok_or_else(|| Error::Conflict("bind task files before sharing the runtime".into()))?
            .task_files = Some(files);
        Ok(self)
    }

    /// Bind canonical streaming projections to the authorized private task files.
    pub fn bind_task_files(&mut self, files: Option<Arc<crate::task_volume::TaskFiles<P>>>) {
        self.task_files = files;
    }

    /// Binds root private files independently of the project's default output.
    pub(super) fn bind_private_volume(
        mut self,
        volume: &VolumeRef,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if self
            ._native_configuration_custody
            .as_ref()
            .is_some_and(|custody| {
                io.and_then(super::super::PreparationIo::original_native_work)
                    .is_none_or(|work| !custody.work().shares_ledger(work))
            })
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "private binding differs from original runtime work",
            ));
        }
        if !private_owner_matches(volume, self.signed.agent()) {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "private content requires the signed agent's private volume",
            ));
        }
        let harness = Arc::get_mut(&mut self.harness).ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "bind private content before sharing the task runtime",
            )
        })?;
        harness.bind_private_output(
            FilesystemContentPublisher::new(
                self.filesystem.clone(),
                volume,
                &self.verifier,
                &self.signed,
                self.maximum_payload_bytes,
                io,
            )?,
            io,
        )?;
        Ok(self)
    }

    /// Binds workspace command execution before sharing the runtime.
    pub fn with_workspace_tools(
        mut self,
        executor: Arc<dyn crate::tool::ToolExecutor>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if self._native_configuration_custody.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workspace tool binding requires ordinary runtime construction",
            });
        }
        let harness = Arc::get_mut(&mut self.harness).ok_or_else(|| {
            Error::Conflict("bind workspace tools before sharing the runtime".into())
        })?;
        harness.workspace_tools = Some(executor);
        Ok(self)
    }

    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    pub(crate) fn with_content_publisher(
        mut self,
        writer: Arc<dyn crate::conversation::ContentPublisher>,
    ) -> Result<Self> {
        if self._native_configuration_custody.is_some() {
            return Err(Error::Unsupported(
                "checkout content requires ordinary construction".into(),
            ));
        }
        Arc::get_mut(&mut self.harness)
            .ok_or_else(|| Error::Conflict("bind content before sharing the runtime".into()))?
            .bind_output(writer, None)?;
        Ok(self)
    }

    /// Binds the trusted root's default output volume before sharing the runtime.
    /// Task scopes still require the volume's write grant; created children retain
    /// their independently authenticated output selection.
    pub fn with_output_volume(
        mut self,
        volume: &VolumeRef,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if self
            ._native_configuration_custody
            .as_ref()
            .is_some_and(|custody| {
                io.and_then(super::super::PreparationIo::original_native_work)
                    .is_none_or(|work| !custody.work().shares_ledger(work))
            })
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "output binding differs from original runtime work",
            ));
        }
        let harness = Arc::get_mut(&mut self.harness).ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "bind root output before sharing the task runtime",
            )
        })?;
        let writer = FilesystemContentPublisher::new(
            self.filesystem.clone(),
            volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes,
            io,
        )?;
        harness.bind_output(writer, io)?;
        Ok(self)
    }
}

fn private_owner_matches(volume: &VolumeRef, agent: Option<crate::AgentId>) -> bool {
    volume.class() == crate::conversation::VolumeClass::AgentPrivate
        && matches!(volume.owner(), crate::conversation::VolumeOwner::Agent(owner)
            if Some(*owner) == agent)
}

#[cfg(test)]
mod tests;
