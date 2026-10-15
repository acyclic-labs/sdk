use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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
