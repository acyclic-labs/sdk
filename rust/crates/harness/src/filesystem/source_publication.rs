use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct SourcePublicationFailureSlot {
    source: Box<Error>,
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SourcePublicationFailureSlot {
    pub(super) fn prepare(io: Option<&PreparationIo>) -> Result<Self> {
        let bytes = std::mem::size_of::<Error>() as u64;
        let custody = if let Some(io) = io {
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            if io.original_native_work().is_some() {
                Some(io.take_native_custody(bytes)?)
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self {
            source: Box::new(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Indeterminate,
                reason: "reserved source publication failure",
            }),
            custody,
        })
    }

    pub(super) fn finish(
        self,
        generation: acyclic_fs::GenerationId,
        source: Error,
        retention_failure: Option<acyclic_fs::LazyWorkspaceError>,
    ) -> Error {
        let Self {
            source: mut slot,
            custody,
        } = self;
        *slot = source;
        Error::NativeSourcePublicationUncertain {
            generation,
            source: slot,
            retention_failure,
            custody,
        }
    }

    pub(super) fn discard(self, io: Option<&PreparationIo>) -> Result<()> {
        drop(self);
        if let Some(io) = io
            && io.original_native_work().is_none()
        {
            io.release(std::mem::size_of::<Error>() as u64)?;
        }
        Ok(())
    }
}
