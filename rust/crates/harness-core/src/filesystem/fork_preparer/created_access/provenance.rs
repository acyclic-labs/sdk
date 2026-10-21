//! Original creation identity retained by the existing content providers.
use super::*;

pub(in crate::filesystem) struct CreatedProvenance {
    pub(super) request: ResourcePreparationRequest,
    pub(super) proof: ResourceCreationProofRef,
    pub(super) origin: ResourceCreationOrigin,
    pub(super) volume: VolumeRef,
    pub(super) generation: GenerationRef,
    pub(super) prepared_generation: GenerationRef,
}

impl CreatedProvenance {
    pub(in crate::filesystem) fn volume(&self) -> &VolumeRef {
        &self.volume
    }
    pub(in crate::filesystem) fn owner(&self) -> &Authority {
        &self.origin.owner
    }
    pub(in crate::filesystem) fn verify<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        owner: &'a Authority,
        volume: &'a VolumeRef,
        exact_file: Option<&'a FileRef>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let scratch = io
                .and_then(PreparationIo::original_native_work)
                .map(PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let io = scratch.as_ref().or(io);
            if exact_file.is_none() && volume != &self.volume {
                return Err(Error::Unauthorized(
                    "created content handle names another volume".into(),
                ));
            }
            let current = authenticate(host, owner, &self.request, &self.proof, io).await?;
            if current.origin != self.origin
                || current.volume != self.volume
                || current.generation != self.generation
                || current.prepared_generation != self.prepared_generation
            {
                return Err(Error::Unauthorized(
                    "original created allocation evidence changed".into(),
                ));
            }
            if exact_file.is_some_and(|file| !current.reference_files.contains(file)) {
                return Err(Error::Unauthorized(
                    "original inherited reference evidence changed".into(),
                ));
            }
            Ok(())
        })
    }
}

impl crate::resource_preparation::ScratchCreationIdentity for CreatedProvenance {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }
    fn origin(&self) -> &ResourceCreationOrigin {
        &self.origin
    }
}
