use super::*;

pub(super) struct MemoryContentPublisher<A = MemoryAuthorityBackend, O = MemoryObjectBackend> {
    pub(super) files: Arc<FilesystemContentPublisher<A, O>>,
    pub(super) memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher
    for MemoryContentPublisher<A, O>
{
    fn volume(&self) -> &VolumeRef {
        self.files.volume()
    }

    fn supports_stage(&self, io: Option<&crate::filesystem::PreparationIo>) -> Result<()> {
        self.files.supports_stage(io)
    }

    fn stage_once<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        BoundedContentFuture::new(io, || async move {
            let (persisted, hold) = self
                .files
                .stage_once(
                    operation_id,
                    path,
                    bytes,
                    media_type,
                    display_name,
                    crate::conversation::ContentAccess { ceiling, io },
                )?
                .await?;
            self.retain(&persisted, bytes, io)?;
            Ok((persisted, hold))
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation_id: OperationId,
        maximum_bytes: u64,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        self.files.observe_staged_once(
            operation_id,
            maximum_bytes,
            crate::conversation::ContentAccess { ceiling, io },
        )
    }

    fn stage_at<'a>(
        &'a self,
        operation_id: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let (persisted, hold) = self
                .files
                .stage_at(operation_id, source, bytes, ceiling, io)?
                .await?;
            self.retain(&persisted, bytes, io)?;
            Ok((persisted, hold))
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        BoundedContentFuture::new(io, || async move {
            let (persisted, hold) = self
                .files
                .stage(
                    operation_id,
                    path,
                    bytes,
                    media_type,
                    display_name,
                    crate::conversation::ContentAccess { ceiling, io },
                )?
                .await?;
            self.retain(&persisted, bytes, io)?;
            Ok((persisted, hold))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> MemoryContentPublisher<A, O> {
    pub(super) fn retain(
        &self,
        persisted: &FileRef,
        bytes: &[u8],
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        self.memory_store
            .lock()
            .map_err(|_| Error::Storage("memory content core lock is poisoned".into()))?
            .retain_reference(persisted, bytes, true, io)?;
        if let Some(io) = io {
            io.remaining()?;
        }
        Ok(())
    }
}
