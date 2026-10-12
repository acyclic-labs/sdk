use super::*;

pub(super) struct MemoryContentPublisher {
    pub(super) files: Arc<FilesystemContentPublisher<MemoryAuthorityBackend, MemoryObjectBackend>>,
    pub(super) memory_store: Arc<std::sync::Mutex<crate::memory_store::MemoryStore>>,
}

impl ContentPublisher for MemoryContentPublisher {
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
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let persisted = self
                .files
                .publish_once(
                    operation_id,
                    path,
                    bytes,
                    media_type,
                    display_name,
                    io,
                    None,
                    None,
                )
                .await?;
            let hold = io
                .map(|io| io.guard_owned(persisted.logical_owned_bytes()?))
                .transpose()?;
            self.retain(&persisted, bytes, io)?;
            Ok((persisted, hold))
        })
    }

    fn observe_staged_once<'a>(
        &'a self,
        operation_id: OperationId,
        maximum_bytes: u64,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        self.files
            .observe_staged_once(operation_id, maximum_bytes, io)
    }

    fn stage_at<'a>(
        &'a self,
        operation_id: OperationId,
        source: &'a FileRef,
        bytes: &'a [u8],
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let persisted = self
                .files
                .replace(operation_id, source, bytes, io, None, None)
                .await?;
            let hold = io
                .map(|io| io.guard_owned(persisted.logical_owned_bytes()?))
                .transpose()?;
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
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        BoundedContentFuture::new(io, || async move {
            let (persisted, hold) = self
                .files
                .stage(operation_id, path, bytes, media_type, display_name, io)?
                .await?;
            self.retain(&persisted, bytes, io)?;
            Ok((persisted, hold))
        })
    }
}

impl MemoryContentPublisher {
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
