use super::*;

/// The record blob binding one file's volume, path, descriptor and display
/// name to its content. Content blobs are account-wide, so a read serves a
/// version only through this volume's record of it.
#[derive(serde::Serialize, serde::Deserialize)]
pub(super) struct ContentRecord {
    pub(super) volume: String,
    pub(super) path: String,
    pub(super) descriptor: FileDescriptor,
    pub(super) display_name: String,
    pub(super) content: String,
}

impl ObjectContentStore {
    /// The content blob a file's version names.
    pub(super) fn content_digest(&self, file: &FileRef) -> Result<Digest> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume || file.descriptor().byte_length() > self.maximum_bytes {
            return Err(Error::Unauthorized(
                "file is outside this Objects binding".into(),
            ));
        }
        Digest::from_hex(file.version())
            .map_err(|_| Error::Invalid("Objects file version is not a blob digest".into()))
    }

    /// The content blob of `file`, once this volume's record binds it to the
    /// file's path: a version staged in another volume, or at another path,
    /// has no record here.
    pub(super) async fn bound_content(&self, file: &FileRef) -> Result<Digest> {
        let content = self.content_digest(file)?;
        let record = Digest::of(&self.record(file)?);
        match self.objects.head(record).await {
            Ok(head) if head.references == [content] => Ok(content),
            Ok(_) => Err(Error::Unauthorized(
                "file is not bound to this Objects volume".into(),
            )),
            Err(error) if error.code == acyclic_objects::wire::ErrorCode::NotFound => Err(
                Error::Unauthorized("file is not bound to this Objects volume".into()),
            ),
            Err(error) => Err(storage(error)),
        }
    }

    pub(super) async fn read_exact(
        &self,
        file: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        self.read_content(file, maximum_bytes).await
    }

    pub(super) async fn read_content(
        &self,
        file: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let read = self
            .objects
            .get(
                self.bound_content(file).await?,
                ByteRange::FULL,
                maximum_bytes.min(self.maximum_bytes),
            )
            .await
            .map_err(storage)?;
        self.verify_length(file, read.head().length)?;
        file.descriptor().verify(read.body(), &mut None)?;
        Ok(read.into_body())
    }

    pub(super) fn verify_length(&self, file: &FileRef, length: u64) -> Result<()> {
        if length != file.descriptor().byte_length() {
            return Err(Error::Storage(
                "Objects returned another file length".into(),
            ));
        }
        Ok(())
    }

    /// Selects this volume's immutable content as an artifact without copying it.
    pub fn artifact_ref(&self, file: &FileRef) -> Result<ArtifactRef> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume {
            return Err(Error::Unauthorized(
                "artifact file is outside this Objects volume".into(),
            ));
        }
        let record = self.record(file)?;
        ArtifactRef::new(
            self.volume.provider().clone(),
            format!(
                "{}/{}/@content/{}",
                self.volume.storage_name(&mut None)?,
                file.path(),
                Digest::of(&record).to_hex()
            )
            .into_bytes(),
            None,
        )
    }

    pub(super) fn record(&self, file: &FileRef) -> Result<Vec<u8>> {
        let content = self.content_digest(file)?;
        // A struct encodes deterministically: fixed field order, no maps.
        serde_json::to_vec(&ContentRecord {
            volume: self.volume.storage_name(&mut None)?,
            path: file.path().to_owned(),
            descriptor: file.descriptor().clone(),
            display_name: file.display_name().to_owned(),
            content: content.to_hex(),
        })
        .map_err(|_| Error::Invalid("Objects content record is not encodable".into()))
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "maps the owned provider error at async boundaries"
)]
pub(in crate::objects) fn storage(error: acyclic_objects::Error) -> Error {
    Error::Storage(error.to_string())
}

impl ContentResidencyVerifier for ObjectContentStore {
    fn read_range<'a>(
        &'a self,
        file: &'a FileRef,
        range: acyclic_fs::ByteRange,
        _ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.supports_read(file.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || self.read_interval(file, range))
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        _ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.supports_read(reference.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid(
                    "content exceeds selected byte allowance".into(),
                ));
            }
            self.read_exact(reference, maximum_bytes).await
        })
    }
}
