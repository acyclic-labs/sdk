use super::*;
impl ObjectContentStore {
    pub(super) fn key(&self, path: &str, content: &str) -> Result<String> {
        validate_content_path(path, &mut None)?;
        if content.len() != 64
            || !content
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::Invalid("Objects content identity is invalid".into()));
        }
        let key = format!(
            "{}/{}/@content/{}",
            self.volume.storage_name(&mut None)?,
            path,
            content
        );
        if key.len() > acyclic_objects::wire::ObjectsLimit::MaxKeyBytes as usize {
            return Err(Error::Invalid(
                "Objects content key exceeds provider limit".into(),
            ));
        }
        Ok(key)
    }

    pub(super) async fn read_exact(
        &self,
        file: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let grant = ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        grant.require_file_read(file, None)?;
        self.read_content(file, maximum_bytes).await
    }

    pub(super) async fn read_content(
        &self,
        file: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume || file.descriptor().byte_length() > self.maximum_bytes {
            return Err(Error::Unauthorized(
                "file is outside this Objects binding".into(),
            ));
        }
        if file.version() != content_identity(file.descriptor(), file.display_name())? {
            return Err(Error::Invalid(
                "Objects file content identity does not match its descriptor".into(),
            ));
        }
        let read = self
            .objects
            .get(
                wire::GetObjectRequest {
                    bucket: Some(self.bucket.clone()),
                    object_key: self.key(file.path(), file.version())?,
                    ..Default::default()
                },
                maximum_bytes.min(self.maximum_bytes),
            )
            .await
            .map_err(storage)?;
        let info = read
            .header()
            .object
            .as_ref()
            .ok_or_else(|| Error::Storage("Objects returned no content metadata".into()))?;
        if info.size != file.descriptor().byte_length()
            || info.metadata.as_ref().is_none_or(|metadata| {
                metadata.content_type != file.descriptor().media_type()
                    || metadata
                        .user
                        .get("harness-display-name")
                        .is_none_or(|name| name != file.display_name())
            })
        {
            return Err(Error::Storage(
                "Objects returned another file length or metadata".into(),
            ));
        }
        file.descriptor().verify(read.body(), &mut None)?;
        Ok(read.into_body())
    }

    /// Selects this bucket's immutable content as an artifact without copying it.
    pub fn artifact_ref(&self, file: &FileRef) -> Result<ArtifactRef> {
        file.validate(&mut None)?;
        if file.volume() != &self.volume {
            return Err(Error::Unauthorized(
                "artifact file is outside this Objects bucket".into(),
            ));
        }
        ArtifactRef::new(
            self.volume.provider().clone(),
            self.key(file.path(), file.version())?.into_bytes(),
            None,
        )
    }

    pub(in crate::objects) async fn verify_artifact(&self, artifact: &ArtifactRef) -> Result<()> {
        artifact.validate(&mut None)?;
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Read,
            self.maximum_bytes,
            &mut None,
        )?;
        if artifact.as_resource().provider() != self.volume.provider() {
            return Err(Error::Unauthorized(
                "artifact belongs to another Objects provider".into(),
            ));
        }
        let key = std::str::from_utf8(artifact.as_resource().key())
            .map_err(|_| Error::Invalid("Objects artifact key is not UTF-8".into()))?;
        let prefix = format!("{}/", self.volume.storage_name(&mut None)?);
        let relative_key = key
            .strip_prefix(&prefix)
            .filter(|relative| !relative.is_empty())
            .ok_or_else(|| Error::Unauthorized("artifact is outside this Objects volume".into()))?;
        let (path, content) = relative_key
            .rsplit_once("/@content/")
            .ok_or_else(|| Error::Invalid("Objects artifact has no content identity".into()))?;
        if artifact.as_resource().version().is_some() || self.key(path, content)? != key {
            return Err(Error::Invalid(
                "Objects artifact key is not canonical".into(),
            ));
        }
        let observed = self
            .objects
            .get(
                wire::GetObjectRequest {
                    bucket: Some(self.bucket.clone()),
                    object_key: key.to_owned(),
                    ..Default::default()
                },
                self.maximum_bytes,
            )
            .await
            .map_err(storage)?;
        let metadata = observed
            .header()
            .object
            .as_ref()
            .and_then(|object| object.metadata.as_ref())
            .ok_or_else(|| Error::Storage("Objects artifact metadata is missing".into()))?;
        let display_name = metadata
            .user
            .get("harness-display-name")
            .ok_or_else(|| Error::Storage("Objects artifact display name is missing".into()))?;
        let descriptor =
            FileDescriptor::from_bytes(observed.body(), &metadata.content_type, &mut None)?;
        if content_identity(&descriptor, display_name)? != content {
            return Err(Error::Conflict(
                "Objects artifact content identity does not match its bytes".into(),
            ));
        }
        Ok(())
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "maps the owned provider error at async boundaries"
)]
pub(in crate::objects) fn storage(error: acyclic_objects::v1::Error) -> Error {
    Error::Storage(error.to_string())
}

pub(in crate::objects) fn content_identity(
    descriptor: &FileDescriptor,
    display_name: &str,
) -> Result<String> {
    Ok(
        blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
            &(descriptor, display_name),
            u64::MAX,
            &mut None,
        )?)
        .to_hex()
        .to_string(),
    )
}

impl ContentResidencyVerifier for ObjectContentStore {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
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
