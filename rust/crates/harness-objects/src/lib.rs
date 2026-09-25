#![doc = include_str!("../README.md")]
#![cfg_attr(test, allow(clippy::too_many_lines))]

use acyclic_harness::{
    Error, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, FileDescriptor,
        FileRef, Limits, VolumeOperation, VolumeRef, decode_attachment_manifest,
    },
    core::{AuthorityVerifier, Scope},
    resources::ProviderRef,
    runtime::ContentBindings,
};
use acyclic_objects::{GetRequest, ObjectsProvider, PutRequest, ReadTarget, wire};
use futures::future::BoxFuture;
use std::sync::Arc;

/// One authenticated Objects bucket/volume binding. The bucket identity is
/// part of the logical volume, so a ref cannot silently route to another bucket.
pub struct ObjectContentStore {
    objects: Arc<dyn ObjectsProvider>,
    bucket: wire::BucketRef,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_bytes: u64,
    writer: Option<ContentGrant>,
}

impl ObjectContentStore {
    /// Checks bucket identity and the signed reader/writer scope before use.
    #[allow(
        clippy::too_many_arguments,
        reason = "binds each independent Objects authority and volume boundary"
    )]
    pub async fn new(
        objects: Arc<dyn ObjectsProvider>,
        bucket: wire::BucketRef,
        expected_provider: ProviderRef,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        owner_scope: Scope,
        scope: Scope,
        maximum_bytes: u64,
    ) -> Result<Arc<Self>> {
        volume.validate()?;
        expected_provider.validate()?;
        verifier.verify(&scope)?;
        ContentGrant::verify(&verifier, &owner_scope, &volume, VolumeOperation::Write)?;
        if maximum_bytes == 0
            || maximum_bytes > acyclic_objects::limits::OBJECT_BYTES
            || expected_provider.family() != "objects"
            || volume.provider() != &expected_provider
            || volume.id() != bucket.bucket_id
            || bucket.bucket_id.is_empty()
            || bucket.name.is_empty()
        {
            return Err(Error::Invalid("Objects content binding is invalid".into()));
        }
        let resolved = objects.head_bucket(&bucket).await.map_err(storage)?;
        if resolved.bucket.as_ref() != Some(&bucket) {
            return Err(Error::Storage(
                "Objects provider resolved another bucket".into(),
            ));
        }
        let write_capability = volume.capability(VolumeOperation::Write)?;
        let writer = if scope.capabilities().contains(&write_capability) {
            Some(ContentGrant::verify(
                &verifier,
                &scope,
                &volume,
                VolumeOperation::Write,
            )?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            objects,
            bucket,
            volume,
            verifier,
            scope,
            maximum_bytes,
            writer,
        }))
    }

    /// Exposes the reader and an optional original-owner writer independently.
    #[must_use]
    pub fn bindings(self: &Arc<Self>) -> ContentBindings {
        ContentBindings {
            reader: self.clone(),
            writer: self
                .writer
                .as_ref()
                .map(|_| Arc::clone(self) as Arc<dyn ContentPublisher>),
        }
    }

    fn key(&self, path: &str) -> Result<String> {
        let key = format!("{}/{}", self.volume.storage_name()?, path);
        if key.len() > acyclic_objects::limits::KEY_BYTES {
            return Err(Error::Invalid(
                "Objects content key exceeds provider limit".into(),
            ));
        }
        Ok(key)
    }

    async fn read_exact(&self, file: &FileRef) -> Result<Vec<u8>> {
        let grant = ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        grant.require_file_read(file)?;
        self.read_version(file).await
    }

    async fn read_version(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate()?;
        if file.volume() != &self.volume || file.descriptor().byte_length() > self.maximum_bytes {
            return Err(Error::Unauthorized(
                "file is outside this Objects binding".into(),
            ));
        }
        let read = self
            .objects
            .get(GetRequest {
                target: ReadTarget::Bucket(self.bucket.clone()),
                object_key: self.key(file.path())?,
                version_id: Some(file.version().to_owned()),
                range: None,
                if_match: None,
                if_none_match: None,
                maximum_bytes: self.maximum_bytes,
            })
            .await
            .map_err(storage)?;
        if read.version.version_id != file.version()
            || read.version.delete_marker
            || read.version.size != file.descriptor().byte_length()
            || read.version.metadata.as_ref().is_none_or(|metadata| {
                metadata.content_type != file.descriptor().media_type()
                    || metadata
                        .user
                        .get("harness-display-name")
                        .is_none_or(|name| name != file.display_name())
            })
        {
            return Err(Error::Storage(
                "Objects returned another file version or media type".into(),
            ));
        }
        file.descriptor().verify(&read.body)?;
        Ok(read.body.to_vec())
    }

    async fn manifest(&self, file: &FileRef, count: u32) -> Result<Vec<Attachment>> {
        let bytes = self.read_exact(file).await?;
        decode_attachment_manifest(file, &bytes, count)
    }
}

impl ContentPublisher for ObjectContentStore {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move {
            let grant = self.writer.as_ref().ok_or_else(|| {
                Error::Unauthorized("Objects writer is not bound to the original owner".into())
            })?;
            grant.require(&self.volume, VolumeOperation::Write)?;
            if bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid(
                    "Objects upload exceeds content limit".into(),
                ));
            }
            let descriptor = FileDescriptor::from_bytes(bytes, media_type)?;
            // Validate every public field before an external write is attempted.
            FileRef::new(
                self.volume.clone(),
                path,
                "staged",
                descriptor.clone(),
                display_name,
            )?;
            let key = self.key(path)?;
            let retry = format!(
                "harness-upload:{}:{operation_id}",
                self.volume.storage_name()?
            );
            let version = self
                .objects
                .put(PutRequest {
                    bucket: self.bucket.clone(),
                    object_key: key,
                    body: bytes::Bytes::copy_from_slice(bytes),
                    metadata: wire::ObjectMetadata {
                        content_type: media_type.to_owned(),
                        user: [("harness-display-name".to_owned(), display_name.to_owned())].into(),
                        ..Default::default()
                    },
                    condition: None,
                    idempotency_key: Some(retry),
                })
                .await
                .map_err(storage)?;
            if version.delete_marker
                || version.size != bytes.len() as u64
                || version.metadata.as_ref().is_none_or(|metadata| {
                    metadata.content_type != media_type
                        || metadata
                            .user
                            .get("harness-display-name")
                            .is_none_or(|name| name != display_name)
                })
            {
                return Err(Error::Storage(
                    "Objects publication returned inconsistent metadata".into(),
                ));
            }
            let reference = FileRef::new(
                self.volume.clone(),
                path,
                version.version_id,
                descriptor,
                display_name,
            )?;
            // A provider may return the retained result of an earlier request
            // with this operation identity. Never publish a ref until the exact
            // immutable version is proven to contain these bytes.
            let committed = self.read_version(&reference).await?;
            if committed.as_slice() != bytes {
                return Err(Error::Conflict(
                    "upload identity belongs to different content".into(),
                ));
            }
            Ok(reference)
        })
    }
}

impl ContentResidencyVerifier for ObjectContentStore {
    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.read_exact(reference).await.map(|_| ()) })
    }

    fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.read_exact(reference).await })
    }

    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            limits.validate_file(reference)?;
            for attachment in self.manifest(reference, item_count).await? {
                limits.validate_file(&attachment.file)?;
                self.verify(&attachment.file).await?;
            }
            Ok(())
        })
    }

    fn load_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move { self.manifest(reference, item_count).await })
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "maps the owned provider error at async boundaries"
)]
fn storage(error: acyclic_objects::ObjectsError) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        AgentId, Capabilities,
        conversation::{VolumeClass, VolumeOwner},
        core::{AggregateKind, Authority, AuthorityIssuer},
        resources::ProviderRef,
    };
    use acyclic_objects::MemoryObjects;

    #[tokio::test]
    async fn staged_objects_are_exact_and_attached_readers_never_gain_write() -> Result<()> {
        let objects = Arc::new(MemoryObjects::default());
        let bucket = objects
            .create_bucket("harness-content".into(), Some("objects-bucket".into()))
            .await
            .map_err(storage)?
            .bucket
            .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
        let owner = AgentId::from_bytes([7; 16]);
        let reader = AgentId::from_bytes([8; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("local", "objects", "1")?,
            bucket.bucket_id.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
        )?;
        let issuer = AuthorityIssuer::new(
            "objects-test",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "objects-test".into(),
            },
        );
        let owner_scope = issuer.root_for_agent(
            owner,
            "owner",
            Capabilities::new([
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let provider = volume.provider().clone();
        let owner_store = ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            issuer.verifier(),
            owner_scope.clone(),
            owner_scope.clone(),
            4_096,
        )
        .await?;
        let file = owner_store
            .stage(
                OperationId::from_bytes([10; 16]),
                "notes/one.txt",
                b"owner bytes",
                "text/plain",
                "one.txt",
            )
            .await?;
        assert_eq!(
            owner_store
                .stage(
                    OperationId::from_bytes([10; 16]),
                    "notes/one.txt",
                    b"owner bytes",
                    "text/plain",
                    "one.txt"
                )
                .await?,
            file
        );
        assert_eq!(owner_store.read(&file).await?.as_slice(), b"owner bytes");
        assert!(owner_store.bindings().writer.is_some());
        let items = vec![Attachment {
            file: file.clone(),
            label: None,
        }];
        let manifest_bytes =
            serde_json::to_vec(&items).map_err(|error| Error::Invalid(error.to_string()))?;
        let manifest = owner_store
            .stage(
                OperationId::from_bytes([12; 16]),
                "lists/one.json",
                &manifest_bytes,
                "application/vnd.acyclic.harness.attachments+json",
                "one.json",
            )
            .await?;
        owner_store
            .verify_manifest(&manifest, 1, &Limits::default())
            .await?;
        assert_eq!(owner_store.load_manifest(&manifest, 1).await?, items);
        assert!(owner_store.load_manifest(&manifest, 2).await.is_err());
        let delegated =
            issuer.delegate_private_file_read(&owner_scope, reader, "attached", &file)?;
        let attached = ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            issuer.verifier(),
            owner_scope.clone(),
            delegated,
            4_096,
        )
        .await?;
        assert_eq!(attached.read(&file).await?.as_slice(), b"owner bytes");
        assert!(attached.bindings().writer.is_none());
        assert!(
            attached
                .stage(
                    OperationId::from_bytes([11; 16]),
                    "notes/denied.txt",
                    b"denied",
                    "text/plain",
                    "denied.txt"
                )
                .await
                .is_err()
        );
        assert!(
            owner_store
                .stage(
                    OperationId::from_bytes([10; 16]),
                    "notes/one.txt",
                    b"owner bytes",
                    "text/plain",
                    "changed.txt"
                )
                .await
                .is_err()
        );
        assert!(
            owner_store
                .stage(
                    OperationId::from_bytes([10; 16]),
                    "notes/one.txt",
                    b"other bytes",
                    "text/plain",
                    "one.txt"
                )
                .await
                .is_err()
        );
        assert!(
            owner_store
                .stage(
                    OperationId::from_bytes([10; 16]),
                    "notes/two.txt",
                    b"owner bytes",
                    "text/plain",
                    "one.txt"
                )
                .await
                .is_err()
        );
        let impostor = AuthorityIssuer::new(
            "impostor",
            [13; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "impostor".into(),
            },
        );
        let impostor_scope = impostor.root_for_agent(
            reader,
            "forged",
            Capabilities::new([volume.capability(VolumeOperation::Read)?]),
        );
        assert!(
            ObjectContentStore::new(
                objects.clone(),
                bucket.clone(),
                provider.clone(),
                volume.clone(),
                impostor.verifier(),
                owner_scope.clone(),
                impostor_scope,
                4_096
            )
            .await
            .is_err()
        );
        assert!(
            ObjectContentStore::new(
                objects.clone(),
                bucket.clone(),
                ProviderRef::new("impostor", "objects", "1")?,
                volume.clone(),
                issuer.verifier(),
                owner_scope.clone(),
                owner_scope,
                4_096
            )
            .await
            .is_err()
        );
        let wrong = FileRef::new(
            volume,
            "notes/one.txt",
            file.version(),
            FileDescriptor::from_bytes(b"other", "text/plain")?,
            "one.txt",
        )?;
        assert!(owner_store.read(&wrong).await.is_err());
        Ok(())
    }
}
