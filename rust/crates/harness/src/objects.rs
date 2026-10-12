#![doc = include_str!("../docs/objects.md")]
#![cfg_attr(
    test,
    allow(
        clippy::too_many_lines,
        reason = "one ordered scenario keeps each step next to the state it checks"
    )
)]

use crate::{
    Error, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, FileDescriptor,
        FileRef, VolumeOperation, VolumeRef,
        validate_content_path,
    },
    core::{AuthorityVerifier, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier,
        ForkSelection, ResourceRevision,
    },
    resources::{ArtifactRef, ProviderRef},
    runtime::ContentBindings,
};
use acyclic_objects::v1::{ObjectsProvider, wire};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One authenticated Objects bucket/volume binding.
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
        volume.validate(&mut None)?;
        expected_provider.validate()?;
        verifier.verify(&scope, &mut None)?;
        ContentGrant::verify(&verifier, &owner_scope, &volume, VolumeOperation::Write, maximum_bytes, &mut None)?;
        if maximum_bytes == 0
            || expected_provider.family() != "objects"
            || volume.provider() != &expected_provider
            || volume.id() != bucket.name
            || bucket.name.is_empty()
        {
            return Err(Error::Invalid("Objects content binding is invalid".into()));
        }
        let resolved = objects
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(bucket.clone()),
            })
            .await
            .map_err(storage)?;
        if resolved.bucket.as_ref() != Some(&bucket) {
            return Err(Error::Storage(
                "Objects provider resolved another bucket".into(),
            ));
        }
        let write_capability = volume.capability(VolumeOperation::Write, &mut None)?;
        let writer = if scope.capabilities().contains(&write_capability) {
            Some(ContentGrant::verify(
                &verifier,
                &scope,
                &volume,
                VolumeOperation::Write,
             maximum_bytes, &mut None,)?)
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

    fn key(&self, path: &str, content: &str) -> Result<String> {
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

    async fn read_exact(&self, file: &FileRef, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes> {
        let grant = ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        grant.require_file_read(file, None)?;
        self.read_content(file, maximum_bytes).await
    }

    async fn read_content(&self, file: &FileRef, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes> {
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
                    range: None,
                    if_match: String::new(),
                    if_none_match: String::new(),
                },
                maximum_bytes.min(self.maximum_bytes),
            )
            .await
            .map_err(storage)?;
        let info = read
            .header()
            .object.as_ref()
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

    async fn pin_upload(
        &self,
        operation_id: OperationId,
        path: &str,
        descriptor: &FileDescriptor,
        display_name: &str,
    ) -> Result<()> {
        let intent = crate::contract::canonical_json_digest(&(path, descriptor, display_name))?;
        let operation = blake3::Hash::from_bytes(crate::contract::canonical_json_digest(&(
            "harness-upload-intent-v1",
            operation_id.to_string(),
        ))?)
        .to_hex();
        let namespace = self.volume.storage_name(&mut None)?;
        let object_key = format!("{namespace}/.system/uploads/{operation}");
        let content_type = "application/vnd.acyclic.harness.upload-intent-v1";
        match self
            .objects
            .put(
                wire::PutObjectHeader {
                    bucket: Some(self.bucket.clone()),
                    object_key: object_key.clone(),
                    metadata: Some(wire::ObjectMetadata {
                        content_type: content_type.into(),
                        ..Default::default()
                    }),
                    preconditions: Some(wire::Preconditions {
                        condition: Some(wire::preconditions::Condition::IfAbsent(true)),
                    }),
                    mutation: Some(wire::MutationIdentity {
                        idempotency_key: format!(
                            "harness-upload-intent:{namespace}:{operation_id}"
                        ),
                    }),
                },
                acyclic_stream::OwnedBytes::copy_from_slice(&intent),
            )
            .await
        {
            Ok(_) => {}
            Err(error) if error.code == wire::ErrorCode::PreconditionFailed => {}
            Err(error) => return Err(storage(error)),
        }
        let pinned = self
            .objects
            .get(
                wire::GetObjectRequest {
                    bucket: Some(self.bucket.clone()),
                    object_key,
                    ..Default::default()
                },
                32,
            )
            .await
            .map_err(storage)?;
        if pinned.header().content_range.is_some()
            || pinned.header().object.as_ref().is_none_or(|info| {
                info.size != 32
                    || info
                        .metadata
                        .as_ref()
                        .is_none_or(|metadata| metadata.content_type != content_type)
            })
            || pinned.body().as_ref() != intent
        {
            return Err(Error::Conflict(
                "upload identity belongs to different content".into(),
            ));
        }
        Ok(())
    }

    /// Converts content in this bucket into an immutable artifact key selection without copying its bytes or weakening the bucket boundary.
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

    async fn verify_artifact(&self, artifact: &ArtifactRef) -> Result<()> {
        artifact.validate()?;
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Read,
         self.maximum_bytes, &mut None,)?;
        if artifact.as_resource().provider() != self.volume.provider() {
            return Err(Error::Unauthorized(
                "artifact belongs to another Objects provider".into(),
            ));
        }
        let key = std::str::from_utf8(artifact.as_resource().key())
            .map_err(|_| Error::Invalid("Objects artifact key is not UTF-8".into()))?;
        let prefix = format!("{}/", self.volume.storage_name(&mut None)?);
        if !key.starts_with(&prefix) || key.len() == prefix.len() {
            return Err(Error::Unauthorized(
                "artifact is outside this Objects volume".into(),
            ));
        }
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
                    if_match: String::new(),
                    if_none_match: String::new(),
                    range: None,
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
        let descriptor = FileDescriptor::from_bytes(observed.body(), &metadata.content_type, &mut None)?;
        if content_identity(&descriptor, display_name)? != content {
            return Err(Error::Conflict(
                "Objects artifact content identity does not match its bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Lazily routes immutable Objects refs to separately registered owner bindings.
#[derive(Default)]
pub struct ObjectContentMountResolver {
    stores: BTreeMap<String, Arc<ObjectContentStore>>,
}

impl ObjectContentMountResolver {
    /// Creates a resolver from independently authenticated bucket bindings.
    pub fn new(stores: impl IntoIterator<Item = Arc<ObjectContentStore>>) -> Result<Self> {
        let mut resolver = Self::default();
        for store in stores {
            store.volume.validate(&mut None)?;
            let key = store.volume.capability(VolumeOperation::Read, &mut None)?;
            if resolver.stores.contains_key(&key) {
                return Err(Error::Invalid("Objects content volume is registered twice".into()));
            }
            resolver.stores.insert(key, store);
        }
        Ok(resolver)
    }

    fn store(&self, volume: &VolumeRef) -> Result<Arc<ObjectContentStore>> {
        volume.validate(&mut None)?;
        self.stores
            .get(&volume.capability(VolumeOperation::Read, &mut None)?)
            .cloned()
            .ok_or_else(|| Error::Unsupported("Objects content volume is not registered".into()))
    }
}

impl crate::conversation::ContentMountResolver for ObjectContentMountResolver {
    fn mount<'a>(&'a self, reference: &'a FileRef)
        -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>> {
        Box::pin(async move {
            reference.validate(&mut None)?;
            let store = self.store(reference.volume())?;
            // Authenticate the current scope against this exact content reference before returning either the grant or the owner reader.
            let grant = ContentGrant::verify_read(&store.verifier, &store.scope, reference)?;
            grant.require_file_read(reference, None)?;
            Ok((grant, store as Arc<dyn ContentResidencyVerifier>))
        })
    }
}

impl ForkCaptureProvider for ObjectContentStore {
    fn provider(&self) -> &ProviderRef {
        self.volume.provider()
    }

    fn capture<'a>(
        &'a self,
        _request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Capture>> {
        Box::pin(async move {
            let ResourceRevision::Artifact(artifact) = &selection.revision else {
                return Ok(Capture::Unsupported(
                    "Objects captures only artifacts".into(),
                ));
            };
            self.verify_artifact(artifact).await?;
            Ok(Capture::Captured(CapturedResource {
                source: selection.revision.clone(),
                revision: selection.revision.clone(),
            }))
        })
    }

    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> BoxFuture<'a, Result<Option<Capture>>> {
        // Content verification is read-only and safe to observe again.
        Box::pin(async move { self.capture(request, selection).await.map(Some) })
    }
}

impl ForkSeedVerifier for ObjectContentStore {
    fn provider(&self) -> &ProviderRef {
        self.volume.provider()
    }

    fn verify<'a, 'w: 'a>(&'a self, seed: &'a ForkSeed, work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>)
        -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() { return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Unsupported, "Objects fork verification has no original-work provider")); }
            seed.validate(&mut None)?;
            if seed.child_private_volume.provider() == self.volume.provider() {
                return Err(Error::Unsupported(
                    "Objects cannot verify a child-private workspace".into(),
                ));
            }
            for resource in &seed.resources {
                for revision in [&resource.source, &resource.revision] {
                    if revision.provider() != self.volume.provider() {
                        continue;
                    }
                    let ResourceRevision::Artifact(artifact) = revision else {
                        return Err(Error::Unsupported(
                            "Objects cannot verify this fork resource".into(),
                        ));
                    };
                    self.verify_artifact(artifact).await?;
                }
            }
            Ok(())
        })
    }

    fn read_manifest<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>)
        -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>> {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() { return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Unsupported, "Objects fork verification has no original-work provider")); }
            Ok((self.read_exact(manifest, self.maximum_bytes).await?, 0))
        })
    }

    fn verify_file<'a, 'w: 'a>(&'a self, _seed: &'a ForkSeed, file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>)
        -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() { return Err(crate::fork::boundary_error(work, crate::wire::ErrorCode::Unsupported, "Objects fork verification has no original-work provider")); }
            self.read_exact(file, self.maximum_bytes).await.map(|_| ())
        })
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
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, (FileRef, Option<crate::conversation::ContentHold<'a>>)>> {
        self.supports_stage(io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let grant = self.writer.as_ref().ok_or_else(|| {
                Error::Unauthorized("Objects writer is not bound to the original owner".into())
            })?;
            grant.require(&self.volume, VolumeOperation::Write, None)?;
            if bytes.len() as u64 > self.maximum_bytes {
                return Err(Error::Invalid(
                    "Objects upload exceeds content limit".into(),
                ));
            }
            let descriptor = FileDescriptor::from_bytes(bytes, media_type, &mut None)?;
            // Validate every public field before an external write is attempted.
            FileRef::new(
                self.volume.clone(),
                path.to_owned(),
                "staged".to_owned(),
                descriptor.clone(),
                display_name.to_owned(), &mut None
            )?;
            let content = content_identity(&descriptor, display_name)?;
            let key = self.key(path, &content)?;
            self.pin_upload(operation_id, path, &descriptor, display_name)
                .await?;
            let retry = format!(
                "harness-upload:{}:{operation_id}",
                self.volume.storage_name(&mut None)?
            );
            let published = self
                .objects
                .put(
                    wire::PutObjectHeader {
                        bucket: Some(self.bucket.clone()),
                        object_key: key,
                        metadata: Some(wire::ObjectMetadata {
                            content_type: media_type.to_owned(),
                            user: [("harness-display-name".to_owned(), display_name.to_owned())]
                                .into(),
                            ..Default::default()
                        }),
                        preconditions: Some(wire::Preconditions {
                            condition: Some(wire::preconditions::Condition::IfAbsent(true)),
                        }),
                        mutation: Some(wire::MutationIdentity {
                            idempotency_key: retry,
                        }),
                    },
                    acyclic_stream::OwnedBytes::copy_from_slice(bytes),
                )
                .await;
            let info = match published {
                Ok(info) => Some(info),
                Err(error) if error.code == wire::ErrorCode::PreconditionFailed => None,
                Err(error) => return Err(storage(error)),
            };
            if info.as_ref().is_some_and(|info| {
                info.size != bytes.len() as u64
                    || info.metadata.as_ref().is_none_or(|metadata| {
                        metadata.content_type != media_type
                            || metadata
                                .user
                                .get("harness-display-name")
                                .is_none_or(|name| name != display_name)
                    })
            }) {
                return Err(Error::Storage(
                    "Objects publication returned inconsistent metadata".into(),
                ));
            }
            let reference =
                FileRef::new(self.volume.clone(), path.to_owned(), content, descriptor, display_name.to_owned(), &mut None)?;
            // A retry receipt does not establish continued byte residency.
            let committed = self.read_content(&reference, self.maximum_bytes).await?;
            if committed.as_slice() != bytes {
                return Err(Error::Conflict(
                    "upload identity belongs to different content".into(),
                ));
            }
            Ok((reference, None))
        })
    }
}

impl ContentResidencyVerifier for ObjectContentStore {
    fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        self.supports_read(reference.volume(), io)?;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(Error::Invalid("content exceeds selected byte allowance".into()));
            }
            self.read_exact(reference, maximum_bytes).await
        })
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "maps the owned provider error at async boundaries"
)]
fn storage(error: acyclic_objects::v1::Error) -> Error {
    Error::Storage(error.to_string())
}

fn content_identity(descriptor: &FileDescriptor, display_name: &str) -> Result<String> {
    Ok(
        blake3::Hash::from_bytes(crate::contract::canonical_json_digest(&(
            descriptor,
            display_name,
        ))?)
        .to_hex()
        .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        conversation::{Attachment, ContentMountResolver, Limits, VolumeClass, VolumeOwner},
        core::{AggregateKind, Authority, AuthorityIssuer},
        fork::ForkPreparation,
        resources::{GenerationRef, ProviderRef, StreamRef},
    };
    use acyclic_objects::v1::{MemoryObjects, MemoryOptions};

    #[tokio::test]
    async fn staged_objects_are_exact_and_attached_readers_never_gain_write() -> Result<()> {
        let objects = Arc::new(MemoryObjects::new(MemoryOptions::default()).map_err(storage)?);
        let bucket = objects
            .create_bucket(wire::CreateBucketRequest {
                name: "harness-content".into(),
                mutation: None,
            })
            .await
            .map_err(storage)?
            .bucket
            .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
        let owner = AgentId::from_bytes([7; 16]);
        let reader = AgentId::from_bytes([8; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("local", "objects", "1")?,
            bucket.name.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner), &mut None
        )?;
        let issuer = AuthorityIssuer::new(
            "objects-test",
            [9; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "objects-test".into(),
            },
        );
        let owner_scope = issuer.issue("owner", &Capabilities::new([
                volume.capability(VolumeOperation::Read, &mut None)?,
                volume.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(owner), None, &mut None).expect("scope proof");
        let provider = volume.provider().clone();
        let owner_store = ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
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
                "one.txt", None
            )?
            .await?.0;
        assert_eq!(
            owner_store
                .stage(
                    OperationId::from_bytes([10; 16]),
                    "notes/one.txt",
                    b"owner bytes",
                    "text/plain",
                    "one.txt", None
                )?
                .await?.0,
            file
        );
        assert_eq!(owner_store.read(&file, file.descriptor().byte_length(), None)?.await?.as_ref(), b"owner bytes");
        let duplicate = owner_store
            .stage(
                OperationId::from_bytes([42; 16]),
                "notes/one.txt",
                b"owner bytes",
                "text/plain",
                "one.txt", None
            )?
            .await?.0;
        assert_eq!(duplicate, file);
        assert!(
            owner_store
                .stage(
                    OperationId::from_bytes([42; 16]),
                    "notes/other.txt",
                    b"owner bytes",
                    "text/plain",
                    "one.txt", None
                )?
                .await
                .is_err()
        );
        let artifact = owner_store.artifact_ref(&file)?;
        owner_store.verify_artifact(&artifact).await?;
        let missing_version = ArtifactRef::new(
            provider.clone(),
            artifact.as_resource().key(),
            Some("missing-version".into()),
        )?;
        assert!(owner_store.verify_artifact(&missing_version).await.is_err());
        let unpinned = ArtifactRef::new(provider.clone(), artifact.as_resource().key(), None)?;
        owner_store.verify_artifact(&unpinned).await?;
        let traversal = ArtifactRef::new(
            provider.clone(),
            format!("{}/../outside", owner_store.volume.storage_name(&mut None)?).into_bytes(),
            Some(file.version().into()),
        )?;
        assert!(matches!(
            owner_store.verify_artifact(&traversal).await,
            Err(Error::Invalid(_))
        ));
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "artifact-parent".into(),
        };
        let filesystem = ProviderRef::new("local", "filesystem", "2")?;
        let project_owner = VolumeOwner::Project("artifact-project".into());
        let child_agent = AgentId::from_bytes([14; 16]);
        let request = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes([15; 16]),
            parent: parent.clone(),
            parent_revision: 1,
            child: Authority {
                kind: AggregateKind::Conversation,
                id: "artifact-child".into(),
            },
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_result_volume: None,
                summary: None,
                child_project_volume: VolumeRef::new(
                    filesystem.clone(),
                    "artifact-child-project",
                    VolumeClass::Project,
                    project_owner.clone(), &mut None
                )?,
                child_private_volume: VolumeRef::new(
                    filesystem.clone(),
                    "artifact-child-private",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(child_agent), &mut None
                )?,
                inherited_through_sequence: 0,
                maximum_inherited_messages: 1,
                maximum_inherited_bytes: 1_024,
                maximum_inherited_references: 1,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: VolumeRef::new(
                            filesystem.clone(),
                            "parent-scratch",
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(AgentId::from_bytes([1; 16])), &mut None
                        )?,
                        generation: GenerationRef::new(filesystem.clone(), [99; 32], None)?,
                        paths: Vec::new(),
                    },
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        ProviderRef::new("local", "stream", "2")?,
                        parent.stream_path(&mut None)?.into_bytes(),
                        Some("1".into()),
                    )?),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: VolumeRef::new(
                            filesystem.clone(),
                            "artifact-parent-project",
                            VolumeClass::Project,
                            project_owner, &mut None
                        )?,
                        generation: GenerationRef::new(filesystem, [16; 32], Some("1".into()))?,
                    },
                },
                ForkSelection {
                    required: false,
                    revision: ResourceRevision::Artifact(artifact.clone()),
                },
            ],
            boundary: None,
        };
        request.validate()?;
        let [
            _scratch_selection,
            history_selection,
            project_selection,
            artifact_selection,
        ] = request.selections.as_slice()
        else {
            return Err(Error::Invalid(
                "fork request selections changed after validation".into(),
            ));
        };
        assert_eq!(
            owner_store.capture(&request, artifact_selection).await?,
            Capture::Captured(CapturedResource {
                source: ResourceRevision::Artifact(artifact.clone()),
                revision: ResourceRevision::Artifact(artifact.clone()),
            }),
        );
        let seed = ForkSeed { history_witness: None, summary: None,
        authorized_source: request.authorized_source.clone(),
        child_result_volume: request.preparation.child_result_volume.clone(),
        child_result_generation: None,
        operation_id: request.operation_id,
        parent: request.parent.clone(),
        parent_revision: request.parent_revision,
        child: request.child.clone(),
        child_agent: request.child_agent,
        attached_agents: Vec::new(),
        resources: vec![
            CapturedResource {
                source: _scratch_selection.revision.clone(),
                revision: ResourceRevision::PrivateVolume {
                    volume: request.preparation.child_private_volume.clone(),
                    generation: GenerationRef::new(
                        request.preparation.child_private_volume.provider().clone(),
                        [18; 32],
                        Some("1".into()),
                    )?,
                    paths: Vec::new(),
                },
            },
            CapturedResource {
                source: history_selection.revision.clone(),
                revision: history_selection.revision.clone(),
            },
            CapturedResource {
                source: project_selection.revision.clone(),
                revision: ResourceRevision::Project {
                    volume: request.preparation.child_project_volume.clone(),
                    generation: GenerationRef::new(
                        request.preparation.child_project_volume.provider().clone(),
                        [17; 32],
                        Some("1".into()),
                    )?,
                },
            },
            CapturedResource {
                source: artifact_selection.revision.clone(),
                revision: artifact_selection.revision.clone(),
            },
        ],
        omissions: Vec::new(),
        child_private_volume: request.preparation.child_private_volume.clone(),
        child_private_generation: GenerationRef::new(
            request.preparation.child_private_volume.provider().clone(),
            [18; 32],
            Some("1".into()),
        )?,
        inherited_context: Vec::new(),
        inherited_through_sequence: 0,
        shared_grants: Vec::new(),
        reference_grants: Vec::new(),
        attachment_manifests: Vec::new(),
        boundary: None, };
        ForkSeedVerifier::verify(owner_store.as_ref(), &seed, &mut None)?.await?;
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
                "one.json", None
            )?
            .await?.0;
        owner_store
            .verify_manifest(&manifest, 1, &Limits::default(), None)?
            .await?;
        assert_eq!(crate::conversation::load_manifest(&owner_store, &manifest, 1, None).await.map(|(items, _)| items)?, items);
        assert!(crate::conversation::load_manifest(&owner_store, &manifest, 2, None).await.map(|(items, _)| items).is_err());
        let delegated =
            issuer.delegate_private_file_read(&owner_scope, reader, "attached", &file)?;
        let attached = ObjectContentStore::new(
            objects.clone(),
            bucket.clone(),
            provider.clone(),
            volume.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope.clone(),
            delegated,
            4_096,
        )
        .await?;
        assert_eq!(attached.read(&file, file.descriptor().byte_length(), None)?.await?.as_ref(), b"owner bytes");
        assert!(attached.bindings().writer.is_none());
        assert!(
            attached
                .stage(
                    OperationId::from_bytes([11; 16]),
                    "notes/denied.txt",
                    b"denied",
                    "text/plain",
                    "denied.txt", None
                )?
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
                    "changed.txt", None
                )?
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
                    "one.txt", None
                )?
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
                    "one.txt", None
                )?
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
        let impostor_scope = impostor.issue("forged", &Capabilities::new([volume.capability(VolumeOperation::Read, &mut None)?]), Some(reader), None, &mut None).expect("scope proof");
        assert!(
            ObjectContentStore::new(
                objects.clone(),
                bucket.clone(),
                provider.clone(),
                volume.clone(),
                impostor.verifier(&mut None).expect("verifier copy"),
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
                issuer.verifier(&mut None).expect("verifier copy"),
                owner_scope.clone(),
                owner_scope,
                4_096
            )
            .await
            .is_err()
        );
        let wrong = FileRef::new(
            volume,
            "notes/one.txt".to_owned(),
            file.version().to_owned(),
            FileDescriptor::from_bytes(b"other", "text/plain", &mut None)?,
            "one.txt".to_owned(), &mut None
        )?;
        assert!(owner_store.read(&wrong, wrong.descriptor().byte_length(), None)?.await.is_err());
        let replacement = owner_store
            .stage(
                OperationId::from_bytes([19; 16]),
                "notes/one.txt",
                b"replacement bytes",
                "text/plain",
                "one.txt", None
            )?
            .await?.0;
        assert_ne!(replacement.version(), file.version());
        assert_eq!(owner_store.read(&file, file.descriptor().byte_length(), None)?.await?.as_ref(), b"owner bytes");
        assert_eq!(
            owner_store.read(&replacement, replacement.descriptor().byte_length(), None)?.await?.as_ref(),
            b"replacement bytes"
        );
        let replacement_artifact = owner_store.artifact_ref(&replacement)?;
        assert!(replacement_artifact.as_resource().version().is_none());
        owner_store.verify_artifact(&replacement_artifact).await?;
        owner_store
            .objects
            .put(
                wire::PutObjectHeader {
                    bucket: Some(owner_store.bucket.clone()),
                    object_key: String::from_utf8(
                        replacement_artifact.as_resource().key().to_vec(),
                    )
                    .map_err(|_| Error::Invalid("invalid test key".into()))?,
                    metadata: Some(wire::ObjectMetadata {
                        content_type: "text/plain".into(),
                        user: [("harness-display-name".into(), "one.txt".into())].into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                acyclic_stream::OwnedBytes::from_static(b"corrupt replacement"),
            )
            .await
            .map_err(storage)?;
        assert!(
            owner_store
                .verify_artifact(&replacement_artifact)
                .await
                .is_err()
        );
        assert!(owner_store.read(&replacement, replacement.descriptor().byte_length(), None)?.await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn lazy_mount_routes_registered_buckets_and_requires_exact_grant() -> Result<()> {
        let objects = Arc::new(MemoryObjects::new(MemoryOptions::default()).map_err(storage)?);
        let bucket_a = objects
            .create_bucket(wire::CreateBucketRequest {
                name: "harness-owner-a".into(),
                mutation: None,
            })
            .await
            .map_err(storage)?
            .bucket
            .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
        let bucket_b = objects
            .create_bucket(wire::CreateBucketRequest {
                name: "harness-owner-b".into(),
                mutation: None,
            })
            .await
            .map_err(storage)?
            .bucket
            .ok_or_else(|| Error::Storage("bucket identity is missing".into()))?;
        let owner_a = AgentId::from_bytes([31; 16]);
        let owner_b = AgentId::from_bytes([32; 16]);
        let attached = AgentId::from_bytes([33; 16]);
        let issuer = AuthorityIssuer::new(
            "objects-mount-test",
            [34; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "objects-mount-test".into(),
            },
        );
        let provider = ProviderRef::new("local", "objects", "1")?;
        let volume_a = VolumeRef::new(
            provider.clone(),
            bucket_a.name.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner_a), &mut None
        )?;
        let volume_b = VolumeRef::new(
            provider.clone(),
            bucket_b.name.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner_b), &mut None
        )?;
        let owner_scope_a = issuer.issue("owner-a", &Capabilities::new([
                volume_a.capability(VolumeOperation::Read, &mut None)?,
                volume_a.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(owner_a), None, &mut None).expect("scope proof");
        let owner_scope_b = issuer.issue("owner-b", &Capabilities::new([
                volume_b.capability(VolumeOperation::Read, &mut None)?,
                volume_b.capability(VolumeOperation::Write, &mut None)?,
            ]), Some(owner_b), None, &mut None).expect("scope proof");
        let owner_store_a = ObjectContentStore::new(
            objects.clone(),
            bucket_a.clone(),
            provider.clone(),
            volume_a.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a.clone(),
            owner_scope_a.clone(),
            4_096,
        )
        .await?;
        let owner_store_b = ObjectContentStore::new(
            objects.clone(),
            bucket_b.clone(),
            provider.clone(),
            volume_b.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_b.clone(),
            owner_scope_b.clone(),
            4_096,
        )
        .await?;
        let file_a = owner_store_a
            .stage(
                OperationId::from_bytes([35; 16]),
                "a.txt",
                b"bucket a",
                "text/plain",
                "a.txt", None
            )?
            .await?.0;
        let file_b = owner_store_b
            .stage(
                OperationId::from_bytes([36; 16]),
                "b.txt",
                b"bucket b",
                "text/plain",
                "b.txt", None
            )?
            .await?.0;
        let attached_a_scope =
            issuer.delegate_private_file_read(&owner_scope_a, attached, "attached-a", &file_a)?;
        let attached_b_scope =
            issuer.delegate_private_file_read(&owner_scope_b, attached, "attached-b", &file_b)?;
        let attached_a = ObjectContentStore::new(
            objects.clone(),
            bucket_a.clone(),
            provider.clone(),
            volume_a.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a.clone(),
            attached_a_scope,
            4_096,
        )
        .await?;
        let attached_b = ObjectContentStore::new(
            objects.clone(),
            bucket_b.clone(),
            provider.clone(),
            volume_b.clone(),
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_b.clone(),
            attached_b_scope,
            4_096,
        )
        .await?;
        let resolver = ObjectContentMountResolver::new(vec![attached_a, attached_b])?;
        let (grant_a, reader_a) = resolver.mount(&file_a).await?;
        grant_a.require_file_read(&file_a, None)?;
        assert_eq!(reader_a.read(&file_a, file_a.descriptor().byte_length(), None)?.await?.as_ref(), b"bucket a");
        let (grant_b, reader_b) = resolver.mount(&file_b).await?;
        grant_b.require_file_read(&file_b, None)?;
        assert_eq!(reader_b.read(&file_b, file_b.descriptor().byte_length(), None)?.await?.as_ref(), b"bucket b");

        let no_grant =
            issuer.issue("no-grant", &Capabilities::new([] as [String; 0]), Some(attached), None, &mut None).expect("scope proof");
        let denied_store = ObjectContentStore::new(
            objects,
            bucket_a,
            provider,
            volume_a,
            issuer.verifier(&mut None).expect("verifier copy"),
            owner_scope_a,
            no_grant,
            4_096,
        )
        .await?;
        let denied = ObjectContentMountResolver::new(vec![denied_store])?;
        assert!(denied.mount(&file_a).await.is_err());
        Ok(())
    }
}
