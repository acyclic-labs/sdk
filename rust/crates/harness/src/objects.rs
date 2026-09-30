#![doc = include_str!("../docs/objects.md")]
#![cfg_attr(test, allow(clippy::too_many_lines))]

use crate::{
    Error, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, FileDescriptor,
        FileRef, Limits, VolumeOperation, VolumeRef, decode_attachment_manifest,
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
use acyclic_objects::v2::{ObjectsProvider, wire};
use futures::future::BoxFuture;
use std::collections::BTreeMap;
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
            || maximum_bytes > 5 * 1024 * 1024 * 1024
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

    fn key(&self, path: &str, content: &str) -> Result<String> {
        validate_content_path(path)?;
        if content.len() != 64
            || !content
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Error::Invalid("Objects content identity is invalid".into()));
        }
        let key = format!(
            "{}/{}/@content/{}",
            self.volume.storage_name()?,
            path,
            content
        );
        if key.len() > 1024 {
            return Err(Error::Invalid(
                "Objects content key exceeds provider limit".into(),
            ));
        }
        Ok(key)
    }

    async fn read_exact(&self, file: &FileRef) -> Result<Vec<u8>> {
        let grant = ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        grant.require_file_read(file)?;
        self.read_content(file).await
    }

    async fn read_content(&self, file: &FileRef) -> Result<Vec<u8>> {
        file.validate()?;
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
                self.maximum_bytes,
            )
            .await
            .map_err(storage)?;
        let info = read
            .header
            .object
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
        file.descriptor().verify(&read.body)?;
        Ok(read.body.to_vec())
    }

    async fn manifest(&self, file: &FileRef, count: u32) -> Result<Vec<Attachment>> {
        let bytes = self.read_exact(file).await?;
        decode_attachment_manifest(file, &bytes, count)
    }

    /// Converts content in this bucket into an immutable artifact key
    /// selection without copying its bytes or weakening the bucket boundary.
    pub fn artifact_ref(&self, file: &FileRef) -> Result<ArtifactRef> {
        file.validate()?;
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
        )?;
        if artifact.as_resource().provider() != self.volume.provider() {
            return Err(Error::Unauthorized(
                "artifact belongs to another Objects provider".into(),
            ));
        }
        let key = std::str::from_utf8(artifact.as_resource().key())
            .map_err(|_| Error::Invalid("Objects artifact key is not UTF-8".into()))?;
        let prefix = format!("{}/", self.volume.storage_name()?);
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
            .header
            .object
            .as_ref()
            .and_then(|object| object.metadata.as_ref())
            .ok_or_else(|| Error::Storage("Objects artifact metadata is missing".into()))?;
        let display_name = metadata
            .user
            .get("harness-display-name")
            .ok_or_else(|| Error::Storage("Objects artifact display name is missing".into()))?;
        let descriptor = FileDescriptor::from_bytes(&observed.body, &metadata.content_type)?;
        if content_identity(&descriptor, display_name)? != content {
            return Err(Error::Conflict(
                "Objects artifact content identity does not match its bytes".into(),
            ));
        }
        Ok(())
    }
}

/// Lazily routes immutable Objects refs to separately registered owner
/// bindings.  A registration captures an authenticated owner or delegated
/// reader scope; the scope is checked again for the exact ref on every mount.
/// The router contains no raw Objects handle and never treats a ref as a
/// bearer read capability.
pub struct ObjectContentMountResolver {
    stores: BTreeMap<String, Arc<ObjectContentStore>>,
}

impl ObjectContentMountResolver {
    /// Creates a resolver from independently authenticated bucket bindings.
    /// Duplicate logical volumes are rejected rather than silently replaced.
    pub fn new(stores: impl IntoIterator<Item = Arc<ObjectContentStore>>) -> Result<Self> {
        let mut resolver = Self {
            stores: BTreeMap::new(),
        };
        for store in stores {
            resolver.register(store)?;
        }
        Ok(resolver)
    }

    /// Creates an empty resolver for callers that prefer incremental setup.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            stores: BTreeMap::new(),
        }
    }

    /// Registers one exact owner-volume binding before the resolver is shared.
    pub fn register(&mut self, store: Arc<ObjectContentStore>) -> Result<()> {
        let volume = store.volume.clone();
        volume.validate()?;
        let key = volume.capability(VolumeOperation::Read)?;
        if self.stores.contains_key(&key) {
            return Err(Error::Invalid(
                "Objects content volume is registered twice".into(),
            ));
        }
        self.stores.insert(key, store);
        Ok(())
    }

    fn store(&self, volume: &VolumeRef) -> Result<Arc<ObjectContentStore>> {
        volume.validate()?;
        self.stores
            .get(&volume.capability(VolumeOperation::Read)?)
            .cloned()
            .ok_or_else(|| Error::Unsupported("Objects content volume is not registered".into()))
    }
}

impl Default for ObjectContentMountResolver {
    fn default() -> Self {
        Self::empty()
    }
}

/// Short alias for applications that use the adapter as a content router.
pub type ObjectContentRouter = ObjectContentMountResolver;

impl crate::conversation::ContentMountResolver for ObjectContentMountResolver {
    fn mount<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            reference.validate()?;
            let store = self.store(reference.volume())?;
            // Authenticate the current scope against this exact content reference before
            // returning either the grant or the owner reader.  ObjectContentStore
            // repeats this check when bytes are read, closing the ref-as-grant gap
            // even if the returned reader is retained by a caller.
            let grant = ContentGrant::verify_read(&store.verifier, &store.scope, reference)?;
            grant.require_file_read(reference)?;
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

    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            seed.validate()?;
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

    fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.read_exact(manifest).await })
    }

    fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.read_exact(file).await.map(|_| ()) })
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
            let content = content_identity(&descriptor, display_name)?;
            let key = self.key(path, &content)?;
            let retry = format!(
                "harness-upload:{}:{operation_id}",
                self.volume.storage_name()?
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
                    bytes::Bytes::copy_from_slice(bytes),
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
                FileRef::new(self.volume.clone(), path, content, descriptor, display_name)?;
            // A retry receipt does not establish continued byte residency.
            // Verify the exact immutable content key before returning the reference.
            let committed = self.read_content(&reference).await?;
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
                ContentResidencyVerifier::verify(self, &attachment.file).await?;
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
fn storage(error: acyclic_objects::v2::Error) -> Error {
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
        conversation::{ContentMountResolver, VolumeClass, VolumeOwner},
        core::{AggregateKind, Authority, AuthorityIssuer},
        fork::ForkPreparation,
        resources::{GenerationRef, ProviderRef, StreamRef},
    };
    use acyclic_objects::v2::{MemoryObjects, MemoryOptions};

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
            format!("{}/../outside", owner_store.volume.storage_name()?).into_bytes(),
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
                child_project_volume: VolumeRef::new(
                    filesystem.clone(),
                    "artifact-child-project",
                    VolumeClass::Project,
                    project_owner.clone(),
                )?,
                child_private_volume: VolumeRef::new(
                    filesystem.clone(),
                    "artifact-child-private",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(child_agent),
                )?,
                inherited_through_sequence: 0,
                maximum_inherited_messages: 1,
                maximum_inherited_bytes: 1_024,
                maximum_inherited_references: 1,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        ProviderRef::new("local", "stream", "2")?,
                        parent.stream_path()?.into_bytes(),
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
                            project_owner,
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
        let [history_selection, project_selection, artifact_selection] =
            request.selections.as_slice()
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
        let seed = ForkSeed {
            operation_id: request.operation_id,
            parent: request.parent.clone(),
            parent_revision: request.parent_revision,
            child: request.child.clone(),
            child_agent: request.child_agent,
            attached_agents: Vec::new(),
            resources: vec![
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
            boundary: None,
        };
        ForkSeedVerifier::verify(owner_store.as_ref(), &seed).await?;
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
        let replacement = owner_store
            .stage(
                OperationId::from_bytes([19; 16]),
                "notes/one.txt",
                b"replacement bytes",
                "text/plain",
                "one.txt",
            )
            .await?;
        assert_ne!(replacement.version(), file.version());
        assert_eq!(owner_store.read(&file).await?.as_slice(), b"owner bytes");
        assert_eq!(
            owner_store.read(&replacement).await?.as_slice(),
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
                bytes::Bytes::from_static(b"corrupt replacement"),
            )
            .await
            .map_err(storage)?;
        assert!(
            owner_store
                .verify_artifact(&replacement_artifact)
                .await
                .is_err()
        );
        assert!(owner_store.read(&replacement).await.is_err());
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
            VolumeOwner::Agent(owner_a),
        )?;
        let volume_b = VolumeRef::new(
            provider.clone(),
            bucket_b.name.clone(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner_b),
        )?;
        let owner_scope_a = issuer.root_for_agent(
            owner_a,
            "owner-a",
            Capabilities::new([
                volume_a.capability(VolumeOperation::Read)?,
                volume_a.capability(VolumeOperation::Write)?,
            ]),
        );
        let owner_scope_b = issuer.root_for_agent(
            owner_b,
            "owner-b",
            Capabilities::new([
                volume_b.capability(VolumeOperation::Read)?,
                volume_b.capability(VolumeOperation::Write)?,
            ]),
        );
        let owner_store_a = ObjectContentStore::new(
            objects.clone(),
            bucket_a.clone(),
            provider.clone(),
            volume_a.clone(),
            issuer.verifier(),
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
            issuer.verifier(),
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
                "a.txt",
            )
            .await?;
        let file_b = owner_store_b
            .stage(
                OperationId::from_bytes([36; 16]),
                "b.txt",
                b"bucket b",
                "text/plain",
                "b.txt",
            )
            .await?;
        let attached_a_scope =
            issuer.delegate_private_file_read(&owner_scope_a, attached, "attached-a", &file_a)?;
        let attached_b_scope =
            issuer.delegate_private_file_read(&owner_scope_b, attached, "attached-b", &file_b)?;
        let attached_a = ObjectContentStore::new(
            objects.clone(),
            bucket_a.clone(),
            provider.clone(),
            volume_a.clone(),
            issuer.verifier(),
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
            issuer.verifier(),
            owner_scope_b.clone(),
            attached_b_scope,
            4_096,
        )
        .await?;
        let resolver = ObjectContentMountResolver::new(vec![attached_a, attached_b])?;
        let (grant_a, reader_a) = resolver.mount(&file_a).await?;
        grant_a.require_file_read(&file_a)?;
        assert_eq!(reader_a.read(&file_a).await?.as_slice(), b"bucket a");
        let (grant_b, reader_b) = resolver.mount(&file_b).await?;
        grant_b.require_file_read(&file_b)?;
        assert_eq!(reader_b.read(&file_b).await?.as_slice(), b"bucket b");

        let no_grant =
            issuer.root_for_agent(attached, "no-grant", Capabilities::new([] as [String; 0]));
        let denied_store = ObjectContentStore::new(
            objects,
            bucket_a,
            provider,
            volume_a,
            issuer.verifier(),
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
