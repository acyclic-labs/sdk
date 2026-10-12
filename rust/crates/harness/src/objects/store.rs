use super::*;

/// One authenticated Objects bucket/volume binding.
pub struct ObjectContentStore {
    objects: Arc<dyn ObjectsProvider>,
    pub(super) bucket: wire::BucketRef,
    pub(super) volume: VolumeRef,
    pub(super) verifier: AuthorityVerifier,
    pub(super) scope: Scope,
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
        expected_provider.validate(&mut None)?;
        verifier.verify(&scope, &mut None)?;
        ContentGrant::verify(
            &verifier,
            &owner_scope,
            &volume,
            VolumeOperation::Write,
            maximum_bytes,
            &mut None,
        )?;
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
                maximum_bytes,
                &mut None,
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

    async fn read_exact(
        &self,
        file: &FileRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let grant = ContentGrant::verify_read(&self.verifier, &self.scope, file)?;
        grant.require_file_read(file, None)?;
        self.read_content(file, maximum_bytes).await
    }

    async fn read_content(
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

    pub(super) async fn verify_artifact(&self, artifact: &ArtifactRef) -> Result<()> {
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

    fn verify<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() {
                return Err(crate::fork::boundary_error(
                    work,
                    crate::wire::ErrorCode::Unsupported,
                    "Objects fork verification has no original-work provider",
                ));
            }
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

    fn read_manifest<'a, 'w: 'a>(
        &'a self,
        _seed: &'a ForkSeed,
        manifest: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, (acyclic_stream::OwnedBytes, u64)>>
    {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() {
                return Err(crate::fork::boundary_error(
                    work,
                    crate::wire::ErrorCode::Unsupported,
                    "Objects fork verification has no original-work provider",
                ));
            }
            Ok((self.read_exact(manifest, self.maximum_bytes).await?, 0))
        })
    }

    fn verify_file<'a, 'w: 'a>(
        &'a self,
        _seed: &'a ForkSeed,
        file: &'a FileRef,
        work: &'a mut Option<&'w mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        crate::fork::admit_fork_future(work, |work| async move {
            if work.is_some() {
                return Err(crate::fork::boundary_error(
                    work,
                    crate::wire::ErrorCode::Unsupported,
                    "Objects fork verification has no original-work provider",
                ));
            }
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
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
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
                display_name.to_owned(),
                &mut None,
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
            let reference = FileRef::new(
                self.volume.clone(),
                path.to_owned(),
                content,
                descriptor,
                display_name.to_owned(),
                &mut None,
            )?;
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "maps the owned provider error at async boundaries"
)]
pub(super) fn storage(error: acyclic_objects::v1::Error) -> Error {
    Error::Storage(error.to_string())
}

pub(super) fn content_identity(descriptor: &FileDescriptor, display_name: &str) -> Result<String> {
    Ok(
        blake3::Hash::from_bytes(crate::contract::canonical_json_digest(&(
            descriptor,
            display_name,
        ))?)
        .to_hex()
        .to_string(),
    )
}
