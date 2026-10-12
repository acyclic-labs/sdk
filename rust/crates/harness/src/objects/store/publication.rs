use super::*;
impl ObjectContentStore {
    pub(super) async fn pin_upload(
        &self,
        operation_id: OperationId,
        path: &str,
        descriptor: &FileDescriptor,
        display_name: &str,
    ) -> Result<()> {
        let intent = crate::contract::canonical_json_digest(
            &(path, descriptor, display_name),
            u64::MAX,
            &mut None,
        )?;
        let operation = blake3::Hash::from_bytes(crate::contract::canonical_json_digest(
            &("harness-upload-intent-v1", operation_id.to_string()),
            u64::MAX,
            &mut None,
        )?)
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
            let reference = FileRef::new(
                self.volume.clone(),
                path.to_owned(),
                content_identity(&descriptor, display_name)?,
                descriptor,
                display_name.to_owned(),
                &mut None,
            )?;
            let key = self.key(reference.path(), reference.version())?;
            self.pin_upload(
                operation_id,
                reference.path(),
                reference.descriptor(),
                reference.display_name(),
            )
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
