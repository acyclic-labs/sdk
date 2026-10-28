use super::*;
impl ObjectContentStore {
    /// Stores `body` and returns its verified digest.
    async fn publish(&self, body: acyclic_stream::OwnedBytes, references: Vec<Digest>) -> Result<Digest> {
        let length = body.len() as u64;
        let digest = Digest::of(&body);
        match self.objects.put(body, references).await.map_err(storage)? {
            acyclic_objects::Put::Created { verified }
            | acyclic_objects::Put::AlreadyExists { verified }
                if verified.digest == digest && verified.length == length =>
            {
                Ok(digest)
            }
            acyclic_objects::Put::Unknown => Err(Error::Storage(
                "Objects publication outcome is unknown; retrying is safe".into(),
            )),
            _ => Err(Error::Storage(
                "Objects publication returned another digest".into(),
            )),
        }
    }

    /// The root that retains every record staged into this volume.
    fn root(&self) -> Result<acyclic_objects::RootId> {
        acyclic_objects::RootId::new(
            format!("harness-content:{}", self.volume.storage_name(&mut None)?).into_bytes(),
        )
        .map_err(storage)
    }
}

impl ContentPublisher for ObjectContentStore {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        _operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        access: crate::conversation::ContentAccess<'a, 'a>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (FileRef, Option<crate::conversation::ContentHold<'a>>),
        >,
    > {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        self.supports_stage(io)?;
        self.require_publication_ceiling(ceiling, io)?;
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
            validate_content_path(path, &mut None)?;
            // Staging is idempotent by content, so no per-operation intent is kept.
            let content = self
                .publish(acyclic_stream::OwnedBytes::copy_from_slice(bytes), Vec::new())
                .await?;
            let reference = FileRef::new(
                self.volume.clone(),
                path.to_owned(),
                content.to_hex(),
                descriptor,
                display_name.to_owned(),
                &mut None,
            )?;
            // The record binds the descriptor and name to the content; retaining
            // it keeps both through collection.
            let record = self
                .publish(self.record(&reference)?.into(), vec![content])
                .await?;
            self.objects
                .retain(&self.root()?, vec![record])
                .await
                .map_err(storage)?;
            Ok((reference, None))
        })
    }
}

impl ObjectContentStore {
    fn require_publication_ceiling(
        &self,
        ceiling: Option<&crate::Capabilities>,
        io: Option<&crate::conversation::ContentIo>,
    ) -> Result<()> {
        if let Some(grants) = ceiling
            && !crate::executor::with_history_io(io, |work| {
                crate::core::volume_write_granted_with_work(grants, &self.volume, work)
            })?
        {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a volume write grant",
            ));
        }
        Ok(())
    }
}
