use super::*;
impl ObjectContentStore {
    /// Verifies each ordered batch outcome before retaining the complete upload.
    fn verify_publication(
        put: std::result::Result<acyclic_objects::Put, acyclic_objects::Error>,
        (digest, length): (Digest, u64),
    ) -> Result<()> {
        let put = put.map_err(|error| {
            if error.code == acyclic_objects::wire::ErrorCode::Conflict {
                Error::Conflict("upload identity belongs to different content".into())
            } else {
                storage(error)
            }
        })?;
        match put {
            acyclic_objects::Put::Created { verified }
            | acyclic_objects::Put::AlreadyExists { verified }
                if verified.digest == digest && verified.length == length =>
            {
                Ok(())
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
        operation_id: OperationId,
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
            let content =
                acyclic_objects::Sealed::new(acyclic_stream::OwnedBytes::copy_from_slice(bytes));
            let content_digest = Digest::sealed(&content);
            let reference = FileRef::new(
                self.volume.clone(),
                path.to_owned(),
                content_digest.to_hex(),
                descriptor,
                display_name.to_owned(),
                &mut None,
            )?;
            // The record binds the descriptor and name to the content. The
            // intent's bytes name only the operation and its one reference is
            // the record, so a retry naming other content conflicts; retaining
            // it keeps all three through collection.
            let record = acyclic_objects::Sealed::new(self.record(&reference)?.into());
            let record_digest = Digest::sealed(&record);
            let intent = format!(
                "harness-upload-intent-v1:{}:{operation_id}",
                self.volume.storage_name(&mut None)?
            );
            let intent = acyclic_objects::Sealed::new(intent.into_bytes().into());
            let intent_digest = Digest::sealed(&intent);
            let expected = [&content, &record, &intent]
                .map(|body| (Digest::sealed(body), body.bytes().len() as u64));
            let outcomes = self
                .objects
                .put_batch_sealed(vec![
                    (content, Vec::new()),
                    (record, vec![content_digest]),
                    (intent, vec![record_digest]),
                ])
                .await;
            if outcomes.len() != expected.len() {
                return Err(Error::Storage(
                    "Objects publication returned another batch length".into(),
                ));
            }
            for (outcome, expected) in outcomes.into_iter().zip(expected) {
                Self::verify_publication(outcome, expected)?;
            }
            self.objects
                .retain(&self.root()?, vec![intent_digest])
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
