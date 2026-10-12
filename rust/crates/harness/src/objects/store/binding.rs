use super::*;
impl ObjectContentStore {
    /// Checks the bucket and original owner before binding it.
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

    /// Exposes the authenticated reader and optional original-owner writer.
    pub fn bindings(self: &Arc<Self>) -> ContentBindings {
        ContentBindings {
            reader: self.clone(),
            writer: self
                .writer
                .as_ref()
                .map(|_| Arc::clone(self) as Arc<dyn ContentPublisher>),
        }
    }
}
