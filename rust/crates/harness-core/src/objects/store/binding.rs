use super::*;
impl ObjectContentStore {
    /// Checks the original owner before binding the account's blobs.
    pub fn new(
        objects: Arc<dyn Blobs>,
        expected_provider: ProviderRef,
        volume: VolumeRef,
        authorization: (AuthorityVerifier, Scope),
        scope: Scope,
        maximum_bytes: u64,
    ) -> Result<Arc<Self>> {
        let (verifier, owner_scope) = authorization;
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
        {
            return Err(Error::Invalid("Objects content binding is invalid".into()));
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
