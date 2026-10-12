use super::*;

/// Provider-side proof for a parent-published project merge receipt.
pub struct FilesystemProjectMergeVerifier<A, O> {
    pub(in crate::filesystem) host: Arc<FilesystemHost<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_binding: Option<NativeFilesystemVerificationBinding>,
}

impl UploadRecord for JoinCommitWitness {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(std::mem::size_of::<Self>() as u64)
    }
}

impl fork_preparer_io::PreparationRecord for JoinCommitWitness {
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<serde_json::Value>().max(std::mem::size_of::<Self>()) as u64
    }
}

impl<A, O> FilesystemProjectMergeVerifier<A, O> {
    /// Binds verification to one exact Filesystem provider identity.
    #[must_use]
    pub fn new(host: Arc<FilesystemHost<A, O>>) -> Self {
        Self {
            host,
            #[cfg(not(target_arch = "wasm32"))]
            native_binding: None,
        }
    }

    /// Binds the original parent publication MAC and current grants.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, verifier: &AuthorityVerifier,
        scope: &Scope, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        host.provider.validate(&mut Some(&mut crate::store::native_work_io(work)?))?;
        let binding = NativeFilesystemVerificationBinding::new(verifier, scope, work)?;
        Ok(Self { host, native_binding: Some(binding) })
    }



    #[cfg(not(target_arch = "wasm32"))]
    /// Shares the verifier after admitting its allocation on the original resource.
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        let binding = self.native_binding.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "merge verifier has no native authority" })?;
        binding.authenticate(work)?;
        let mut header = work.claim(std::mem::size_of::<Self>() as u64, u64::from((std::mem::size_of::<Self>() as u64) != 0), acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(Error::from)?;
        binding.custody.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }
}

impl<A, O> ProjectMergeVerifier for FilesystemProjectMergeVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(&'a self, receipt: &'a ProjectMergeReceipt) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if receipt.source_project.provider() != &self.host.provider
                || receipt.target_project.provider() != &self.host.provider
                || receipt.provider_proof.provider != self.host.provider
                || receipt.provider_operation_id.as_slice()
                    != project_join_key(receipt.operation_id, None)?
                        .into_bytes()
                        .as_slice()
            {
                return Err(Error::Invalid(
                    "merge receipt belongs to another provider or retry identity".into(),
                ));
            }
            receipt.provider_proof.validate(&mut None)?;
            let source = workspace_ref(
                self.host.provider.clone(),
                &receipt.source_project.storage_name(&mut None)?,
            )?;
            let target = workspace_ref(
                self.host.provider.clone(),
                &receipt.target_project.storage_name(&mut None)?,
            )?;
            let source_workspace = self.host.open(&source).await?;
            let target_workspace = self.host.open(&target).await?;
            if receipt.provider_proof.format != FILESYSTEM_JOIN_PROOF_FORMAT {
                return Err(Error::Invalid(
                    "merge receipt has an unsupported provider proof".into(),
                ));
            }
            let witness: JoinCommitWitness =
                serde_json::from_value(receipt.provider_proof.statement.clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let source_generation = self
                .host
                .generation(&source_workspace, &receipt.source_generation)
                .await?;
            let expected = self
                .host
                .generation(&target_workspace, &receipt.expected_target_generation)
                .await?;
            let result = self
                .host
                .generation(&target_workspace, &receipt.result_generation)
                .await?;
            if witness.source_workspace != source_workspace.id().volume_id()
                || witness.target_workspace != target_workspace.id().volume_id()
                || witness.source_generation != source_generation.id()
                || witness.expected_target != expected.id()
                || witness.result_generation != result.id()
                || witness.operation_id.into_bytes().as_slice()
                    != receipt.provider_operation_id.as_slice()
                || witness.history != JoinHistory::Merge
                || !target_workspace
                    .verify_join_commit(&witness)
                    .await
                    .map_err(map_error)?
            {
                return Err(Error::Invalid(
                    "merge receipt does not prove the committed Filesystem join".into(),
                ));
            }
            let normalized_source = source_generation
                .normalized_join_parent_for(&expected)
                .await
                .map_err(map_error)?;
            let parents = result.parents(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
            if parents.as_slice() != [expected.id(), normalized_source] {
                return Err(Error::Invalid(
                    "merge result does not bind the inspected child and target generations".into(),
                ));
            }
            self.host
                .retain_generation(&source, &receipt.source_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.expected_target_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.result_generation)
                .await?;
            Ok(())
        })
    }
}
