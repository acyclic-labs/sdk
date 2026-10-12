use super::*;

/// Provider-side proof for a parent-published project merge receipt.
pub struct FilesystemProjectMergeVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemProjectMergeVerifier<A, O> {
    /// Binds the provider and optionally authenticates and funds its shared allocation.
    pub fn new(host: Arc<FilesystemHost<A, O>>,
        original: Option<(&AuthorityVerifier, &Scope, &PreparationIo)>,
    ) -> Result<Arc<Self>> {
        #[cfg(not(target_arch = "wasm32"))]
        let allocation = original.map(|(verifier, scope, io)| {
            crate::executor::with_history_io(Some(io), |work| {
                host.provider.validate(work)?;
                verifier.verify(scope, work)
            })?;
            let work = io.original_native_work().ok_or_else(|| io.invalid("merge verifier requires original native work"))?;
            work.claim(std::mem::size_of::<Self>() as u64, 1, WorkCounters::UNCHARGED).map_err(Error::from)
        }).transpose()?;
        #[cfg(target_arch = "wasm32")]
        if original.is_some() { return Err(Error::Unsupported("native merge verification is unavailable".into())); }
        Ok(Arc::new(Self { host,
            #[cfg(not(target_arch = "wasm32"))] _allocation: allocation }))
    }
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
                    .verify_join_commit(&witness, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?
            {
                return Err(Error::Invalid(
                    "merge receipt does not prove the committed Filesystem join".into(),
                ));
            }
            let normalized_source = source_generation
                .normalized_join_parent_for(&expected, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
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
