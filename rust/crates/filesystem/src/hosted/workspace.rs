/// One hosted named workspace with an exact last-observed head.
#[derive(Clone)]
pub struct HostedWorkspace {
    filesystem: HostedFs,
    reference: wire::WorkspaceRef,
    profile: wire::FilesystemProfile,
    head: wire::GenerationRef,
}

impl HostedWorkspace {
    /// Stable opaque workspace identity.
    #[must_use]
    pub fn id(&self) -> &[u8] {
        &self.reference.workspace_id
    }

    /// Canonical workspace name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.reference.name
    }

    /// Exact profile selected at creation.
    #[must_use]
    pub const fn profile(&self) -> wire::FilesystemProfile {
        self.profile
    }

    /// Resolves the current exact immutable head.
    pub async fn head(&self) -> Result<HostedGeneration, HostedFsError> {
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_head(
                self.filesystem
                    .request(wire::GetHeadRequest {
                        workspace: Some(self.reference.clone()),
                    })
                    .await?,
            )
            .await?
            .into_inner();
        self.generation_handle(response.generation)
    }

    /// Returns the current provider-backed source lifecycle state.
    pub async fn source_state(&self) -> Result<HostedSourceResult, HostedFsError> {
        self.require_source_reconciliation()?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_source_state(
                self.filesystem
                    .request(wire::SourceStateRequest {
                        workspace: Some(self.reference.clone()),
                    })
                    .await?,
            )
            .await?
            .into_inner();
        self.source_result(&response)
    }

    /// Reconciles pending provider events with exactly idempotent retry.
    pub async fn reconcile_source(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.source_operation(idempotency_key, SourceOperation::Reconcile)
            .await
    }

    /// Rebuilds provider state from an authoritative source scan.
    pub async fn rescan_source(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.source_operation(idempotency_key, SourceOperation::Rescan)
            .await
    }

    /// Seals the provider-backed source and returns its immutable generation.
    pub async fn seal(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<HostedGeneration, HostedFsError> {
        self.require_source_reconciliation()?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .seal_source(
                self.filesystem
                    .request(wire::SourceOperationRequest {
                        workspace: Some(self.reference.clone()),
                        operation: Some(operation(idempotency_key)),
                    })
                    .await?,
            )
            .await?
            .into_inner();
        let result = self.source_result(&response)?;
        if result.state != HostedSourceState::Sealed {
            return Err(HostedFsError::InvalidResponse(
                "seal did not return a sealed generation",
            ));
        }
        self.generation_handle(response.generation)
    }

    fn require_source_reconciliation(&self) -> Result<(), HostedFsError> {
        if self.filesystem.capabilities.source_reconciliation {
            Ok(())
        } else {
            Err(HostedFsError::InvalidOptions(
                "hosted deployment does not support source reconciliation",
            ))
        }
    }

    async fn source_operation(
        &self,
        idempotency_key: IdempotencyKey,
        kind: SourceOperation,
    ) -> Result<HostedSourceResult, HostedFsError> {
        self.require_source_reconciliation()?;
        let request = self
            .filesystem
            .request(wire::SourceOperationRequest {
                workspace: Some(self.reference.clone()),
                operation: Some(operation(idempotency_key)),
            })
            .await?;
        let mut client = self.filesystem.client.clone();
        let response = match kind {
            SourceOperation::Reconcile => client.reconcile_source(request).await?.into_inner(),
            SourceOperation::Rescan => client.rescan_source(request).await?.into_inner(),
        };
        self.source_result(&response)
    }

    fn source_result(
        &self,
        response: &wire::SourceResponse,
    ) -> Result<HostedSourceResult, HostedFsError> {
        let invalidation = match wire::SourceInvalidationReason::try_from(response.reason) {
            Ok(wire::SourceInvalidationReason::Unspecified) => None,
            Ok(wire::SourceInvalidationReason::InitialSnapshotRequired) => {
                Some(HostedSourceInvalidation::InitialSnapshotRequired)
            }
            Ok(wire::SourceInvalidationReason::QueueOverflow) => {
                Some(HostedSourceInvalidation::QueueOverflow)
            }
            Ok(wire::SourceInvalidationReason::NativeRescanRequired) => {
                Some(HostedSourceInvalidation::NativeRescanRequired)
            }
            Ok(wire::SourceInvalidationReason::BackendError) => {
                Some(HostedSourceInvalidation::BackendError)
            }
            Ok(wire::SourceInvalidationReason::UnrepresentablePath) => {
                Some(HostedSourceInvalidation::UnrepresentablePath)
            }
            Ok(wire::SourceInvalidationReason::AmbiguousRename) => {
                Some(HostedSourceInvalidation::AmbiguousRename)
            }
            Ok(wire::SourceInvalidationReason::RootChanged) => {
                Some(HostedSourceInvalidation::RootChanged)
            }
            Err(_) => {
                return Err(HostedFsError::InvalidResponse(
                    "source invalidation reason is invalid",
                ));
            }
        };
        let state = match (wire::SourceState::try_from(response.state), invalidation) {
            (Ok(wire::SourceState::Clean), None) => HostedSourceState::Clean,
            (Ok(wire::SourceState::PendingCapture), None) => HostedSourceState::PendingCapture,
            (Ok(wire::SourceState::NeedsRescan), Some(reason)) => {
                HostedSourceState::NeedsRescan(reason)
            }
            (Ok(wire::SourceState::Conflict), None) => HostedSourceState::Conflict,
            (Ok(wire::SourceState::Sealed), None) => HostedSourceState::Sealed,
            _ => return Err(HostedFsError::InvalidResponse("source state is invalid")),
        };
        let has_generation = response.generation.is_some();
        if matches!(state, HostedSourceState::Clean | HostedSourceState::Sealed) != has_generation {
            return Err(HostedFsError::InvalidResponse(
                "source generation does not match its state",
            ));
        }
        let generation_id = response
            .generation
            .as_ref()
            .map(|generation| {
                validate_generation(generation, &self.reference)?;
                let bytes: [u8; 32] =
                    generation
                        .generation_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| {
                            HostedFsError::InvalidResponse(
                                "generation identity has the wrong length",
                            )
                        })?;
                Ok::<GenerationId, HostedFsError>(GenerationId::new(Digest::from_bytes(bytes)))
            })
            .transpose()?;
        Ok(HostedSourceResult {
            state,
            generation_id,
        })
    }

    /// Reopens and authenticates one exact generation.
    pub async fn generation(
        &self,
        generation_id: impl Into<Vec<u8>>,
    ) -> Result<HostedGeneration, HostedFsError> {
        let selected = wire::GenerationRef {
            workspace: Some(self.reference.clone()),
            generation_id: generation_id.into(),
        };
        validate_generation(&selected, &self.reference)?;
        let mut client = self.filesystem.client.clone();
        let response = client
            .get_generation(
                self.filesystem
                    .request(wire::GetGenerationRequest {
                        generation: Some(selected),
                    })
                    .await?,
            )
            .await?
            .into_inner();
        self.generation_handle(response.generation)
    }

    fn generation_handle(
        &self,
        value: Option<wire::GenerationRef>,
    ) -> Result<HostedGeneration, HostedFsError> {
        let reference = value.ok_or(HostedFsError::InvalidResponse("generation is absent"))?;
        validate_generation(&reference, &self.reference)?;
        Ok(HostedGeneration {
            workspace: self.clone(),
            reference,
        })
    }

    /// Begins a sparse transaction against the current observed head.
    pub fn begin_transaction(&self, idempotency_key: IdempotencyKey) -> HostedTransaction {
        HostedTransaction {
            workspace: self.clone(),
            base: self.head.clone(),
            idempotency_key,
            mutations: Vec::new(),
        }
    }

    /// Deletes this workspace head with exactly idempotent retry.
    pub async fn delete(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::MutationResponse, HostedFsError> {
        let mut client = self.filesystem.client.clone();
        Ok(client
            .delete_workspace(
                self.filesystem
                    .request(wire::DeleteWorkspaceRequest {
                        workspace: Some(self.reference.clone()),
                        operation: Some(operation(idempotency_key)),
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Observation-safe rebases this fork using bounded history/change/conflict work.
    pub async fn rebase(
        &self,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RebaseResponse, HostedFsError> {
        self.filesystem
            .require_page_bound(maximum_generations, "rebase generations")?;
        self.filesystem
            .require_page_bound(maximum_changes, "rebase changes")?;
        self.filesystem
            .require_page_bound(maximum_conflicts, "rebase conflicts")?;
        let mut client = self.filesystem.client.clone();
        Ok(client
            .rebase(
                self.filesystem
                    .request(wire::RebaseRequest {
                        workspace: Some(self.reference.clone()),
                        maximum_conflicts,
                        operation: Some(operation(idempotency_key)),
                        maximum_generations,
                        maximum_changes,
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Issues a short-lived mount capability scoped to one exact generation.
    pub async fn issue_mount_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.issue_credential(
            generation,
            writable,
            expires_after_seconds,
            idempotency_key,
            false,
        )
        .await
    }

    /// Issues short-lived S3 credentials scoped to one exact generation.
    pub async fn issue_s3_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.issue_credential(
            generation,
            writable,
            expires_after_seconds,
            idempotency_key,
            true,
        )
        .await
    }

    /// Discovers the deployment endpoint and region and issues read-only S3 access
    /// for the workspace's current immutable head.
    pub async fn s3_access(&self) -> Result<HostedS3Access, HostedFsError> {
        self.s3_access_with_options(HostedS3AccessOptions::default())
            .await
    }

    /// Discovers and validates S3 access using explicit scope, lifetime, and retry identity.
    pub async fn s3_access_with_options(
        &self,
        options: HostedS3AccessOptions,
    ) -> Result<HostedS3Access, HostedFsError> {
        if !self.filesystem.capabilities.s3_credentials {
            return Err(HostedFsError::InvalidOptions(
                "hosted deployment does not support S3 credentials",
            ));
        }
        let generation = self.head().await?;
        let response = self
            .issue_s3_credential(
                &generation,
                options.writable,
                options.expires_after_seconds,
                options.idempotency_key,
            )
            .await?;
        hosted_s3_access(response)
    }

    async fn issue_credential(
        &self,
        generation: &HostedGeneration,
        writable: bool,
        expires_after_seconds: u64,
        idempotency_key: IdempotencyKey,
        s3: bool,
    ) -> Result<wire::CredentialResponse, HostedFsError> {
        self.filesystem.require_owner(&generation.workspace)?;
        let request = self
            .filesystem
            .request(wire::CredentialRequest {
                workspace: Some(self.reference.clone()),
                generation: Some(generation.reference.clone()),
                writable,
                expires_after_seconds,
                operation: Some(operation(idempotency_key)),
            })
            .await?;
        let mut client = self.filesystem.client.clone();
        if s3 {
            Ok(client.issue_s3_credential(request).await?.into_inner())
        } else {
            Ok(client.issue_mount_credential(request).await?.into_inner())
        }
    }
}

#[derive(Clone, Copy)]
enum SourceOperation {
    Reconcile,
    Rescan,
}

fn hosted_s3_access(response: wire::CredentialResponse) -> Result<HostedS3Access, HostedFsError> {
    let Some(wire::credential_response::Credential::S3(credential)) = response.credential else {
        return Err(HostedFsError::InvalidResponse(
            "S3 credential response has the wrong credential kind",
        ));
    };
    if response.endpoint.is_empty()
        || response.expires_at_unix_seconds <= 0
        || credential.bucket.is_empty()
        || credential.region.is_empty()
        || credential.access_key_id.is_empty()
        || credential.secret_access_key.is_empty()
    {
        return Err(HostedFsError::InvalidResponse(
            "S3 credential response is incomplete",
        ));
    }
    Ok(HostedS3Access {
        endpoint: response.endpoint,
        expires_at_unix_seconds: response.expires_at_unix_seconds,
        bucket: credential.bucket,
        region: credential.region,
        access_key_id: credential.access_key_id,
        secret_access_key: credential.secret_access_key,
        session_token: credential.session_token,
    })
}
