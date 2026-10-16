/// One exact immutable hosted generation.
#[derive(Clone)]
pub struct HostedGeneration {
    workspace: HostedWorkspace,
    reference: wire::GenerationRef,
}

impl HostedGeneration {
    /// Exact 32-byte generation identity.
    #[must_use]
    pub fn id(&self) -> &[u8] {
        &self.reference.generation_id
    }

    /// Reads a bounded byte range without materializing unrelated extents.
    pub async fn read_range(
        &self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "read bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read(
                self.workspace
                    .filesystem
                    .request(wire::ReadRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        range: Some(wire::ByteRange { offset, length }),
                        maximum_bytes,
                    })
                    .await?,
            )
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Reads one complete regular file up to an explicit maximum.
    pub async fn read(
        &self,
        path: impl Into<String>,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "read bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read(
                self.workspace
                    .filesystem
                    .request(wire::ReadRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        range: None,
                        maximum_bytes,
                    })
                    .await?,
            )
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Returns exact metadata for one path.
    pub async fn stat(&self, path: impl Into<String>) -> Result<wire::FileStat, HostedFsError> {
        let mut client = self.workspace.filesystem.client.clone();
        client
            .stat(
                self.workspace
                    .filesystem
                    .request(wire::StatRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                    })
                    .await?,
            )
            .await?
            .into_inner()
            .stat
            .ok_or(HostedFsError::InvalidResponse("file stat is absent"))
    }

    /// Lists one bounded directory page.
    pub async fn list_directory(
        &self,
        path: impl Into<String>,
        after: Option<wire::LogicalName>,
        maximum_items: u32,
    ) -> Result<wire::DirectoryPage, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_items, "directory items")?;
        let mut client = self.workspace.filesystem.client.clone();
        client
            .list_directory(
                self.workspace
                    .filesystem
                    .request(wire::ListDirectoryRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        page: Some(wire::PageOptions {
                            maximum_items,
                            after,
                        }),
                    })
                    .await?,
            )
            .await?
            .into_inner()
            .page
            .ok_or(HostedFsError::InvalidResponse("directory page is absent"))
    }

    /// Reads a bounded symbolic-link target.
    pub async fn read_symbolic_link(
        &self,
        path: impl Into<String>,
        maximum_bytes: u64,
    ) -> Result<Bytes, HostedFsError> {
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "symbolic-link bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        let response = client
            .read_link(
                self.workspace
                    .filesystem
                    .request(wire::ReadLinkRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        maximum_bytes,
                    })
                    .await?,
            )
            .await?
            .into_inner();
        Ok(Bytes::from(response.contents))
    }

    /// Plans only the sparse extents intersecting one requested range.
    pub async fn plan_extents(
        &self,
        path: impl Into<String>,
        offset: u64,
        length: u64,
        maximum_extents: u32,
    ) -> Result<wire::PlanExtentsResponse, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_extents, "extent items")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .plan_extents(
                self.workspace
                    .filesystem
                    .request(wire::PlanExtentsRequest {
                        generation: Some(self.reference.clone()),
                        path: path.into(),
                        range: Some(wire::ByteRange { offset, length }),
                        maximum_extents,
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Computes a bounded semantic diff to another generation in this client.
    pub async fn diff(
        &self,
        to: &HostedGeneration,
        maximum_changes: u32,
    ) -> Result<wire::DiffResponse, HostedFsError> {
        self.workspace.filesystem.require_owner(&to.workspace)?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_changes, "diff changes")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .diff(
                self.workspace
                    .filesystem
                    .request(wire::DiffRequest {
                        from: Some(self.reference.clone()),
                        to: Some(to.reference.clone()),
                        maximum_changes,
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Plans a bounded merge/rebase/squash/cherry-pick into an exact target.
    pub async fn plan_join(
        &self,
        target: &HostedGeneration,
        history: wire::JoinHistory,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<wire::JoinPlan, HostedFsError> {
        self.workspace.filesystem.require_owner(&target.workspace)?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_generations, "join generations")?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_changes, "join changes")?;
        self.workspace
            .filesystem
            .require_page_bound(maximum_conflicts, "join conflicts")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .plan_join(
                self.workspace
                    .filesystem
                    .request(wire::PlanJoinRequest {
                        source: Some(self.reference.clone()),
                        target: Some(target.reference.clone()),
                        maximum_changes,
                        maximum_conflicts,
                        maximum_generations,
                        history: history as i32,
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Streams one bounded canonical export without whole-generation buffering.
    pub async fn export(
        &self,
        after: Vec<u8>,
        maximum_objects: u32,
        maximum_bytes: u64,
    ) -> Result<tonic::Streaming<wire::ExportChunk>, HostedFsError> {
        self.workspace
            .filesystem
            .require_page_bound(maximum_objects, "export objects")?;
        self.workspace
            .filesystem
            .require_response_bound(maximum_bytes, "export bytes")?;
        let mut client = self.workspace.filesystem.client.clone();
        Ok(client
            .export(
                self.workspace
                    .filesystem
                    .request(wire::ExportRequest {
                        generation: Some(self.reference.clone()),
                        after,
                        maximum_objects,
                        maximum_bytes,
                    })
                    .await?,
            )
            .await?
            .into_inner())
    }

    /// Forks this exact generation without copying its immutable closure.
    pub async fn fork(
        &self,
        destination_name: impl Into<String>,
        idempotency_key: IdempotencyKey,
        paths: Option<Vec<String>>,
    ) -> Result<HostedWorkspace, HostedFsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.hosted.fork",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                let mut client = self.workspace.filesystem.client.clone();
                let response = client
                    .fork_workspace(
                        self.workspace
                            .filesystem
                            .request(wire::ForkWorkspaceRequest {
                                source: Some(self.reference.clone()),
                                destination_name: destination_name.into(),
                                operation: Some(operation(idempotency_key)),
                                selection: paths.map(|paths| wire::ForkPathSelection { paths }),
                            })
                            .await?,
                    )
                    .await?
                    .into_inner();
                self.workspace.filesystem.workspace(response.workspace)
            })
            .await,
        )
    }

    /// Retains this exact generation as a named checkpoint.
    pub async fn checkpoint(
        &self,
        identity: impl Into<String>,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        self.retain(identity.into(), idempotency_key, false).await
    }

    /// Retains this exact generation as an opaque pin.
    pub async fn pin(
        &self,
        identity: impl Into<String>,
        idempotency_key: IdempotencyKey,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        self.retain(identity.into(), idempotency_key, true).await
    }

    async fn retain(
        &self,
        identity: String,
        idempotency_key: IdempotencyKey,
        pin: bool,
    ) -> Result<wire::RetainGenerationResponse, HostedFsError> {
        let request = self
            .workspace
            .filesystem
            .request(wire::RetainGenerationRequest {
                generation: Some(self.reference.clone()),
                identity,
                operation: Some(operation(idempotency_key)),
            })
            .await?;
        let mut client = self.workspace.filesystem.client.clone();
        if pin {
            Ok(client.pin(request).await?.into_inner())
        } else {
            Ok(client.checkpoint(request).await?.into_inner())
        }
    }
}
