impl<A, O> FilesystemWireService<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    async fn rpc_export(
        &self,
        request: Request<wire::ExportRequest>,
    ) -> Result<Response<WireStream<wire::ExportChunk>>, Status> {
        self.admit(&request)?;
        let request = request.into_inner();
        if request.maximum_objects == 0
            || request.maximum_objects > self.limits.maximum_page_items
            || request.maximum_bytes == 0
            || request.maximum_bytes > self.limits.maximum_response_bytes
        {
            return Err(Status::invalid_argument("export bounds are invalid"));
        }
        let generation = self.generation(request.generation).await?;
        let checkout = generation
            .workspace
            .checkout(
                crate::model::GenerationSelector::Exact(generation.id()),
                crate::model::CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        let manifest = checkout
            .export_manifest(crate::WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await;
        check_export_workspace(&generation.workspace).await?;
        let manifest = manifest
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?
            .value;
        let encoded = crate::encode_generation_export_manifest(&manifest)
            .map_err(|error| Status::data_loss(error.to_string()))?;
        let retained = u64::try_from(encoded.len()).unwrap_or(u64::MAX);
        if retained > request.maximum_bytes {
            return Err(Status::resource_exhausted(
                "export manifest exceeds byte bound",
            ));
        }
        let start = transfer_cursor(&request.after)?;
        let start_index = usize::try_from(start)
            .map_err(|_| Status::invalid_argument("export cursor is out of range"))?;
        if start_index > manifest.objects.len() {
            return Err(Status::invalid_argument("export cursor is out of range"));
        }
        let manifest_chunk = (start == 0).then(|| wire::ExportChunk {
            cursor: 0_u64.to_le_bytes().to_vec(),
            object_id: Vec::new(),
            contents: encoded,
            terminal: manifest.objects.is_empty(),
        });
        let state = ExportState {
            filesystem: self.filesystem.clone(),
            workspace: generation.workspace,
            manifest: manifest_chunk,
            objects: manifest.objects,
            index: start_index,
            cursor: start,
            retained,
            emitted: 0,
            maximum_objects: request.maximum_objects,
            maximum_bytes: request.maximum_bytes,
        };
        let object_stream = stream::try_unfold(state, ExportState::next);
        Ok(Response::new(Box::pin(object_stream)))
    }

    async fn rpc_import(
        &self,
        request: Request<tonic::Streaming<wire::ImportChunk>>,
    ) -> Result<Response<wire::ImportResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("import stream is empty"))?;
        self.admit_message(&first)?;
        if !first.object_id.is_empty() || first.contents.is_empty() {
            return Err(Status::invalid_argument(
                "import must begin with a manifest chunk",
            ));
        }
        let workspace_ref = required(first.workspace, "workspace")?;
        let operation_bytes = first.operation_id.clone();
        let operation_id = operation(Some(wire::OperationOptions {
            idempotency_key: operation_bytes.clone(),
        }))?
        .operation_id();
        let manifest = crate::decode_generation_export_manifest(
            &first.contents,
            self.limits.maximum_response_bytes,
            u64::from(self.limits.maximum_page_items),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let expected_workspace = self
            .filesystem
            .workspace_id(&workspace_ref.name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        if expected_workspace.into_bytes().as_slice() != workspace_ref.workspace_id
            || expected_workspace.volume_id() != manifest.volume_id
        {
            return Err(Status::failed_precondition(
                "import workspace identity mismatch",
            ));
        }
        let mut expected_index = 0_usize;
        let mut terminal = first.terminal;
        while let Some(chunk) = stream.message().await? {
            self.admit_message(&chunk)?;
            if expected_index >= self.limits.maximum_page_items as usize {
                return Err(Status::resource_exhausted(
                    "import object count exceeds service bound",
                ));
            }
            if chunk.workspace.as_ref() != Some(&workspace_ref)
                || chunk.operation_id != operation_bytes
                || terminal
            {
                return Err(Status::invalid_argument(
                    "import stream continuity mismatch",
                ));
            }
            let object = decode_object_id(&chunk.object_id)?;
            if manifest.objects.get(expected_index).copied() != Some(object) {
                return Err(Status::invalid_argument("import object order mismatch"));
            }
            self.filesystem
                .import_object(
                    object,
                    chunk.contents,
                    crate::WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
            expected_index = expected_index.saturating_add(1);
            if transfer_cursor(&chunk.cursor)? != expected_index as u64 {
                return Err(Status::invalid_argument("import cursor mismatch"));
            }
            terminal = chunk.terminal;
        }
        if !terminal || expected_index != manifest.objects.len() {
            return Err(Status::invalid_argument(
                "import stream ended before its closure",
            ));
        }
        self.filesystem
            .restore_volume(
                &manifest,
                operation_id,
                crate::WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| Status::unavailable(failure.error.to_string()))?;
        self.import_response(&workspace_ref).await
    }
    async fn import_response(
        &self,
        workspace_ref: &wire::WorkspaceRef,
    ) -> Result<Response<wire::ImportResponse>, Status> {
        let workspace = self
            .filesystem
            .open_workspace(
                &workspace_ref.name,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| status(&failure.error))?
            .value;
        Ok(Response::new(wire::ImportResponse {
            outcome: Some(mutation(
                wire::MutationStatus::Committed,
                Some(generation_ref(
                    &workspace
                        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(|failure| status(&failure.error))?
                        .value,
                )),
                None,
            )),
        }))
    }
}

async fn check_export_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &crate::Workspace<A, O>,
) -> Result<(), Status> {
    #[cfg(test)]
    if tests::omitted_export_fence() {
        return Ok(());
    }
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    workspace
        .check_current_access(&mut accounting, &cancellation)
        .await
        .map_err(|error| status(&error))
}

struct ExportState<A, O> {
    filesystem: Fs<A, O>,
    workspace: Workspace<A, O>,
    manifest: Option<wire::ExportChunk>,
    objects: Vec<ObjectId>,
    index: usize,
    cursor: u64,
    retained: u64,
    emitted: u32,
    maximum_objects: u32,
    maximum_bytes: u64,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ExportState<A, O> {
    async fn next(mut self) -> Result<Option<(wire::ExportChunk, Self)>, Status> {
        check_export_workspace(&self.workspace).await?;
        if let Some(chunk) = self.manifest.take() {
            return Ok(Some((chunk, self)));
        }
        if self.index == self.objects.len()
            || self.emitted == self.maximum_objects
            || self.retained == self.maximum_bytes
        {
            return Ok(None);
        }
        let Some(&object) = self.objects.get(self.index) else {
            return Ok(None);
        };
        let remaining = self.maximum_bytes.saturating_sub(self.retained);
        let body = self
            .filesystem
            .export_object(
                object,
                remaining,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await;
        check_export_workspace(&self.workspace).await?;
        let body = body
            .map_err(|failure| Status::resource_exhausted(failure.error.to_string()))?
            .value
            .bytes;
        self.retained = self
            .retained
            .checked_add(u64::try_from(body.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| Status::resource_exhausted("export byte count overflow"))?;
        self.index = self.index.saturating_add(1);
        self.cursor = self.cursor.saturating_add(1);
        self.emitted = self.emitted.saturating_add(1);
        let chunk = wire::ExportChunk {
            cursor: self.cursor.to_le_bytes().to_vec(),
            object_id: object.to_bytes().to_vec(),
            contents: body.to_vec(),
            terminal: self.index == self.objects.len(),
        };
        Ok(Some((chunk, self)))
    }
}
