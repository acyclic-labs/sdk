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
            .await
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
        let filesystem = self.filesystem.clone();
        let objects = manifest.objects;
        let maximum_objects = request.maximum_objects;
        let maximum_bytes = request.maximum_bytes;
        let object_stream = stream::try_unfold(
            (filesystem, objects, start_index, start, retained, 0_u32),
            move |(filesystem, objects, index, cursor, retained, emitted)| async move {
                if index == objects.len() || emitted == maximum_objects || retained == maximum_bytes
                {
                    return Ok(None);
                }
                let Some(&object) = objects.get(index) else {
                    return Ok(None);
                };
                let remaining = maximum_bytes.saturating_sub(retained);
                let body = filesystem
                    .export_object(
                        object,
                        remaining,
                        crate::WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| Status::resource_exhausted(failure.error.to_string()))?
                    .value
                    .bytes;
                let retained = retained
                    .checked_add(u64::try_from(body.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| Status::resource_exhausted("export byte count overflow"))?;
                let next_index = index.saturating_add(1);
                let next_cursor = cursor.saturating_add(1);
                let chunk = wire::ExportChunk {
                    cursor: next_cursor.to_le_bytes().to_vec(),
                    object_id: object.to_bytes().to_vec(),
                    contents: body.to_vec(),
                    terminal: next_index == objects.len(),
                };
                Ok(Some((
                    chunk,
                    (
                        filesystem,
                        objects,
                        next_index,
                        next_cursor,
                        retained,
                        emitted.saturating_add(1),
                    ),
                )))
            },
        );
        let output = stream::iter(manifest_chunk.map(Ok)).chain(object_stream);
        Ok(Response::new(Box::pin(output)))
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
