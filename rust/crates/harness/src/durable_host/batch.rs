//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    pub(super) fn batch_stream(&self, batch_id: BatchId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            self.stream.clone(),
            format!("harness/v2/batches/{batch_id}"),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) fn batch_cancellation_stream(
        &self,
        batch_id: BatchId,
    ) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            self.stream.clone(),
            format!("harness/v2/batch-cancellations/{batch_id}"),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) async fn retained_batch_cancellation(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchCancellation>> {
        let stream = self.batch_cancellation_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch cancellation history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage(
                "batch cancellation record is missing".into(),
            ));
        };
        let declaration: BatchCancellation = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if declaration.batch_id != batch_id
            || crate::contract::canonical_json_bytes(&declaration)? != record.value.as_ref()
        {
            return Err(Error::Storage(
                "batch cancellation record is not canonical".into(),
            ));
        }
        Ok(Some(declaration))
    }

    pub(super) async fn declare_batch_cancellation(
        &self,
        request: &DurableBatchRequest,
    ) -> Result<()> {
        let declaration = BatchCancellation {
            batch_id: request.batch_id,
            group_id: request.group_id,
        };
        let bytes = crate::contract::canonical_json_bytes(&declaration)?;
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        self.publish_control(
            &self.batch_cancellation_stream(request.batch_id)?,
            "batch-cancel",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &bytes,
        )
        .await?;
        if self.retained_batch_cancellation(request.batch_id).await? != Some(declaration) {
            return Err(Error::Conflict(
                "batch cancellation belongs to another group".into(),
            ));
        }
        Ok(())
    }

    pub(super) async fn retained_batch(&self, batch_id: BatchId) -> Result<Option<Value>> {
        let stream = self.batch_stream(batch_id)?;
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if bounds.tail != 1 {
            return Err(Error::Storage(
                "batch manifest history is not exactly retained".into(),
            ));
        }
        let records = stream.read(0, 2).await?.try_collect::<Vec<_>>().await?;
        let [record] = records.as_slice() else {
            return Err(Error::Storage("batch manifest record is missing".into()));
        };
        let reference: FileRef = crate::contract::json_from_slice(&record.value)
            .map_err(|error| Error::Storage(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&reference)? != record.value.as_ref() {
            return Err(Error::Storage("batch manifest ref is not canonical".into()));
        }
        self.read_json(&reference)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed batch manifest content is missing".into())
                }
                other => other,
            })
            .map(Some)
    }

    pub(super) async fn commit_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        let canonical = request.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > request.scope.limits().file_bytes {
            return Err(Error::Invalid("batch manifest exceeds file limit".into()));
        }
        let operation_id = OperationId::from_bytes(request.batch_id.into_bytes());
        let key = format!("batch-request:{}", request.batch_id);
        let reference = self
            .payloads
            .stage(operation_id, &key, &bytes, None)?
            .await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        if self.read_json(&reference).await? != canonical {
            return Err(Error::Storage("staged batch request changed".into()));
        }
        let published = crate::contract::canonical_json_bytes(&reference)?;
        self.publish_control(
            &self.batch_stream(request.batch_id)?,
            "batch",
            TaskId::from_bytes(request.batch_id.into_bytes()),
            operation_id,
            &published,
        )
        .await?;
        if self.retained_batch(request.batch_id).await? != Some(canonical) {
            return Err(Error::Conflict(
                "batch identity belongs to another request".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn validate_batch(&self, request: &DurableBatchRequest) -> Result<()> {
        request.validate()?;
        if request.execution.is_some()
            || request.policy != self.policy.as_ref().map(|policy| policy.identity())
            || request.machine.name != request.task.name
            || request.machine.version != request.task.version
            || request.machine.digest == [0; 32]
            || self
                .machines
                .resolve(&request.machine, &mut None)?
                .is_none()
        {
            return Err(Error::Invalid(
                "batch implementation or policy is invalid".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &request.task,
            &request.machine,
            Some(&request.input_schema),
            &request.output_schema,
            None,
            &mut None,
        )?;
        self.root_scope
            .narrow(request.scope.grants().clone(), request.scope.limits())?
            .with_run_limits(request.scope.run_limits(), &mut None)?;
        if request.parent.is_some() {
            require_descendant_grant(request.scope.grants(), &request.task)?;
        }
        if request.scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "batch extensions differ from owner bindings".into(),
            ));
        }
        validate_task_schemas(
            &request.input_schema,
            &request.output_schema,
            true,
            &mut None,
        )?;
        let input =
            crate::contract::compile_json_schema(&request.input_schema, "batch input", &mut None)?;
        for value in &request.inputs {
            input.validate(value).map_err(|error| {
                Error::Invalid(format!("batch input failed validation: {error}"))
            })?;
        }
        Ok(())
    }
}
