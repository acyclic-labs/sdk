use super::*;

impl TaskContext {
    pub(super) async fn verify_model_files(&self, messages: &[ModelMessage]) -> Result<()> {
        let limits = self.scope.limits();
        let mut files = Vec::new();
        let mut configurations = Vec::new();
        for message in messages {
            message.content.validate_limits(limits, &mut None)?;
            for file in message.content.file_refs() {
                limits.validate_file(file, &mut None)?;
                if !read_granted(self.scope.grants(), file)? {
                    return Err(Error::Unauthorized(
                        "task scope cannot project this file".into(),
                    ));
                }
                files.push(file);
            }
            configurations.extend(message.content.native_configurations());
        }
        let proofs = configurations
            .iter()
            .map(|binding| {
                binding.verify_original(
                    self.scope.extensions.as_ref(),
                    self.scope.extension_schemas.as_ref(),
                    self.scope.extension_runtime.as_deref(),
                    None,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if files.is_empty() {
            return Ok(());
        }
        for (binding, (schemas, expected)) in configurations.iter().zip(proofs) {
            let content = self
                .content_binding(Some(binding.configuration.content.volume()))
                .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?;
            let bytes = content
                .reader
                .read(
                    &binding.configuration.content,
                    binding.configuration.content.descriptor().byte_length(),
                    None,
                )?
                .await?;
            schemas.verify_configuration_binding(
                &binding.configuration,
                expected,
                &bytes,
                limits,
                None,
            )?;
        }
        for file in files {
            if !configurations
                .iter()
                .any(|binding| &binding.configuration.content == file)
            {
                self.content_binding(Some(file.volume()))
                    .ok_or_else(|| Error::Unsupported("content reader is not bound".into()))?
                    .reader
                    .verify(file, file.descriptor().byte_length(), None)?
                    .await?;
            }
        }
        Ok(())
    }

    /// Runs one bounded, provider-neutral model request from live task code.
    #[allow(
        clippy::too_many_lines,
        reason = "keep authorization, token admission and bounded streaming in execution order"
    )]
    pub async fn model_events(
        &self,
        messages: Vec<ModelMessage>,
        max_output_tokens: Option<u32>,
    ) -> Result<Vec<ModelEvent>> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable model request requires a recorded effect".into(),
            ));
        }
        if !self.scope.grants().contains(capability::MODEL_GENERATE) {
            return Err(Error::Unauthorized(
                "task scope lacks model:generate".into(),
            ));
        }
        self.scope.run_limits().check_deadline()?;
        let deadline = self.scope.run_limits().deadline_epoch_ms;
        let binding = self
            .harness
            .model
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task model provider is not bound".into()))?;
        if max_output_tokens == Some(0) {
            return Err(Error::Invalid(
                "model output token bound must be positive".into(),
            ));
        }
        let tools = self
            .harness
            .tools
            .definitions(None)?
            .into_iter()
            .filter(|tool| {
                self.scope
                    .grants()
                    .contains(&capability::tool_call(&tool.name))
            })
            .collect();
        let step_bound = self
            .scope
            .run_limits()
            .max_steps
            .map_or(self.scope.limits().model_steps, |bound| {
                bound.min(self.scope.limits().model_steps)
            });
        let request = ModelRequest {
            model: binding.model.clone(),
            messages,
            tools,
            max_output_tokens,
        };
        let request =
            crate::model::PreparedModelRequest::prepare(request, self.scope.limits(), None)?;
        self.verify_model_files(&request.request().messages).await?;
        let capacity = binding.provider.context_capacity(&binding.model)?;
        capacity.validate()?;
        let output_tokens = max_output_tokens.unwrap_or(capacity.output_tokens);
        if output_tokens > capacity.output_tokens {
            return Err(Error::Invalid(
                "model output budget exceeds selected capacity".into(),
            ));
        }
        let request = if max_output_tokens.is_none() {
            let mut bounded = request.request().clone();
            bounded.max_output_tokens = Some(output_tokens);
            crate::model::PreparedModelRequest::prepare(bounded, self.scope.limits(), None)?
        } else {
            request
        };
        let input_tokens = binding
            .provider
            .count_tokens(&request)?
            .validate(&request, &mut None)?;
        if input_tokens
            .checked_add(u64::from(output_tokens))
            .is_none_or(|total| total > u64::from(capacity.context_tokens))
        {
            return Err(Error::Invalid(
                "model request exceeds selected token capacity".into(),
            ));
        }
        let step = self
            .model_steps
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                (used < step_bound).then_some(used + 1)
            })
            .map_err(|_| Error::Invalid("task exceeded its model step limit".into()))?;
        let mut events = Vec::new();
        let mut admission = ModelEventAdmission::default();
        let mut bytes = 0_u64;
        let dispatch = crate::model::ModelDispatch {
            operation_id: self.task_id,
            step: u32::try_from(step)
                .map_err(|_| Error::Invalid("model step exceeds portable bound".into()))?,
            request_digest: request.request_digest(),
        };
        let mut stream = binding.provider.generate(request, dispatch);
        loop {
            let next = match deadline {
                Some(deadline) => crate::live::timeout_until(deadline, stream.next()).await?,
                None => stream.next().await,
            };
            let Some(event) = next else { break };
            let event = event?;
            admission.observe(&event, self.scope.limits(), None)?;
            let event_bytes = crate::contract::canonical_json_bytes(&event)?;
            bytes = bytes
                .checked_add(event_bytes.len() as u64)
                .ok_or_else(|| Error::Invalid("model output size overflow".into()))?;
            if bytes > self.scope.limits().file_bytes {
                return Err(Error::Invalid("model output exceeds file limit".into()));
            }
            events.push(event);
        }
        if !admission.completed {
            return Err(Error::Invalid(
                "model stream ended without completion".into(),
            ));
        }
        Ok(events)
    }

    /// Adds a locally pinned policy layer.
    pub fn scoped_policy(&self, policy: Arc<dyn ToolPolicy>) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable scoped policy requires owner-host admission".into(),
            ));
        }
        let identity = policy.identity();
        validate_policy_identity(&identity, None)?;
        if let Some(parent) = &self.harness.policy
            && self.harness.policy_identity.as_ref() == Some(&identity)
        {
            if Arc::ptr_eq(parent, &policy) {
                return Ok(self.clone());
            }
            return Err(Error::Conflict(
                "scoped policy identity is already bound to another implementation".into(),
            ));
        }
        for (existing, bound) in &self.policy_overrides {
            if existing == &identity {
                if Arc::ptr_eq(bound, &policy) {
                    return Ok(self.clone());
                }
                return Err(Error::Conflict(
                    "scoped policy identity is already bound to another implementation".into(),
                ));
            }
        }
        let mut child = self.clone();
        child.policy_overrides.push((identity, policy));
        Ok(child)
    }
}
