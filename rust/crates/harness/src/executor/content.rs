use super::*;

pub(super) async fn verify_model_contents_scoped<'a>(
    journal: &dyn ExecutionJournal,
    contents: impl Iterator<Item = &'a ModelContent> + Clone,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    for content in contents.clone() {
        validate_model_content_scope(content, limits, scope, io)?;
    }
    for content in contents {
        match io {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(io) if io.original_native_work().is_some() => {
                journal
                    .verify_model_content_with_native_work(
                        content,
                        io.original_native_work()
                            .expect("native branch retains original work"),
                    )?
                    .await?;
            }
            #[cfg(feature = "filesystem")]
            Some(io) => io.receive_content(
                journal
                    .verify_model_content_measured(content, io.remaining()?, io.cancellation())
                    .await,
            )?,
            _ => journal.verify_model_content(content).await?,
        }
    }
    Ok(())
}

pub(super) fn validate_model_content_scope(
    content: &ModelContent,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| content.validate_limits(limits, work))?;
    if let Some(scope) = scope {
        crate::executor::with_history_io(io, |work| content.validate_limits(scope.limits(), work))?;
    }
    let schemas = scope.and_then(RuntimeScope::extension_schema_registry);
    let runtime = scope.and_then(RuntimeScope::extension_runtime);
    for (file, binding) in content.file_bindings() {
        if let Some(scope) = scope
            && !with_history_io(io, |work| {
                crate::runtime::read_granted_with_work(scope.grants(), file, work)
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "attenuated task cannot read model content",
            ));
        }
        if let Some(binding) = binding {
            let scope = scope.ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "original native task scope is unavailable",
                )
            })?;
            binding.verify_original(scope.extensions(), schemas, runtime.as_deref(), io)?;
        }
    }
    Ok(())
}

impl StockExecution<'_> {
    pub(super) async fn verify_model_content(
        &self,
        journal: &dyn ExecutionJournal,
        content: &ModelContent,
    ) -> Result<()> {
        verify_model_contents_scoped(
            journal,
            std::iter::once(content),
            self.stock.limits,
            self.original_context()
                .map(crate::runtime::TaskContext::scope),
            self.io,
        )
        .await
    }

    pub(super) async fn verify_model_request_content(
        &self,
        journal: &dyn ExecutionJournal,
        request: &crate::model::PreparedModelRequest,
    ) -> Result<()> {
        verify_model_contents_scoped(
            journal,
            request
                .request()
                .messages
                .iter()
                .map(|message| &message.content),
            self.stock.limits,
            self.original_context()
                .map(crate::runtime::TaskContext::scope),
            self.io,
        )
        .await
    }

    pub(super) async fn validate_turn_input(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
    ) -> Result<()> {
        self.stock.limits.validate(&mut None)?;
        self.model_capacity()?;
        if input.max_steps == 0 || input.max_steps as usize > self.stock.limits.model_steps {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "max_steps exceeds configured model step bound",
            ));
        }
        with_history_io(self.io, |work| input.input.validate_user_input(work))?;
        let scope = self
            .original_context()
            .map(crate::runtime::TaskContext::scope);
        validate_model_content_scope(&input.input, self.stock.limits, scope, self.io)?;
        if let Some(selected) = &input.selected_context {
            selected.validate_for_input(&input.input, self.io)?;
            if selected.messages.len() > self.stock.limits.context_messages {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Invalid,
                    "selected context exceeds configured message limit",
                ));
            }
            for message in &selected.messages {
                validate_model_content_scope(&message.content, self.stock.limits, scope, self.io)?;
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = self.native_work() {
                journal
                    .verify_selected_context_with_native_work(input.operation_id, selected, work)?
                    .await?;
            } else {
                journal
                    .verify_selected_context(input.operation_id, selected)
                    .await?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            journal
                .verify_selected_context(input.operation_id, selected)
                .await?;
            for message in &selected.messages {
                crate::executor::with_history_io(self.io, |work| {
                    message.content.validate_limits(self.stock.limits, work)
                })?;
                self.verify_model_content(journal, &message.content).await?;
            }
        }
        self.verify_model_content(journal, &input.input).await?;
        Ok(())
    }
}
