use super::*;

pub(crate) async fn verify_model_contents_scoped<'a>(
    journal: &dyn ExecutionJournal,
    contents: impl Iterator<Item = &'a ModelContent> + Clone + acyclic_stream::ProviderTask,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
    context: Option<&crate::runtime::TaskContext>,
) -> Result<()> {
    verify_model_contents_with_summary_authorization::<true, true>(
        journal, contents, limits, scope, io, context,
    )
    .await
}

// False flags are used only by the authorization and accounting negative controls.
#[doc(hidden)]
pub async fn verify_model_contents_with_summary_authorization<
    'a,
    const SUMMARIES: bool,
    const RELEASE: bool,
>(
    journal: &dyn ExecutionJournal,
    contents: impl Iterator<Item = &'a ModelContent> + Clone + acyclic_stream::ProviderTask,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
    context: Option<&crate::runtime::TaskContext>,
) -> Result<()> {
    tracing::trace!(target: "acyclic_harness::read_profile", phase = "content:begin");
    for content in contents.clone() {
        validate_model_content_limits(content, limits, scope, io)?;
    }
    let mut summaries = Vec::new();
    let result = async {
        for content in contents.clone() {
            for (file, binding) in content.file_bindings() {
                let granted = match scope {
                    Some(scope) => with_history_io(io, |work| {
                        crate::runtime::read_granted_with_work(scope.grants(), file, work)
                    })?,
                    None => true,
                };
                if (!granted
                    || (scope.is_none() && crate::conversation::is_internal_path(file.path())))
                    && SUMMARIES
                    && binding.is_none()
                    && journal.authenticate_summary(file, io)?.await?
                {
                    stock_vec_push(&mut summaries, file, io)?;
                } else if !granted {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "attenuated task cannot read model content",
                    ));
                }
                validate_model_binding(binding, scope, io)?;
            }
        }
        for content in contents {
            journal
                .verify_model_content(content, io, context, &summaries)?
                .await?;
        }
        Ok(())
    }
    .await;
    #[cfg(feature = "filesystem")]
    let owned = (summaries.capacity() * std::mem::size_of::<&FileRef>()) as u64;
    drop(summaries);
    #[cfg(feature = "filesystem")]
    if RELEASE && let Some(io) = io {
        io.release(owned)?;
    }
    tracing::trace!(target: "acyclic_harness::read_profile", phase = "content:end");
    result
}

pub(super) fn validate_model_content_scope(
    content: &ModelContent,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    validate_model_content_limits(content, limits, scope, io)?;
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
        validate_model_binding(binding, scope, io)?;
    }
    Ok(())
}

fn validate_model_content_limits(
    content: &ModelContent,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| content.validate_limits(limits, work))?;
    if let Some(scope) = scope {
        with_history_io(io, |work| content.validate_limits(scope.limits(), work))?;
    }
    Ok(())
}

fn validate_model_binding(
    binding: Option<&crate::model::NativeConfigurationBinding>,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if let Some(binding) = binding {
        let scope = scope.ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original native task scope is unavailable",
            )
        })?;
        binding.verify_original(
            scope.extensions(),
            scope.extension_schema_registry(),
            scope.extension_runtime().as_deref(),
            io,
        )?;
    }
    Ok(())
}

impl StockExecution<'_> {
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
            self.original_context(),
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
            journal
                .verify_selected_context(input.operation_id, selected, self.io)?
                .await?;
            // Selection authenticates the projected files. Scope/grant checks
            // above apply to every message; native options additionally require
            // their original execution binding, not a second plain-file read.
            for message in &selected.messages {
                if message.content.native_configurations().next().is_some() {
                    journal
                        .verify_model_content(
                            &message.content,
                            self.io,
                            self.original_context(),
                            &[],
                        )?
                        .await?;
                }
            }
        } else {
            // Selected history already includes this exact input as its final
            // message, checked by validate_for_input before any journal I/O.
            verify_model_contents_scoped(
                journal,
                std::iter::once(&input.input),
                self.stock.limits,
                scope,
                self.io,
                self.original_context(),
            )
            .await?;
        }
        Ok(())
    }
}
