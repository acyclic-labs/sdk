use super::*;

#[derive(Clone, Copy)]
pub(super) enum TextKind {
    Read(ReadOptions),
    Search(SearchOptions),
}

pub(super) struct TextTool {
    pub(super) kind: TextKind,
    pub(super) maximum_result_bytes: u64,
}

impl TextTool {
    // This preflight is also used when the executor authorizes durable replay.
    pub(super) fn preflight(
        &self,
        scope: &RuntimeScope,
        invocation: &ToolInvocation,
    ) -> Result<FileRef> {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
        crate::contract::json_byte_length(&invocation.arguments, scope.limits().file_bytes, &mut None).map(|_| ())?;
        let result_bound = self.maximum_result_bytes.min(scope.limits().file_bytes);
        let file = match self.kind {
            TextKind::Read(options) => {
                options.validate()?;
                let input: ReadInput = decode(&invocation.arguments, None)?.0;
                let length = input
                    .range
                    .end
                    .checked_sub(input.range.start)
                    .ok_or_else(|| Error::Invalid("read range is reversed".into()))?;
                if input.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.range.end > input.file.descriptor().byte_length()
                    || length > options.maximum_text_bytes.min(result_bound)
                {
                    return Err(Error::Invalid(
                        "read exceeds its admitted byte bounds".into(),
                    ));
                }
                input.file
            }
            TextKind::Search(options) => {
                options.validate()?;
                let input: SearchInput = decode(&invocation.arguments, None)?.0;
                if input.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.query.is_empty()
                    || input.query.len() as u64 > options.maximum_query_bytes
                    || u64::from(options.maximum_matches) * std::mem::size_of::<TextRange>() as u64
                        > result_bound
                {
                    return Err(Error::Invalid(
                        "search exceeds its admitted input or retained-position bounds".into(),
                    ));
                }
                input.file
            }
        };
        crate::conversation::validate_content_path(file.path(), &mut None)?;
        if crate::conversation::is_internal_path(file.path())
            || !crate::runtime::read_granted(scope.grants(), &file)?
        {
            return Err(Error::Unauthorized(
                "text tool requires a public owner-authorized file".into(),
            ));
        }
        if file.descriptor().byte_length() > scope.limits().file_bytes {
            return Err(Error::Invalid(
                "text source exceeds original task file bound".into(),
            ));
        }
        Ok(file)
    }

    fn preflight_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<FileRef> {
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "text tool requires its exact admitted call context".into(),
            ));
        }
        self.preflight(context.task().scope(), invocation)
    }

    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        let file = self.preflight_with_context(&context, &invocation)?;
        let task = context.task();
        let bytes = task.read_file(&file).await?;
        let source = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Invalid("text tool requires UTF-8 content".into()))?;
        let maximum = self
            .maximum_result_bytes
            .min(task.scope().limits().file_bytes);
        let value = match self.kind {
            TextKind::Read(options) => {
                let input: ReadInput = decode(&invocation.arguments, None)?.0;
                let value = ReadResult {
                    file,
                    selection: super::text::read_range(
                        source,
                        input.range,
                        options,
                        &mut super::edit::AlgorithmWork::ordinary(),
                    )?,
                };
                crate::contract::json_byte_length(&value, maximum, &mut None).map(|_| ())?;
                serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?
            }
            TextKind::Search(options) => {
                let input: SearchInput = decode(&invocation.arguments, None)?.0;
                let matches = super::text::literal_search(
                    source,
                    &input.query,
                    options,
                    &mut super::edit::AlgorithmWork::ordinary(),
                )?;
                let value = SearchResult {
                    file,
                    query: input.query,
                    matches,
                };
                crate::contract::json_byte_length(&value, maximum, &mut None).map(|_| ())?;
                serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for TextTool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn authorize_with_native_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        use super::files::native;
        native::context(context, invocation, work)?;
        let scope = context.effective_scope();
        native::bound(&invocation.arguments, scope.limits().file_bytes, work)?;
        let maximum = self.maximum_result_bytes.min(scope.limits().file_bytes);
        match self.kind {
            TextKind::Read(options) => {
                let input = super::decode::<ReadInput>(&invocation.arguments, Some(work))?;
                let length = input
                    .0
                    .range
                    .end
                    .checked_sub(input.0.range.start)
                    .ok_or_else(|| native::invalid("read range is reversed"))?;
                if input.0.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.0.range.end > input.0.file.descriptor().byte_length()
                    || length > options.maximum_text_bytes.min(maximum)
                {
                    return Err(native::invalid("read exceeds its admitted byte bounds"));
                }
                native::public_path(input.0.file.path(), work)?;
                context.authorize_file_read_with_native_work(&input.0.file, work)
            }
            TextKind::Search(options) => {
                let input = super::decode::<SearchInput>(&invocation.arguments, Some(work))?;
                if input.0.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.0.query.is_empty()
                    || input.0.query.len() as u64 > options.maximum_query_bytes
                    || u64::from(options.maximum_matches) * std::mem::size_of::<TextRange>() as u64
                        > maximum
                {
                    return Err(native::invalid(
                        "search exceeds its admitted input or retained-position bounds",
                    ));
                }
                native::public_path(input.0.file.path(), work)?;
                context.authorize_file_read_with_native_work(&input.0.file, work)
            }
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.run_native(context, invocation, work).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let result = self.run_native(context, invocation, work).await?;
            Ok((Some(result.0), result.1))
        })
    }

    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("text tool requires original runtime scope".into())
        })?;
        self.preflight(scope, invocation)?;
        Err(Error::Unauthorized(
            "portable text tool requires original task context".into(),
        ))
    }

    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.preflight_with_context(context, invocation).map(|_| ())
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "text tool requires owner-bound task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(context, invocation))
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "text reconciliation requires owner-bound task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // Immutable reads have no publication effect; retry uses the same pinned source.
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl TextTool {
    async fn run_native(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        use super::files::native;
        self.authorize_with_native_context(context, invocation, work)?;
        let maximum = self
            .maximum_result_bytes
            .min(context.effective_scope().limits().file_bytes);
        match self.kind {
            TextKind::Read(options) => {
                let input = super::decode::<ReadInput>(&invocation.arguments, Some(work))?;
                let read = context
                    .read_file_with_native_work(&input.0.file, work)
                    .await?;
                let source = std::str::from_utf8(read.as_ref())
                    .map_err(|_| native::invalid("text tool requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::native(work, std::mem::size_of::<TextSelection>())?;
                let value = ReadResult {
                    selection: super::text::read_range(
                        source,
                        input.0.range,
                        options,
                        &mut allocation,
                    )?,
                    file: input.0.file,
                };
                native::bound(&value, maximum, work)?;
                let encoded = super::value(&value, work.remaining()?.bytes_encoded, work)?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
            TextKind::Search(options) => {
                let input = super::decode::<SearchInput>(&invocation.arguments, Some(work))?;
                let read = context
                    .read_file_with_native_work(&input.0.file, work)
                    .await?;
                let source = std::str::from_utf8(read.as_ref())
                    .map_err(|_| native::invalid("text tool requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::native(work, std::mem::size_of::<SearchMatches>())?;
                let value = SearchResult {
                    matches: super::text::literal_search(
                        source,
                        &input.0.query,
                        options,
                        &mut allocation,
                    )?,
                    file: input.0.file,
                    query: input.0.query,
                };
                native::bound(&value, maximum, work)?;
                let encoded = super::value(&value, work.remaining()?.bytes_encoded, work)?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
        }
    }
}
