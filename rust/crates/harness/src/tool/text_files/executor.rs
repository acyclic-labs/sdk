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
    pub(super) fn preflight(
        &self,
        scope: &RuntimeScope,
        invocation: &ToolInvocation,
    ) -> Result<FileRef> {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
        crate::executor::json::json_byte_length(
            &invocation.arguments,
            scope.limits().file_bytes,
            &mut None,
        )
        .map(|_| ())?;
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

    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<&crate::executor::PreparationIo> = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            self.authorize_with_native_context(context, invocation, work)?;
        } else {
            self.preflight_with_context(context, invocation)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.preflight_with_context(context, invocation)?;
        let maximum = self
            .maximum_result_bytes
            .min(context.effective_scope().limits().file_bytes);
        let invalid = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return super::files::native::invalid("text tool requires UTF-8 content");
            }
            Error::Invalid("text tool requires UTF-8 content".into())
        };
        let encoded = match self.kind {
            TextKind::Read(options) => {
                let input = decode::<ReadInput>(&invocation.arguments, work)?;
                let bytes = context.read_file(&input.0.file, io).await?;
                let source = std::str::from_utf8(bytes.as_ref()).map_err(|_| invalid())?;
                let mut allocation =
                    super::edit::AlgorithmWork::new(work, std::mem::size_of::<TextSelection>())?;
                let value = ReadResult {
                    selection: super::text::read_range(
                        source,
                        input.0.range,
                        options,
                        &mut allocation,
                    )?,
                    file: input.0.file,
                };
                super::value(&value, maximum, work)?
            }
            TextKind::Search(options) => {
                let input = decode::<SearchInput>(&invocation.arguments, work)?;
                let bytes = context.read_file(&input.0.file, io).await?;
                let source = std::str::from_utf8(bytes.as_ref()).map_err(|_| invalid())?;
                let mut allocation =
                    super::edit::AlgorithmWork::new(work, std::mem::size_of::<SearchMatches>())?;
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
                super::value(&value, maximum, work)?
            }
        };
        Ok((ToolResult { value: encoded.0 }, encoded.1))
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
            let (result, custody) = self.run(context, invocation, Some(work)).await?;
            Ok((result, custody.expect("native text result")))
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
            let (result, custody) = self.run(context, invocation, Some(work)).await?;
            Ok((Some(result), custody.expect("native text result")))
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

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let Some(context) = context else {
            return Box::pin(async {
                Err(Error::Unsupported(
                    "text tool requires owner-bound task context".into(),
                ))
            });
        };
        Box::pin(async move {
            self.run(&context, &invocation, None)
                .await
                .map(|result| result.0)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let Some(context) = context else {
            return Box::pin(async {
                Err(Error::Unsupported(
                    "text reconciliation requires owner-bound task context".into(),
                ))
            });
        };
        // Immutable reads have no publication effect; retry uses the same pinned source.
        Box::pin(async move {
            self.run(&context, &invocation, None)
                .await
                .map(|result| Some(result.0))
        })
    }
}
