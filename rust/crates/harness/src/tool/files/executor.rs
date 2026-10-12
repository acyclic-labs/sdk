use super::*;

#[derive(Clone, Copy)]
pub(super) enum FileTool {
    Read,
    Write,
    Edit,
    Patch {
        maximum_work: u64,
        maximum_hunks: u32,
    },
}

fn public_path(path: &str) -> Result<()> {
    crate::conversation::validate_content_path(path, &mut None)?;
    if crate::conversation::is_internal_path(path) {
        return Err(Error::Unauthorized(
            "file tools cannot access internal storage".into(),
        ));
    }
    Ok(())
}

impl FileTool {
    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
        let task = context.task();
        self.authorize_with_context(&context, &invocation)?;
        let value = match self {
            Self::Read => {
                let input: ReadFileInput = decode(&invocation.arguments, None)?.0;
                let bytes = task.read_file(&input.file).await?;
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("file read requires UTF-8 content".into()))?
                    .to_owned();
                Value::String(text)
            }
            Self::Write => {
                let input: WriteFileInput = decode(&invocation.arguments, None)?.0;
                let file = task
                    .stage_file_once(
                        invocation.operation_id,
                        &input.path,
                        input.text.as_bytes(),
                        &input.media_type,
                        &input.display_name,
                    )
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => {
                let input: PatchFileInput = decode(&invocation.arguments, None)?.0;
                let limits = super::patch::PatchLimits {
                    maximum_bytes: task.scope().limits().file_bytes,
                    maximum_work: *maximum_work,
                    maximum_hunks: *maximum_hunks,
                };
                limits.validate()?;
                if input.file.descriptor().byte_length() > limits.maximum_bytes
                    || input.diff.len() as u64 > limits.maximum_bytes
                {
                    return Err(Error::Invalid(
                        "patch input exceeds original task file bound".into(),
                    ));
                }
                let bytes = task.read_file(&input.file).await?;
                let source = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("patch editing requires UTF-8 content".into()))?;
                let edited = super::patch::apply_update(
                    source,
                    &input.diff,
                    limits,
                    &mut super::edit::AlgorithmWork::ordinary(),
                )?;
                let file = task
                    .stage_file_at(invocation.operation_id, &input.file, edited.as_bytes())
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            Self::Edit => {
                let input: EditFileInput = decode(&invocation.arguments, None)?.0;
                let maximum = task.scope().limits().file_bytes;
                super::edit::validate_edit_bound(maximum)?;
                let bytes = task.read_file(&input.file).await?;
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("exact text edit requires UTF-8 content".into()))?;
                let edited = super::edit::exact_replace(
                    text,
                    &input.old_text,
                    &input.new_text,
                    maximum,
                    &mut super::edit::AlgorithmWork::ordinary(),
                )?;
                let file = task
                    .stage_file_at(invocation.operation_id, &input.file, edited.as_bytes())
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
        };
        Ok(ToolResult { value })
    }
}

impl FileTool {
    fn authorize_scope(
        &self,
        scope: Option<&RuntimeScope>,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("file tool requires original runtime scope".into())
        })?;
        crate::contract::validate_json_byte_bound(
            &invocation.arguments,
            scope.limits().file_bytes,
        )?;
        if let Self::Write = self {
            let path = invocation
                .arguments
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("file write requires a destination path".into()))?;
            public_path(path)?;
            if path.len() > scope.limits().path_bytes {
                return Err(Error::Invalid(
                    "file path exceeds original task bound".into(),
                ));
            }
        } else {
            let value = invocation.arguments.get("file").ok_or_else(|| {
                Error::Invalid("file tool requires an original source reference".into())
            })?;
            let file: FileRef = decode(value, None)?.0;
            public_path(file.path())?;
            scope.limits().validate_file(&file, &mut None)?;
            if !crate::runtime::read_granted(scope.grants(), &file)? {
                return Err(Error::Unauthorized(
                    "file tool requires original source read grant".into(),
                ));
            }
            if let Self::Read = self {
                let maximum = scope.limits().render_bytes.min(scope.limits().file_bytes);
                super::edit::validate_edit_bound(maximum)?;
                if file.descriptor().byte_length() > maximum {
                    return Err(Error::Invalid("file exceeds text rendering bound".into()));
                }
            }
        }
        Ok(())
    }
}

impl ToolExecutor for FileTool {
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
        native::context(context, invocation, work)?;
        native::bound(
            &invocation.arguments,
            context.effective_scope().limits().file_bytes,
            work,
        )?;
        match self {
            Self::Write => {
                let input = super::decode::<WriteFileInput>(&invocation.arguments, Some(work))?;
                native::public_path(&input.0.path, work)?;
                if input.0.path.len() > context.effective_scope().limits().path_bytes {
                    return Err(native::invalid("file path exceeds original task bound"));
                }
                context.authorize_content_publication_with_native_work(None, work)
            }
            _ => {
                let value = invocation.arguments.get("file").ok_or_else(|| {
                    native::invalid("file tool requires an original source reference")
                })?;
                // Edit/patch DTOs include extra fields; decode only the pinned ref.
                let file = super::decode::<FileRef>(value, Some(work))?;
                native::public_path(file.0.path(), work)?;
                context.authorize_file_read_with_native_work(&file.0, work)?;
                if let Self::Read = self {
                    let maximum = context
                        .effective_scope()
                        .limits()
                        .render_bytes
                        .min(context.effective_scope().limits().file_bytes);
                    if maximum == 0
                        || maximum == u64::MAX
                        || file.0.descriptor().byte_length() > maximum
                    {
                        return Err(native::invalid("file exceeds text rendering bound"));
                    }
                    Ok(())
                } else {
                    context.authorize_content_publication_with_native_work(Some(&file.0), work)
                }
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
            self.authorize_with_native_context(context, invocation, work)?;
            if let Self::Read = self {
                let result = self.run_native(context, invocation, work).await?;
                return Ok((Some(result.0), result.1));
            }
            // Never redispatch an uncertain publication.
            let receipt = context
                .observe_staged_file_once_with_native_work(invocation.operation_id, work)
                .await?;
            match &receipt.0 {
                Some(file) => {
                    let encoded = super::value(
                        &native::BorrowedFileResult { file },
                        work.remaining()?.bytes_encoded,
                        work,
                    )?;
                    Ok((Some(ToolResult { value: encoded.0 }), encoded.1))
                }
                None => Ok((None, receipt.1)),
            }
        })
    }
    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        self.authorize_scope(scope, invocation)?;
        Err(Error::Unauthorized(
            "portable file tool requires original task context".into(),
        ))
    }

    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "file tool requires its exact admitted call context".into(),
            ));
        }
        self.authorize_scope(Some(context.task().scope()), invocation)?;
        match self {
            Self::Read => Ok(()),
            Self::Write => context.task().authorize_content_publication(None),
            Self::Edit | Self::Patch { .. } => {
                let file: FileRef = decode(
                    invocation.arguments.get("file").ok_or_else(|| {
                        Error::Invalid("file tool requires an original source reference".into())
                    })?,
                    None,
                )?
                .0;
                context.task().authorize_content_publication(Some(&file))
            }
        }
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "portable file tool requires owner-bound task context".into(),
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
                "portable file reconciliation requires owner-bound task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // Reads always use the original immutable ref; publications resolve the existing operation receipt before attempting a generation-checked CAS.
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl FileTool {
    async fn run_native(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        self.authorize_with_native_context(context, invocation, work)?;
        match self {
            Self::Read => {
                let input = super::decode::<ReadFileInput>(&invocation.arguments, Some(work))?;
                let read = context
                    .read_file_with_native_work(&input.0.file, work)
                    .await?;
                let source = std::str::from_utf8(read.as_ref())
                    .map_err(|_| native::invalid("file read requires UTF-8 content"))?;
                let (text, mut custody) = work.display(source)?;
                custody
                    .resize_retained(custody.bytes() + std::mem::size_of::<ToolResult>() as u64)
                    .map_err(super::edit::native_error)?;
                Ok((
                    ToolResult {
                        value: Value::String(text),
                    },
                    custody,
                ))
            }
            Self::Write => {
                let input = super::decode::<WriteFileInput>(&invocation.arguments, Some(work))?;
                let file = context
                    .stage_file_once_with_native_work(
                        invocation.operation_id,
                        &input.0.path,
                        input.0.text.as_bytes(),
                        &input.0.media_type,
                        &input.0.display_name,
                        work,
                    )
                    .await?;
                let encoded = super::value(
                    &native::BorrowedFileResult { file: &file.0 },
                    work.remaining()?.bytes_encoded,
                    work,
                )?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
            Self::Edit => {
                let input = super::decode::<EditFileInput>(&invocation.arguments, Some(work))?;
                let read = context
                    .read_file_with_native_work(&input.0.file, work)
                    .await?;
                let source = std::str::from_utf8(read.as_ref())
                    .map_err(|_| native::invalid("exact text edit requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
                let edited = super::edit::exact_replace(
                    source,
                    &input.0.old_text,
                    &input.0.new_text,
                    context.effective_scope().limits().file_bytes,
                    &mut allocation,
                )?;
                let file = context
                    .stage_file_at_with_native_work(
                        invocation.operation_id,
                        &input.0.file,
                        edited.as_bytes(),
                        work,
                    )
                    .await?;
                let encoded = super::value(
                    &native::BorrowedFileResult { file: &file.0 },
                    work.remaining()?.bytes_encoded,
                    work,
                )?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => {
                let input = super::decode::<PatchFileInput>(&invocation.arguments, Some(work))?;
                let limits = super::patch::PatchLimits {
                    maximum_bytes: context.effective_scope().limits().file_bytes,
                    maximum_work: *maximum_work,
                    maximum_hunks: *maximum_hunks,
                };
                if input.0.file.descriptor().byte_length() > limits.maximum_bytes
                    || input.0.diff.len() as u64 > limits.maximum_bytes
                {
                    return Err(native::invalid(
                        "patch input exceeds original task file bound",
                    ));
                }
                let read = context
                    .read_file_with_native_work(&input.0.file, work)
                    .await?;
                let source = std::str::from_utf8(read.as_ref())
                    .map_err(|_| native::invalid("patch editing requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
                let edited =
                    super::patch::apply_update(source, &input.0.diff, limits, &mut allocation)?;
                let file = context
                    .stage_file_at_with_native_work(
                        invocation.operation_id,
                        &input.0.file,
                        edited.as_bytes(),
                        work,
                    )
                    .await?;
                let encoded = super::value(
                    &native::BorrowedFileResult { file: &file.0 },
                    work.remaining()?.bytes_encoded,
                    work,
                )?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
        }
    }
}
