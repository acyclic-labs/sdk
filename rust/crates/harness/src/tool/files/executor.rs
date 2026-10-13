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
            self.authorize_with_context(context, invocation)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.authorize_with_context(context, invocation)?;
        let invalid = |reason: &'static str| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return native::invalid(reason);
            }
            Error::Invalid(reason.into())
        };
        let file = match self {
            Self::Read => {
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                let bytes = context.read_file(&input.0.file, io).await?;
                let source = std::str::from_utf8(bytes.as_ref())
                    .map_err(|_| invalid("file read requires UTF-8 content"))?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(work) = work {
                    let (text, mut custody) = work.display(source)?;
                    custody
                        .resize(custody.bytes() + std::mem::size_of::<ToolResult>() as u64)
                        .map_err(super::edit::native_error)?;
                    return Ok((
                        ToolResult {
                            value: Value::String(text),
                        },
                        Some(custody),
                    ));
                }
                return Ok((
                    ToolResult {
                        value: Value::String(source.to_owned()),
                    },
                    None,
                ));
            }
            Self::Write => {
                let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
                context
                    .stage_file_once(
                        invocation.operation_id,
                        &input.0.path,
                        input.0.text.as_bytes(),
                        &input.0.media_type,
                        &input.0.display_name,
                        io,
                    )
                    .await?
            }
            Self::Edit => {
                let input = decode::<EditFileInput>(&invocation.arguments, work)?;
                let maximum = context.effective_scope().limits().file_bytes;
                if work.is_none() {
                    super::edit::validate_edit_bound(maximum)?;
                }
                let bytes = context.read_file(&input.0.file, io).await?;
                let source = std::str::from_utf8(bytes.as_ref())
                    .map_err(|_| invalid("exact text edit requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::new(work, std::mem::size_of::<String>())?;
                let edited = super::edit::exact_replace(
                    source,
                    &input.0.old_text,
                    &input.0.new_text,
                    maximum,
                    &mut allocation,
                )?;
                context
                    .stage_file_at(
                        invocation.operation_id,
                        &input.0.file,
                        edited.as_bytes(),
                        io,
                    )
                    .await?
            }
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => {
                let input = decode::<PatchFileInput>(&invocation.arguments, work)?;
                let limits = super::patch::PatchLimits {
                    maximum_bytes: context.effective_scope().limits().file_bytes,
                    maximum_work: *maximum_work,
                    maximum_hunks: *maximum_hunks,
                };
                if work.is_none() {
                    limits.validate()?;
                }
                if input.0.file.descriptor().byte_length() > limits.maximum_bytes
                    || input.0.diff.len() as u64 > limits.maximum_bytes
                {
                    return Err(invalid("patch input exceeds original task file bound"));
                }
                let bytes = context.read_file(&input.0.file, io).await?;
                let source = std::str::from_utf8(bytes.as_ref())
                    .map_err(|_| invalid("patch editing requires UTF-8 content"))?;
                let mut allocation =
                    super::edit::AlgorithmWork::new(work, std::mem::size_of::<String>())?;
                let edited =
                    super::patch::apply_update(source, &input.0.diff, limits, &mut allocation)?;
                context
                    .stage_file_at(
                        invocation.operation_id,
                        &input.0.file,
                        edited.as_bytes(),
                        io,
                    )
                    .await?
            }
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _file_custody = io.map(|io| io.take_native_custody(file.1)).transpose()?;
        let encoded = super::value(&FileResult { file: file.0 }, u64::MAX, work)?;
        Ok((ToolResult { value: encoded.0 }, encoded.1))
    }

    fn authorize_scope(
        &self,
        scope: Option<&RuntimeScope>,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), &mut None)?;
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("file tool requires original runtime scope".into())
        })?;
        crate::executor::json::json_byte_length(
            &invocation.arguments,
            scope.limits().file_bytes,
            &mut None,
        )
        .map(|_| ())?;
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
            let (result, custody) = self.run(context, invocation, Some(work)).await?;
            Ok((result, custody.expect("native file result")))
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
                let (result, custody) = self.run(context, invocation, Some(work)).await?;
                return Ok((Some(result), custody.expect("native file result")));
            }
            // Observe uncertain publications without redispatch.
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let receipt = context
                .observe_staged_file_once(invocation.operation_id, Some(&io))
                .await?;
            match receipt.0 {
                Some(file) => {
                    let encoded = super::value(&FileResult { file }, u64::MAX, Some(work))?;
                    Ok((
                        Some(ToolResult { value: encoded.0 }),
                        encoded.1.expect("native file result"),
                    ))
                }
                None => Ok((None, io.take_native_custody(receipt.1)?)),
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

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let Some(context) = context else {
            return Box::pin(async {
                Err(Error::Unsupported(
                    "portable file tool requires owner-bound task context".into(),
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
                    "portable file reconciliation requires owner-bound task context".into(),
                ))
            });
        };

        // Reads always use the original immutable ref; publications resolve the existing operation receipt before attempting a generation-checked CAS.
        Box::pin(async move {
            self.run(&context, &invocation, None)
                .await
                .map(|result| Some(result.0))
        })
    }
}
