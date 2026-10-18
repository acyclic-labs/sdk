use super::*;

#[derive(Clone, Copy)]
pub(crate) enum FileTool {
    Read,
    Write,
    Edit,
    Patch {
        maximum_work: u64,
        maximum_hunks: u32,
    },
    Search {
        options: super::text::SearchOptions,
        maximum_result_bytes: u64,
    },
}

impl FileTool {
    pub(in crate::tool) fn is_read(&self) -> bool {
        matches!(self, Self::Read | Self::Search { .. })
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
        self.authorize(
            Some(context.effective_scope()),
            Some(context),
            invocation,
            work,
        )?;

        let file = match self {
            Self::Search {
                options,
                maximum_result_bytes,
            } => {
                return self
                    .search(
                        context,
                        invocation,
                        (*options, *maximum_result_bytes),
                        work,
                        io,
                    )
                    .await;
            }

            Self::Read => return self.read(context, invocation, work, io).await,

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
            Self::Edit => self.edit(context, invocation, work, io).await?,
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => {
                self.patch(
                    context,
                    invocation,
                    (*maximum_work, *maximum_hunks),
                    work,
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
    async fn search(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        bounds: (super::text::SearchOptions, u64),
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let (options, maximum_result_bytes) = bounds;
        let invalid = |reason| rejection(work, reason);

        let input = decode::<super::super::text_files::SearchInput>(&invocation.arguments, work)?;
        let bytes = context.read_file(&input.0.file, io).await?;
        let source = std::str::from_utf8(bytes.as_ref())
            .map_err(|_| invalid("text tool requires UTF-8 content"))?;
        let mut allocation =
            super::edit::allocation(work, std::mem::size_of::<super::text::SearchMatches>())?;
        let result = super::super::text_files::SearchResult {
            matches: super::text::literal_search(source, &input.0.query, options, &mut allocation)?,
            file: input.0.file,
            query: input.0.query,
        };
        let encoded = super::value(
            &result,
            maximum_result_bytes.min(context.effective_scope().limits().file_bytes),
            work,
        )?;
        Ok((ToolResult { value: encoded.0 }, encoded.1))
    }
    async fn patch(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        bounds: (u64, u32),
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let (maximum_work, maximum_hunks) = bounds;
        let invalid = |reason| rejection(work, reason);

        let input = decode::<PatchFileInput>(&invocation.arguments, work)?;
        let limits = super::patch::PatchLimits {
            maximum_bytes: context.effective_scope().limits().file_bytes,
            maximum_work,
            maximum_hunks,
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
        let mut allocation = super::edit::allocation(work, std::mem::size_of::<String>())?;
        let edited = super::patch::apply_update(source, &input.0.diff, limits, &mut allocation)?;
        context
            .stage_file_at(
                invocation.operation_id,
                &input.0.file,
                edited.as_bytes(),
                io,
            )
            .await
    }
    async fn read(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let invalid = |reason| rejection(work, reason);

        let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
        let length = input.0.file.descriptor().byte_length();
        let empty = input.0.range.is_some_and(|range| range.start == range.end);
        let bytes = if let Some(range) = input.0.range {
            let count = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| invalid("read range is reversed"))?;
            if range.end > length {
                return Err(invalid("read range exceeds the file"));
            }
            context
                .read_file_range(
                    &input.0.file,
                    acyclic_fs::ByteRange {
                        offset: range.start,
                        // An empty interval still has to lie on a UTF-8 boundary.
                        length: if empty && range.start < length {
                            1
                        } else {
                            count
                        },
                    },
                    io,
                )
                .await?
        } else {
            context.read_file(&input.0.file, io).await?
        };
        let source = read_text(bytes.as_ref(), empty, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let (text, mut custody) = work.display(source)?;
            custody
                .resize(custody.bytes() + std::mem::size_of::<ToolResult>() as u64)
                .map_err(Error::from)?;
            return Ok((
                ToolResult {
                    value: Value::String(text),
                },
                Some(custody),
            ));
        }
        Ok((
            ToolResult {
                value: Value::String(source.to_owned()),
            },
            None,
        ))
    }
    async fn edit(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(FileRef, u64)> {
        let invalid = |reason| rejection(work, reason);

        let input = decode::<EditFileInput>(&invocation.arguments, work)?;
        let maximum = context.effective_scope().limits().file_bytes;
        if work.is_none() {
            super::edit::validate_edit_bound(maximum)?;
        }
        let bytes = context.read_file(&input.0.file, io).await?;
        let source = std::str::from_utf8(bytes.as_ref())
            .map_err(|_| invalid("exact text edit requires UTF-8 content"))?;
        let mut allocation = super::edit::allocation(work, std::mem::size_of::<String>())?;
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
            .await
    }
}

impl ToolExecutor for FileTool {
    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        let unauthorized = || {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "file tool requires its exact admitted call context",
                };
            }
            Error::Unauthorized("file tool requires its exact admitted call context".into())
        };
        let scope = context
            .map(ToolContext::effective_scope)
            .or(scope)
            .ok_or_else(unauthorized)?;
        if context.is_some_and(|context| {
            context.operation_id() != invocation.operation_id
                || context.call_id() != invocation.call_id
        }) {
            return Err(unauthorized());
        }
        let (file, _custody) = self.preflight(scope, invocation, work)?;
        let path = file
            .as_ref()
            .map(FileRef::path)
            .or_else(|| invocation.arguments.get("path").and_then(Value::as_str))
            .ok_or_else(unauthorized)?;
        let context = context.ok_or_else(unauthorized)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return if self.is_read() {
                context.authorize_file_read_with_native_work(
                    file.as_ref().ok_or_else(|| Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "file read source is missing",
                    })?,
                    work,
                )
            } else {
                context.authorize_content_publication_with_native_work(path, file.as_ref(), work)
            };
        }
        if self.is_read() {
            Ok(())
        } else {
            context.task().authorize_content_publication(
                path,
                file.as_ref(),
                context.effective_scope(),
                None,
            )
        }
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
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
            Ok((
                result,
                custody.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native file result lacks custody",
                })?,
            ))
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
            self.authorize(
                Some(context.effective_scope()),
                Some(context),
                invocation,
                Some(work),
            )?;
            if self.is_read() {
                let (result, custody) = self.run(context, invocation, Some(work)).await?;
                return Ok((
                    Some(result),
                    custody.ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "native file result lacks custody",
                    })?,
                ));
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
                        encoded.1.ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Storage,
                            reason: "native file result lacks custody",
                        })?,
                    ))
                }
                None => Ok((None, io.take_native_custody(receipt.1)?)),
            }
        })
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

        Box::pin(async move {
            self.run(&context, &invocation, None)
                .await
                .map(|result| Some(result.0))
        })
    }
}

/// Only content/selection rejections use this path. Accounting and provider errors propagate.
fn rejection(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
    if work.is_some() {
        Error::tool_rejected(reason)
    } else {
        Error::Invalid(reason.into())
    }
}

pub(super) fn read_text<'a>(
    bytes: &'a [u8],
    empty: bool,
    work: crate::NativeWork<'_>,
) -> Result<&'a str> {
    if empty {
        if bytes.first().is_some_and(|byte| byte & 0xc0 == 0x80) {
            return Err(rejection(work, "read range splits UTF-8"));
        }
        Ok("")
    } else {
        std::str::from_utf8(bytes).map_err(|_| rejection(work, "file read requires UTF-8 content"))
    }
}
