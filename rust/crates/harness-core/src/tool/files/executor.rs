use super::*;

#[derive(Clone, Copy)]
pub(crate) enum FileTool {
    Read,
    Write,
    Remove,
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

/// Keep source fields ahead of their admission on every return and unwind.
pub(super) struct Source {
    pub file: FileRef,
    pub bytes: Option<acyclic_stream::OwnedBytes>,
    _custody: crate::NativeCustody,
}

impl Source {
    pub async fn read(
        context: &ToolContext,
        path: &str,
        generation: Option<&crate::resources::GenerationRef>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let custody;
        let (file, bytes, retained) = context.read_tool_path(path, generation, io).await?;
        custody = super::window::custody(io, retained)?;
        Ok(Self {
            file,
            bytes: Some(bytes),
            _custody: custody,
        })
    }
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

            Self::Remove => {
                let input = decode::<RemoveFileInput>(&invocation.arguments, work)?;
                let (volume, path) = context.tool_write_path_selection(&input.0.path, io)?;
                let _source_custody;
                let source = if input.0.if_version == "any" {
                    _source_custody = None;
                    None
                } else {
                    let (source, custody) = super::version::decode_tool_version(
                        Some(context),
                        &input.0.path,
                        &input.0.if_version,
                        work,
                    )?;
                    _source_custody = custody;
                    Some(source)
                };
                let change = match &source {
                    Some(super::version::SelectedVersion::Content(source)) => {
                        crate::conversation::ContentChange::DeleteExpected { source }
                    }
                    Some(super::version::SelectedVersion::Authored(source)) => {
                        crate::conversation::ContentChange::DeleteAt { source }
                    }
                    None => crate::conversation::ContentChange::Delete { path: &path },
                };
                let (receipt, retained) = context
                    .stage_tool_changes(invocation.operation_id, volume, &[change], io)
                    .await?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let _receipt_custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let _ = retained;
                return self.batch_result(receipt, &path, work);
            }

            Self::Write => {
                let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
                match input.0.if_version.as_deref().unwrap_or("any") {
                    "any" => {
                        context
                            .stage_tool_file_once(
                                invocation.operation_id,
                                &input.0.path,
                                input.0.content.as_bytes(),
                                "text/plain",
                                input.0.path.rsplit('/').next().unwrap_or(&input.0.path),
                                io,
                            )
                            .await?
                    }
                    "absent" => {
                        let (volume, path) =
                            context.tool_write_path_selection(&input.0.path, io)?;
                        let (result, retained) = context
                            .stage_tool_changes(
                                invocation.operation_id,
                                volume,
                                &[crate::conversation::ContentChange::CreateAbsent {
                                    path: &path,
                                    bytes: input.0.content.as_bytes(),
                                    media_type: "text/plain",
                                    display_name: path.rsplit('/').next().unwrap_or(&path),
                                }],
                                io,
                            )
                            .await?;
                        (created_file(result, &path, work)?, retained)
                    }
                    version => {
                        let source = self
                            .source(context, &input.0.path, Some(version), work, io)
                            .await?;
                        context
                            .stage_file_at(
                                invocation.operation_id,
                                &source.file,
                                input.0.content.as_bytes(),
                                io,
                            )
                            .await?
                    }
                }
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
    pub(super) async fn source(
        &self,
        context: &ToolContext,
        path: &str,
        version: Option<&str>,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Source> {
        if let Some(version) = version {
            let original_custody;
            let (file, custody) =
                super::version::decode_tool_version(Some(context), path, version, work)?;
            original_custody = custody;
            match file {
                super::version::SelectedVersion::Content(file) => {
                    return Ok(Source {
                        file,
                        bytes: None,
                        _custody: original_custody,
                    });
                }
                super::version::SelectedVersion::Authored(selected) => {
                    let source =
                        Source::read(context, path, Some(&selected.generation), io).await?;
                    return Ok(source);
                }
            }
        }
        Source::read(context, path, None, io).await
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
        if input.0.notebook.is_some()
            && (!input.0.old_text.is_empty() || !input.0.new_text.is_empty() || input.0.replace_all)
        {
            return Err(invalid("notebook and text edits are mutually exclusive"));
        }
        let maximum = context.effective_scope().limits().file_bytes;
        if work.is_none() {
            super::edit::validate_edit_bound(maximum)?;
        }
        let selected = self
            .source(context, &input.0.path, input.0.version.as_deref(), work, io)
            .await?;
        let bytes = match selected.bytes {
            Some(bytes) => bytes,
            None => context.read_file(&selected.file, io).await?,
        };
        let source = std::str::from_utf8(bytes.as_ref())
            .map_err(|_| invalid("exact text edit requires UTF-8 content"))?;
        let mut allocation = super::edit::allocation(work, std::mem::size_of::<String>())?;
        let _notebook_hold;
        let _notebook_facts;
        let (edited, notebook_hold, notebook_facts) = if let Some(operation) = &input.0.notebook {
            let (edited, hold, facts) = super::notebook::edit(source, operation, work)?;
            (edited, hold, Some(facts))
        } else {
            (
                super::edit::replace(
                    source,
                    &input.0.old_text,
                    &input.0.new_text,
                    input.0.replace_all,
                    maximum,
                    &mut allocation,
                )?,
                None,
                None,
            )
        };
        _notebook_hold = notebook_hold;
        _notebook_facts = notebook_facts;
        if edited.len() as u64 > maximum {
            return Err(invalid("edited file exceeds admitted file credit"));
        }
        context
            .stage_file_at(
                invocation.operation_id,
                &selected.file,
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
        let (file, _custody) = self.preflight(scope, context, invocation, work)?;
        if matches!(self, Self::Read)
            && invocation
                .arguments
                .get("selection")
                .and_then(|selection| selection.get("kind"))
                .and_then(Value::as_str)
                == Some("directory")
        {
            // Preflight validated the directory selection and its exact List policy.
            return Ok(());
        }
        let path = file
            .as_ref()
            .map(FileRef::path)
            .or_else(|| invocation.arguments.get("path").and_then(Value::as_str))
            .ok_or_else(unauthorized)?;
        let context = context.ok_or_else(unauthorized)?;
        if file.is_none() && matches!(self, Self::Read | Self::Edit) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owned_io = work
                .map(crate::executor::PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let io = owned_io.as_ref();
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let io = None;
            // Effective reads were authorized against their selected layer in preflight.
            if !path.starts_with(crate::layers::EFFECTIVE_PATH) {
                context.authorize_tool_path_read(path, io)?;
            }
            if self.is_read() {
                return Ok(());
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return if self.is_read() {
                context.authorize_file_selection_with_native_work(
                    file.as_ref().ok_or_else(|| Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "file read source is missing",
                    })?,
                    matches!(self, Self::Read).then_some(acyclic_fs::ByteRange {
                        offset: 0,
                        length: 0,
                    }),
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
            if self.uses_batch_receipt(invocation) {
                let (result, custody) = self
                    .recover_batch(context, invocation, Some(work), Some(&io))
                    .await?;
                return Ok((
                    result,
                    custody.ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "native file result lacks custody",
                    })?,
                ));
            }
            let source = if matches!(self, Self::Write) {
                match invocation
                    .arguments
                    .get("ifVersion")
                    .and_then(Value::as_str)
                    .filter(|version| !matches!(*version, "any" | "absent"))
                {
                    Some(version) => {
                        let path = invocation
                            .arguments
                            .get("path")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                rejection(Some(work), "write receipt requires its path")
                            })?;
                        let (source, custody) = super::version::decode_tool_version(
                            Some(context),
                            path,
                            version,
                            Some(work),
                        )?;
                        (Some(source), custody)
                    }
                    None => (None, None),
                }
            } else {
                let (source, custody) = self.preflight(
                    context.effective_scope(),
                    Some(context),
                    invocation,
                    Some(work),
                )?;
                (
                    source.map(super::version::SelectedVersion::Content),
                    custody,
                )
            };
            let receipt = if matches!(self, Self::Write) && source.0.is_none() {
                let path = invocation
                    .arguments
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| rejection(Some(work), "write receipt requires its path"))?;
                context
                    .observe_tool_path_once(invocation.operation_id, path, Some(&io))
                    .await?
            } else {
                context
                    .observe_tool_source_file_once(
                        invocation.operation_id,
                        source
                            .0
                            .as_ref()
                            .map(super::version::SelectedVersion::volume),
                        source
                            .0
                            .as_ref()
                            .map(super::version::SelectedVersion::path)
                            .or_else(|| invocation.arguments.get("path").and_then(Value::as_str)),
                        Some(&io),
                    )
                    .await?
            };
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
            if self.uses_batch_receipt(&invocation) {
                self.authorize(
                    Some(context.effective_scope()),
                    Some(&context),
                    &invocation,
                    None,
                )?;
                return self
                    .recover_batch(&context, &invocation, None, None)
                    .await
                    .map(|result| result.0);
            }
            // The stock journal has authenticated the full original invocation.
            // Resolving an unversioned path again could select a later source.
            if matches!(self, Self::Edit)
                && invocation
                    .arguments
                    .get("version")
                    .is_none_or(Value::is_null)
            {
                self.authorize(
                    Some(context.effective_scope()),
                    Some(&context),
                    &invocation,
                    None,
                )?;
                let source =
                    self.preflight(context.effective_scope(), Some(&context), &invocation, None)?;
                let receipt = context
                    .observe_tool_source_file_once(
                        invocation.operation_id,
                        source.0.as_ref().map(FileRef::volume),
                        invocation.arguments.get("path").and_then(Value::as_str),
                        None,
                    )
                    .await?;
                return receipt
                    .0
                    .map(|file| {
                        let encoded = super::value(&FileResult { file }, u64::MAX, None)?;
                        Ok(ToolResult { value: encoded.0 })
                    })
                    .transpose();
            }
            self.run(&context, &invocation, None)
                .await
                .map(|result| Some(result.0))
        })
    }
}

impl FileTool {
    fn uses_batch_receipt(&self, invocation: &ToolInvocation) -> bool {
        matches!(self, Self::Remove)
            || (matches!(self, Self::Write)
                && invocation
                    .arguments
                    .get("ifVersion")
                    .and_then(Value::as_str)
                    == Some("absent"))
    }

    async fn recover_batch(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<ToolResult>, crate::NativeCustody)> {
        let path = invocation
            .arguments
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| rejection(work, "batch receipt requires its path"))?;
        let (volume, relative) = context.tool_write_path_selection(path, io)?;
        let (receipt, retained) = context
            .observe_tool_changes(invocation.operation_id, volume, io)
            .await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _hold = io.map(|io| io.take_native_custody(retained)).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        match receipt {
            Some(receipt) => self
                .batch_result(receipt, &relative, work)
                .map(|(result, custody)| (Some(result), custody)),
            None => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = io.map(|io| io.take_native_custody(0)).transpose()?;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let custody = None;
                Ok((None, custody))
            }
        }
    }

    fn batch_result(
        &self,
        mut receipt: crate::conversation::ContentChanges,
        path: &str,
        work: crate::NativeWork<'_>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        let encoded = if matches!(self, Self::Remove) {
            if receipt.members.len() != 1
                || !matches!(receipt.members.pop(),
                Some(crate::conversation::ContentChangeResult::Deleted(deleted)) if deleted == path)
            {
                return Err(rejection(
                    work,
                    "remove receipt differs from its original path or action",
                ));
            }
            super::value(&(), u64::MAX, work)?
        } else {
            let file = created_file(receipt, path, work)?;
            super::value(&FileResult { file }, u64::MAX, work)?
        };
        Ok((ToolResult { value: encoded.0 }, encoded.1))
    }
}

fn created_file(
    mut receipt: crate::conversation::ContentChanges,
    path: &str,
    work: crate::NativeWork<'_>,
) -> Result<FileRef> {
    if receipt.members.len() == 1
        && let Some(crate::conversation::ContentChangeResult::Created(file)) = receipt.members.pop()
        && file.path() == path
    {
        return Ok(file);
    }
    Err(rejection(
        work,
        "create receipt differs from its original path or action",
    ))
}

/// Only content/selection rejections use this path. Accounting and provider errors propagate.
pub(super) fn rejection(work: crate::NativeWork<'_>, reason: &'static str) -> Error {
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
