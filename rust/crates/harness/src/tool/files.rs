//! Individually assembled portable file tools using owner-bound task providers.

use super::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult, decode,
};
use crate::{
    Error, Result,
    conversation::FileRef,
    runtime::{RuntimeScope, ToolContext},
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Reads exact UTF-8 from a pinned ref using the task's authenticated reader.
pub fn read_file() -> Result<Tool> {
    assemble(FileTool::Read)
}

/// Publishes UTF-8 through the task's original owner-bound writer.
pub fn write_file() -> Result<Tool> {
    assemble(FileTool::Write)
}

/// Replaces one exact, unambiguous match at the source's pinned generation.
pub fn edit_file() -> Result<Tool> {
    assemble(FileTool::Edit)
}

/// Explicitly selects exact V4A update-file editing with pinned finite work limits.
///
/// The original task's file-byte ceiling bounds input and output. The limits
/// below are part of the definition schema digest, so different configurations
/// cannot silently reuse an admitted definition.
pub fn patch_file(maximum_work: u64, maximum_hunks: u32) -> Result<Tool> {
    super::patch::PatchLimits {
        maximum_bytes: 1,
        maximum_work,
        maximum_hunks,
    }
    .validate()?;
    assemble(FileTool::Patch {
        maximum_work,
        maximum_hunks,
    })
}

#[derive(Clone, Copy)]
enum FileTool {
    Read,
    Write,
    Edit,
    Patch {
        maximum_work: u64,
        maximum_hunks: u32,
    },
}

fn assemble(adapter: FileTool) -> Result<Tool> {
    let (name, description, mut input_schema, output_schema, projection_schema) = match adapter {
        FileTool::Read => (
            "acyclic.read_file",
            "Read exact bounded UTF-8 from an authorized immutable file",
            super::schema::input::<ReadFileInput>()?,
            super::schema::output::<String>()?,
            super::schema::json_projection::<String>()?,
        ),
        FileTool::Write => (
            "acyclic.write_file",
            "Write bounded UTF-8 through the original owner-bound content publisher",
            super::schema::input::<WriteFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
        FileTool::Edit => (
            "acyclic.edit_file",
            "Replace one exact UTF-8 match at an authorized pinned source generation",
            super::schema::input::<EditFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
        FileTool::Patch { .. } => (
            "acyclic.patch_file",
            "Apply exact V4A update-file diff hunks to one authorized immutable file; @@ headers, exact context and optional EOF; ambiguous context fails",
            super::schema::input::<PatchFileInput>()?,
            super::schema::output::<FileResult>()?,
            super::schema::json_projection::<FileResult>()?,
        ),
    };
    if let FileTool::Patch {
        maximum_work,
        maximum_hunks,
    } = adapter
    {
        input_schema
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("generated patch schema is not an object".into()))?
            .insert(
                "x-harness-patch-limits".into(),
                json!({"maximum_work":maximum_work,"maximum_hunks":maximum_hunks}),
            );
    }
    let definition = ToolDefinition {
        name: name.into(),
        revision: match adapter {
            FileTool::Patch { .. } => "portable-patch-4",
            _ => "portable-5",
        }
        .into(),
        description: description.into(),
        input_schema,
        projection_schema,
        output_schema,
    };
    definition.validate()?;
    let implementation = Arc::new(adapter);
    Ok(Tool {
        definition,
        executor: implementation.clone(),
        projection: implementation,
    })
}

/// Exact immutable UTF-8 file read arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

/// Single-operation UTF-8 publication arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WriteFileInput {
    /// Valid public destination path.
    pub path: String,
    /// Exact content bytes encoded as UTF-8.
    pub text: String,
    /// Stored MIME type.
    pub media_type: String,
    /// Human-readable file name.
    pub display_name: String,
}

/// Exact replacement against one pinned file generation.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EditFileInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// One exact nonempty unambiguous needle.
    #[schemars(length(min = 1))]
    pub old_text: String,
    /// Exact replacement, including an empty deletion.
    pub new_text: String,
}

/// Explicit single-file V4A update arguments.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PatchFileInput {
    /// Owner-authorized immutable source generation.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Nonempty exact V4A update diff fragment.
    #[schemars(length(min = 1))]
    pub diff: String,
}

/// Canonical successful write/edit result, independent of model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FileResult {
    /// Exact retained publication result with workspace and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
}

fn public_path(path: &str) -> Result<()> {
    crate::conversation::validate_content_path(path)?;
    if crate::conversation::is_internal_path(path) {
        return Err(Error::Unauthorized(
            "file tools cannot access internal storage".into(),
        ));
    }
    Ok(())
}

impl FileTool {
    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        invocation.validate()?;
        let task = context.task();
        self.authorize_with_context(&context, &invocation)?;
        let value = match self {
            Self::Read => {
                let input: ReadFileInput = decode(&invocation.arguments)?;
                let bytes = task.read_file(&input.file).await?;
                let text = String::from_utf8(bytes)
                    .map_err(|_| Error::Invalid("file read requires UTF-8 content".into()))?;
                Value::String(text)
            }
            Self::Write => {
                let input: WriteFileInput = decode(&invocation.arguments)?;
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
                let input: PatchFileInput = decode(&invocation.arguments)?;
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
                let edited = super::patch::apply_update(source, &input.diff, limits)?;
                let file = task
                    .stage_file_at(invocation.operation_id, &input.file, edited.as_bytes())
                    .await?;
                serde_json::to_value(FileResult { file })
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            Self::Edit => {
                let input: EditFileInput = decode(&invocation.arguments)?;
                let maximum = task.scope().limits().file_bytes;
                super::edit::validate_edit_bound(maximum)?;
                let bytes = task.read_file(&input.file).await?;
                let text = std::str::from_utf8(&bytes)
                    .map_err(|_| Error::Invalid("exact text edit requires UTF-8 content".into()))?;
                let edited =
                    super::edit::exact_replace(text, &input.old_text, &input.new_text, maximum)?;
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
        invocation.validate()?;
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("file tool requires original runtime scope".into())
        })?;
        crate::contract::validate_json_byte_bound(
            &invocation.arguments,
            scope.limits().file_bytes,
        )?;
        if let Self::Write = self {
            // The original TaskContext publisher owns the destination volume and
            // checks its exact write grant; the template does not capture a writer.
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
            let file: FileRef = decode(value)?;
            public_path(file.path())?;
            scope.limits().validate_file(&file)?;
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
    fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn authorize_with_native_context(
        &self, context: &ToolContext, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        native::context(context, invocation, work)?;
        native::bound(&invocation.arguments, context.task().scope().limits().file_bytes, work)?;
        match self {
            Self::Write => {
                let input = super::decode_with_native_work::<WriteFileInput>(&invocation.arguments, work)?;
                native::public_path(&input.0.path, work)?;
                if input.0.path.len() > context.task().scope().limits().path_bytes {
                    return Err(native::invalid("file path exceeds original task bound"));
                }
                context.task().authorize_content_publication_with_native_work(None, work)
            }
            _ => {
                let value = invocation.arguments.get("file")
                    .ok_or_else(|| native::invalid("file tool requires an original source reference"))?;
                // Edit/patch DTOs include extra fields; decode only the pinned ref.
                let file = super::decode_with_native_work::<FileRef>(value, work)?;
                native::public_path(file.0.path(), work)?;
                context.task().authorize_file_read_with_native_work(&file.0, work)?;
                if let Self::Read = self {
                    let maximum = context.task().scope().limits().render_bytes
                        .min(context.task().scope().limits().file_bytes);
                    if maximum == 0 || maximum == u64::MAX
                        || file.0.descriptor().byte_length() > maximum
                    {
                        return Err(native::invalid("file exceeds text rendering bound"));
                    }
                    Ok(())
                } else {
                    context.task().authorize_content_publication_with_native_work(Some(&file.0), work)
                }
            }
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self, context: &'a ToolContext, invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.run_native(context, invocation, work).await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self, context: &'a ToolContext, invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.authorize_with_native_context(context, invocation, work)?;
            if let Self::Read = self {
                let result = self.run_native(context, invocation, work).await?;
                return Ok((Some(result.0), result.1));
            }
            // Never redispatch an uncertain publication. The original owner
            // receipt, not an edited reconstruction, supplies reconciliation.
            let receipt = context.task().observe_staged_file_once_with_native_work(
                invocation.operation_id, work,
            ).await?;
            match &receipt.0 {
                Some(file) => {
                    let encoded = super::value_with_native_work(&native::BorrowedFileResult { file }, work)?;
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
                let file: FileRef = decode(invocation.arguments.get("file").ok_or_else(|| {
                    Error::Invalid("file tool requires an original source reference".into())
                })?)?;
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
        // Reads always use the original immutable ref; publications resolve the
        // existing operation receipt before attempting a generation-checked CAS.
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

/// Replaceable projector for the canonical published file result.
#[derive(Clone, Copy)]
pub struct FileResultProjection(pub super::schema::ProjectionMode);

impl FileResultProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<FileResult>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for FileResultProjection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn project_with_native_work(
        &self, _: &ToolInvocation, result: &ToolResult, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let value = super::decode_with_native_work::<FileResult>(&result.value, work)?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json_with_native_work(&value.0, work),
            super::schema::ProjectionMode::Reference => super::schema::project_reference_with_native_work(&value.0.file,
                "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.", work),
        }
    }

    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let value: FileResult = decode(&result.value)?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(value),
            super::schema::ProjectionMode::Reference => super::schema::project_reference(value.file,
                "File publication completed. The original output reference, workspace and content identity remain in the canonical tool result. Body bytes are omitted from this model projection.".into()),
        }
    }
}

/// Replaceable projector for the exact reader's unchanged canonical string.
#[derive(Clone, Copy)]
pub struct ReadFileProjection(pub super::schema::ProjectionMode);

impl ReadFileProjection {
    /// Complete generated model envelope contract for this representation.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::json_projection::<String>(),
            super::schema::ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for ReadFileProjection {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn project_with_native_work(
        &self, invocation: &ToolInvocation, result: &ToolResult,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let text = super::decode_with_native_work::<native::ReadText>(&result.value, work)?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json_with_native_work(&text.0, work),
            super::schema::ProjectionMode::Reference => {
                use sha2::Digest as _;
                let input = super::decode_with_native_work::<ReadFileInput>(&invocation.arguments, work)?;
                work.charge(acyclic_fs::WorkCounters {
                    bytes_hashed: text.0.0.len() as u64, ..Default::default()
                }).map_err(super::edit::native_error)?;
                if input.0.file.descriptor().byte_length() != text.0.0.len() as u64
                    || input.0.file.descriptor().sha256() != &<[u8; 32]>::from(sha2::Sha256::digest(text.0.0.as_bytes()))
                {
                    return Err(native::invalid("file descriptor differs from exact text result"));
                }
                native::summary_reference(&input.0.file, 160, format_args!(
                    "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                    text.0.0.len()
                ), work)
            }
        }
    }

    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let text: String = decode(&result.value)?;
        match self.0 {
            super::schema::ProjectionMode::Full => super::schema::project_json(text),
            super::schema::ProjectionMode::Reference => {
                let input: ReadFileInput = decode(&invocation.arguments)?;
                input.file.descriptor().verify(text.as_bytes())?;
                super::schema::project_reference(
                    input.file,
                    format!(
                        "Read {} UTF-8 bytes. The exact text is retained in the canonical tool result and omitted from this model projection.",
                        text.len()
                    ),
                )
            }
        }
    }
}

impl ToolProjection for FileTool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_projection(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn project_with_native_work(
        &self, invocation: &ToolInvocation, result: &ToolResult,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        match self {
            Self::Read => ReadFileProjection(super::schema::ProjectionMode::Full)
                .project_with_native_work(invocation, result, work),
            _ => FileResultProjection(super::schema::ProjectionMode::Full)
                .project_with_native_work(invocation, result, work),
        }
    }

    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        match self {
            Self::Read => {
                ReadFileProjection(super::schema::ProjectionMode::Full).project(invocation, result)
            }
            Self::Write | Self::Edit | Self::Patch { .. } => {
                FileResultProjection(super::schema::ProjectionMode::Full)
                    .project(invocation, result)
            }
        }
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl FileTool {
    async fn run_native(
        &self, context: &ToolContext, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        self.authorize_with_native_context(context, invocation, work)?;
        let task = context.task();
        match self {
            Self::Read => {
                let input = super::decode_with_native_work::<ReadFileInput>(&invocation.arguments, work)?;
                let mut read = task.read_file_with_native_work(&input.0.file, work).await?;
                read.1.resize_retained(read.1.bytes() + std::mem::size_of::<ToolResult>() as u64)
                    .map_err(super::edit::native_error)?;
                let text = String::from_utf8(std::mem::take(&mut read.0))
                    .map_err(|_| native::invalid("file read requires UTF-8 content"))?;
                Ok((ToolResult { value: Value::String(text) }, read.1))
            }
            Self::Write => {
                let input = super::decode_with_native_work::<WriteFileInput>(&invocation.arguments, work)?;
                let file = task.stage_file_once_with_native_work(
                    invocation.operation_id, &input.0.path, input.0.text.as_bytes(),
                    &input.0.media_type, &input.0.display_name, work,
                ).await?;
                let encoded = super::value_with_native_work(&native::BorrowedFileResult { file: &file.0 }, work)?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
            Self::Edit => {
                let input = super::decode_with_native_work::<EditFileInput>(&invocation.arguments, work)?;
                let read = task.read_file_with_native_work(&input.0.file, work).await?;
                let source = std::str::from_utf8(&read.0)
                    .map_err(|_| native::invalid("exact text edit requires UTF-8 content"))?;
                let mut allocation = super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
                let edited = super::edit::exact_replace_engine(
                    source, &input.0.old_text, &input.0.new_text,
                    task.scope().limits().file_bytes, &mut allocation,
                )?;
                let file = task.stage_file_at_with_native_work(
                    invocation.operation_id, &input.0.file, edited.as_bytes(), work,
                ).await?;
                let encoded = super::value_with_native_work(&native::BorrowedFileResult { file: &file.0 }, work)?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
            Self::Patch { maximum_work, maximum_hunks } => {
                let input = super::decode_with_native_work::<PatchFileInput>(&invocation.arguments, work)?;
                let limits = super::patch::PatchLimits {
                    maximum_bytes: task.scope().limits().file_bytes,
                    maximum_work: *maximum_work, maximum_hunks: *maximum_hunks,
                };
                if input.0.file.descriptor().byte_length() > limits.maximum_bytes
                    || input.0.diff.len() as u64 > limits.maximum_bytes
                {
                    return Err(native::invalid("patch input exceeds original task file bound"));
                }
                let read = task.read_file_with_native_work(&input.0.file, work).await?;
                let source = std::str::from_utf8(&read.0)
                    .map_err(|_| native::invalid("patch editing requires UTF-8 content"))?;
                let mut allocation = super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
                let edited = super::patch::apply_update_engine(source, &input.0.diff, limits, &mut allocation)?;
                let file = task.stage_file_at_with_native_work(
                    invocation.operation_id, &input.0.file, edited.as_bytes(), work,
                ).await?;
                let encoded = super::value_with_native_work(&native::BorrowedFileResult { file: &file.0 }, work)?;
                Ok((ToolResult { value: encoded.0 }, encoded.1))
            }
        }
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) mod native {
    use super::*;

    pub(super) fn invalid(reason: &'static str) -> Error {
        Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason }
    }

    pub(super) fn context(
        context: &ToolContext, invocation: &ToolInvocation, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "file/text tool requires its exact admitted call context",
            });
        }
        if !crate::contract::is_valid_component_label(&invocation.name)
            || invocation.call_id.is_empty()
            || invocation.call_id.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
            || invocation.call_id.chars().any(char::is_control)
            || invocation.call_id.contains('/') || invocation.call_id.contains('\\')
            || invocation.call_id == "." || invocation.call_id == ".."
        {
            return Err(invalid("tool call identity is invalid"));
        }
        Ok(())
    }

    pub(super) fn public_path(path: &str, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        crate::conversation::validate_content_path_with_native_work(path, work)?;
        if crate::conversation::is_internal_path(path) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "file tools cannot access internal storage",
            });
        }
        Ok(())
    }

    pub(super) fn bound<T: Serialize>(value: &T, maximum: u64, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        // Count the canonical typed serializer without allocating a discarded
        // encoded buffer. Only audited file/text DTOs use this writer.
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let diagnostic_bytes = crate::contract::sdk_json_diagnostic_bytes();
        io.claim(diagnostic_bytes)?;
        let custody = io.into_native_custody(diagnostic_bytes)?;
        struct Counter<'a> {
            remaining: u64,
            work: &'a acyclic_fs::NativeMaterializationWork,
            denial: Option<Error>,
        }
        impl std::io::Write for Counter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let Some(remaining) = self.remaining.checked_sub(bytes.len() as u64) else {
                    self.denial = Some(invalid("JSON exceeds declared byte bound"));
                    return Err(std::io::ErrorKind::WriteZero.into());
                };
                if let Err(cause) = self.work.charge(acyclic_fs::WorkCounters {
                    bytes_encoded: bytes.len() as u64, ..Default::default()
                }) {
                    self.denial = Some(super::super::edit::native_error(cause));
                    return Err(std::io::ErrorKind::Interrupted.into());
                }
                self.remaining = remaining;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let mut counter = Counter { remaining: maximum, work, denial: None };
        let encoded = serde_json::to_writer(&mut counter, value);
        if let Some(error) = counter.denial.take() {
            // Drop serde's opaque error metadata while the original diagnostic
            // quota is still paid; propagate an actual native denial unchanged.
            drop(encoded);
            return Err(error);
        }
        encoded.map_err(|source| Error::NativeEncode {
            code: crate::wire::ErrorCode::Invalid, source, custody: Some(custody),
        })
    }


    pub(super) fn summary_reference(
        file: &FileRef, maximum: usize, arguments: std::fmt::Arguments<'_>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        use std::fmt::Write as _;
        let mut allocation = super::super::edit::AlgorithmWork::native(work, std::mem::size_of::<String>())?;
        let mut text = String::new();
        allocation.reserve_string(&mut text, maximum)?;
        struct FixedText<'a> {
            text: &'a mut String,
            work: &'a super::super::edit::AlgorithmWork,
            denial: Option<Error>,
        }
        impl std::fmt::Write for FixedText<'_> {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                if value.len() > self.text.capacity() - self.text.len() {
                    self.denial = Some(invalid("file summary exceeds its preclaimed capacity"));
                    return Err(std::fmt::Error);
                }
                if let Err(error) = self.work.copy(value.len()) {
                    self.denial = Some(error);
                    return Err(std::fmt::Error);
                }
                self.text.push_str(value);
                Ok(())
            }
        }
        let mut writer = FixedText { text: &mut text, work: &allocation, denial: None };
        if writer.write_fmt(arguments).is_err() {
            return Err(writer.denial.take().unwrap_or_else(|| invalid("file summary formatting failed")));
        }
        drop(writer);
        super::super::schema::project_reference_with_native_work(file, &text, work)
    }

    #[derive(Serialize)]
    pub(super) struct BorrowedFileResult<'a> { pub(super) file: &'a FileRef }

    #[derive(Serialize, Deserialize)]
    #[serde(transparent)]
    pub(super) struct ReadText(pub(super) String);

    impl crate::filesystem::UploadRecord for ReadText {
        fn logical_owned_bytes(&self) -> Result<u64> {
            Ok((std::mem::size_of::<Self>() + self.0.capacity()) as u64)
        }
    }

    macro_rules! record {
        ($ty:ty, $value:ident, $($bytes:expr),+ $(,)?) => {
            impl crate::filesystem::UploadRecord for $ty {
                fn logical_owned_bytes(&$value) -> Result<u64> {
                    let mut bytes = std::mem::size_of::<Self>() as u64;
                    $(bytes = bytes.checked_add($bytes as u64)
                        .ok_or_else(|| invalid("file tool DTO ownership overflow"))?;)+
                    Ok(bytes)
                }
            }
        };
    }
    record!(ReadFileInput, self, self.file.logical_owned_bytes()?);
    record!(WriteFileInput, self, self.path.capacity(), self.text.capacity(), self.media_type.capacity(), self.display_name.capacity());
    record!(EditFileInput, self, self.file.logical_owned_bytes()?, self.old_text.capacity(), self.new_text.capacity());
    record!(PatchFileInput, self, self.file.logical_owned_bytes()?, self.diff.capacity());
    record!(FileResult, self, self.file.logical_owned_bytes()?);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, OperationId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };

    fn file() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("file-preflight", "filesystem", "1")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([61; 16])),
            )?,
            "source.txt",
            "generation-1",
            FileDescriptor::from_bytes(b"exact", "text/plain")?,
            "source.txt",
        )
    }

    #[test]
    fn context_free_gate_rejects_even_valid_source_grants() -> Result<()> {
        let file = file()?;
        let missing = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        let granted = RuntimeScope::new(
            Capabilities::new([file.read_capability()?]),
            Limits::default(),
        )?;
        for (tool, arguments) in [
            (
                read_file()?,
                serde_json::to_value(ReadFileInput { file: file.clone() })
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
            (
                edit_file()?,
                serde_json::to_value(EditFileInput {
                    file: file.clone(),
                    old_text: "exact".into(),
                    new_text: "changed".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
            (
                patch_file(4096, 16)?,
                serde_json::to_value(PatchFileInput {
                    file: file.clone(),
                    diff: "@@\n-exact\n+changed\n".into(),
                })
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
        ] {
            let invocation = ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "preflight".into(),
                name: tool.definition.name.clone(),
                arguments,
            };
            assert!(matches!(
                tool.executor.authorize(None, &invocation),
                Err(Error::Unauthorized(_))
            ));
            assert!(matches!(
                tool.executor.authorize(Some(&missing), &invocation),
                Err(Error::Unauthorized(_))
            ));
            assert!(matches!(
                tool.executor.authorize(Some(&granted), &invocation),
                Err(Error::Unauthorized(_))
            ));
        }
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "read".into(),
            name: "acyclic.read_file".into(),
            arguments: json!({"file":file}),
        };
        let narrow = RuntimeScope::new(
            Capabilities::new([file.read_capability()?]),
            Limits {
                render_bytes: 4,
                ..Limits::default()
            },
        )?;
        assert!(matches!(
            read_file()?.executor.authorize(Some(&narrow), &invocation),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[test]
    fn publication_template_rejects_missing_scope_and_invalid_path_before_dispatch() -> Result<()> {
        let tool = write_file()?;
        let scope = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "write".into(),
            name: tool.definition.name.clone(),
            arguments: json!({"path":"../escape.txt","text":"body","media_type":"text/plain","display_name":"body"}),
        };
        assert!(matches!(
            tool.executor.authorize(None, &invocation),
            Err(Error::Unauthorized(_))
        ));
        assert!(tool.executor.authorize(Some(&scope), &invocation).is_err());
        assert_eq!(tool.definition.revision, "portable-5");
        assert_eq!(
            patch_file(4096, 16)?.definition.revision,
            "portable-patch-4"
        );
        Ok(())
    }
}
