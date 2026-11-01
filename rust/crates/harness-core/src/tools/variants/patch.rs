//! The trained V4A operation uses the original tool journal and one conditional
//! filesystem publication, including multi-file changes and moves.
use crate::conversation::{ContentChange, FileRef, VolumeRef};
use crate::tool::edit::{allocation, charge, reserve_string, reserve_vec};
use crate::{
    Error, NativeCustody, NativeWork, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult,
        patch::{FileChange, parse_document},
        schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct PatchInput {
    patch: String,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "PatchResult")]
struct PatchResult<Files = Vec<FileRef>> {
    files: Files,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for PatchResult {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.files.iter().try_fold(
            self.files.capacity() as u64 * std::mem::size_of::<FileRef>() as u64,
            |bytes, file| crate::core::owned_sum([bytes, file.logical_owned_bytes()?]),
        )
    }
}

fn input(arguments: &Value) -> Result<&str> {
    let object = arguments
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| Error::tool_rejected("apply_patch requires exactly patch"))?;
    object
        .get("patch")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::tool_rejected("apply_patch.patch must be text"))
}

struct Plan<'a> {
    source: Option<FileRef>,
    output: Option<String>,
    target: std::borrow::Cow<'a, str>,
    moving: bool,
    _source_custody: NativeCustody,
}

#[derive(Clone, Copy)]
struct ApplyPatch;

/// Declares the canonical client implementation. The model binding must expose
/// OpenAI's trained native declaration, preserve its raw custom input, and map
/// that input to this `patch` argument at the original dispatch boundary.
pub fn apply_patch() -> Result<Tool> {
    let mut definition = ToolDefinition {
        name: "apply_patch".into(), revision: "openai-v4a-1".into(),
        description: "Apply exact V4A add, update, delete and move operations in one atomic publication. Start with *** Begin Patch and finish with *** End Patch; update hunks use @@ and exact context".into(),
        input_schema: schema::input::<PatchInput>()?,
        output_schema: schema::output::<PatchResult>()?,
        projection_schema: schema::text_projection_schema()?,
    };
    definition.input_schema["x-harness-capability"] = Value::String("edit".into());
    Ok(Tool {
        definition,
        hook_names: &["Write", "Edit"],
        executor: Arc::new(ApplyPatch),
        projection: Arc::new(ApplyPatch),
    })
}

impl ApplyPatch {
    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
        recovering: bool,
    ) -> Result<(Option<ToolResult>, NativeCustody)> {
        let context = admitted_context(Some(context), invocation)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let mut meter = allocation(work, 0)?;
        let actions = parse_document(input(&invocation.arguments)?, &mut meter)?;
        let volume = authorize_actions(context, &actions, io)?;
        if recovering {
            let observed = context
                .observe_tool_changes(invocation.operation_id, volume, io)
                .await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let _receipt = io
                .map(|io| io.take_native_custody(observed.1))
                .transpose()?;
            return match observed.0 {
                Some(files) => {
                    let encoded = crate::tool::value(&PatchResult { files }, u64::MAX, work)?;
                    Ok((Some(ToolResult { value: encoded.0 }), encoded.1))
                }
                None => Ok((None, allocation(work, 0)?)),
            };
        }
        let mut plans = Vec::new();
        reserve_vec(&mut meter, &mut plans, actions.len())?;
        for action in &actions {
            let (_, target) =
                context.tool_write_path_selection(action.move_to().unwrap_or(action.path()), io)?;
            let maximum = context.effective_scope().limits().file_bytes;
            let (source, output, custody) = if matches!(action, FileChange::Add { .. }) {
                (None, action.materialize("", maximum, &mut meter)?, None)
            } else {
                let (file, bytes, retained) =
                    context.read_tool_path(action.path(), None, io).await?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let custody = {
                    let _ = retained;
                    None
                };
                let source = if matches!(action, FileChange::Update { .. }) {
                    std::str::from_utf8(&bytes)
                        .map_err(|_| Error::tool_rejected("patch updates require UTF-8"))?
                } else {
                    ""
                };
                (
                    Some(file),
                    action.materialize(source, maximum, &mut meter)?,
                    custody,
                )
            };
            plans.push(Plan {
                source,
                output,
                target,
                moving: action.move_to().is_some(),
                _source_custody: custody,
            });
        }
        let mut changes = Vec::new();
        let count = plans
            .len()
            .checked_add(plans.iter().filter(|plan| plan.moving).count())
            .ok_or_else(|| Error::tool_rejected("patch operation count overflows"))?;
        reserve_vec(&mut meter, &mut changes, count)?;
        for plan in &plans {
            match (&plan.source, &plan.output) {
                (None, Some(text)) => changes.push(ContentChange::CreateAbsent {
                    path: &plan.target,
                    bytes: text.as_bytes(),
                    media_type: "text/plain",
                    display_name: plan.target.rsplit('/').next().unwrap_or(&plan.target),
                }),
                (Some(file), Some(text)) if plan.moving => {
                    changes.push(ContentChange::CreateAbsent {
                        path: &plan.target,
                        bytes: text.as_bytes(),
                        media_type: file.descriptor().media_type(),
                        display_name: plan.target.rsplit('/').next().unwrap_or(&plan.target),
                    });
                    changes.push(ContentChange::DeleteExpected { source: file });
                }
                (Some(file), Some(text)) => changes.push(ContentChange::UpdateExpected {
                    source: file,
                    bytes: text.as_bytes(),
                }),
                (Some(file), None) => changes.push(ContentChange::DeleteExpected { source: file }),
                (None, None) => return Err(Error::tool_rejected("patch member has no effect")),
            }
        }
        let (files, retained) = context
            .stage_tool_changes(invocation.operation_id, volume, &changes, io)
            .await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _receipt = io.map(|io| io.take_native_custody(retained)).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        let encoded = crate::tool::value(&PatchResult { files }, u64::MAX, work)?;
        Ok((Some(ToolResult { value: encoded.0 }), encoded.1))
    }
}

fn admitted_context<'a>(
    context: Option<&'a ToolContext>,
    invocation: &ToolInvocation,
) -> Result<&'a ToolContext> {
    context
        .filter(|context| {
            context.operation_id() == invocation.operation_id
                && context.call_id() == invocation.call_id
                && invocation.name == "apply_patch"
        })
        .ok_or_else(|| {
            Error::tool_rejected("apply_patch requires its original admitted call context")
        })
}

fn authorize_actions<'a>(
    context: &'a ToolContext,
    actions: &[FileChange<'a>],
    io: Option<&crate::executor::PreparationIo>,
) -> Result<&'a VolumeRef> {
    let first = actions
        .first()
        .ok_or_else(|| Error::tool_rejected("patch requires changes"))?;
    let (selected, _) = context.tool_write_path_selection(first.path(), io)?;
    for action in actions {
        for path in std::iter::once(action.path()).chain(action.move_to()) {
            let (volume, relative) = context.tool_write_path_selection(path, io)?;
            if volume != selected {
                return Err(Error::tool_rejected(
                    "one atomic patch must select one volume",
                ));
            }
            crate::executor::with_history_io(io, |history| {
                crate::conversation::validate_content_path(&relative, history)
            })?;
            if relative.len() > context.effective_scope().limits().path_bytes
                || crate::conversation::is_internal_path(&relative)
            {
                return Err(Error::tool_rejected(
                    "patch path exceeds its original public admission",
                ));
            }
        }
        if !matches!(action, FileChange::Add { .. }) {
            context.authorize_tool_path_read(action.path(), io)?;
        }
    }
    Ok(selected)
}

impl ToolExecutor for ApplyPatch {
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<()> {
        let context = admitted_context(context, invocation)?;
        let mut meter = allocation(work, 0)?;
        let actions = parse_document(input(&invocation.arguments)?, &mut meter)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        authorize_actions(context, &actions, io).map(|_| ())
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let context =
                context.ok_or_else(|| Error::tool_rejected("patch context is unavailable"))?;
            self.run(&context, &invocation, None, false)
                .await?
                .0
                .ok_or_else(|| Error::Storage("patch publication receipt is missing".into()))
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let context =
                context.ok_or_else(|| Error::tool_rejected("patch context is unavailable"))?;
            self.run(&context, &invocation, None, true)
                .await
                .map(|result| result.0)
        })
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
            let (result, custody) = self.run(context, invocation, Some(work), false).await?;
            Ok((
                result.ok_or_else(|| Error::tool_rejected("patch receipt is missing"))?,
                custody
                    .ok_or_else(|| Error::tool_rejected("patch result lacks original custody"))?,
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
            let (result, custody) = self.run(context, invocation, Some(work), true).await?;
            Ok((
                result,
                custody
                    .ok_or_else(|| Error::tool_rejected("patch recovery lacks original custody"))?,
            ))
        })
    }
}

impl ToolProjection for ApplyPatch {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: NativeWork<'_>,
    ) -> Result<(Value, NativeCustody)> {
        let _result = crate::tool::decode::<PatchResult>(&result.value, work)?;
        let mut meter = allocation(work, 0)?;
        let actions = parse_document(input(&invocation.arguments)?, &mut meter)?;
        let mut text = String::new();
        append_summary(
            &mut text,
            "Success. Updated the following files:\n",
            &mut meter,
        )?;
        // The pinned public harness groups additions, modifications and deletions
        // in this order, retaining request order within each group.
        for kind in ['A', 'M', 'D'] {
            for action in &actions {
                let (marker, path) = match action {
                    FileChange::Add { path, .. } => ('A', *path),
                    FileChange::Update { path, move_to, .. } => ('M', move_to.unwrap_or(path)),
                    FileChange::Delete { path } => ('D', *path),
                };
                if marker == kind {
                    append_summary(
                        &mut text,
                        match marker {
                            'A' => "A ",
                            'M' => "M ",
                            _ => "D ",
                        },
                        &mut meter,
                    )?;
                    append_summary(&mut text, path, &mut meter)?;
                    append_summary(&mut text, "\n", &mut meter)?;
                }
            }
        }
        schema::project_text(&text, work)
    }
    fn supports_native_projection(&self, work: NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
        }
        let _ = work;
        Ok(())
    }
}

fn append_summary(text: &mut String, part: &str, meter: &mut NativeCustody) -> Result<()> {
    let length = text
        .len()
        .checked_add(part.len())
        .ok_or_else(|| Error::tool_rejected("patch summary size overflows"))?;
    reserve_string(meter, text, length)?;
    charge(meter, part.len() as u64, 0)?;
    text.push_str(part);
    Ok(())
}
