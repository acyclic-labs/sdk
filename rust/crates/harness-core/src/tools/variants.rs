//! Trained file argument shapes over the original file authorization and journal.
//!
//! These are client-executed functions. Provider-native declarations belong to
//! the model binding; registering a function never enables a server-side tool.

use crate::{
    Error, NativeCustody, NativeWork, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
        files::{FileProjection, FileResult, FileTool},
        schema::{self, ProjectionMode},
    },
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// The immutable trained function revision, also pinned by model files.
pub const REVISION: &str = "trained-file-1";

/// One skill call carried through the frozen generic declaration. The original
/// invocation must be journaled before its target enters ordinary tool admission;
/// it is never executed by a nested executor that could bypass policy.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CallInput {
    /// Exact discovered skill identity, pinned at its declaration revision.
    pub tool: String,
    /// Input checked against that skill's admitted schema.
    pub args: Value,
}

/// Borrowed target of the original generic skill invocation.
pub struct CallTarget<'a> {
    /// Registry identity resolved by ordinary admission and approval.
    pub tool: &'a str,
    /// Original arguments; no second schema or authority path.
    pub args: &'a Value,
}

impl CallInput {
    /// Resolves only the fixed envelope. The caller still checks that the target
    /// is an admitted skill and validates its original schema, grant and policy.
    /// This operation allocates nothing and does not execute any tool.
    pub fn target(arguments: &Value) -> Result<CallTarget<'_>> {
        let object = arguments
            .as_object()
            .ok_or_else(|| Error::tool_rejected("call requires an object"))?;
        if object.len() != 2 {
            return Err(Error::tool_rejected("call requires exactly tool and args"));
        }
        let tool = object
            .get("tool")
            .and_then(Value::as_str)
            .filter(|tool| crate::contract::is_valid_component_label(tool) && *tool != "call")
            .ok_or_else(|| {
                Error::tool_rejected("call requires an exact nonrecursive skill identity")
            })?;
        let args = object
            .get("args")
            .ok_or_else(|| Error::tool_rejected("call requires args"))?;
        Ok(CallTarget { tool, args })
    }
}

/// Gemini's trained function spellings. Both use the original file executor;
/// an invocation's operation/call identity and original journal stay unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileVariant {
    /// Exact replacement; `instruction` expresses intent but cannot grant access.
    Replace,
    /// Complete UTF-8 file publication.
    WriteFile,
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Replace<'a> {
    file_path: &'a str,
    instruction: &'a str,
    #[schemars(length(min = 1))]
    old_string: &'a str,
    new_string: &'a str,
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct WriteFile<'a> {
    file_path: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct Edit<'a> {
    path: &'a str,
    old_text: &'a str,
    new_text: &'a str,
}

#[derive(Serialize)]
struct Write<'a> {
    path: &'a str,
    content: &'a str,
}

struct Normalized {
    // Retire the owned values before their original native-ledger custody.
    invocation: ToolInvocation,
    _arguments: NativeCustody,
    _identity: NativeCustody,
}

impl FileVariant {
    /// Exact model-visible name; no provider or caller rename is accepted.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::WriteFile => "write_file",
        }
    }

    fn executor(self) -> FileTool {
        match self {
            Self::Replace => FileTool::Edit,
            Self::WriteFile => FileTool::Write,
        }
    }

    /// Assembles an ordinary registry entry, retaining the shared publication
    /// projection, approval hook names and original grant checks.
    pub fn bind(self) -> Result<Tool> {
        let (description, input_schema, hook_names): (_, _, &'static [&'static str]) = match self {
            Self::Replace => (
                "Replace one exact unambiguous old_string in file_path with new_string. instruction states the intended change. The selected project grant and compare-and-publish still apply",
                schema::input::<Replace<'_>>()?,
                &["Edit"],
            ),
            Self::WriteFile => (
                "Write complete UTF-8 content to file_path in the selected project under its original publication grant",
                schema::input::<WriteFile<'_>>()?,
                &["Write"],
            ),
        };
        let definition = ToolDefinition {
            name: self.name().into(),
            revision: REVISION.into(),
            description: description.into(),
            input_schema,
            output_schema: schema::output::<FileResult>()?,
            projection_schema: FileProjection::model_schema(ProjectionMode::Full)?,
        };
        definition.validate(&mut None)?;
        Ok(Tool {
            definition,
            hook_names,
            executor: Arc::new(self),
            projection: Arc::new(FileProjection::Publication(ProjectionMode::Full)),
        })
    }

    fn arguments(self, original: &Value, work: NativeWork<'_>) -> Result<(Value, NativeCustody)> {
        let object = original
            .as_object()
            .ok_or_else(|| rejected(work, "trained file arguments must be an object"))?;
        let field = |name| {
            object
                .get(name)
                .and_then(Value::as_str)
                .ok_or_else(|| rejected(work, "trained file argument is absent or not text"))
        };
        let path = field("file_path")?;
        match self {
            Self::Replace => {
                if object.len() != 4 {
                    return Err(rejected(
                        work,
                        "replace requires exactly its four declared fields",
                    ));
                }
                let _intent = field("instruction")?;
                let old_text = field("old_string")?;
                if old_text.is_empty() {
                    return Err(rejected(work, "replace requires a nonempty old_string"));
                }
                crate::tool::value(
                    &Edit {
                        path,
                        old_text,
                        new_text: field("new_string")?,
                    },
                    u64::MAX,
                    work,
                )
            }
            Self::WriteFile => {
                if object.len() != 2 {
                    return Err(rejected(
                        work,
                        "write_file requires exactly its two declared fields",
                    ));
                }
                crate::tool::value(
                    &Write {
                        path,
                        content: field("content")?,
                    },
                    u64::MAX,
                    work,
                )
            }
        }
    }

    fn normalize(self, original: &ToolInvocation, work: NativeWork<'_>) -> Result<Normalized> {
        if original.name != self.name() {
            return Err(rejected(
                work,
                "trained file variant differs from its admitted name",
            ));
        }
        let (arguments, arguments_custody) = self.arguments(&original.arguments, work)?;
        let (call_id, name, identity_custody) = copy_identity(original, work)?;
        Ok(Normalized {
            invocation: ToolInvocation {
                operation_id: original.operation_id,
                call_id,
                // FileTool checks the unchanged call identity and original file
                // grants; this name remains the admitted registry variant.
                name,
                arguments,
            },
            _arguments: arguments_custody,
            _identity: identity_custody,
        })
    }
}

fn rejected(work: NativeWork<'_>, reason: &'static str) -> Error {
    if work.is_some() {
        Error::tool_rejected(reason)
    } else {
        Error::Invalid(reason.into())
    }
}

fn copy_identity(
    original: &ToolInvocation,
    work: NativeWork<'_>,
) -> Result<(String, String, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        use crate::store::history::HistoryIo;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let call_id = history.clone_text(&original.call_id)?;
        let name = history.clone_text(&original.name)?;
        let retained = (call_id.capacity() as u64)
            .checked_add(name.capacity() as u64)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "trained file identity ownership overflow",
            })?;
        let custody = io.into_native_custody(retained)?;
        return Ok((call_id, name, Some(custody)));
    }
    let _ = work;
    Ok((original.call_id.clone(), original.name.clone(), None))
}

impl ToolExecutor for FileVariant {
    fn authorize(
        &self,
        scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<()> {
        let normalized = self.normalize(invocation, work)?;
        self.executor()
            .authorize(scope, context, &normalized.invocation, work)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let normalized = self.normalize(&invocation, None)?;
            self.executor()
                .execute(normalized.invocation, context)
                .await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let normalized = self.normalize(&invocation, None)?;
            // The original journal authenticates the complete trained call,
            // including instruction, before the existing operation observation.
            self.executor()
                .reconcile(normalized.invocation, context)
                .await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.executor().supports_native_execution(work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let normalized = self.normalize(invocation, Some(work))?;
            self.executor()
                .execute_with_native_context(context, &normalized.invocation, work)?
                .await
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
            let normalized = self.normalize(invocation, Some(work))?;
            self.executor()
                .reconcile_with_native_context(context, &normalized.invocation, work)?
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trained_arguments_keep_effect_bytes_and_refuse_undeclared_fields() -> Result<()> {
        let original = json!({"file_path":"src/a.rs","instruction":"keep exact whitespace","old_string":"a\n","new_string":"b\n"});
        let canonical = FileVariant::Replace.arguments(&original, None)?.0;
        assert_eq!(
            canonical,
            json!({"path":"src/a.rs","old_text":"a\n","new_text":"b\n"})
        );
        assert_eq!(original["instruction"], "keep exact whitespace");
        let mut forged = original.clone();
        forged["version"] = json!("caller-chosen");
        assert!(FileVariant::Replace.arguments(&forged, None).is_err());
        let mut empty = original;
        empty["old_string"] = json!("");
        assert!(FileVariant::Replace.arguments(&empty, None).is_err());
        assert!(
            FileVariant::WriteFile
                .arguments(
                    &json!({"file_path":"a","content":"text","media_type":"x"}),
                    None
                )
                .is_err()
        );
        Ok(())
    }
}
