//! Trained file argument shapes over the original file authorization and journal.
//!
//! These are client-executed functions. Provider-native declarations belong to
//! the model binding; registering a function never enables a server-side tool.

use crate::{
    Error, NativeCustody, NativeWork, Result,
    runtime::{RuntimeScope, ToolContext},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult,
        files::{FileProjection, FileResult, FileTool},
        schema::{self, ProjectionMode},
    },
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

mod patch;
pub use patch::apply_patch;

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

/// The frozen generic skill envelope. Its executable is admitted through the
/// original dispatcher; constructing this declaration does not register a tool.
pub fn call_definition() -> Result<ToolDefinition> {
    let definition = ToolDefinition {
        name: "call".into(),
        revision: "skill-call-1".into(),
        description:
            "Call one currently available skill by its exact declared identity and input schema"
                .into(),
        input_schema: schema::input::<CallInput>()?,
        output_schema: serde_json::json!({}),
        projection_schema: serde_json::json!({}),
    };
    definition.validate(&mut None)?;
    Ok(definition)
}

/// Gemini's trained function spellings. Both use the original file executor;
/// an invocation's operation/call identity and original journal stay unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileVariant {
    /// Exact replacement; `instruction` expresses intent but cannot grant access.
    Replace,
    /// Complete UTF-8 file publication.
    WriteFile,
    /// Client implementation of Anthropic's trained text-editor commands.
    TextEditor,
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

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct TextEditor<'a> {
    command: &'a str,
    path: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_text: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    old_str: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    new_str: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    insert_line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    view_range: Option<[i64; 2]>,
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

#[derive(Serialize)]
struct Read<'a> {
    path: &'a str,
}

#[derive(Serialize)]
struct Insert<'a> {
    path: &'a str,
    insert_line: u64,
    new_text: &'a str,
}

struct Normalized {
    // Retire the owned values before their original native-ledger custody.
    invocation: ToolInvocation,
    executor: FileTool,
    _arguments: NativeCustody,
    _identity: NativeCustody,
}

impl FileVariant {
    /// Exact model-visible name; no provider or caller rename is accepted.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::WriteFile => "write_file",
            Self::TextEditor => "str_replace_based_edit_tool",
        }
    }

    fn executor(self, arguments: &Value) -> Result<FileTool> {
        match self {
            Self::Replace => Ok(FileTool::Edit),
            Self::WriteFile => Ok(FileTool::Write),
            Self::TextEditor => match arguments.get("command").and_then(Value::as_str) {
                Some("view") => Ok(FileTool::Read),
                Some("create") => Ok(FileTool::Create),
                Some("str_replace") => Ok(FileTool::Edit),
                Some("insert") => Ok(FileTool::Insert),
                _ => Err(Error::tool_rejected(
                    "text editor command must be view, create, str_replace or insert",
                )),
            },
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
            Self::TextEditor => (
                "View UTF-8 lines, create a file, replace one exact old_str, or insert new_str after insert_line. Every command uses the selected project and original file grants",
                schema::input::<TextEditor<'_>>()?,
                &["Read", "Write", "Edit"],
            ),
        };
        let definition = ToolDefinition {
            name: self.name().into(),
            revision: if self == Self::TextEditor {
                "text_editor_20250728"
            } else {
                REVISION
            }
            .into(),
            description: description.into(),
            input_schema,
            output_schema: if self == Self::TextEditor {
                serde_json::json!({"oneOf":[schema::output::<FileResult>()?, schema::output::<crate::tool::files::ReadFileResult>()?]})
            } else {
                schema::output::<FileResult>()?
            },
            projection_schema: FileProjection::model_schema(ProjectionMode::Full)?,
        };
        definition.validate(&mut None)?;
        Ok(Tool {
            definition,
            hook_names,
            skill_source: None,
            executor: Arc::new(self),
            projection: Arc::new(self),
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
        let path = field(if self == Self::TextEditor {
            "path"
        } else {
            "file_path"
        })?;
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
            Self::TextEditor => {
                let command = field("command")?;
                let required: &[&str] = match command {
                    "view" => &["command", "path"],
                    "create" => &["command", "path", "file_text"],
                    "str_replace" => &["command", "path", "old_str", "new_str"],
                    "insert" => &["command", "path", "insert_line", "new_str"],
                    _ => return Err(rejected(work, "text editor command is unsupported")),
                };
                if object.keys().any(|key| {
                    !required.contains(&key.as_str()) && !(command == "view" && key == "view_range")
                }) {
                    return Err(rejected(
                        work,
                        "text editor command contains an unrelated field",
                    ));
                }
                match command {
                    "view" => {
                        view_range(original, work)?;
                        crate::tool::value(&Read { path }, u64::MAX, work)
                    }
                    "create" => crate::tool::value(
                        &Write {
                            path,
                            content: field("file_text")?,
                        },
                        u64::MAX,
                        work,
                    ),
                    "str_replace" => {
                        let old_text = field("old_str")?;
                        if old_text.is_empty() {
                            return Err(rejected(work, "str_replace requires nonempty old_str"));
                        }
                        crate::tool::value(
                            &Edit {
                                path,
                                old_text,
                                new_text: field("new_str")?,
                            },
                            u64::MAX,
                            work,
                        )
                    }
                    "insert" => {
                        let insert_line = object
                            .get("insert_line")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                rejected(work, "insert_line must be a nonnegative integer")
                            })?;
                        crate::tool::value(
                            &Insert {
                                path,
                                insert_line,
                                new_text: field("new_str")?,
                            },
                            u64::MAX,
                            work,
                        )
                    }
                    _ => Err(rejected(work, "text editor command is unsupported")),
                }
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
            executor: self.executor(&original.arguments)?,
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

fn view_range(arguments: &Value, work: NativeWork<'_>) -> Result<Option<(u64, Option<u64>)>> {
    let Some(range) = arguments.get("view_range") else {
        return Ok(None);
    };
    let range = range
        .as_array()
        .filter(|range| range.len() == 2)
        .ok_or_else(|| rejected(work, "view_range requires [first,last]"))?;
    let first = range[0]
        .as_u64()
        .filter(|first| *first > 0)
        .ok_or_else(|| rejected(work, "view_range first line must be positive"))?;
    let last = if range[1].as_i64() == Some(-1) {
        None
    } else {
        Some(
            range[1]
                .as_u64()
                .filter(|last| *last >= first)
                .ok_or_else(|| rejected(work, "view_range last line precedes its first"))?,
        )
    };
    Ok(Some((first, last)))
}

impl ToolProjection for FileVariant {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: NativeWork<'_>,
    ) -> Result<(Value, NativeCustody)> {
        let normalized = self.normalize(invocation, work)?;
        if !matches!(normalized.executor, FileTool::Read) {
            return FileProjection::Publication(ProjectionMode::Full).project(
                &normalized.invocation,
                result,
                work,
            );
        }
        // Validate the complete immutable read using the original projector.
        // The canonical result retains every byte; only this presentation is
        // selected. In-place selection keeps the original allocation custody.
        let (mut projected, custody) = FileProjection::Read(ProjectionMode::Full).project(
            &normalized.invocation,
            result,
            work,
        )?;
        if let Some((first, last)) = view_range(&invocation.arguments, work)? {
            let text = projected
                .get_mut("value")
                .and_then(|value| value.get_mut("text"))
                .and_then(|value| match value {
                    Value::String(text) => Some(text),
                    _ => None,
                })
                .ok_or_else(|| rejected(work, "text editor read projection is absent"))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = work {
                let charge = acyclic_fs::WorkCounters {
                    items_examined: text.len() as u64,
                    bytes_copied: text.len() as u64,
                    ..Default::default()
                };
                work.admit(charge)?;
            }
            let mut start = None;
            let mut end = text.len();
            let mut offset = 0;
            for (index, line) in text.split_inclusive('\n').enumerate() {
                let number = index as u64 + 1;
                if number == first {
                    start = Some(offset);
                }
                offset += line.len();
                if last == Some(number) {
                    end = offset;
                    break;
                }
            }
            let start = start.ok_or_else(|| rejected(work, "view_range starts beyond the file"))?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = work {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: offset as u64,
                    bytes_copied: if start == 0 { 0 } else { (end - start) as u64 },
                    ..Default::default()
                })?;
            }
            text.truncate(end);
            text.drain(..start);
        }
        Ok((projected, custody))
    }

    fn supports_native_projection(&self, work: NativeWork<'_>) -> Result<()> {
        FileProjection::Publication(ProjectionMode::Full).supports_native_projection(work)
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
        normalized
            .executor
            .authorize(scope, context, &normalized.invocation, work)
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let normalized = self.normalize(&invocation, None)?;
            normalized
                .executor
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
            normalized
                .executor
                .reconcile(normalized.invocation, context)
                .await
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        FileTool::Edit.supports_native_execution(work)
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
            normalized
                .executor
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
            normalized
                .executor
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
