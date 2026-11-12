//! Typed file records use the driver's ordinary ToolCompleted publication.
//! Preparation has no external effect, second append, store or recovery path.
use super::*;
use crate::{OperationId, live::feedback};

/// Authored payload retained by the original tool completion. Provenance is
/// projected from its authenticated execution record, never from this payload.
#[derive(Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct RecordResult {
    /// Original validated authored report.
    pub report: feedback::Report,
}

/// Original publication results of the shared write tool.
#[derive(Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WriteResult {
    /// A replaceable volume file.
    File(FileResult),
    /// An append-only execution observation.
    Record(RecordResult),
}

#[derive(Serialize, schemars::JsonSchema)]
pub(super) struct Ack<'a> {
    pub path: &'a str,
    #[schemars(with = "String")]
    pub version: OperationId,
}

pub(super) fn selected(invocation: &ToolInvocation) -> bool {
    invocation.arguments.get("path").and_then(Value::as_str) == Some(feedback::PATH)
}

pub(super) fn prepare(
    context: &ToolContext,
    invocation: &ToolInvocation,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(ToolResult, acyclic_fs::NativeCustody)> {
    let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    let io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let io: Option<crate::executor::PreparationIo> = None;
    let _hold;
    let report: feedback::Report = if let Some(io) = io.as_ref() {
        let (report, hold) = io.decode(
            std::marker::PhantomData::<feedback::Report>,
            input.0.content.as_bytes(),
            crate::wire::ErrorCode::Invalid,
        )?;
        _hold = Some(hold);
        report
    } else {
        _hold = None;
        crate::contract::json_from_slice(input.0.content.as_bytes())
            .map_err(|error| Error::Invalid(error.to_string()))?
    };
    report.validate()?;
    let result = super::value(
        &RecordResult { report },
        context.effective_scope().limits().file_bytes,
        work,
    )?;
    Ok((ToolResult { value: result.0 }, result.1))
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::filesystem::UploadRecord for RecordResult {
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.report.what.capacity() as u64,
            self.report.evidence.capacity() as u64,
            self.report.suggestion.as_ref().map_or(0, String::capacity) as u64,
        ])
    }
}
