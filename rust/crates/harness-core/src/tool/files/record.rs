//! Typed file records use the driver's ordinary ToolCompleted publication.
//! Preparation has no external effect, second append, store or recovery path.
use super::*;
use crate::{OperationId, live::feedback};

/// Authored payload retained by the original tool completion. Provenance is
/// projected from its authenticated execution record, never from this payload.
#[derive(Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct RecordResult {
    /// Persisted publication schema version, currently one.
    pub version: u32,
    /// Original validated authored reports, in their publication order.
    pub reports: Vec<feedback::Report>,
}

impl RecordResult {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return Err(Error::Invalid(
                "feedback publication requires schema version 1".into(),
            ));
        }
        if self.reports.is_empty() {
            return Err(Error::Invalid(
                "feedback requires at least one report".into(),
            ));
        }
        self.reports.iter().try_for_each(feedback::Report::validate)
    }
}

/// Original publication results of the shared write tool.
#[derive(Serialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WriteResult {
    /// A replaceable volume file.
    File(Box<FileResult>),
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
    invocation
        .arguments
        .get("path")
        .and_then(Value::as_str)
        .is_some_and(feedback::is_sink)
}

pub(super) async fn run(
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
    let records = decode_reports(&input.0.content, work, io.as_ref())?;
    let receipt = context
        .observe_tool_path_once(invocation.operation_id, feedback::PATH, io.as_ref())
        .await?;
    if receipt.0.is_none() {
        let existing = match context
            .read_tool_path_bounded(feedback::PATH, None, None, io.as_ref())
            .await
        {
            Ok((_, custody, retained)) => {
                #[cfg(not(target_arch = "wasm32"))]
                let _read_custody = io
                    .as_ref()
                    .map(|io| io.take_native_custody(retained))
                    .transpose()?;
                custody.as_ref().to_vec()
            }
            Err(error) if error.code() == crate::wire::ErrorCode::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        let mut bytes = existing;
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        for report in &records.result.reports {
            bytes.extend_from_slice(&crate::contract::canonical_json_bytes(report)?);
            bytes.push(b'\n');
        }
        let staged = context
            .stage_tool_file_once(
                invocation.operation_id,
                feedback::PATH,
                &bytes,
                "application/jsonl",
                "feedback.jsonl",
                io.as_ref(),
            )
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let _file_custody = io
            .as_ref()
            .map(|io| io.take_native_custody(staged.1))
            .transpose()?;
        let result = super::value(
            &records.result,
            context.effective_scope().limits().file_bytes,
            work,
        )?;
        return Ok((ToolResult { value: result.0 }, result.1));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _observed_custody = io
        .as_ref()
        .map(|io| io.take_native_custody(receipt.1))
        .transpose()?;
    let result = super::value(
        &records.result,
        context.effective_scope().limits().file_bytes,
        work,
    )?;
    Ok((ToolResult { value: result.0 }, result.1))
}

pub(super) async fn reconcile(
    context: &ToolContext,
    invocation: &ToolInvocation,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(Option<ToolResult>, acyclic_fs::NativeCustody)> {
    let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
    #[cfg(not(target_arch = "wasm32"))]
    let io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(target_arch = "wasm32")]
    let io: Option<crate::executor::PreparationIo> = None;
    let records = decode_reports(&input.0.content, work, io.as_ref())?;
    let receipt = context
        .observe_tool_path_once(invocation.operation_id, feedback::PATH, io.as_ref())
        .await?;
    if receipt.0.is_none() {
        #[cfg(not(target_arch = "wasm32"))]
        return Ok((
            None,
            io.as_ref()
                .map(|io| io.take_native_custody(receipt.1))
                .transpose()?,
        ));
        #[cfg(target_arch = "wasm32")]
        return Ok((None, None));
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _observed_custody = io
        .as_ref()
        .map(|io| io.take_native_custody(receipt.1))
        .transpose()?;
    let result = super::value(
        &records.result,
        context.effective_scope().limits().file_bytes,
        work,
    )?;
    Ok((Some(ToolResult { value: result.0 }), result.1))
}

// Payloads retire before the original admissions that own their storage.
struct PreparedRecords<'a> {
    result: RecordResult,
    _decoded: Option<crate::filesystem::DecodeReservation<'a>>,
    _custody: acyclic_fs::NativeCustody,
}

fn decode_reports<'a>(
    content: &str,
    work: acyclic_fs::NativeWork<'_>,
    io: Option<&'a crate::executor::PreparationIo>,
) -> Result<PreparedRecords<'a>> {
    use crate::executor::json::{JsonBounds, JsonCodecCharge, JsonSpans, SpanEvent};
    let mut records = PreparedRecords {
        result: RecordResult {
            version: 1,
            reports: Vec::new(),
        },
        _decoded: None,
        _custody: super::super::edit::allocation(work, std::mem::size_of::<JsonSpans>())?,
    };
    let mut cursor = JsonSpans::sequence();
    let mut event = |event| -> Result<bool> {
        if let SpanEvent::End {
            depth: 0,
            start,
            end,
            ..
        } = event
        {
            let count = records.result.reports.len().checked_add(1).ok_or_else(|| {
                super::executor::rejection(work, "feedback report count overflows")
            })?;
            super::super::edit::reserve_vec(
                &mut records._custody,
                &mut records.result.reports,
                count,
            )?;
            let bytes = super::window::span(content.as_bytes(), start..end).ok_or_else(|| {
                super::executor::rejection(work, "feedback report span exceeds its source")
            })?;
            let report: feedback::Report = if let Some(io) = io {
                let (report, hold) = io.decode(
                    std::marker::PhantomData::<feedback::Report>,
                    bytes,
                    crate::wire::ErrorCode::Invalid,
                )?;
                if let Some(decoded) = &mut records._decoded {
                    decoded.absorb(hold)?;
                } else {
                    records._decoded = Some(hold);
                }
                report
            } else {
                crate::contract::json_from_slice(bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            };
            report.validate()?;
            records.result.reports.push(report);
        }
        Ok(false)
    };
    crate::executor::with_history_io(io, |history| -> Result<()> {
        if let Some(history) = history.as_deref_mut() {
            history.codec_charge(JsonCodecCharge::Read {
                bytes: content.len() as u64,
            })?;
        }
        cursor.feed(
            content.as_bytes(),
            JsonBounds {
                maximum_bytes: Some(content.len() as u64),
                maximum_items: None,
                maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
            },
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| {
                if let Some(history) = history.as_deref_mut() {
                    history.codec_charge(charge)?;
                }
                Ok(())
            },
            &mut event,
        )?;
        cursor.finish(Some(crate::wire::ErrorCode::Invalid), &mut event)
    })?;
    if records.result.reports.is_empty() {
        return Err(super::executor::rejection(
            work,
            "feedback requires at least one report",
        ));
    }
    Ok(records)
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::filesystem::UploadRecord for RecordResult {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.reports.iter().try_fold(
            crate::core::owned_buffer::<feedback::Report>(self.reports.capacity())?,
            |bytes, report| {
                crate::core::owned_sum([
                    bytes,
                    report.what.capacity() as u64,
                    report.evidence.capacity() as u64,
                    report.suggestion.as_ref().map_or(0, String::capacity) as u64,
                ])
            },
        )
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
