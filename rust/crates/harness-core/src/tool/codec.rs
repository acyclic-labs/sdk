use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::filesystem::UploadRecord as DecodeRecord;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
use serde::de::DeserializeOwned as DecodeRecord;

/// Decodes a borrowed JSON value and retains its fields on the supplied ledger.
pub(crate) fn decode<T: DecodeRecord>(
    value: &Value,
    work: crate::NativeWork<'_>,
) -> Result<(T, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value: T = io.value_decode(value, crate::wire::ErrorCode::Invalid)?;
        let retained = (std::mem::size_of::<T>() as u64)
            .checked_add(value.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool DTO ownership overflow",
            })?;
        let custody = io.into_native_custody(retained)?;
        return Ok((value, Some(custody)));
    }
    let _ = work;
    Ok((
        T::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))?,
        None,
    ))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for String {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(self.capacity() as u64)
    }
}

/// Copies a Value under the original ledger and optional wire-size bound.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_value(
    value: &Value,
    maximum_bytes: Option<u64>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    if let Some(maximum_bytes) = maximum_bytes {
        io.check_json_byte_bound(value, maximum_bytes)?;
    }
    io.retain_owned(std::mem::size_of::<Value>() as u64)?;
    let copied = crate::model::clone_value(value, Some(&io))?;
    let retained = crate::executor::json::json_value_logical_owned_bytes(&copied)?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

/// Builds the canonical result under its byte limit and optional ledger.
#[doc(hidden)]
pub fn value<T: Serialize>(
    value: &T,
    maximum_bytes: u64,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value = io.value_json(value, maximum_bytes.min(work.remaining()?.bytes_encoded))?;
        let retained = (std::mem::size_of::<Value>() as u64)
            .checked_add(crate::executor::json::json_value_logical_owned_bytes(
                &value,
            )?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "tool JSON ownership overflow",
            })?;
        let custody = io.into_native_custody(retained)?;
        return Ok((value, Some(custody)));
    }
    let _ = work;
    if maximum_bytes != u64::MAX {
        crate::executor::json::json_byte_length(value, maximum_bytes, &mut None)?;
    }
    serde_json::to_value(value)
        .map(|value| (value, None))
        .map_err(|error| Error::Invalid(error.to_string()))
}

/// Closed tool DTO diagnostic labels.
#[cfg(feature = "filesystem")]
pub(crate) const TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES: u64 = {
    const LABELS: &[&str] = &[
        "ToolDefinition",
        "ToolInvocation",
        "ToolResult",
        "ReadFileInput",
        "WriteFileInput",
        "EditFileInput",
        "PatchFileInput",
        "FileResult",
        "ReadText",
        "SearchInput",
        "SearchResult",
        "TextRange",
        "SearchMatches",
        "ListFilesInput",
        "WorkflowAdmission",
        "MachineCheckpoint",
        "MachineIdentity",
        "RecordedInvocation",
        "invocation",
        "metadata",
        "operation_id",
        "call_id",
        "name",
        "revision",
        "description",
        "input_schema",
        "output_schema",
        "projection_schema",
        "arguments",
        "value",
        "file",
        "path",
        "text",
        "media_type",
        "display_name",
        "old_text",
        "new_text",
        "diff",
        "range",
        "query",
        "selection",
        "matches",
        "start",
        "end",
        "omitted_before",
        "omitted_after",
        "total_matches",
        "omitted_matches",
        "work",
        "expected_generation",
        "after",
        "maximum_entries",
        "request_digest",
        "initial",
        "machine",
        "version",
        "digest",
        "state",
    ];
    let mut bytes = crate::executor::json::preparation_serde_diagnostic_label_bytes();
    let mut remaining = LABELS;
    while let Some((label, rest)) = remaining.split_first() {
        bytes += label.len() as u64 + 4;
        remaining = rest;
    }
    bytes
};
