//! One lossless spill envelope and one encoded-byte window for all tools.
use super::*;

/// A shared rendering quantum, not a capacity limit. Omitted bytes remain in
/// an immutable volume file and are read through the ordinary file reader.
pub(crate) const VIEW_BYTES: u64 = 30_000;

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SpillRef<'a> {
    path: &'a str,
    version: &'a str,
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SpillView<'a> {
    output_spill: SpillRef<'a>,
    text: &'a str,
    omitted_bytes: u64,
}

pub(crate) fn schema() -> Result<Value> {
    super::schema::ProjectionMode::Full.schema::<SpillView<'_>>()
}

pub(crate) fn is_spill(value: &Value) -> bool {
    value
        .get("value")
        .and_then(|value| value.get("output_spill"))
        .is_some()
}

pub(crate) async fn spill(
    projection: &Value,
    context: &crate::runtime::TaskContext,
    operation: OperationId,
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, crate::NativeCustody)> {
    let bytes;
    #[cfg(feature = "filesystem")]
    let _bytes_hold;
    #[cfg(feature = "filesystem")]
    let encoded = if let Some(io) = io {
        bytes = io.encode(projection, io.remaining()?.bytes_encoded, false)?;
        _bytes_hold = io.guard_bytes(&bytes)?;
        bytes.as_ref()
    } else {
        bytes = acyclic_stream::OwnedBytes::from(
            serde_json::to_vec(projection).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        _bytes_hold = None;
        bytes.as_ref()
    };
    #[cfg(not(feature = "filesystem"))]
    let encoded = {
        bytes =
            serde_json::to_vec(projection).map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.as_slice()
    };
    let (file, path) = context.spill_tool_output(operation, encoded, io).await?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let work = io.and_then(crate::executor::PreparationIo::original_native_work);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let work = None;
    let (version, _version_hold) = super::files::encode_version(&file, work)?;
    let text = std::str::from_utf8(encoded)
        .map_err(|_| Error::tool_rejected("tool projection is not UTF-8 JSON"))?;
    let empty = SpillView {
        output_spill: SpillRef {
            path: &path,
            version: &version,
        },
        text: "",
        omitted_bytes: u64::MAX,
    };
    let overhead = crate::executor::with_history_io(io, |history| {
        crate::executor::json::json_byte_length(&empty, u64::MAX, history)
    })? + super::schema::json_envelope_bytes()? as u64;
    let budget = maximum.checked_sub(overhead).ok_or_else(|| {
        Error::tool_rejected("render credit cannot hold an output file reference")
    })?;
    let shown = crate::tools::bounds::prefix(text, usize::try_from(budget).unwrap_or(usize::MAX));
    super::schema::project_json(
        &SpillView {
            output_spill: SpillRef {
                path: &path,
                version: &version,
            },
            text: shown,
            omitted_bytes: (text.len() - shown.len()) as u64,
        },
        work,
    )
}
