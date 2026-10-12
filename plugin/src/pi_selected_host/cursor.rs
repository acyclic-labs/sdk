//! Bounded selected-page continuation protocol.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CursorEnvelope<S = String, C = PageCursor> {
    pub(super) caller: TaskId,
    pub(super) source: TaskId,
    pub(super) root: S,
    pub(super) kind: S,
    pub(super) directory: S,
    pub(super) cursor: C,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub(super) enum PageCursor<I = PiNativeInspectionCursor, D = PiNativeDiffCursor> {
    Inspect(I),
    Diff(D),
}
pub(super) fn encode_cursor(
    input: &PiSelectedCommand,
    root: &str,
    kind: &str,
    cursor: PageCursor<&PiNativeInspectionCursor, &PiNativeDiffCursor>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let encoded = crate::pi_root_provider::guarded_pi_bytes(
        &CursorEnvelope {
            caller: input.caller,
            source: input.source,
            root,
            kind,
            directory: input.directory.as_str(),
            cursor,
        },
        (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64,
        work,
    )?;
    let length = (encoded.0.len() as u64)
        .checked_mul(2)
        .ok_or_else(|| invalid("cursor size overflow"))?;
    let custody = work.claim(
        length,
        u64::from(length != 0),
        WorkCounters {
            bytes_copied: length,
            ..WorkCounters::default()
        },
    )?;
    work.cancellation()
        .check()
        .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
    Ok((hex::encode(&encoded.0), custody))
}
pub(super) fn decode_cursor(
    value: &str,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(CursorEnvelope, acyclic_fs::NativeAllocation)> {
    if value.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES || value.len() % 2 != 0 {
        return Err(invalid("cursor exceeds bounded transport"));
    }
    work.cancellation()
        .check()
        .map_err(|_| acyclic_fs::NativeTreeMaterializationError::Cancelled)?;
    let length = value.len() as u64 / 2;
    let _custody = work.claim(
        length,
        u64::from(length != 0),
        WorkCounters {
            bytes_copied: length,
            ..WorkCounters::default()
        },
    )?;
    let bytes = hex::decode(value).map_err(|_| invalid("cursor is not hexadecimal"))?;
    crate::pi_root_provider::guarded_pi_decode(
        &bytes,
        (crate::MAXIMUM_CONTROL_MESSAGE_BYTES / 2) as u64,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        acyclic_harness::wire::ErrorCode::Invalid,
        acyclic_harness::JsonValueSchema::None,
        SELECTED_DIAGNOSTIC_LABEL_BYTES,
        work,
    )
}
