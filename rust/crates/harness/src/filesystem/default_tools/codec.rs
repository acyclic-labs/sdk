//! Codec for portable recursive tools.

use super::*;

pub(super) fn command_stage(
    command: &WorkflowCommand,
    report: Option<&ForkReport>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<&'static str> {
    match command.kind.as_str() {
        TASK_ADMIT_PREPARED_COMMAND_KIND => Ok(if report.is_some() {
            "child-admit"
        } else {
            "prepare"
        }),
        FORK_OBSERVE => Ok("observe"),
        FORK_PUBLISH => Ok("fork-publish"),
        MERGE_APPLY => Ok("merge-apply"),
        MERGE_PUBLISH => Ok("merge-publish"),
        _ => Err(recursive_reject(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "recursive command identity or kind changed",
        )),
    }
}
pub(super) fn command_kind(stage: &str) -> &'static str {
    match stage {
        "prepare" | "child-admit" => TASK_ADMIT_PREPARED_COMMAND_KIND,
        "observe" => FORK_OBSERVE,
        "fork-publish" => FORK_PUBLISH,
        "merge-apply" => MERGE_APPLY,
        "merge-publish" => MERGE_PUBLISH,
        _ => unreachable!("recursive command stage is derived before dispatch"),
    }
}
pub(super) fn next_stage(stage: &str, value: &Value) -> Option<&'static str> {
    match stage {
        "prepare" => Some("observe"),
        "observe" => Some("fork-publish"),
        "fork-publish" => Some("child-admit"),
        "merge-apply" if value.get("status").and_then(Value::as_str) == Some("applied") => {
            Some("merge-publish")
        }
        _ => None,
    }
}
pub(in crate::filesystem) fn recursive_decode_slot_bytes<T>() -> u64 {
    let typed = [
        std::mem::size_of::<State>(),
        std::mem::size_of::<Plan>(),
        std::mem::size_of::<RecursiveForkToolPlan>(),
        std::mem::size_of::<RecursiveMergeToolPlan>(),
        std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>(),
        std::mem::size_of::<TaskAdmissionRecord>(),
        std::mem::size_of::<ForkRequest>(),
        std::mem::size_of::<ForkReport>(),
        std::mem::size_of::<ForkSeed>(),
        std::mem::size_of::<crate::fork::ForkSelection>(),
        std::mem::size_of::<crate::fork::CapturedResource>(),
        std::mem::size_of::<crate::fork::Capture>(),
        std::mem::size_of::<crate::fork::ResourceRevision>(),
        std::mem::size_of::<crate::fork::SummaryForkCapture>(),
        std::mem::size_of::<ProjectMergeReceipt>(),
        std::mem::size_of::<crate::merge::ProviderJoinProof>(),
        std::mem::size_of::<WorkflowCommand>(),
        std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0) as u64;
    let content =
        std::mem::size_of::<serde::__private229::de::Content<'static>>().max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    crate::filesystem::preparation_slot_bytes::<T>() + typed + 2 * 2 * content
}
macro_rules! recursive_preparation_records {
    ($($record:ty => $schema:ident),+ $(,)?) => {$(
        impl crate::filesystem::PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { recursive_decode_slot_bytes::<Self>() }
            fn json_value_schema() -> crate::contract::JsonValueSchema {
                crate::contract::JsonValueSchema::$schema
            }
        }
    )+};
}
recursive_preparation_records!(
    ForkToolArguments => None, MergeToolArguments => None,
    RecursiveForkSourceIntent => RecursiveForkSourceIntent, RecursiveForkToolPlan => RecursiveForkToolPlan,
    RecursiveMergeToolPlan => RecursiveMergeToolPlan,
    Plan => RecursivePlan, State => RecursiveState, InitialPlan => RecursiveInitialPlan,
    SourcePin => RecursiveSourcePin, Receipt => RecursiveReceipt,
    ForkSeed => ForkSeed,
    TaskId => None, ProjectMergeReceipt => ProjectMergeReceipt,
);

pub(in crate::filesystem) use crate::filesystem::interaction_host::interaction_boundary as recursive_reject;
pub(super) fn recursive_inspect(
    io: Option<&crate::filesystem::PreparationIo>,
    items: u64,
) -> Result<()> {
    if let Some(io) = io {
        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items }, false)?;
    }
    Ok(())
}
pub(super) fn recursive_byte_bound(
    value: &Value,
    maximum: u64,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return io.check_json_byte_bound(value, maximum);
    }
    let _ = io;
    crate::contract::json_byte_length(value, maximum, &mut None).map(|_| ())
}
pub(in crate::filesystem) fn decode<T: crate::filesystem::PreparationRecord>(
    value: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return Ok(crate::filesystem::retain_decoded(
            io.value_decode_preparation::<T>(value, crate::wire::ErrorCode::Invalid)?,
        ));
    }
    let _ = io;
    serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))
}
pub(super) fn recursive_decoded<T: crate::filesystem::PreparationRecord, R>(
    value: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
    operation: impl FnOnce(&T) -> Result<R>,
) -> Result<R> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let decoded = io.value_decode_preparation::<T>(value, crate::wire::ErrorCode::Invalid)?;
        return operation(&decoded.0);
    }
    let _ = io;
    operation(&decode::<T>(value, None)?)
}
pub(super) fn recursive_temporary_io(
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Option<crate::filesystem::PreparationIo>> {
    #[cfg(not(target_arch = "wasm32"))]
    return io
        .and_then(crate::filesystem::PreparationIo::original_native_work)
        .map(crate::filesystem::PreparationIo::from_native_work)
        .transpose();
    #[cfg(target_arch = "wasm32")]
    {
        let _ = io;
        Ok(None)
    }
}
pub(super) fn recursive_release_since(
    io: Option<&crate::filesystem::PreparationIo>,
    before: Option<u64>,
) -> Result<()> {
    if let (Some(io), Some(before)) = (io, before) {
        io.release(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
            recursive_reject(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "recursive temporary custody underflow",
            )
        })?)?;
    }
    Ok(())
}
pub(in crate::filesystem) fn encode<T: Serialize>(
    value: &T,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return io.value_json(value, io.remaining()?.bytes_encoded);
    }
    let _ = io;
    serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))
}
pub(super) fn recursive_encoded_equal<T: Serialize>(
    expected: &T,
    actual: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<bool> {
    let before = io.map(|io| io.owned_bytes()).transpose()?;
    let encoded = encode(expected, io)?;
    let equal = encoded == *actual;
    drop(encoded);
    recursive_release_since(io, before)?;
    Ok(equal)
}
pub(in crate::filesystem) fn recursive_box<T>(
    value: T,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Box<T>> {
    if let Some(io) = io {
        let bytes = std::mem::size_of::<T>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    Ok(Box::new(value))
}
pub(in crate::filesystem) fn recursive_command_outbox(
    command: WorkflowCommand,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<Vec<WorkflowCommand>> {
    let mut commands = Vec::new();
    crate::executor::stock_vec_push(&mut commands, command, io)?;
    Ok(commands)
}
pub(super) fn derived(
    effect: OperationId,
    label: &str,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<OperationId> {
    if let Some(io) = io {
        let work = acyclic_fs::WorkCounters {
            bytes_hashed: (b"harness/v2/recursive-tool\0".len() + 16 + label.len()) as u64,
            ..Default::default()
        };
        io.preflight(work)?;
        io.charge(work)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/recursive-tool\0");
    hash.update(&effect.into_bytes());
    hash.update(label.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}

pub(super) fn transition_identity(
    effect: OperationId,
    revision: u64,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<(OperationId, IdempotencyKey)> {
    let operation = {
        let _label;
        let label = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("transition-{revision}"), work))?;
        _label = io
            .map(|io| io.guard_owned(label.capacity() as u64))
            .transpose()?;
        derived(effect, &label, io)?
    };
    let key = crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("recursive-tool:{effect}:{revision}"), work))?;
    if let Some(io) = io {
        io.retain_owned(std::mem::size_of::<IdempotencyKey>() as u64)?;
    }
    Ok((operation, IdempotencyKey(key)))
}
pub(super) fn recursive_conflict_keys<S: serde::Serializer>(
    conflicts: &[acyclic_fs::MergeConflict],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq as _;
    struct Key<'a>(&'a acyclic_fs::MergeConflict);
    impl Serialize for Key<'_> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use acyclic_fs::kernel::NameEncoding;
            use serde::ser::SerializeSeq as _;
            match self.0 {
                acyclic_fs::MergeConflict::File(id) => {
                    let mut sequence = serializer.serialize_seq(Some(17))?;
                    sequence.serialize_element(&1_u8)?;
                    for byte in id.into_bytes() {
                        sequence.serialize_element(&byte)?;
                    }
                    sequence.end()
                }
                acyclic_fs::MergeConflict::Binding { directory_id, name } => {
                    let mut sequence =
                        serializer.serialize_seq(Some(18 + name.as_bytes().len()))?;
                    sequence.serialize_element(&2_u8)?;
                    for byte in directory_id.into_bytes() {
                        sequence.serialize_element(&byte)?;
                    }
                    sequence.serialize_element(&match name.encoding() {
                        NameEncoding::Utf8 => 1_u8,
                        NameEncoding::PosixBytes => 2,
                        NameEncoding::WindowsUtf16Le => 3,
                    })?;
                    for byte in name.as_bytes() {
                        sequence.serialize_element(byte)?;
                    }
                    sequence.end()
                }
            }
        }
    }
    let mut sequence = serializer.serialize_seq(Some(conflicts.len()))?;
    for conflict in conflicts {
        sequence.serialize_element(&Key(conflict))?;
    }
    sequence.end()
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn recursive_owned_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "recursive payload ownership overflow",
    })
}
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) fn recursive_value_owned_bytes(value: &Value) -> Result<u64> {
    recursive_owned_add(
        std::mem::size_of::<Value>() as u64,
        crate::contract::json_value_logical_owned_bytes(value)?,
    )
}
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) fn recursive_transition_owned_bytes(
    transition: &MachineTransition,
    io: &crate::filesystem::PreparationIo,
) -> Result<u64> {
    let mut bytes = recursive_owned_add(
        std::mem::size_of::<MachineTransition>() as u64,
        crate::contract::json_value_logical_owned_bytes(&transition.state)?,
    )?;
    let slots = (transition.commands.capacity() as u64)
        .checked_mul(std::mem::size_of::<WorkflowCommand>() as u64)
        .ok_or_else(|| {
            recursive_reject(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "recursive outbox ownership overflow",
            )
        })?;
    bytes = recursive_owned_add(bytes, slots)?;
    for command in &transition.commands {
        recursive_inspect(Some(io), 1)?;
        bytes = recursive_owned_add(bytes, command.kind.capacity() as u64)?;
        bytes = recursive_owned_add(bytes, command.payload.logical_owned_bytes()?)?;
    }
    let status = match &transition.status {
        MachineStatus::Suspended => 0,
        MachineStatus::Completed { value } => {
            crate::contract::json_value_logical_owned_bytes(value)?
        }
        MachineStatus::Failed { message } => message.capacity() as u64,
    };
    recursive_owned_add(bytes, status)
}
