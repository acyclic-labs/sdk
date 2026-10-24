//! Codec for portable recursive tools.

use super::*;

pub(in crate::filesystem) fn recursive_decode_slot_bytes<T>() -> u64 {
    let typed = [
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
    crate::filesystem::preparation_slot_bytes::<T>().max(typed + 2 * 5 * content) + 2 * 2 * content
}
macro_rules! recursive_preparation_records {
    ($($record:ty => $schema:ident),+ $(,)?) => {$(
        impl crate::filesystem::PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { recursive_decode_slot_bytes::<Self>() }
            fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
                crate::executor::json::JsonValueSchema::$schema
            }
        }
    )+};
}
recursive_preparation_records!(
    ForkToolArguments => None, MergeToolArguments => None,
    RecursiveForkSourceIntent => RecursiveForkSourceIntent, RecursiveForkToolPlan => RecursiveForkToolPlan,
    RecursiveMergeToolPlan => RecursiveMergeToolPlan,
    Plan => RecursivePlan,
    TaskId => None, ProjectMergeReceipt => ProjectMergeReceipt,
);

pub(in crate::filesystem) use crate::filesystem::interaction_host::interaction_boundary as recursive_reject;
pub(super) fn recursive_inspect(
    io: Option<&crate::filesystem::PreparationIo>,
    items: u64,
) -> Result<()> {
    if let Some(io) = io {
        io.codec_charge(
            crate::executor::json::JsonCodecCharge::Inspect { items },
            false,
        )?;
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
    crate::executor::json::json_byte_length(value, maximum, &mut None).map(|_| ())
}
pub(in crate::filesystem) fn decode<T: crate::filesystem::PreparationRecord>(
    value: &Value,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return crate::filesystem::retain_decoded(
            io.value_decode_preparation::<T>(value, crate::wire::ErrorCode::Invalid)?,
        );
    }
    let _ = io;
    T::deserialize(value).map_err(|error| Error::Invalid(error.to_string()))
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
    crate::filesystem::PreparationIo::release_since(io, before)?;
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
            let (tag, id, name) = match self.0 {
                acyclic_fs::MergeConflict::File(id) => (1_u8, *id, None),
                acyclic_fs::MergeConflict::Binding { directory_id, name } => {
                    (2_u8, *directory_id, Some(name))
                }
            };
            let mut sequence = serializer
                .serialize_seq(Some(17 + name.map_or(0, |name| 1 + name.as_bytes().len())))?;
            sequence.serialize_element(&tag)?;
            for byte in id.into_bytes() {
                sequence.serialize_element(&byte)?;
            }
            if let Some(name) = name {
                sequence.serialize_element(&match name.encoding() {
                    NameEncoding::Utf8 => 1_u8,
                    NameEncoding::PosixBytes => 2,
                    NameEncoding::WindowsUtf16Le => 3,
                })?;
                for byte in name.as_bytes() {
                    sequence.serialize_element(byte)?;
                }
            }
            sequence.end()
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
        crate::executor::json::json_value_logical_owned_bytes(value)?,
    )
}
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) fn recursive_transition_owned_bytes(
    transition: &MachineTransition,
    io: &crate::filesystem::PreparationIo,
) -> Result<u64> {
    let mut bytes = recursive_owned_add(
        std::mem::size_of::<MachineTransition>() as u64,
        crate::executor::json::json_value_logical_owned_bytes(&transition.state)?,
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
            crate::executor::json::json_value_logical_owned_bytes(value)?
        }
        MachineStatus::Failed { message } => message.capacity() as u64,
    };
    recursive_owned_add(bytes, status)
}

pub(super) fn recursive_decode_bytes<T: crate::filesystem::PreparationRecord>(
    bytes: &[u8],
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<T> {
    if let Some(io) = io {
        return crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?);
    }
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}
