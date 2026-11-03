use super::*;
use prost::Message;
use std::fmt::Debug;

mod guest;

fn capabilities() -> BTreeSet<Capability> {
    BTreeSet::from([
        Capability::ElasticCpu,
        Capability::ElasticMemory,
        Capability::LiveCheckpoint,
        Capability::LiveFork,
        Capability::SuspendResume,
        Capability::LiveMovement,
        Capability::DiskFork,
    ])
}

fn project<W, T>(bytes: &[u8], decode: impl Fn(W) -> Result<T, ProviderError>) -> Option<T>
where
    W: Message + Default,
    T: Debug + PartialEq,
{
    let wire = W::decode(bytes).ok()?;
    let canonical = wire.encode_to_vec();
    let result = decode(wire);
    assert_eq!(
        decode(W::decode(canonical.as_slice()).expect("writer output decodes")),
        result,
        "protobuf spelling cannot change semantic admission",
    );
    result.ok()
}

fn identity<T: Copy>(
    bytes: &[u8],
    actual: Result<T, ProviderError>,
    original: impl Fn(T) -> [u8; 16],
) -> Result<T, ProviderError> {
    let expected = <[u8; 16]>::try_from(bytes)
        .ok()
        .filter(|raw| raw.iter().any(|byte| *byte != 0));
    assert_eq!(
        actual.as_ref().ok().copied().map(original),
        expected,
        "wire identity admission must retain exactly sixteen non-nil original bytes"
    );
    if let Err(error) = &actual {
        assert!(matches!(error, ProviderError::Rejected(_)));
    }
    actual
}

/// Feed the generated protobuf writers with original public identity bytes and
/// their malformed descendants, using the same oracle as the protobuf targets.
pub fn check_identities(bytes: &[u8]) {
    macro_rules! check {
        ($($name:ident),+ $(,)?) => { $(
            let value = wire::$name { value: bytes.to_vec() };
            check(concat!("acyclic.machines.v1.", stringify!($name)), &value.encode_to_vec());
        )+ };
    }
    check!(MachineId, CheckpointId, OperationId, IdempotencyKey);
}

fn machine_page(
    page: wire::MachinePage,
    after: Option<MachineId>,
    limit: u32,
) -> Result<MachinePage, ProviderError> {
    let actual = decode_machine_page(page.clone(), after, limit);
    let machines = page
        .machines
        .into_iter()
        .map(MachineObservation::try_from)
        .collect::<Result<Vec<_>, _>>();
    let next = page
        .next
        .as_ref()
        .map(|id| decode_machine(Some(id)))
        .transpose();
    let expected = machines.ok().zip(next.ok()).and_then(|(machines, next)| {
        let ordered = machines
            .iter()
            .zip(machines.iter().skip(1))
            .all(|(left, right)| left.id < right.id);
        let follows =
            after.is_none_or(|cursor| machines.first().is_none_or(|first| first.id > cursor));
        let bounded = u32::try_from(machines.len()).is_ok_and(|count| count <= limit);
        let retained = next.is_none() || next == machines.last().map(|machine| machine.id);
        (ordered && follows && bounded && retained).then_some(MachinePage { machines, next })
    });
    assert_eq!(
        actual.as_ref().ok(),
        expected.as_ref(),
        "machine page admission retains original order, credit and cursor"
    );
    if let Err(error) = &actual {
        assert!(matches!(error, ProviderError::Rejected(_)));
    }
    actual
}

/// Admit arbitrary protobuf output with the caller's retained identity. These
/// checks do not dispatch a request, replace a provider, or wait for a producer.
pub fn check(name: &str, bytes: &[u8]) {
    guest::check(name, bytes);
    let machine = MachineId::from_digest([1; 32]);
    let checkpoint = CheckpointId::from_digest([2; 32]);
    let operation = OperationId::from_digest([3; 32]);
    match name {
        "acyclic.machines.v1.MachineId" => {
            let _ = project::<wire::MachineId, _>(bytes, |value| {
                identity(
                    &value.value,
                    decode_machine(Some(&value)),
                    MachineId::as_bytes,
                )
            });
        }
        "acyclic.machines.v1.CheckpointId" => {
            let _ = project::<wire::CheckpointId, _>(bytes, |value| {
                identity(
                    &value.value,
                    decode_checkpoint(Some(&value)),
                    CheckpointId::as_bytes,
                )
            });
        }
        "acyclic.machines.v1.OperationId" => {
            let _ = project::<wire::OperationId, _>(bytes, |value| {
                identity(
                    &value.value,
                    decode_operation(Some(&value)),
                    OperationId::as_bytes,
                )
            });
        }
        "acyclic.machines.v1.IdempotencyKey" => {
            let _ = project::<wire::IdempotencyKey, _>(bytes, |value| {
                identity(&value.value, decode_uuid(&value.value), |id| *id.as_bytes())
            });
        }
        "acyclic.machines.v1.Image" => {
            if let Some(image) =
                project::<wire::Image, _>(bytes, |value| decode_image(Some(&value)))
            {
                let encoded = encode_image(&image).expect("admitted image is representable");
                assert_eq!(decode_image(Some(&encoded)), Ok(image));
            }
        }
        "acyclic.machines.v1.CompatibilityPolicy" => {
            let available = capabilities();
            if let Some(policy) = project::<wire::CompatibilityPolicy, _>(bytes, |value| {
                decode_compatibility(Some(&value), &available)
            }) {
                let encoded =
                    encode_compatibility(&policy).expect("admitted policy is representable");
                if let CompatibilityPolicy::Require(_) = policy {
                    assert!(decode_compatibility(Some(&encoded), &BTreeSet::new()).is_err());
                }
                assert_eq!(decode_compatibility(Some(&encoded), &available), Ok(policy));
            }
        }
        "acyclic.machines.v1.SuspensionPolicy" => {
            if let Some(policy) =
                project::<wire::SuspensionPolicy, _>(bytes, |value| decode_suspension(Some(&value)))
            {
                let encoded =
                    encode_suspension(policy).expect("admitted duration is representable");
                assert_eq!(decode_suspension(Some(&encoded)), Ok(policy));
            }
        }
        "acyclic.machines.v1.ExpirationPolicy" => {
            if let Some(policy) =
                project::<wire::ExpirationPolicy, _>(bytes, |value| decode_expiration(Some(&value)))
            {
                let encoded =
                    encode_expiration(policy).expect("admitted duration is representable");
                assert_eq!(decode_expiration(Some(&encoded)), Ok(policy));
            }
        }
        "acyclic.machines.v1.ImageQualification" => {
            if let Some(qualification) = project::<wire::ImageQualification, _>(bytes, |value| {
                decode_qualification(value, None)
            }) {
                let wire = wire::ImageQualification::decode(bytes).unwrap();
                assert_eq!(
                    decode_qualification(wire.clone(), Some(&qualification.image)),
                    Ok(qualification.clone())
                );
                let foreign = if qualification.image == Image::custom([4; 32]).unwrap() {
                    Image::custom([5; 32]).unwrap()
                } else {
                    Image::custom([4; 32]).unwrap()
                };
                assert!(decode_qualification(wire, Some(&foreign)).is_err());
            }
        }
        "acyclic.machines.v1.MachineContract" => {
            let _ =
                project::<wire::MachineContract, _>(bytes, |value| decode_contract(Some(&value)));
        }
        "acyclic.machines.v1.MachineState" => {
            if let Some(original) =
                project::<wire::MachineState, _>(bytes, MachineObservation::try_from)
            {
                let foreign = if original.id == MachineId::from_digest([9; 32]) {
                    machine
                } else {
                    MachineId::from_digest([9; 32])
                };
                assert!(
                    decode_machine_observation(wire::MachineState::decode(bytes).unwrap(), foreign)
                        .is_err()
                );
            }
        }
        "acyclic.machines.v1.MachinePage" => {
            for limit in [1, MAX_PAGE_SIZE] {
                if let Some(original) =
                    project::<wire::MachinePage, _>(bytes, |page| machine_page(page, None, limit))
                {
                    if let Some(first) = original.machines.first() {
                        let _ = machine_page(
                            wire::MachinePage::decode(bytes).unwrap(),
                            Some(first.id),
                            limit,
                        );
                    }
                }
            }
        }
        "acyclic.machines.v1.CheckpointState" => {
            if project::<wire::CheckpointState, _>(bytes, |value| {
                decode_checkpoint_observation(&value, checkpoint)
            })
            .is_some()
            {
                assert!(
                    decode_checkpoint_observation(
                        &wire::CheckpointState::decode(bytes).unwrap(),
                        CheckpointId::from_digest([9; 32])
                    )
                    .is_err()
                );
            }
        }
        "acyclic.machines.v1.MachineEvent" => {
            if let Some(original) = project::<wire::MachineEvent, _>(bytes, |value| {
                decode_event(&value, decode_machine(value.machine.as_ref())?)
            }) {
                let foreign = if original.machine == MachineId::from_digest([9; 32]) {
                    MachineId::from_digest([8; 32])
                } else {
                    MachineId::from_digest([9; 32])
                };
                assert!(
                    decode_event(&wire::MachineEvent::decode(bytes).unwrap(), foreign).is_err(),
                    "original machine event must refuse a substituted caller identity"
                );
            }
        }
        "acyclic.machines.v1.OperationState" => {
            if project::<wire::OperationState, _>(bytes, |value| {
                decode_operation_observation(&value, operation)
            })
            .is_some()
            {
                assert!(
                    decode_operation_observation(
                        &wire::OperationState::decode(bytes).unwrap(),
                        OperationId::from_digest([9; 32])
                    )
                    .is_err()
                );
            }
        }
        "acyclic.machines.v1.UsageReceipt" => {
            if project::<wire::UsageReceipt, _>(bytes, |value| {
                decode_usage_receipt(value, machine, 1, 2)
            })
            .is_some()
            {
                let wire = wire::UsageReceipt::decode(bytes).unwrap();
                assert!(
                    decode_usage_receipt(wire.clone(), MachineId::from_digest([9; 32]), 1, 2)
                        .is_err()
                );
                assert!(decode_usage_receipt(wire.clone(), machine, 0, 2).is_err());
                assert!(decode_usage_receipt(wire, machine, 1, 3).is_err());
            }
        }
        _ => {}
    }
}

/// The public observation writer and reader must retain the original producer's
/// result before its wire bytes become a corpus input.
pub fn capture_machine(original: &MachineObservation) -> Result<Vec<u8>, ProviderError> {
    let value = wire::MachineState::try_from(original.clone())?;
    assert_eq!(
        MachineObservation::try_from(value.clone()),
        Ok(original.clone()),
        "original machine response must retain its producer observation"
    );
    let bytes = value.encode_to_vec();
    check("acyclic.machines.v1.MachineState", &bytes);
    Ok(bytes)
}

/// Retain the actual event producer's identity, cursor, time and fact through the
/// shared serving conversion before admitting its wire bytes to either target.
pub fn capture_event(original: &MachineEvent) -> Vec<u8> {
    let value = wire::MachineEvent::from(original.clone());
    assert_eq!(
        decode_event(&value, original.machine),
        Ok(original.clone()),
        "original machine event must retain its producer observation"
    );
    let bytes = value.encode_to_vec();
    for end in 0..=bytes.len() {
        check("acyclic.machines.v1.MachineEvent", &bytes[..end]);
    }
    bytes
}

/// Capture an actual provider's list page through the shared serving writer and
/// require its original cursor and caller credit to survive the client decoder.
pub fn capture_machine_page(
    original: &MachinePage,
    after: Option<MachineId>,
    limit: u32,
) -> Result<Vec<u8>, ProviderError> {
    let value = wire::MachinePage::try_from(original.clone())?;
    assert_eq!(
        machine_page(value.clone(), after, limit)?,
        *original,
        "original machine page must retain its producer observations and cursor"
    );
    let bytes = value.encode_to_vec();
    check("acyclic.machines.v1.MachinePage", &bytes);
    Ok(bytes)
}

/// Capture the existing request-side codec writers. Service response producers
/// are a separate provenance obligation, not reconstructed by a reverse codec.
pub fn seeds() -> Result<Vec<(&'static str, &'static str, Vec<u8>)>, ProviderError> {
    let mut seeds = Vec::new();
    macro_rules! seed {
        ($message:literal, $variant:literal, $wire:expr) => {
            seeds.push((
                concat!("acyclic.machines.v1.", $message),
                $variant,
                $wire.encode_to_vec(),
            ));
        };
    }
    seed!(
        "MachineId",
        "canonical",
        encode_machine(MachineId::from_digest([1; 32]))
    );
    seed!(
        "CheckpointId",
        "canonical",
        encode_checkpoint(CheckpointId::from_digest([2; 32]))
    );
    seed!(
        "IdempotencyKey",
        "canonical",
        encode_key(IdempotencyKey::from_digest([3; 32]))
    );
    let request = operation_request(OperationId::from_digest([3; 32]));
    seed!(
        "OperationId",
        "canonical",
        request.operation.as_ref().unwrap()
    );
    seed!("OperationRequest", "canonical", request);
    for (name, image) in [
        (
            "managed",
            Image::managed([6; 32]).map_err(|error| ProviderError::Invalid(error.to_string()))?,
        ),
        (
            "custom",
            Image::custom([7; 32]).map_err(|error| ProviderError::Invalid(error.to_string()))?,
        ),
        (
            "checkpoint",
            Image::Checkpoint(CheckpointId::from_digest([2; 32])),
        ),
    ] {
        let wire = encode_image(&image)?;
        assert_eq!(decode_image(Some(&wire)), Ok(image));
        seeds.push(("acyclic.machines.v1.Image", name, wire.encode_to_vec()));
    }
    for policy in [
        CompatibilityPolicy::BestEffort,
        CompatibilityPolicy::Require(capabilities()),
    ] {
        let wire = encode_compatibility(&policy)?;
        assert_eq!(
            decode_compatibility(Some(&wire), &capabilities()),
            Ok(policy)
        );
        seeds.push((
            "acyclic.machines.v1.CompatibilityPolicy",
            if wire.required.is_empty() {
                "best-effort"
            } else {
                "require"
            },
            wire.encode_to_vec(),
        ));
    }
    for (name, policy) in [
        ("manual", SuspensionPolicy::Manual),
        (
            "idle",
            SuspensionPolicy::AfterIdle(Duration::from_millis(u64::MAX)),
        ),
    ] {
        let wire = encode_suspension(policy)?;
        assert_eq!(decode_suspension(Some(&wire)), Ok(policy));
        seeds.push((
            "acyclic.machines.v1.SuspensionPolicy",
            name,
            wire.encode_to_vec(),
        ));
    }
    for (name, policy) in [
        ("never", ExpirationPolicy::Never),
        (
            "age",
            ExpirationPolicy::MaxAge(Duration::from_millis(u64::MAX)),
        ),
        ("at", ExpirationPolicy::AtUnixMs(u64::MAX)),
        (
            "idle",
            ExpirationPolicy::Idle(Duration::from_millis(u64::MAX)),
        ),
    ] {
        let wire = encode_expiration(policy)?;
        assert_eq!(decode_expiration(Some(&wire)), Ok(policy));
        seeds.push((
            "acyclic.machines.v1.ExpirationPolicy",
            name,
            wire.encode_to_vec(),
        ));
    }
    Ok(seeds)
}
