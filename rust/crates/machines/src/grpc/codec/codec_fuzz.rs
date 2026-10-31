use super::*;
use prost::Message;
use std::fmt::Debug;

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

/// Admit arbitrary protobuf output with the caller's retained identity. These
/// checks do not dispatch a request, replace a provider, or wait for a producer.
pub fn check(name: &str, bytes: &[u8]) {
    let machine = MachineId::from_digest([1; 32]);
    let checkpoint = CheckpointId::from_digest([2; 32]);
    let operation = OperationId::from_digest([3; 32]);
    match name {
        "acyclic.machines.v1.MachineId" => {
            let _ = project::<wire::MachineId, _>(bytes, |value| decode_machine(Some(&value)));
        }
        "acyclic.machines.v1.CheckpointId" => {
            let _ =
                project::<wire::CheckpointId, _>(bytes, |value| decode_checkpoint(Some(&value)));
        }
        "acyclic.machines.v1.OperationId" => {
            let _ = project::<wire::OperationId, _>(bytes, |value| decode_operation(Some(&value)));
        }
        "acyclic.machines.v1.IdempotencyKey" => {
            let _ = project::<wire::IdempotencyKey, _>(bytes, |value| decode_uuid(&value.value));
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
                assert_eq!(decode_compatibility(Some(&encoded), &available), Ok(policy));
                if let CompatibilityPolicy::Require(_) = policy {
                    assert!(decode_compatibility(Some(&encoded), &BTreeSet::new()).is_err());
                }
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
            if project::<wire::MachineState, _>(bytes, |value| {
                decode_machine_observation(value, machine)
            })
            .is_some()
            {
                assert!(
                    decode_machine_observation(
                        wire::MachineState::decode(bytes).unwrap(),
                        MachineId::from_digest([9; 32])
                    )
                    .is_err()
                );
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
            if project::<wire::MachineEvent, _>(bytes, |value| decode_event(&value, machine))
                .is_some()
            {
                assert!(
                    decode_event(
                        &wire::MachineEvent::decode(bytes).unwrap(),
                        MachineId::from_digest([9; 32])
                    )
                    .is_err()
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
