//! Strict generated-protobuf admission for simulator requests.

use acyclic_machines::{
    self as domain, Budgets, Capability, CheckpointId, CompatibilityPolicy, CreateMachine,
    ExpirationPolicy, IdempotencyKey, Image, Performance, ProviderError, SuspensionPolicy, wire,
};
use prost::Message as _;
use std::{collections::BTreeSet, time::Duration};
use uuid::Uuid;

fn invalid(message: &str) -> ProviderError {
    ProviderError::Invalid(message.into())
}

fn identity(bytes: &[u8]) -> Result<String, ProviderError> {
    let id = Uuid::from_slice(bytes).map_err(|_| invalid("identity must be 16 bytes"))?;
    if id.is_nil() {
        return Err(invalid("identity cannot be nil"));
    }
    Ok(id.to_string())
}

fn digest(bytes: &[u8], name: &str) -> Result<[u8; 32], ProviderError> {
    let digest: [u8; 32] = bytes
        .try_into()
        .map_err(|_| invalid(&format!("{name} must be 32 bytes")))?;
    if digest == [0; 32] {
        return Err(invalid(&format!("{name} cannot be zero")));
    }
    Ok(digest)
}

pub(crate) fn capability(value: i32) -> Result<Capability, ProviderError> {
    match wire::Capability::try_from(value).map_err(|_| invalid("unknown capability"))? {
        wire::Capability::ElasticCpu => Ok(Capability::ElasticCpu),
        wire::Capability::ElasticMemory => Ok(Capability::ElasticMemory),
        wire::Capability::LiveCheckpoint => Ok(Capability::LiveCheckpoint),
        wire::Capability::LiveFork => Ok(Capability::LiveFork),
        wire::Capability::SuspendResume => Ok(Capability::SuspendResume),
        wire::Capability::LiveMovement => Ok(Capability::LiveMovement),
        wire::Capability::DiskFork => Ok(Capability::DiskFork),
        wire::Capability::Unspecified => Err(invalid("unspecified capability")),
    }
}

fn compatibility(
    value: Option<wire::CompatibilityPolicy>,
) -> Result<CompatibilityPolicy, ProviderError> {
    let value = value.ok_or_else(|| invalid("compatibility policy is missing"))?;
    let required: BTreeSet<_> = value
        .required
        .iter()
        .copied()
        .map(capability)
        .collect::<Result<_, _>>()?;
    if required.len() != value.required.len() {
        return Err(invalid("duplicate required capability"));
    }
    match wire::CompatibilityMode::try_from(value.mode)
        .map_err(|_| invalid("compatibility mode is invalid"))?
    {
        wire::CompatibilityMode::BestEffort if required.is_empty() => {
            Ok(CompatibilityPolicy::BestEffort)
        }
        wire::CompatibilityMode::Require if !required.is_empty() => {
            Ok(CompatibilityPolicy::Require(required))
        }
        _ => Err(invalid("compatibility policy is contradictory")),
    }
}

pub fn performance(value: u32) -> Result<Performance, ProviderError> {
    let value = i32::try_from(value).map_err(|_| invalid("performance enum is invalid"))?;
    match wire::Performance::try_from(value).map_err(|_| invalid("performance enum is invalid"))? {
        wire::Performance::Elastic => Ok(Performance::Elastic),
        wire::Performance::Dedicated => Ok(Performance::Dedicated),
        wire::Performance::Unspecified => Err(invalid("performance is unspecified")),
    }
}

fn suspension(value: wire::SuspensionPolicy) -> Result<SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    match value.policy {
        Some(Policy::Manual(true)) => Ok(SuspensionPolicy::Manual),
        Some(Policy::AfterIdleMs(milliseconds)) if milliseconds != 0 => Ok(
            SuspensionPolicy::AfterIdle(Duration::from_millis(milliseconds)),
        ),
        _ => Err(invalid("suspension policy is invalid")),
    }
}

#[cfg(target_arch = "wasm32")]
pub fn suspension_bytes(bytes: &[u8]) -> Result<SuspensionPolicy, ProviderError> {
    let value = wire::SuspensionPolicy::decode(bytes)
        .map_err(|_| invalid("suspension protobuf is invalid"))?;
    suspension(value)
}

fn expiration(value: wire::ExpirationPolicy) -> Result<ExpirationPolicy, ProviderError> {
    match wire::ExpirationKind::try_from(value.kind)
        .map_err(|_| invalid("expiration kind is invalid"))?
    {
        wire::ExpirationKind::Never if value.value_ms == 0 => Ok(ExpirationPolicy::Never),
        wire::ExpirationKind::MaxAge if value.value_ms != 0 => Ok(ExpirationPolicy::MaxAge(
            Duration::from_millis(value.value_ms),
        )),
        wire::ExpirationKind::At if value.value_ms != 0 => {
            Ok(ExpirationPolicy::AtUnixMs(value.value_ms))
        }
        wire::ExpirationKind::Idle if value.value_ms != 0 => Ok(ExpirationPolicy::Idle(
            Duration::from_millis(value.value_ms),
        )),
        _ => Err(invalid("expiration policy is contradictory")),
    }
}

fn image(value: wire::Image) -> Result<Image, ProviderError> {
    use wire::image::ImmutableReference;
    let kind =
        wire::ImageKind::try_from(value.kind).map_err(|_| invalid("image kind is invalid"))?;
    match (kind, value.immutable_reference) {
        (wire::ImageKind::ManagedOci, Some(ImmutableReference::ManagedDigest(bytes))) => {
            Image::managed(digest(&bytes, "managed image digest")?)
        }
        (wire::ImageKind::Custom, Some(ImmutableReference::CustomDigest(bytes))) => {
            Image::custom(digest(&bytes, "custom image digest")?)
        }
        (wire::ImageKind::Checkpoint, Some(ImmutableReference::Checkpoint(id))) => {
            CheckpointId::parse(&identity(&id.value)?)
                .map(Image::Checkpoint)
                .map_err(|_| invalid("checkpoint identity is invalid"))
        }
        _ => Err(invalid("image kind and reference disagree")),
    }
}

#[cfg(target_arch = "wasm32")]
pub fn image_bytes(bytes: &[u8]) -> Result<Image, ProviderError> {
    image(wire::Image::decode(bytes).map_err(|_| invalid("image protobuf is invalid"))?)
}

pub fn create(bytes: &[u8]) -> Result<CreateMachine, ProviderError> {
    let value = wire::CreateMachineRequest::decode(bytes)
        .map_err(|_| invalid("create protobuf is invalid"))?;
    let version = value
        .protocol
        .ok_or_else(|| invalid("protocol version is missing"))?;
    if version.major != domain::PROTOCOL_MAJOR || version.minor > domain::PROTOCOL_MINOR {
        return Err(invalid("unsupported Machines protocol version"));
    }
    let key = value
        .idempotency_key
        .ok_or_else(|| invalid("idempotency key is missing"))?;
    let idempotency_key = IdempotencyKey::parse(&identity(&key.value)?)
        .map_err(|_| invalid("idempotency key is invalid"))?;
    let budgets = value
        .budgets
        .ok_or_else(|| invalid("budgets are missing"))?;
    Ok(CreateMachine {
        idempotency_key,
        image: image(value.image.ok_or_else(|| invalid("image is missing"))?)?,
        compatibility: compatibility(value.compatibility)?,
        performance: performance(
            u32::try_from(value.performance).map_err(|_| invalid("performance is invalid"))?,
        )?,
        suspension: suspension(
            value
                .suspension
                .ok_or_else(|| invalid("suspension policy is missing"))?,
        )?,
        expiration: expiration(
            value
                .expiration
                .ok_or_else(|| invalid("expiration policy is missing"))?,
        )?,
        network_policy_digest: digest(&value.network_policy_digest, "network policy digest")?,
        budgets: Budgets {
            spend_micros: budgets.spend_micros,
            concurrency: budgets.concurrency,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted() -> wire::CreateMachineRequest {
        wire::CreateMachineRequest {
            protocol: Some(wire::ProtocolVersion {
                major: domain::PROTOCOL_MAJOR,
                minor: domain::PROTOCOL_MINOR,
            }),
            idempotency_key: Some(wire::IdempotencyKey {
                value: [1; 16].to_vec(),
            }),
            image: Some(wire::Image {
                kind: wire::ImageKind::ManagedOci as i32,
                immutable_reference: Some(wire::image::ImmutableReference::ManagedDigest(
                    [2; 32].to_vec(),
                )),
            }),
            compatibility: Some(wire::CompatibilityPolicy {
                mode: wire::CompatibilityMode::BestEffort as i32,
                required: Vec::new(),
            }),
            performance: wire::Performance::Elastic as i32,
            suspension: Some(wire::SuspensionPolicy {
                policy: Some(wire::suspension_policy::Policy::AfterIdleMs(15_000)),
            }),
            expiration: Some(wire::ExpirationPolicy {
                kind: wire::ExpirationKind::Never as i32,
                value_ms: 0,
            }),
            network_policy_digest: [3; 32].to_vec(),
            budgets: Some(wire::Budgets {
                spend_micros: u64::MAX,
                concurrency: 2,
            }),
        }
    }

    #[test]
    fn generated_request_preserves_exact_budget_and_rejects_contradictions()
    -> Result<(), ProviderError> {
        let request = admitted();
        let decoded = create(&request.encode_to_vec())?;
        assert_eq!(decoded.budgets.spend_micros, u64::MAX);
        assert_eq!(decoded.image, Image::managed([2; 32])?);

        let mut mismatch = request.clone();
        mismatch
            .image
            .as_mut()
            .ok_or_else(|| invalid("fixture image missing"))?
            .kind = wire::ImageKind::Custom as i32;
        assert!(create(&mismatch.encode_to_vec()).is_err());

        let mut nil = request.clone();
        nil.idempotency_key
            .as_mut()
            .ok_or_else(|| invalid("fixture key missing"))?
            .value = [0; 16].to_vec();
        assert!(create(&nil.encode_to_vec()).is_err());

        let mut duplicate = request;
        duplicate.compatibility = Some(wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::Require as i32,
            required: vec![wire::Capability::LiveFork as i32; 2],
        });
        assert!(create(&duplicate.encode_to_vec()).is_err());

        let mut zero_policy = admitted();
        zero_policy.network_policy_digest = [0; 32].to_vec();
        assert!(create(&zero_policy.encode_to_vec()).is_err());
        Ok(())
    }
}
