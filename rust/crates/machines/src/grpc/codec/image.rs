use super::*;

pub(in super::super) fn encode_image(value: &Image) -> Result<wire::Image, ProviderError> {
    use wire::image::ImmutableReference;
    let (kind, immutable_reference) = match value {
        Image::ManagedOci(digest) => (
            wire::ImageKind::ManagedOci,
            ImmutableReference::ManagedDigest(digest.as_bytes().to_vec()),
        ),
        Image::Custom(digest) => (
            wire::ImageKind::Custom,
            ImmutableReference::CustomDigest(digest.as_bytes().to_vec()),
        ),
        Image::Checkpoint(id) => (
            wire::ImageKind::Checkpoint,
            ImmutableReference::Checkpoint(encode_checkpoint(*id)),
        ),
    };
    Ok(wire::Image {
        kind: kind as i32,
        immutable_reference: Some(immutable_reference),
    })
}
pub(in super::super) fn decode_image(value: Option<&wire::Image>) -> Result<Image, ProviderError> {
    use wire::image::ImmutableReference;
    let value = value.ok_or_else(|| ProviderError::Rejected("image is missing".into()))?;
    match (
        wire::ImageKind::try_from(value.kind)
            .map_err(|_| ProviderError::Rejected("image kind is invalid".into()))?,
        value.immutable_reference.as_ref(),
    ) {
        (wire::ImageKind::ManagedOci, Some(ImmutableReference::ManagedDigest(digest))) => {
            let digest: [u8; 32] = digest.as_slice().try_into().map_err(|_| {
                ProviderError::Rejected("managed image digest width is invalid".into())
            })?;
            Image::managed(digest)
                .map_err(|_| ProviderError::Rejected("managed image digest is invalid".into()))
        }
        (wire::ImageKind::Custom, Some(ImmutableReference::CustomDigest(digest))) => {
            let digest: [u8; 32] = digest.as_slice().try_into().map_err(|_| {
                ProviderError::Rejected("custom image digest width is invalid".into())
            })?;
            if digest == [0; 32] {
                return Err(ProviderError::Rejected(
                    "custom image digest is zero".into(),
                ));
            }
            Image::custom(digest)
                .map_err(|_| ProviderError::Rejected("custom image digest is invalid".into()))
        }
        (wire::ImageKind::Checkpoint, Some(ImmutableReference::Checkpoint(id))) => {
            decode_checkpoint(Some(id)).map(Image::Checkpoint)
        }
        _ => Err(ProviderError::Rejected("image shape is invalid".into())),
    }
}
pub(in super::super) fn encode_capability(value: Capability) -> i32 {
    (match value {
        Capability::ElasticCpu => wire::Capability::ElasticCpu,
        Capability::ElasticMemory => wire::Capability::ElasticMemory,
        Capability::LiveCheckpoint => wire::Capability::LiveCheckpoint,
        Capability::LiveFork => wire::Capability::LiveFork,
        Capability::SuspendResume => wire::Capability::SuspendResume,
        Capability::LiveMovement => wire::Capability::LiveMovement,
        Capability::DiskFork => wire::Capability::DiskFork,
    }) as i32
}
pub(in super::super) fn decode_capability(value: i32) -> Result<Capability, ProviderError> {
    match wire::Capability::try_from(value)
        .map_err(|_| ProviderError::Rejected("capability is invalid".into()))?
    {
        wire::Capability::ElasticCpu => Ok(Capability::ElasticCpu),
        wire::Capability::ElasticMemory => Ok(Capability::ElasticMemory),
        wire::Capability::LiveCheckpoint => Ok(Capability::LiveCheckpoint),
        wire::Capability::LiveFork => Ok(Capability::LiveFork),
        wire::Capability::SuspendResume => Ok(Capability::SuspendResume),
        wire::Capability::LiveMovement => Ok(Capability::LiveMovement),
        wire::Capability::DiskFork => Ok(Capability::DiskFork),
        wire::Capability::Unspecified => {
            Err(ProviderError::Rejected("capability is unspecified".into()))
        }
    }
}
pub(in super::super) fn encode_compatibility(
    value: &CompatibilityPolicy,
) -> Result<wire::CompatibilityPolicy, ProviderError> {
    let (mode, required) = match value {
        CompatibilityPolicy::BestEffort => (wire::CompatibilityMode::BestEffort, Vec::new()),
        CompatibilityPolicy::Require(required) if !required.is_empty() => (
            wire::CompatibilityMode::Require,
            required.iter().copied().map(encode_capability).collect(),
        ),
        CompatibilityPolicy::Require(_) => {
            return Err(ProviderError::Invalid(
                "required capability set cannot be empty".into(),
            ));
        }
    };
    Ok(wire::CompatibilityPolicy {
        mode: mode as i32,
        required,
    })
}
pub(in super::super) fn decode_compatibility(
    value: Option<&wire::CompatibilityPolicy>,
    capabilities: &BTreeSet<Capability>,
) -> Result<CompatibilityPolicy, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("compatibility policy is missing".into()))?;
    let required = value
        .required
        .iter()
        .map(|item| decode_capability(*item))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if required.len() != value.required.len() {
        return Err(ProviderError::Rejected(
            "capability set contains duplicates".into(),
        ));
    }
    match wire::CompatibilityMode::try_from(value.mode)
        .map_err(|_| ProviderError::Rejected("compatibility mode is invalid".into()))?
    {
        wire::CompatibilityMode::BestEffort if required.is_empty() => {
            Ok(CompatibilityPolicy::BestEffort)
        }
        wire::CompatibilityMode::Require
            if !required.is_empty() && required.is_subset(capabilities) =>
        {
            Ok(CompatibilityPolicy::Require(required))
        }
        _ => Err(ProviderError::Rejected(
            "compatibility policy is contradictory".into(),
        )),
    }
}
pub(in super::super) fn millis(value: Duration) -> Result<u64, ProviderError> {
    u64::try_from(value.as_millis())
        .map_err(|_| ProviderError::Invalid("duration exceeds protocol range".into()))
}
pub(in super::super) fn encode_suspension(
    value: SuspensionPolicy,
) -> Result<wire::SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    let policy = match value {
        SuspensionPolicy::Manual => Policy::Manual(true),
        SuspensionPolicy::AfterIdle(value) if !value.is_zero() => {
            Policy::AfterIdleMs(millis(value)?)
        }
        SuspensionPolicy::AfterIdle(_) => {
            return Err(ProviderError::Invalid(
                "idle duration must be nonzero".into(),
            ));
        }
    };
    Ok(wire::SuspensionPolicy {
        policy: Some(policy),
    })
}
pub(in super::super) fn decode_suspension(
    value: Option<&wire::SuspensionPolicy>,
) -> Result<SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    match value.and_then(|item| item.policy.as_ref()) {
        Some(Policy::Manual(true)) => Ok(SuspensionPolicy::Manual),
        Some(Policy::AfterIdleMs(value)) if *value != 0 => {
            Ok(SuspensionPolicy::AfterIdle(Duration::from_millis(*value)))
        }
        _ => Err(ProviderError::Rejected(
            "suspension policy is invalid".into(),
        )),
    }
}
pub(in super::super) fn encode_expiration(
    value: ExpirationPolicy,
) -> Result<wire::ExpirationPolicy, ProviderError> {
    let (kind, value_ms) = match value {
        ExpirationPolicy::Never => (wire::ExpirationKind::Never, 0),
        ExpirationPolicy::MaxAge(value) if !value.is_zero() => {
            (wire::ExpirationKind::MaxAge, millis(value)?)
        }
        ExpirationPolicy::AtUnixMs(value) if value != 0 => (wire::ExpirationKind::At, value),
        ExpirationPolicy::Idle(value) if !value.is_zero() => {
            (wire::ExpirationKind::Idle, millis(value)?)
        }
        _ => {
            return Err(ProviderError::Invalid(
                "expiration policy is invalid".into(),
            ));
        }
    };
    Ok(wire::ExpirationPolicy {
        kind: kind as i32,
        value_ms,
    })
}
pub(in super::super) fn decode_expiration(
    value: Option<&wire::ExpirationPolicy>,
) -> Result<ExpirationPolicy, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("expiration policy is missing".into()))?;
    match wire::ExpirationKind::try_from(value.kind)
        .map_err(|_| ProviderError::Rejected("expiration kind is invalid".into()))?
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
        _ => Err(ProviderError::Rejected(
            "expiration policy is contradictory".into(),
        )),
    }
}
