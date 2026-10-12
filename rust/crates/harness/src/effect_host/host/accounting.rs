use super::*;

pub(super) fn status_clone_bytes(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_clone_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.len() as u64),
        _ => Ok(0),
    }
}

pub(super) fn status_owned_bytes(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}

pub(super) fn clone_status(
    status: &EffectStatus,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<EffectStatus> {
    match status {
        EffectStatus::Succeeded { result } => Ok(EffectStatus::Succeeded {
            result: result.copy(work)?,
        }),
        EffectStatus::Failed { message } => Ok(EffectStatus::Failed {
            message: match work.as_deref_mut() {
                Some(io) => io.clone_text(message)?,
                None => message.clone(),
            },
        }),
        _ => Ok(status.clone()),
    }
}

pub(super) fn attestation_owned_bytes(value: &EffectAttestation) -> Result<u64> {
    (value.provider.capacity() as u64)
        .checked_add(status_owned_bytes(&value.status)?)
        .ok_or_else(|| Error::Invalid("effect attestation ownership overflows".into()))
}

#[cfg(feature = "filesystem")]
pub(super) fn action_owned_bytes(action: &Action) -> Result<u64> {
    match action {
        Action::PlanEffect {
            provider,
            effect_kind,
            request,
            result_schema,
            ..
        } => (provider.capacity() as u64)
            .checked_add(effect_kind.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(request.logical_owned_bytes().ok()?))
            .and_then(|bytes| bytes.checked_add(result_schema.logical_owned_bytes().ok()?))
            .ok_or_else(|| Error::Invalid("effect action ownership overflows".into())),
        Action::MarkEffectDispatched { .. } => Ok(0),
        _ => Err(Error::Invalid(
            "effect host action is not an effect action".into(),
        )),
    }
}

pub(super) fn effect_key<T: std::fmt::Display>(
    prefix: &str,
    identity: T,
    phase: Option<&str>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<(IdempotencyKey, u64)> {
    use std::fmt::Write as _;
    let capacity = prefix.len() + 36 + phase.map_or(0, |phase| phase.len() + 1);
    let mut text = String::new();
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(capacity as u64)?;
        io.reserve_text(&mut text, capacity)?;
    } else {
        text.try_reserve_exact(capacity)
            .map_err(|_| Error::Invalid("effect identity allocation failed".into()))?;
    }
    write!(&mut text, "{prefix}{identity}").map_err(|_| {
        effect_error(
            work,
            crate::wire::ErrorCode::Conflict,
            "effect identity formatting failed",
        )
    })?;
    if let Some(phase) = phase {
        text.push(':');
        text.push_str(phase);
    }
    if let Some(io) = work.as_deref_mut() {
        io.copy_bytes(text.len() as u64)?;
    }
    let retained = text.capacity() as u64;
    IdempotencyKey::new(text).map(|key| (key, retained))
}
