use super::*;
pub(super) fn status_owned(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => result.logical_owned_bytes(),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}
pub(super) fn copy_status(
    status: &EffectStatus,
    work: &mut Option<&mut dyn HistoryIo>,
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
pub(super) fn attestation_owned(value: &EffectAttestation) -> Result<u64> {
    crate::core::owned_sum([
        value.provider.capacity() as u64,
        status_owned(&value.status)?,
    ])
}
