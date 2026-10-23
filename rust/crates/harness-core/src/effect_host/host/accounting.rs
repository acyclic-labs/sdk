use super::*;
pub(super) fn status_owned(status: &EffectStatus) -> Result<u64> {
    match status {
        EffectStatus::Succeeded { result } => crate::core::owned_sum([
            std::mem::size_of::<crate::conversation::FileRef>() as u64,
            result.logical_owned_bytes()?,
        ]),
        EffectStatus::Failed { message } => Ok(message.capacity() as u64),
        _ => Ok(0),
    }
}
pub(super) fn copy_status(
    status: &EffectStatus,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<EffectStatus> {
    match status {
        EffectStatus::Succeeded { result } => {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let result = crate::core::copy_boxed(
                result.as_ref(),
                &mut quota,
                crate::conversation::FileRef::clone_in_quota,
            )?;
            quota.transfer();
            Ok(EffectStatus::Succeeded { result })
        }
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
