use super::*;
use crate::kernel::allocation::KernelAccounting;

/// The same original publication decoder supplies every backend's index key.
pub(crate) fn generation_fact(
    authority: AuthorityId,
    sequence: Sequence,
    payload: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<GenerationId>, crate::FsError> {
    accounting.charge(WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    })?;
    let created = payload.starts_with(b"acyclic-fs-volume-created-v1\0");
    let published = payload.starts_with(b"acyclic-fs-publish-generation-v1\0");
    if !created && !published {
        return Ok(None);
    }
    let fact = crate::kernel::decode_owned(accounting, |observe| {
        if created {
            crate::kernel::decode_volume_created(
                payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut |step| observe(step),
            )
            .map(|created| (created.volume_id, created.initial_generation_root))
        } else {
            crate::kernel::decode_published_generation(
                payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut |step| observe(step),
            )
            .map(|published| (published.volume_id, published.generation_root))
        }
    })
    .map(|(fact, _)| fact);
    let (volume, root) = match fact {
        Ok(fact) => fact,
        Err(crate::FsError::Decode(_) | crate::FsError::VolumeCreated(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    if crate::kernel::volume_authority_id(volume) != authority
        || created != (sequence == Sequence::new(1))
    {
        return Err(crate::AuthorityStoreError::CorruptStatic(
            "generation fact identity or sequence does not match its authority",
        )
        .into());
    }
    Ok(Some(GenerationId::from_root(root)))
}
