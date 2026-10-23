//! Backend buffers are decoded and dropped before another read can allocate.
use super::*;

type Base = (Head, ObjectId, Option<VolumeConfig>);

pub(super) async fn facts<A: AsyncAuthorityStore>(
    authority: &A,
    request: PublishGenerationRequest,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<(Head, ObjectId, VolumeConfig)>, FsError> {
    let Some((head, root, config)) = base(authority, request, accounting, cancellation).await?
    else {
        return Ok(None);
    };
    let config = match config {
        Some(config) => config,
        None => creation_config(authority, request, accounting, cancellation).await?,
    };
    Ok(Some((head, root, config)))
}

fn backend_work<'a>(
    accounting: &KernelAccounting<'a>,
    cancellation: &'a CancellationToken,
) -> Result<crate::AuthorityWork<'a>, FsError> {
    match accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            Ok(crate::AuthorityWork::native(work, cancellation))
        }
        _ => Ok(crate::AuthorityWork::new(
            accounting.remaining()?,
            cancellation,
        )),
    }
}

async fn base<A: AsyncAuthorityStore>(
    authority: &A,
    request: PublishGenerationRequest,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<Base>, FsError> {
    let mut work = backend_work(accounting, cancellation)?;
    let observed = authority
        .head_payload(
            request.authority_id,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut work,
        )
        .await;
    let spent = work.spent();
    drop(work);
    merge_backend(accounting, spent)?;
    let ((head, payload), _custody) = observed?;
    if head != request.expected {
        return Ok(None);
    }
    let payload = payload.ok_or(FsError::InvalidDiff)?;
    let mut decode = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let (volume, root, config) = if head.sequence == crate::Sequence::new(1) {
        let created = crate::kernel::decode_volume_created(
            &payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut |s| decode.observe(s),
        )?;
        (
            created.volume_id,
            created.initial_generation_root,
            Some(created.config),
        )
    } else {
        let published = crate::kernel::decode_published_generation(
            &payload,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut |s| decode.observe(s),
        )?;
        (published.volume_id, published.generation_root, None)
    };
    let _decoded = decode.into_allocation();
    if volume != request.volume_id {
        return Err(FsError::InvalidDiff);
    }
    // Only these fixed-size authenticated facts escape this scope. Both the
    // physical payload and its native custody die before the next backend call.
    Ok(Some((head, root, config)))
}

async fn creation_config<A: AsyncAuthorityStore>(
    authority: &A,
    request: PublishGenerationRequest,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<VolumeConfig, FsError> {
    let mut work = backend_work(accounting, cancellation)?;
    let observed = authority
        .replay(
            request.authority_id,
            crate::Sequence::GENESIS,
            crate::ReplayLimit {
                records: 1,
                payload_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            },
            &mut work,
        )
        .await;
    let spent = work.spent();
    drop(work);
    merge_backend(accounting, spent)?;
    let (records, _custody) = observed?;
    let first = records.first().ok_or(FsError::InvalidDiff)?;
    let mut decode = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let creation = crate::kernel::decode_volume_created(
        &first.payload,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        &mut |s| decode.observe(s),
    )?;
    let _decoded = decode.into_allocation();
    if creation.volume_id != request.volume_id {
        return Err(FsError::InvalidDiff);
    }
    Ok(creation.config)
}
