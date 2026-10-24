use super::*;
use crate::kernel::{DecodeLimits, GenerationRoot, MetadataField};

pub(super) struct Candidate<'a> {
    pub request: PublishGenerationRequest,
    pub root: &'a GenerationRoot,
    pub lease: &'a crate::access::projection::Lease,
}

pub(super) async fn check<O: crate::AsyncObjectStore, A: crate::AsyncAuthorityStore>(
    objects: &O,
    authority: &A,
    candidate: Candidate<'_>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), crate::FsError> {
    // Every temporary below is retired before reconciling the standalone ledger.
    // Native reads retain the original request's RAII allocation custody.
    let before = accounting
        .standalone_parts()
        .map(|(a, _, _, _)| a.live_bytes());
    let result = check_inner(
        objects,
        authority,
        candidate,
        limits,
        accounting,
        cancellation,
    )
    .await;
    if let Some(before) = before {
        let (allocations, _, _, _) = accounting.standalone_parts().ok_or(WorkError::Overflow)?;
        let retired = allocations
            .live_bytes()
            .checked_sub(before)
            .ok_or(WorkError::Overflow)?;
        allocations
            .release(retired)
            .map_err(|_| WorkError::Overflow)?;
    }
    result.map_err(|error| match error {
        crate::FsError::Publication(_) => error,
        error => PublicationError::ProjectionValidation(Box::new(error)).into(),
    })
}

async fn check_inner<O: crate::AsyncObjectStore, A: crate::AsyncAuthorityStore>(
    objects: &O,
    authority: &A,
    candidate: Candidate<'_>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), crate::FsError> {
    let Candidate { request, root: candidate, lease } = candidate;
    let mut work = match &*accounting {
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => {
            crate::AuthorityWork::native(work, cancellation)
        }
        _ => crate::AuthorityWork::new(accounting.remaining()?, cancellation),
    };
    let current = authority
        .head_payload(request.authority_id, 4096, &mut work)
        .await;
    let spent = work.spent();
    drop(work);
    if accounting.standalone_parts().is_some() {
        accept_backend(accounting, spent)?;
    }
    let ((head, payload), _custody) = current?;
    // Only this exact expected head can win the CAS. A stale candidate cannot
    // change policy, even if another publisher wins while we inspect objects.
    if head != request.expected {
        return Ok(());
    }
    let payload = payload.ok_or(PublicationError::ActiveProjection)?;
    let mut decode = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let (volume, root) = if head.sequence == crate::Sequence::new(1) {
        let created =
            crate::kernel::decode_volume_created(&payload, 4096, &mut |s| decode.observe(s))?;
        (created.volume_id, created.initial_generation_root)
    } else {
        let published = decode_published_generation(&payload, 4096, &mut |s| decode.observe(s))?;
        (published.volume_id, published.generation_root)
    };
    let _decoded = decode.into_allocation();
    if volume != request.volume_id {
        return Err(PublicationError::VolumeMismatch {
            expected: request.volume_id,
            actual: volume,
        }
        .into());
    }
    let (current, _root_custody) =
        crate::kernel::generation::read_generation_root_with_limits_and_accounting(
            objects,
            root,
            limits,
            accounting,
            cancellation,
        )
        .await?;
    let old = revision(objects, &current, limits, accounting, cancellation).await?;
    let new = revision(objects, candidate, limits, accounting, cancellation).await?;
    lease.policy_changed(old != new)
}

async fn revision<O: crate::AsyncObjectStore>(
    objects: &O,
    root: &GenerationRoot,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<MetadataField<ObjectId>, crate::FsError> {
    let (record, _record_custody) = crate::kernel::lookup_file_record_async_with_accounting(
        objects,
        root.file_table,
        root.root_file_id,
        limits,
        accounting,
        cancellation,
        None,
    )
    .await?;
    let record = record.ok_or(ClosureError::MissingRootFile)?;
    let (read, _retained) = crate::kernel::file_read::read_object_with_accounting(
        objects,
        record.metadata,
        limits.maximum_object_bytes,
        accounting,
        cancellation,
    )
    .await?;
    let (metadata, _custody) = crate::kernel::decode_owned(accounting, |observe| {
        crate::kernel::decode_file_metadata(&read.bytes, limits, &mut |s| observe(s))
    })?;
    Ok(metadata.named_attributes)
}
