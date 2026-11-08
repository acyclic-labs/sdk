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
    let mut scratch = accounting.allocation_scope()?;
    let result = check_inner(
        objects,
        authority,
        candidate,
        limits,
        &mut scratch,
        cancellation,
    )
    .await;
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
    let Candidate {
        request,
        root: candidate,
        lease,
    } = candidate;
    let mut work = crate::async_storage::AuthorityFrame::borrow(accounting)?;
    let current = authority
        .head_payload(request.authority_id, 4096, &mut work)
        .await;
    drop(work);
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
    let metadata = crate::kernel::read_file_metadata_with_accounting(
        objects,
        record.metadata,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    Ok(metadata.named_attributes)
}
