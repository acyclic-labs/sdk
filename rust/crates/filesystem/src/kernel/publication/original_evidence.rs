//! Original operation bytes use the existing immutable blob and publication scope.
use super::*;
use crate::kernel::allocation::retained::{Retained, RetainedArc};
use crate::kernel::closure::{ProofContext, ProofMeter};

pub(super) struct StagedEvidence {
    pub(super) objects: Vec<ObjectId>,
    _proof: Option<ProofGuard>,
    availability: Option<RetainedArc<Mutex<Option<crate::collection::Unswept>>>>,
}

impl StagedEvidence {
    fn empty() -> Self {
        Self {
            objects: Vec::new(),
            _proof: None,
            availability: None,
        }
    }

    /// The final publication scope now owns admission in the current epoch.
    pub(super) fn transferred(&self) {
        if let Some(availability) = &self.availability {
            drop(
                availability
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take(),
            );
        }
    }
}

/// Retrying an indexed operation compares original content before any new write.
/// The boolean reports a conflicting original evidence identity.
pub(super) async fn prepare<O: crate::AsyncObjectStore>(
    objects: &O,
    intent: &mut PublicationIntent,
    existing: Option<&crate::DurableCommit>,
    limits: ClosureLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(StagedEvidence, bool), crate::FsError> {
    let original = intent.original_evidence.take();
    let semantic = original
        .as_ref()
        .map(|bytes| {
            if bytes.is_empty() {
                return Err(CanonicalDecodeError::Invariant(
                    crate::kernel::codec::CanonicalInvariant::Static(
                        "empty original publication evidence",
                    ),
                )
                .into());
            }
            let hashing = WorkCounters {
                bytes_hashed: usize_to_u64(EVIDENCE_DOMAIN.len())
                    .checked_add(bytes.len() as u64)
                    .ok_or(WorkError::Overflow)?,
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(hashing)?;
            let digest = original_evidence_digest(bytes);
            accounting.record_completed(hashing)?;
            Ok::<_, crate::FsError>((bytes.len() as u64, digest))
        })
        .transpose()?;
    if let Some(existing) = existing {
        let reference = decode_publication_evidence_with_accounting(
            &existing.payload,
            existing.payload.len() as u64,
            None,
            accounting,
        )?
        .original_evidence;
        let conflicts = reference.map(|evidence| (evidence.bytes, evidence.digest)) != semantic;
        if !conflicts {
            intent.original_evidence_reference = reference;
        }
        return Ok((StagedEvidence::empty(), conflicts));
    }
    let Some(bytes) = original else {
        return Ok((StagedEvidence::empty(), false));
    };
    let availability = match objects.collection() {
        Some(collection) => Some(RetainedArc::new(
            Mutex::new(Some(collection.unswept(accounting).await?)),
            accounting,
        )?),
        None => None,
    };
    let blob = crate::kernel::build_blob_with_accounting(
        objects,
        &mut bytes.as_slice(),
        crate::kernel::BlobBuildOptions::for_limits(limits.decode, Some(bytes.len() as u64)),
        accounting,
        cancellation,
    )
    .await?;
    let (_, digest) = semantic.ok_or(crate::FsError::InvalidDiff)?;
    intent.original_evidence_reference = Some(OriginalEvidence {
        root: blob.root,
        bytes: blob.logical_bytes,
        digest,
    });
    // A queued quote retains the same admission if its caller withdraws.
    // The shared gate is released when the final scope takes over, so an old
    // quote cannot keep collection waiting after publication.
    intent.original_evidence_custody = availability
        .as_ref()
        .map(|availability| {
            Retained::<Vec<u8>>::allocate(0, accounting)?
                .into_bytes(availability.clone(), accounting)
        })
        .transpose()?;
    let (proven, guard) = {
        let mut frame = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let meter = ProofMeter::from_accounting(frame.accounting())?;
        ProofContext::new(objects, limits, meter)
            .publication_order()
            .prove_blob_closure(blob.root, blob.logical_bytes)
            .await?
    };
    Ok((
        StagedEvidence {
            objects: proven,
            _proof: Some(guard),
            availability,
        },
        false,
    ))
}

pub(crate) fn verify(
    reference: OriginalEvidence,
    bytes: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let hashing = WorkCounters {
        bytes_hashed: usize_to_u64(EVIDENCE_DOMAIN.len())
            .checked_add(bytes.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let digest = original_evidence_digest(bytes);
    accounting.record_completed(hashing)?;
    if bytes.len() as u64 != reference.bytes || digest != reference.digest {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static(
                "original publication evidence digest mismatch",
            ),
        )
        .into());
    }
    Ok(())
}
