use super::*;

pub(super) fn verify_record(
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    record: &DurableCommit,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<u64, AccountingError> {
    if record.sequence
        != cursor
            .sequence
            .checked_next()
            .map_err(|_| AccountingError::InvalidHistory)?
        || record.previous_digest != cursor.digest
        || record_digest(authority_id, record, work)? != record.digest
    {
        return Err(AccountingError::InvalidHistory);
    }
    let stamp = record
        .settled_at_micros
        .filter(|stamp| *stamp > 0)
        .ok_or(AccountingError::MissingTime)?;
    if stamp < cursor.settled_at_micros {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(stamp)
}

pub(super) fn record_digest(
    authority_id: AuthorityId,
    record: &DurableCommit,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<Digest, AccountingError> {
    let cost = WorkCounters {
        bytes_hashed: crate::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
            .checked_add(record.payload.len() as u64)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    work.admit(cost)?;
    let digest = authority_commit_digest(
        authority_id,
        record.epoch,
        record.sequence,
        record.operation_id,
        record.fingerprint,
        record.previous_digest,
        &record.payload,
    );
    work.charge(cost)?;
    Ok(digest)
}

pub(super) async fn prove_bytes<O: AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    volume: (VolumeId, VolumeConfig),
    authority_id: AuthorityId,
    record: &DurableCommit,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<u128, AccountingError> {
    let (volume_id, config) = volume;
    // Bound the retained source and both canonical representations before decode.
    let evidence_bound = work.remaining()?.peak_allocation_bytes / 8;
    let evidence_objects = evidence_bound / ObjectId::ENCODED_LEN as u64;
    let archived = objects
        .historical_generation_evidence(
            authority_id,
            record,
            evidence_bound,
            evidence_objects,
            work,
        )
        .await?;
    if let Some(evidence) = archived {
        let mut accounting = work.accounting();
        let mut observed = crate::kernel::OriginalReadAccounting::new(&mut accounting)?;
        let decoded = crate::kernel::decode_historical_generation_evidence_observed(
            &evidence,
            authority_id,
            record,
            evidence_bound,
            evidence_objects,
            &mut |step| observed.observe(step).map_err(AccountingError::from),
        );
        let result = decoded.and_then(|original| {
            if original.manifest.generation_root != root
                || original.root.volume_id != volume_id
                || original.manifest.config != config
            {
                Err(AccountingError::InvalidHistory)
            } else {
                Ok(original.logical_file_bytes)
            }
        });
        // The projection and every partially decoded value have been dropped.
        // Retire only their original backing, including on a typed refusal.
        observed.retire()?;
        return result;
    }
    let limits = ClosureLimits {
        decode: DecodeLimits::for_volume(config),
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    };
    // Without a provider archive, authenticate only the root named by this
    // historical record. Missing collected bodies fail closed; a current head
    // is never a substitute for the original generation.
    let mut accounting = work.accounting();
    let mut frame = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
    let meter = crate::kernel::closure::ProofMeter::from_accounting(frame.accounting())?;
    let (proof, guard) = crate::kernel::closure::ProofContext::new(objects, limits, meter)
        .prove(root)
        .await?;
    let result = if proof.root.volume_id != volume_id {
        Err(AccountingError::InvalidHistory)
    } else {
        Ok(proof.logical_file_bytes)
    };
    drop(proof);
    drop(guard);
    result
}
