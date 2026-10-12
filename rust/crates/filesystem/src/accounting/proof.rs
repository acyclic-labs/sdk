use super::*;

pub(super) fn verify_record(
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    record: &DurableCommit,
) -> Result<u64, AccountingError> {
    if record.sequence
        != cursor
            .sequence
            .checked_next()
            .map_err(|_| AccountingError::InvalidHistory)?
        || record.previous_digest != cursor.digest
        || authority_commit_digest(
            authority_id,
            record.epoch,
            record.sequence,
            record.operation_id,
            record.fingerprint,
            record.previous_digest,
            &record.payload,
        ) != record.digest
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

pub(super) async fn prove_published<O: AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    volume_id: VolumeId,
    config: VolumeConfig,
    authority_id: AuthorityId,
    record: &DurableCommit,
    measured: &mut AccountingReadWork,
    cancellation: &CancellationToken,
) -> Result<AccountingTransition, AccountingError> {
    let bytes = prove_bytes(
        objects,
        root,
        volume_id,
        config,
        authority_id,
        record,
        measured,
        cancellation,
    )
    .await?;
    Ok(AccountingTransition::Published {
        volume_id,
        generation_root: root,
        logical_regular_file_bytes: bytes,
    })
}

pub(super) async fn prove_bytes<O: AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    volume_id: VolumeId,
    config: VolumeConfig,
    authority_id: AuthorityId,
    record: &DurableCommit,
    measured: &mut AccountingReadWork,
    cancellation: &CancellationToken,
) -> Result<u128, AccountingError> {
    // Borrowed canonical fields, decoded manifest/root, exact payload, and
    // canonical re-encoding are bounded together before any archive decode.
    // Eight source lengths cover both representations and the retained source.
    let evidence_bound = config
        .limits
        .maximum_generation_bytes
        .min(measured.remaining()?.peak_allocation_bytes / 8);
    let archived = {
        let __object_args = (
            authority_id,
            record,
            evidence_bound,
            config.limits.maximum_objects_per_generation,
            measured.remaining()?,
            cancellation,
        );
        let mut __object_work = crate::AuthorityWork::new(__object_args.4, __object_args.5);
        let __object_result = objects
            .historical_generation_evidence(
                __object_args.0,
                __object_args.1,
                __object_args.2,
                __object_args.3,
                &mut __object_work,
            )
            .await;
        __object_work.receipt(__object_result)
    }
    .map_err(|failure| AccountingError::Proof(failure.error.to_string()))?;
    measured.backend(archived.work)?;
    if let Some(evidence) = archived.value {
        let original = crate::kernel::decode_historical_generation_evidence(
            &evidence,
            authority_id,
            record,
            evidence_bound,
            config.limits.maximum_objects_per_generation,
        )
        .map_err(|failure| AccountingError::Proof(failure.to_string()))?;
        if original.manifest.generation_root != root
            || original.root.volume_id != volume_id
            || original.manifest.config != config
        {
            return Err(AccountingError::InvalidHistory);
        }
        return Ok(original.logical_file_bytes);
    }
    let limits = ClosureLimits {
        decode: DecodeLimits {
            maximum_object_bytes: config.limits.maximum_object_bytes,
            maximum_name_bytes: config.limits.maximum_component_bytes,
            maximum_page_items: config.limits.maximum_directory_page_entries,
            maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes)
                .unwrap_or(u32::MAX),
            maximum_page_height: config.limits.maximum_page_height,
            maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
                .unwrap_or(u32::MAX),
        },
        maximum_objects: config.limits.maximum_objects_per_generation,
        maximum_files: config.limits.maximum_files_per_generation,
        maximum_object_bytes: config.limits.maximum_generation_bytes,
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    };
    // Without a provider archive, authenticate only the root named by this
    // historical record. Missing collected bodies fail closed; a current head
    // is never a substitute for the original generation.
    let proof =
        prove_generation_closure(objects, root, limits, measured.remaining()?, cancellation)
            .await
            .map_err(|failure| AccountingError::Proof(failure.error.to_string()))?;
    measured.backend(proof.work)?;
    if proof.root.volume_id != volume_id {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(proof.logical_file_bytes)
}
