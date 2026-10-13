//! Seals authenticated creator results into the SDK's existing access data.
use super::*;
use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner};
use crate::resource_preparation::{CreatedResourceAccess, CreatedResourceReceipt};

pub(super) fn consume_receipts(
    admission: &TaskAdmissionRecord,
    receipts: &mut Vec<CreatedResourceReceipt>,
    current: Option<OperationId>,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<CreatedResourceAccess> {
    if receipts.is_empty() || receipts.len() != admission.resource_proofs.len() {
        return Err(Error::Unauthorized("created proof extent differs".into()));
    }
    let digest = crate::executor::json::canonical_json_digest(
        admission,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    let mut quota = crate::wire_codec::CodecQuota::new(
        work,
        std::mem::size_of::<CreatedResourceAccess>() as u64,
    )?;
    let mut grants = BTreeSet::new();
    let mut content = crate::core::reserve_copy_vector(
        receipts.iter().map(|r| r.content.len()).sum(),
        &mut quota,
    )?;
    let mut references = crate::core::reserve_copy_vector(
        receipts.iter().map(|r| r.references.len()).sum(),
        &mut quota,
    )?;
    for (proof, mut receipt) in admission.resource_proofs.iter().zip(receipts.drain(..)) {
        crate::core::inspect_metadata(&(proof, &receipt.origin), quota.work())?;
        if receipt.resource != proof.resource
            || receipt.evidence != proof.evidence
            || receipt.origin.preparation_task != proof.preparation
            || receipt.allocation_generation.as_resource().provider() != &proof.provider
        {
            return Err(Error::Unauthorized(
                "creation receipt differs from admitted proof".into(),
            ));
        }
        for operation in &receipt.operations {
            let label = crate::resource_preparation::created_resource_capability(
                &receipt.resource,
                operation,
                quota.work(),
            )?;
            quota.adopt(label.capacity() as u64)?;
            if grants.contains(&label) {
                return Err(Error::Unauthorized("duplicate creation operation".into()));
            }
            insert_grant(&mut grants, label, &mut quota)?;
        }
        for (file, reader) in receipt.references.drain(..) {
            if references.iter().any(|(prior, _)| prior == &file) {
                return Err(Error::Unauthorized("duplicate created file".into()));
            }
            let label = file.read_capability(quota.work())?;
            quota.adopt(label.capacity() as u64)?;
            insert_grant(&mut grants, label, &mut quota)?;
            references.push((file, reader));
        }
        for mut binding in receipt.content.drain(..) {
            binding.default_writer &= current == Some(receipt.origin.intended_child);
            binding.artifact_writer &= current == Some(receipt.origin.intended_child);
            if binding.volume.provider() != &proof.provider
                || content.iter().any(
                    |prior: &crate::resource_preparation::CreatedContentBinding| {
                        prior.volume == binding.volume
                            || prior.default_writer && binding.default_writer
                            || prior.artifact_writer && binding.artifact_writer
                    },
                )
                || binding
                    .binding
                    .writer
                    .as_ref()
                    .is_some_and(|writer| writer.volume() != &binding.volume)
                || (binding.default_writer || binding.artifact_writer)
                    && (binding.binding.writer.is_none()
                        || binding.volume.class() != VolumeClass::AgentPrivate
                        || !matches!(binding.volume.owner(), VolumeOwner::Agent(_)))
                || binding.default_writer && binding.artifact_writer
            {
                return Err(Error::Unauthorized(
                    "created content authority differs".into(),
                ));
            }
            if let Some(scratch) = &binding.scratch_creation
                && (scratch.volume() != &binding.volume
                    || scratch.origin() != &receipt.origin
                    || binding.artifact_writer)
            {
                return Err(Error::Unauthorized("scratch provenance differs".into()));
            }
            for operation in [
                Some(VolumeOperation::Read),
                binding
                    .binding
                    .writer
                    .as_ref()
                    .map(|_| VolumeOperation::Write),
            ]
            .into_iter()
            .flatten()
            {
                let label = binding.volume.capability(operation, quota.work())?;
                quota.adopt(label.capacity() as u64)?;
                insert_grant(&mut grants, label, &mut quota)?;
            }
            content.push(binding);
        }
    }
    let proofs = RuntimeScope::copy_resource_proofs(&admission.resource_proofs, &mut quota)?;
    let access = CreatedResourceAccess {
        operation: admission.operation_id,
        admission_digest: digest,
        grants: crate::Capabilities(grants),
        content,
        references,
        proofs,
        publication_lease: None,
        #[cfg(not(target_arch = "wasm32"))]
        native_custody: None,
    };
    quota.transfer();
    Ok(access)
}

fn insert_grant(
    grants: &mut BTreeSet<String>,
    label: String,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<()> {
    if grants.contains(&label) {
        let held = label.capacity() as u64;
        drop(label);
        return quota.relinquish(held);
    }
    quota.replace(
        quota
            .retained()
            .checked_add(std::mem::size_of::<String>() as u64)
            .ok_or_else(|| Error::Invalid("created grant ownership overflows".into()))?,
    )?;
    grants.insert(label);
    Ok(())
}
