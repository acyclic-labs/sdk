use super::*;
use crate::conversation::{
    ContentResidencyVerifier, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
};
use std::{collections::BTreeSet, mem::size_of, sync::Arc};
/// Owner-consumed permissions and their authenticated content implementations.
pub struct CreatedResourceAccess {
    pub(crate) operation: OperationId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) grants: Capabilities,
    pub(crate) content: Vec<CreatedContentBinding>,
    pub(crate) references: Vec<(FileRef, Arc<dyn ContentResidencyVerifier>)>,
    pub(crate) proofs: Vec<ResourceCreationProofRef>,
    pub(crate) publication_lease: Option<std::sync::Arc<dyn CreatedTaskLease>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}
impl CreatedResourceAccess {
    pub(crate) fn current_content(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<&CreatedContentBinding>> {
        for binding in &self.content {
            charge(work, 1)?;
            if binding.default_writer {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }
    pub(crate) fn consume(
        admission: &TaskAdmissionRecord,
        receipts: &mut Vec<CreatedResourceReceipt>,
        current: Option<OperationId>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<(Self, u64)> {
        charge(work, 1)?;
        if receipts.is_empty() || receipts.len() != admission.resource_proofs.len() {
            return Err(denied(work));
        }
        let digest = admission.canonical_digest_with_work(work)?;
        let mut labels = BTreeSet::new();
        let mut content: Vec<CreatedContentBinding> = Vec::new();
        let mut references: Vec<(FileRef, Arc<dyn ContentResidencyVerifier>)> = Vec::new();
        reserve(
            &mut content,
            receipts.iter().try_fold(0usize, |n, r| {
                n.checked_add(r.content.len()).ok_or_else(overflow)
            })?,
            work,
        )?;
        reserve(
            &mut references,
            receipts.iter().try_fold(0usize, |n, r| {
                n.checked_add(r.references.len()).ok_or_else(overflow)
            })?,
            work,
        )?;
        for (proof, mut receipt) in admission.resource_proofs.iter().zip(receipts.drain(..)) {
            charge(work, 1)?;
            if receipt.resource != proof.resource
                || receipt.evidence != proof.evidence
                || receipt.origin.preparation_task != proof.preparation
                || receipt.allocation_generation.as_resource().provider() != &proof.provider
            {
                return Err(denied(work));
            }
            let retired = receipt_bytes(&receipt)?;
            for name in &receipt.operations {
                charge(work, 1)?;
                insert_label(
                    &mut labels,
                    created_resource_capability(&receipt.resource, name, work)?,
                    work,
                )?;
            }
            for (file, reader) in receipt.references.drain(..) {
                charge(work, 1 + references.len() as u64)?;
                if references.iter().any(|(prior, _)| prior == &file) {
                    return Err(denied(work));
                }
                insert_label(&mut labels, file.read_capability(work)?, work)?;
                references.push((file, reader));
            }
            for mut binding in receipt.content.drain(..) {
                charge(work, 1 + content.len() as u64)?;
                if (binding.default_writer || binding.artifact_writer)
                    && binding.binding.writer.is_none()
                {
                    return Err(denied(work));
                }
                binding.default_writer &= current == Some(receipt.origin.intended_child);
                binding.artifact_writer &= current == Some(receipt.origin.intended_child);
                #[cfg(feature = "filesystem")]
                if let Some(scratch) = &binding.scratch_creation {
                    if scratch.volume() != &binding.volume
                        || scratch.origin() != &receipt.origin
                        || binding.artifact_writer
                    {
                        return Err(denied(work));
                    }
                }
                if binding.volume.provider() != &proof.provider
                    || content.iter().any(|prior| {
                        prior.volume == binding.volume
                            || (prior.default_writer && binding.default_writer)
                            || (prior.artifact_writer && binding.artifact_writer)
                    })
                    || binding
                        .binding
                        .writer
                        .as_ref()
                        .is_some_and(|writer| writer.volume() != &binding.volume)
                    || ((binding.default_writer || binding.artifact_writer)
                        && (binding.volume.class() != VolumeClass::AgentPrivate
                            || !matches!(binding.volume.owner(), VolumeOwner::Agent(_))))
                    || (binding.default_writer && binding.artifact_writer)
                {
                    return Err(denied(work));
                }
                insert_label(
                    &mut labels,
                    binding.volume.capability(VolumeOperation::Read, work)?,
                    work,
                )?;
                if binding.binding.writer.is_some() {
                    insert_label(
                        &mut labels,
                        binding.volume.capability(VolumeOperation::Write, work)?,
                        work,
                    )?;
                }
                content.push(binding);
            }
            drop(receipt);
            release(work, retired)?;
        }
        let bytes = slots(receipts.capacity(), size_of::<CreatedResourceReceipt>())?;
        drop(std::mem::take(receipts));
        release(work, bytes)?;
        let mut proofs = Vec::new();
        reserve(&mut proofs, admission.resource_proofs.len(), work)?;
        for proof in &admission.resource_proofs {
            proofs.push(crate::core::resource_creation_proof_clone_with_custody(proof, work)?.0);
        }
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(size_of::<Self>() as u64)?;
        }
        let mut access = Self {
            operation: admission.operation_id,
            admission_digest: digest,
            grants: Capabilities(labels),
            content,
            references,
            proofs,
            publication_lease: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        };
        let owned = access.logical_owned_bytes()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work
            .as_deref_mut()
            .filter(|io| io.original_native_work().is_some())
        {
            access.native_custody = Some(io.take_native_custody(owned)?);
            return Ok((access, 0));
        }
        Ok((access, owned))
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        sum([
            size_of::<Self>() as u64,
            self.grants.logical_owned_bytes()?,
            vector_bytes(&self.content, |b| {
                sum([
                    b.volume.logical_owned_bytes()?,
                    b.authority.as_ref().map_or(0, |a| a.id.capacity() as u64),
                ])
            })?,
            vector_bytes(&self.references, |(file, _)| file.logical_owned_bytes())?,
            vector_bytes(&self.proofs, proof_bytes)?,
        ])
    }
}
