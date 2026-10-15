use super::preparation_record::{intent, invalid};
use super::*;
use acyclic_fs::{MaterializationJournal, MaterializationPhase, WorkCounters};

/// Fixed, funded completion accepted before an SDK physical mutation.
pub(crate) struct NativePreparationCompletionTemplate {
    pub(crate) journal: MaterializationJournal,
    pub(crate) binding: (crate::TaskId, crate::scheduler::LeaseFence),
    pub(crate) path: acyclic_stream::StreamPath,
    pub(crate) key: acyclic_stream::IdempotencyKey,
    pub(crate) output: acyclic_stream::PreparedOwnedBytes<NativeAllocation>,
    pub(crate) custody: NativeAllocation,
}

impl NativePreparationCompletionTemplate {
    pub(crate) fn write_journal<W: std::io::Write>(
        journal: &MaterializationJournal,
        writer: W,
    ) -> serde_json::Result<()> {
        serde_json::to_writer(writer, &super::journal_encoding::Canonical(journal))
    }

    pub(super) fn new<P: StreamProvider>(
        mut journal: MaterializationJournal,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let io = PreparationIo::from_native_work(work)?;
        let original = intent(&journal, &io)?;
        let bytes = io.encode(
            &super::journal_encoding::Canonical(&journal),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            false,
        )?;
        let key = super::journal::key(&bytes, work)?;
        // Reserve the widest scalar encoding before the physical action.
        let revision = journal.revision;
        let counters = journal.work;
        journal.revision = revision.checked_add(1).ok_or_else(invalid)?;
        journal.work = WorkCounters::UNBOUNDED;
        let largest = io.encode(
            &super::journal_encoding::Canonical(&journal),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            false,
        )?;
        journal.revision = revision;
        journal.work = counters;
        let mut output = crate::model::vec_with_capacity::<u8>(largest.len(), Some(&io))?;
        if output.capacity() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(invalid());
        }
        work.charge(WorkCounters {
            bytes_copied: output.capacity() as u64,
            ..Default::default()
        })?;
        output.resize(output.capacity(), 0);
        let output_custody = io.take_native_custody(output.capacity() as u64)?;
        let output =
            acyclic_stream::PreparedOwnedBytes::new(output, output_custody, &work.runtime_meter())
                .map_err(|failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        Error::from(work.runtime_error(error))
                    }
                    acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                        acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into()
                    }
                })?;
        let path = super::journal::path(owner.task_binding().0, work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let retained = io.owned_bytes()?;
        let packet = Self {
            journal,
            binding: (original.task, original.original_fence),
            path,
            key,
            output,
            custody: io.into_native_custody(retained)?,
        };
        packet.verify_owner(owner, work)?;
        Ok(packet)
    }

    pub(crate) fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.custody.work().shares_allocation_context(work)
            && self
                .journal
                .custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
    }
    pub(crate) fn verify_owner<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.matches_work(work)
            || (self.binding.0, &self.binding.1) != owner.task_binding()
            || self.journal.phase != MaterializationPhase::Preparing
            || !self.journal.is_valid_for(self.journal.plan.operation_id)
        {
            return Err(invalid());
        }
        let io = PreparationIo::from_native_work(work)?;
        let original = intent(&self.journal, &io)?;
        for mapping in &original.manifest.volumes {
            owner.require_volume_grant(
                &mapping.volume,
                VolumeOperation::Read,
                &mut Some(&mut io.history_io()),
            )?;
            if mapping.writable {
                owner.require_volume_grant(
                    &mapping.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut io.history_io()),
                )?;
            }
        }
        Ok(())
    }
    pub(crate) fn set_completed_work(&mut self, counters: WorkCounters) -> Result<()> {
        self.journal.revision = self.journal.revision.checked_add(1).ok_or_else(invalid)?;
        self.journal.work = counters;
        Ok(())
    }
}
