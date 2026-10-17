use super::preparation_record::{Intent, invalid};
use super::*;
use acyclic_fs::{MaterializationJournal, MaterializationPhase, WorkCounters};

/// Fixed, funded completion accepted before an SDK physical mutation.
pub(crate) struct NativePreparationCompletionTemplate {
    pub(crate) journal: MaterializationJournal,
    pub(crate) binding: (crate::TaskId, crate::scheduler::LeaseFence),
    pub(crate) path: acyclic_stream::StreamPath,
    pub(crate) key: acyclic_stream::IdempotencyKey,
    pub(crate) output: acyclic_stream::PreparedOwnedBytes<NativeAllocation>,
    pub(crate) work_offset: usize,
    pub(crate) custody: NativeAllocation,
}

impl NativePreparationCompletionTemplate {
    #[cfg(test)]
    pub(crate) fn write_journal<W: std::io::Write>(
        journal: &MaterializationJournal,
        writer: W,
    ) -> serde_json::Result<()> {
        serde_json::to_writer(writer, &super::journal_encoding::Canonical(journal))
    }

    pub(crate) fn write_completed_work<W: std::io::Write>(
        work: &WorkCounters,
        mut writer: W,
    ) -> serde_json::Result<()> {
        serde_json::to_writer(&mut writer, &super::journal_encoding::Canonical(work))?;
        writer.write_all(b"}").map_err(serde_json::Error::io)
    }

    pub(super) fn new<P: StreamProvider>(
        mut journal: MaterializationJournal,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
        identity: (u64, [u8; 32]),
        original: &Intent,
    ) -> Result<Self> {
        let io = PreparationIo::from_native_work(work)?;
        if (original.task, &original.original_fence) != owner.task_binding() {
            return Err(invalid());
        }
        let (length, digest) = identity;
        let key = super::journal::key_from_digest(&digest, work)?;
        // Reserve the widest scalar encoding before the physical action.
        // Only the revision and fixed u64 counters change on completion.
        // Widen their decimal widths instead of re-encoding the opaque intent.
        work.charge(WorkCounters {
            items_examined: 25,
            ..Default::default()
        })?;
        let length = usize::try_from(length).map_err(|_| invalid())?;
        let largest =
            super::journal_encoding::completion_capacity(&journal, length).ok_or_else(invalid)?;
        let mut output = crate::model::vec_with_capacity::<u8>(largest, Some(&io))?;
        if output.capacity() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(invalid());
        }
        let counters = crate::executor::json::json_byte_length(
            &super::journal_encoding::Canonical(&WorkCounters::UNBOUNDED),
            largest as u64,
            &mut Some(&mut io.history_io()),
        )? as usize;
        let revision = journal.revision;
        let prior_work = journal.work;
        journal.revision = revision.checked_add(1).ok_or_else(invalid)?;
        journal.work = WorkCounters::UNBOUNDED;
        io.write_json(
            &super::journal_encoding::Canonical(&journal),
            &mut output,
            largest as u64,
        )?;
        journal.revision = revision;
        journal.work = prior_work;
        let work_offset = output.len().checked_sub(counters + 1).ok_or_else(invalid)?;
        work.charge(WorkCounters {
            bytes_copied: (output.capacity() - output.len()) as u64,
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
        let binding = (
            original.task,
            original
                .original_fence
                .clone_with_work(&mut Some(&mut io.history_io()))?,
        );
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let retained = io.owned_bytes()?;
        let packet = Self {
            journal,
            binding,
            path,
            key,
            output,
            work_offset,
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
        Ok(())
    }
    pub(crate) fn set_completed_work(&mut self, counters: WorkCounters) -> Result<()> {
        self.journal.revision = self.journal.revision.checked_add(1).ok_or_else(invalid)?;
        self.journal.work = counters;
        Ok(())
    }
}
