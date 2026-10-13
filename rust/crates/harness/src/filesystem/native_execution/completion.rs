use super::preparation_record::{intent, invalid};
use super::*;
use acyclic_fs::{MaterializationJournal, MaterializationPhase, WorkCounters};

/// Fixed, funded completion accepted before an SDK physical mutation.
pub(crate) struct NativePreparationCompletionTemplate {
    journal: MaterializationJournal,
    binding: (crate::TaskId, crate::scheduler::LeaseFence),
    path: acyclic_stream::StreamPath,
    key: acyclic_stream::IdempotencyKey,
    value: serde_json::Value,
    output: acyclic_stream::PreparedOwnedBytes<NativeAllocation>,
    custody: NativeAllocation,
}

impl NativePreparationCompletionTemplate {
    pub(super) fn new<P: StreamProvider>(
        journal: MaterializationJournal,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let io = PreparationIo::from_native_work(work)?;
        let original = intent(&journal, &io)?;
        let bytes = io.encode(&journal, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
        let key = super::journal::key(&bytes, work)?;
        let mut value = io.value_json(&journal, acyclic_stream::MAX_RECORD_BYTES as u64)?;
        // Reserve the widest scalar encoding before the physical action.
        Self::set_work(&mut value, journal.revision, WorkCounters::UNBOUNDED)?;
        let largest = io.encode(&value, acyclic_stream::MAX_RECORD_BYTES as u64, true)?;
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
            acyclic_stream::PreparedOwnedBytes::new(output, output_custody, work.runtime_meter())
                .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                    Error::from(work.runtime_error(error))
                }
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => {
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into()
                }
            })?;
        Self::set_work(&mut value, journal.revision, journal.work)?;
        let path = super::journal::path(owner.task_binding().0, work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let retained = io.owned_bytes()?;
        let packet = Self {
            journal,
            binding: (original.task, original.original_fence),
            path,
            key,
            value,
            output,
            custody: io.into_native_custody(retained)?,
        };
        packet.verify_owner(owner, work)?;
        Ok(packet)
    }

    pub(crate) fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        (self.binding.0, &self.binding.1)
    }
    pub(crate) fn expected_revision(&self) -> u64 {
        self.journal.revision
    }
    pub(crate) fn path(&self) -> &acyclic_stream::StreamPath {
        &self.path
    }
    pub(crate) fn key(&self) -> &acyclic_stream::IdempotencyKey {
        &self.key
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
            || self.task_binding() != owner.task_binding()
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
    fn set_work(
        value: &mut serde_json::Value,
        expected: u64,
        counters: WorkCounters,
    ) -> Result<()> {
        let revision = expected.checked_add(1).ok_or_else(invalid)?;
        let root = value.as_object_mut().ok_or_else(invalid)?;
        *root.get_mut("revision").ok_or_else(invalid)? = revision.into();
        let work = root
            .get_mut("work")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(invalid)?;
        for (key, value) in [
            ("authorityRecordsRead", counters.authority_records_read),
            (
                "authorityRecordsAppended",
                counters.authority_records_appended,
            ),
            ("authorityBytesRead", counters.authority_bytes_read),
            ("authorityBytesWritten", counters.authority_bytes_written),
            ("objectProbes", counters.object_probes),
            ("backendReadOperations", counters.backend_read_operations),
            ("backendWriteOperations", counters.backend_write_operations),
            ("durabilityOperations", counters.durability_operations),
            ("pageReads", counters.page_reads),
            ("pageWrites", counters.page_writes),
            ("objectBytesRead", counters.object_bytes_read),
            ("objectBytesWritten", counters.object_bytes_written),
            ("bytesHashed", counters.bytes_hashed),
            ("bytesCopied", counters.bytes_copied),
            ("bytesEncoded", counters.bytes_encoded),
            ("sourceBytesRead", counters.source_bytes_read),
            ("sourcePathComponents", counters.source_path_components),
            ("sourceEntriesVisited", counters.source_entries_visited),
            ("outputBytes", counters.output_bytes),
            ("itemsExamined", counters.items_examined),
            ("itemsReturned", counters.items_returned),
            ("allocationOperations", counters.allocation_operations),
            ("peakAllocationBytes", counters.peak_allocation_bytes),
            ("materializations", counters.materializations),
        ] {
            *work.get_mut(key).ok_or_else(invalid)? = value.into();
        }
        Ok(())
    }
    pub(crate) fn set_completed_work(&mut self, counters: WorkCounters) -> Result<()> {
        Self::set_work(&mut self.value, self.journal.revision, counters)?;
        self.journal.work = counters;
        Ok(())
    }
    pub(crate) fn codec_parts(&mut self) -> (&serde_json::Value, &mut [u8], u64) {
        let output = self.output.writable();
        let maximum = output.len() as u64;
        (&self.value, output, maximum)
    }
    pub(crate) fn into_completed_parts(
        self,
    ) -> (
        MaterializationJournal,
        acyclic_stream::PreparedOwnedBytes<NativeAllocation>,
        NativeAllocation,
    ) {
        let Self {
            mut journal,
            output,
            custody,
            ..
        } = self;
        journal.revision += 1;
        (journal, output, custody)
    }
}
