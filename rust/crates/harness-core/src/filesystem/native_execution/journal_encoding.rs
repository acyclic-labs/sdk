use acyclic_fs::{
    MaterializationEdit, MaterializationIntent, MaterializationJournal, MaterializationPlan,
    MaterializationPreparationIdentity, MaterializationPreparationSelection, WorkCounters,
};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq, SerializeStructVariant},
};

// Closed journal records have fixed keys. Borrowing them in sorted order
// preserves canonical bytes and idempotency keys without constructing a Value.
pub(crate) struct Canonical<'a, T: ?Sized>(pub(crate) &'a T);

pub(super) fn completion_capacity(
    journal: &MaterializationJournal,
    encoded: usize,
) -> Option<usize> {
    let digits = |value: u64| value.checked_ilog10().unwrap_or(0) as usize + 1;
    let work = &journal.work;
    let growth = [
        work.allocation_operations,
        work.authority_bytes_read,
        work.authority_bytes_written,
        work.authority_records_appended,
        work.authority_records_read,
        work.backend_read_operations,
        work.backend_write_operations,
        work.bytes_copied,
        work.bytes_encoded,
        work.bytes_hashed,
        work.durability_operations,
        work.items_examined,
        work.items_returned,
        work.materializations,
        work.object_bytes_read,
        work.object_bytes_written,
        work.object_probes,
        work.output_bytes,
        work.page_reads,
        work.page_writes,
        work.peak_allocation_bytes,
        work.source_bytes_read,
        work.source_entries_visited,
        work.source_path_components,
    ]
    .into_iter()
    .map(|value| digits(u64::MAX) - digits(value))
    .sum::<usize>();
    encoded
        .checked_add(growth)?
        .checked_add(digits(journal.revision.checked_add(1)?))?
        .checked_sub(digits(journal.revision))
}

macro_rules! object {
    ($ty:ty, $count:expr, $value:ident, {$($key:literal => $field:expr),+ $(,)?}) => {
        impl Serialize for Canonical<'_, $ty> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let $value = self.0;
                let mut object = serializer.serialize_map(Some($count))?;
                $(object.serialize_entry($key, &$field)?;)+
                object.end()
            }
        }
    };
}

object!(MaterializationJournal, 11, value, {
    "applied" => value.applied,
    "phase" => value.phase,
    "plan" => Canonical(&value.plan),
    "preimages" => value.preimages,
    "preparation_identity" => value.preparation_identity.as_ref().map(Canonical),
    "preparation_progress" => value.preparation_progress,
    "restored" => value.restored,
    "revision" => value.revision,
    "root_identity" => value.root_identity,
    "version" => value.version,
    "work" => Canonical(&value.work),
});
object!(MaterializationPlan, 3, value, {
    "edits" => Canonical(value.edits.as_slice()),
    "intent" => Canonical(&value.intent),
    "operation_id" => value.operation_id,
});
object!(MaterializationPreparationIdentity, 3, value, {
    "backup" => value.backup,
    "operation" => value.operation,
    "target" => value.target,
});
object!(MaterializationPreparationSelection, 3, value, {
    "generation" => value.generation,
    "provider_identity" => value.provider_identity,
    "volume" => value.volume,
});
object!(WorkCounters, 24, value, {
    "allocationOperations" => value.allocation_operations,
    "authorityBytesRead" => value.authority_bytes_read,
    "authorityBytesWritten" => value.authority_bytes_written,
    "authorityRecordsAppended" => value.authority_records_appended,
    "authorityRecordsRead" => value.authority_records_read,
    "backendReadOperations" => value.backend_read_operations,
    "backendWriteOperations" => value.backend_write_operations,
    "bytesCopied" => value.bytes_copied,
    "bytesEncoded" => value.bytes_encoded,
    "bytesHashed" => value.bytes_hashed,
    "durabilityOperations" => value.durability_operations,
    "itemsExamined" => value.items_examined,
    "itemsReturned" => value.items_returned,
    "materializations" => value.materializations,
    "objectBytesRead" => value.object_bytes_read,
    "objectBytesWritten" => value.object_bytes_written,
    "objectProbes" => value.object_probes,
    "outputBytes" => value.output_bytes,
    "pageReads" => value.page_reads,
    "pageWrites" => value.page_writes,
    "peakAllocationBytes" => value.peak_allocation_bytes,
    "sourceBytesRead" => value.source_bytes_read,
    "sourceEntriesVisited" => value.source_entries_visited,
    "sourcePathComponents" => value.source_path_components,
});

impl<T> Serialize for Canonical<'_, [T]>
where
    for<'a> Canonical<'a, T>: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&Canonical(value))?;
        }
        sequence.end()
    }
}

impl Serialize for Canonical<'_, MaterializationIntent> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            MaterializationIntent::GenerationTransition { from, to } => {
                let mut value = serializer.serialize_struct_variant(
                    "MaterializationIntent",
                    0,
                    "GenerationTransition",
                    2,
                )?;
                value.serialize_field("from", from)?;
                value.serialize_field("to", to)?;
                value.end()
            }
            MaterializationIntent::NativePreparation {
                selection,
                original,
            } => {
                let mut value = serializer.serialize_struct_variant(
                    "MaterializationIntent",
                    1,
                    "NativePreparation",
                    2,
                )?;
                value.serialize_field("original", original)?;
                value.serialize_field("selection", &Canonical(selection.as_slice()))?;
                value.end()
            }
        }
    }
}

impl Serialize for Canonical<'_, MaterializationEdit> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            MaterializationEdit::Install { path, image }
            | MaterializationEdit::SetMetadata { path, image } => {
                let (index, name) = if matches!(self.0, MaterializationEdit::Install { .. }) {
                    (0, "Install")
                } else {
                    (3, "SetMetadata")
                };
                let mut value =
                    serializer.serialize_struct_variant("MaterializationEdit", index, name, 2)?;
                value.serialize_field("image", image)?;
                value.serialize_field("path", path)?;
                value.end()
            }
            MaterializationEdit::Remove { path } => {
                let mut value =
                    serializer.serialize_struct_variant("MaterializationEdit", 1, "Remove", 1)?;
                value.serialize_field("path", path)?;
                value.end()
            }
            MaterializationEdit::Rename { from, to } => {
                let mut value =
                    serializer.serialize_struct_variant("MaterializationEdit", 2, "Rename", 2)?;
                value.serialize_field("from", from)?;
                value.serialize_field("to", to)?;
                value.end()
            }
            _ => Err(serde::ser::Error::custom(
                "unsupported native materialization edit",
            )),
        }
    }
}
