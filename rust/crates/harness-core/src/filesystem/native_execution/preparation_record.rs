use super::super::PreparationRecord;
use super::*;
use acyclic_fs::MaterializationJournal;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub(super) task: crate::TaskId,
    pub(super) original_fence: crate::scheduler::LeaseFence,
    pub(super) operation_directory: PathBuf,
    #[serde(serialize_with = "serialize_manifest")]
    pub(super) manifest: NativeViewManifest,
}

fn serialize_manifest<S: serde::Serializer>(
    manifest: &NativeViewManifest,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::Serialize;
    let work = manifest.options.work_per_volume;
    // These are the existing positional forms of the closed Manifest, Options
    // and WorkCounters structs. Their derived decoders also retain object form.
    (
        (
            &manifest.options.root,
            manifest.options.maximum_volumes,
            [
                work.authority_records_read,
                work.authority_records_appended,
                work.authority_bytes_read,
                work.authority_bytes_written,
                work.object_probes,
                work.backend_read_operations,
                work.backend_write_operations,
                work.durability_operations,
                work.page_reads,
                work.page_writes,
                work.object_bytes_read,
                work.object_bytes_written,
                work.bytes_hashed,
                work.bytes_copied,
                work.bytes_encoded,
                work.source_bytes_read,
                work.source_path_components,
                work.source_entries_visited,
                work.output_bytes,
                work.items_examined,
                work.items_returned,
                work.allocation_operations,
                work.peak_allocation_bytes,
                work.materializations,
            ],
        ),
        super::journal_encoding::Canonical(manifest.volumes.as_slice()),
    )
        .serialize(serializer)
}

impl serde::Serialize for super::journal_encoding::Canonical<'_, NativeVolumeMapping> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mapping = self.0;
        let volume = &mapping.volume;
        let provider = volume.provider();
        let generation = mapping.generation.as_resource();
        let generation_provider = generation.provider();
        (
            (
                (provider.namespace(), provider.family(), provider.version()),
                volume.id(),
                volume.class(),
                volume.owner(),
            ),
            (
                generation.kind(),
                (
                    generation_provider.namespace(),
                    generation_provider.family(),
                    generation_provider.version(),
                ),
                generation.key(),
                generation.version(),
            ),
            &mapping.namespace,
            &mapping.path,
            mapping.writable,
            &mapping.authority_revision,
        )
            .serialize(serializer)
    }
}
impl PreparationRecord for Intent {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let mut bytes = crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            self.original_fence.reservation_id.capacity() as u64,
            self.original_fence.placement.capacity() as u64,
            self.operation_directory.capacity() as u64,
            self.manifest.options.root.capacity() as u64,
            (self.manifest.volumes.capacity() as u64)
                .checked_mul(std::mem::size_of::<NativeVolumeMapping>() as u64)
                .ok_or_else(invalid)?,
        ])?;
        for mapping in &self.manifest.volumes {
            bytes = crate::core::owned_sum([
                bytes,
                mapping.path.capacity() as u64,
                mapping.volume.logical_owned_bytes()?,
                mapping.generation.logical_owned_bytes()?,
            ])?;
        }
        Ok(Some(bytes))
    }

    fn decode_owned_allowance(
        shape: crate::executor::json::JsonValueShape,
        scratch: u64,
        input: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        intent_decode_owned_bytes(shape.lexical_items(), input, scratch, shape.is_slice()).ok_or(
            Error::NativeBoundary {
                code: code.unwrap_or(crate::wire::ErrorCode::Invalid),
                reason: "native intent ownership bound overflows",
            },
        )
    }
}

fn intent_decode_owned_bytes(items: u64, input: u64, scratch: u64, slice: bool) -> Option<u64> {
    use std::mem::size_of;
    // A complete mapping contains a [u8; 32] authority revision: at least
    // 33 lexical items. Slice decoding has no sequence size hint, so its
    // mapping Vec grows only after a complete element. Four slots per element
    // cover initial capacity and simultaneous old/new growth. Value decoding
    // can preallocate from an untrusted size hint before validating elements.
    let mappings = if slice {
        (items / 33).checked_mul(4)?
    } else {
        items.checked_mul(3)?
    }
    .checked_mul(size_of::<NativeVolumeMapping>() as u64)?;
    // VolumeOwner's adjacent tag can buffer arbitrary Content before its tag;
    // include map pairs, initial capacities and simultaneous growth there too.
    let slot = 8 * size_of::<(
        serde::__private229::de::Content<'static>,
        serde::__private229::de::Content<'static>,
    )>();
    items
        .checked_mul(slot as u64)?
        .checked_add(mappings)?
        .checked_add(input.checked_mul(4)?)?
        .checked_add(scratch)?
        .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())?
        .checked_add(size_of::<Intent>() as u64)
}
impl PreparationRecord for MaterializationJournal {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        Ok(Some(acyclic_fs::materializer::journal_owned_bytes(self)?))
    }
    fn decode_owned_allowance(
        shape: crate::executor::json::JsonValueShape,
        scratch: u64,
        input: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let scratch = scratch
            .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
            .ok_or_else(invalid)?;
        let slots = shape
            .lexical_containers()
            .map_or(shape.lexical_items(), |containers| {
                shape.lexical_items().min(containers.saturating_mul(4))
            });
        super::journal_allocation::decode_owned_bytes(slots, input, scratch).ok_or(
            Error::NativeBoundary {
                code: code.unwrap_or(crate::wire::ErrorCode::Invalid),
                reason: "native journal ownership bound overflows",
            },
        )
    }
}

pub(super) fn invalid() -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "invalid protected native preparation",
    }
}

/// Exercises bounded Intent decoding and its original allocation custody.
#[cfg(any(test, fuzzing))]
pub fn fuzz_native_preparation_intent(
    input: &[u8],
    work: &NativeMaterializationWork,
) -> Result<()> {
    let io = PreparationIo::from_native_work(work)?;
    let decoded = io.decode(
        std::marker::PhantomData::<Intent>,
        input,
        crate::wire::ErrorCode::Storage,
    )?;
    let (intent, custody) = crate::filesystem::native_decoded(decoded)?;
    drop((intent, custody));
    Ok(())
}

/// Exercises bounded journal decoding and its original allocation custody.
#[cfg(any(test, fuzzing))]
pub fn fuzz_native_preparation_journal(
    input: &[u8],
    work: &NativeMaterializationWork,
) -> Result<()> {
    let io = PreparationIo::from_native_work(work)?;
    let decoded = io.decode(
        std::marker::PhantomData::<MaterializationJournal>,
        input,
        crate::wire::ErrorCode::Storage,
    )?;
    let (journal, custody) = crate::filesystem::native_decoded(decoded)?;
    drop((journal, custody));
    Ok(())
}

pub(super) fn intent(journal: &MaterializationJournal, io: &PreparationIo) -> Result<Intent> {
    let acyclic_fs::MaterializationIntent::NativePreparation { original, .. } =
        &journal.plan.intent
    else {
        return Err(invalid());
    };
    let decoded = io.decode(
        std::marker::PhantomData::<Intent>,
        original,
        crate::wire::ErrorCode::Storage,
    )?;
    let intent = super::super::retain_decoded(decoded)?;
    intent.manifest.validate(Some(io))?;
    Ok(intent)
}

#[cfg(test)]
#[path = "intent_properties.rs"]
mod properties;
