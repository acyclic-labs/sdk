use super::super::PreparationRecord;
use super::*;
use acyclic_fs::MaterializationJournal;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub(super) task: crate::TaskId,
    pub(super) original_fence: crate::scheduler::LeaseFence,
    pub(super) operation_directory: PathBuf,
    pub(super) manifest: NativeViewManifest,
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
        intent_decode_owned_bytes(shape.lexical_items(), input, scratch).ok_or(
            Error::NativeBoundary {
                code: code.unwrap_or(crate::wire::ErrorCode::Invalid),
                reason: "native intent ownership bound overflows",
            },
        )
    }
}

fn intent_decode_owned_bytes(items: u64, input: u64, scratch: u64) -> Option<u64> {
    use std::mem::size_of;
    // Intent's only variable-size non-byte vector contains mappings. Three
    // capacities per lexical item cover initial capacity and old/new growth.
    // VolumeOwner's adjacent tag can buffer arbitrary Content before its tag;
    // include map pairs, initial capacities and simultaneous growth there too.
    let slot = 3 * size_of::<NativeVolumeMapping>()
        + 8 * size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>();
    items
        .checked_mul(slot as u64)?
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
        super::journal_allocation::decode_owned_bytes(shape.lexical_items(), input, scratch).ok_or(
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
