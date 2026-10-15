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
impl PreparationRecord for Intent {}
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
