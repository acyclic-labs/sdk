use super::super::{PreparationIo, native_decoded};
use super::preparation_record::{Intent, invalid};
use super::*;
use acyclic_fs::NativeMaterializationWork;

/// Reads the original preparation on an authenticated admission witness.
pub fn observe_preparation<'a, P: StreamProvider>(
    witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, Option<NativeRecordedPreparation>>> {
    if !work.remaining()?.has_finite_allowances() {
        return Err(invalid());
    }
    crate::NativeFutureBundle::new(work, || async move {
        let Some(mut journal) = super::journal::read(witness, work).await? else {
            return Ok(None);
        };
        let mut custody = journal.custody.take().ok_or_else(invalid)?;
        let io = PreparationIo::from_native_work(work)?;
        let acyclic_fs::MaterializationIntent::NativePreparation {
            selection,
            original,
        } = &journal.plan.intent
        else {
            return Err(invalid());
        };
        let (intent, mut intent_custody) = native_decoded(io.decode(
            std::marker::PhantomData::<Intent>,
            original,
            crate::wire::ErrorCode::Storage,
        )?)?;
        intent.manifest.validate_with_io(Some(&io))?;
        if intent.task != witness.task() || selection.len() != intent.manifest.volumes.len()
            || journal.plan.edits.iter().any(|edit| !matches!(edit, acyclic_fs::MaterializationEdit::Install { image, .. } if image.as_slice() == acyclic_fs::NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) {
            return Err(invalid());
        }
        for (selected, mapping) in selection.iter().zip(&intent.manifest.volumes) {
            let provider = io.encode(
                mapping.volume.provider(),
                work.remaining()?.bytes_encoded,
                true,
            )?;
            if provider.as_slice() != selected.provider_identity
                || selected.generation.digest().as_bytes().as_slice()
                    != mapping.generation.as_resource().key()
            {
                return Err(invalid());
            }
        }
        if let Some(scratch) = witness.scratch_proof() {
            if scratch.original_task() != intent.task
                || !intent
                    .manifest
                    .volumes
                    .iter()
                    .any(|mapping| mapping.writable && &mapping.volume == scratch.volume())
            {
                return Err(invalid());
            }
        }
        custody.absorb(&mut intent_custody)?;
        witness.verify_with_native_work(work).await?;
        Ok(Some(NativeRecordedPreparation {
            task: intent.task,
            original_fence: intent.original_fence,
            manifest: intent.manifest,
            operation_directory: intent.operation_directory,
            journal,
            custody,
        }))
    })
}
