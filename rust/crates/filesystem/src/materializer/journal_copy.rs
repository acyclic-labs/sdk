//! Explicit snapshot copies retain the admitted immutable history identity.
use super::*;
use crate as sdk;

/// Failure to retain the original native allocation while copying a journal.
#[cfg(not(target_arch = "wasm32"))]
pub type MaterializationJournalCopyError = NativeTreeMaterializationError;
/// Portable snapshots have no native allocation to retain.
#[cfg(target_arch = "wasm32")]
pub type MaterializationJournalCopyError = std::convert::Infallible;
impl MaterializationJournal {
    /// Copies a snapshot with its original history identity and native custody.
    pub fn try_clone(&self) -> Result<Self, MaterializationJournalCopyError> {
        let journal = self;
        #[cfg(not(target_arch = "wasm32"))]
        let copied = sdk::WorkCounters {
            bytes_copied: copy_bytes(journal)?,
            ..sdk::WorkCounters::UNCHARGED
        };
        #[cfg(not(target_arch = "wasm32"))]
        let copying = journal
            .custody
            .as_ref()
            .map(|allocation| allocation.work().reserve_work(copied))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let custody = journal
            .custody
            .as_ref()
            .map(|allocation| {
                let bytes = sdk::materializer::journal_owned_bytes(journal)?;
                allocation
                    .work()
                    .claim(bytes, 0, sdk::WorkCounters::UNCHARGED)
            })
            .transpose()?;
        let copy = MaterializationJournal {
            plan: journal.plan.clone(),
            preimages: journal
                .preimages
                .iter()
                .map(|image| sdk::MaterializationPreimage {
                    image: image.image.clone(),
                    #[cfg(not(target_arch = "wasm32"))]
                    custody: None,
                })
                .collect(),
            #[cfg(not(target_arch = "wasm32"))]
            custody,
            ..*journal
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(copying) = copying {
            copying.record_completed(copied)?;
        }
        Ok(copy)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn copy_bytes(
    journal: &MaterializationJournal,
) -> Result<u64, sdk::NativeTreeMaterializationError> {
    use sdk::materializer::{MaterializationEdit as Edit, MaterializationIntent as Intent};
    let mut bytes = 0_u64;
    let mut add = |count: usize, width: usize| -> Result<(), sdk::NativeTreeMaterializationError> {
        bytes = bytes
            .checked_add(count.checked_mul(width).ok_or(sdk::WorkError::Overflow)? as u64)
            .ok_or(sdk::WorkError::Overflow)?;
        Ok(())
    };
    add(journal.plan.edits.len(), std::mem::size_of::<Edit>())?;
    for edit in &journal.plan.edits {
        if let Edit::Install { path, image } | Edit::SetMetadata { path, image } = edit {
            add(path.len(), 1)?;
            add(image.len(), 1)?;
        } else if let Edit::Remove { path } = edit {
            add(path.len(), 1)?;
        } else if let Edit::Rename { from, to } = edit {
            add(from.len(), 1)?;
            add(to.len(), 1)?;
        } else {
            return Err(sdk::NativeTreeMaterializationError::InvalidLayout);
        }
    }
    if let Intent::NativePreparation {
        selection,
        original,
    } = &journal.plan.intent
    {
        add(
            selection.len(),
            std::mem::size_of::<sdk::materializer::MaterializationPreparationSelection>(),
        )?;
        add(original.len(), 1)?;
        for item in selection {
            add(item.provider_identity.len(), 1)?;
        }
    }
    add(
        journal.preimages.len(),
        std::mem::size_of::<sdk::MaterializationPreimage>(),
    )?;
    for image in &journal.preimages {
        add(image.image.len(), 1)?;
    }
    Ok(bytes)
}
