use super::*;
use std::collections::BTreeMap;
use std::sync::Mutex;

/// Process-local journal storage.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: Mutex<BTreeMap<OperationId, MaterializationJournal>>,
}
/// Acknowledgement prepared before a native staging effect.
#[cfg(not(target_arch = "wasm32"))]
pub struct MemoryMaterializationActionCompletion<'a> {
    store: &'a MemoryMaterializationJournalStore,
    journals: Option<(MaterializationJournal, MaterializationJournal)>,
}
impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>
        = MemoryMaterializationActionCompletion<'a>
    where
        Self: 'a;
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'_>, Self::Error> {
        let journals = self.journals.lock().map_err(|_| Self::Error::Unavailable)?;
        let original = journals
            .get(&journal.plan.operation_id)
            .ok_or(Self::Error::Contended)?;
        if *original != journal {
            return Err(Self::Error::Contended);
        }
        match (&original.custody, &journal.custody) {
            (Some(a), Some(b)) => {
                if !a.work().shares_allocation_context(b.work()) {
                    return Err(NativeTreeMaterializationError::InvalidLayout.into());
                }
                if a.bytes() < journal_owned_bytes(original)?
                    || b.bytes() < journal_owned_bytes(&journal)?
                {
                    return Err(NativeTreeMaterializationError::MissingCustody.into());
                }
                a.work().admit(crate::WorkCounters::UNCHARGED)?;
            }
            (None, None) => {}
            _ => return Err(NativeTreeMaterializationError::MissingCustody.into()),
        }
        let mut replacement = duplicate_journal(original)?;
        drop(journals);
        let revision =
            journal
                .revision
                .checked_add(1)
                .ok_or(NativeTreeMaterializationError::Work(
                    crate::WorkError::Overflow,
                ))?;
        journal.revision = revision;
        replacement.revision = revision;
        Ok(MemoryMaterializationActionCompletion {
            store: self,
            journals: Some((journal, replacement)),
        })
    }
    async fn load(
        &self,
        operation: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.journals
            .lock()
            .map_err(|_| Self::Error::Unavailable)?
            .get(&operation)
            .map(duplicate_journal)
            .transpose()
    }
    async fn compare_and_swap(
        &self,
        operation: OperationId,
        expected: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let mut journals = self.journals.lock().map_err(|_| Self::Error::Unavailable)?;
        let stored = crate::record_store::stored_revision(&journals, &operation) == expected;
        if stored {
            journals.insert(operation, duplicate_journal(&replacement)?);
        }
        Ok((stored, replacement))
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Future for MemoryMaterializationActionCompletion<'_> {
    type Output = Result<MaterializationJournal, MemoryMaterializationJournalStoreError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        std::task::Poll::Ready((|| {
            let (mut journal, mut replacement) = this
                .journals
                .take()
                .ok_or(MemoryMaterializationJournalStoreError::Unavailable)?;
            if let Some(allocation) = &replacement.custody {
                journal.work = allocation.work().journal_work()?;
                replacement.work = journal.work;
            }
            let retired = {
                let mut journals = this
                    .store
                    .journals
                    .lock()
                    .map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
                let current = journals
                    .get_mut(&journal.plan.operation_id)
                    .ok_or(MemoryMaterializationJournalStoreError::Contended)?;
                if current.revision != journal.revision - 1 {
                    return Err(MemoryMaterializationJournalStoreError::Contended);
                }
                std::mem::replace(current, replacement)
            };
            drop(retired);
            Ok(journal)
        })())
    }
}
/// Journal synchronization or native accounting failure.
#[derive(Debug, Error)]
pub enum MemoryMaterializationJournalStoreError {
    /// The record lock is poisoned.
    #[error("materialization memory store unavailable")]
    Unavailable,
    /// The original revision changed before acknowledgement.
    #[error("materialization memory store contended")]
    Contended,
    /// Native accounting failed.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] NativeTreeMaterializationError),
}

pub(crate) fn duplicate_journal(
    journal: &MaterializationJournal,
) -> Result<MaterializationJournal, MemoryMaterializationJournalStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(allocation) = journal.custody.as_ref().or_else(|| {
        journal
            .preimages
            .iter()
            .find_map(|preimage| preimage.custody.as_ref())
    }) {
        let work = allocation.work();
        let mut custody = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let intent = match &journal.plan.intent {
            MaterializationIntent::GenerationTransition { from, to } => {
                MaterializationIntent::GenerationTransition {
                    from: *from,
                    to: *to,
                }
            }
            MaterializationIntent::NativePreparation {
                selection,
                original,
            } => {
                let original = copy_image(original, &mut custody)?;
                let mut selected = Vec::new();
                for item in selection {
                    let provider_identity = copy_image(&item.provider_identity, &mut custody)?;
                    native_vec_push(
                        &mut selected,
                        MaterializationPreparationSelection {
                            provider_identity,
                            volume: item.volume,
                            generation: item.generation,
                        },
                        &mut custody,
                    )?;
                }
                MaterializationIntent::NativePreparation {
                    selection: selected,
                    original,
                }
            }
        };
        let mut edits = Vec::new();
        for edit in &journal.plan.edits {
            let edit = duplicate_edit(edit, &mut custody)?;
            native_vec_push(&mut edits, edit, &mut custody)?;
        }
        let mut preimages = Vec::new();
        for preimage in &journal.preimages {
            let image = copy_image(&preimage.image, &mut custody)?;
            native_vec_push(
                &mut preimages,
                MaterializationPreimage {
                    image,
                    custody: None,
                },
                &mut custody,
            )?;
        }
        return Ok(MaterializationJournal {
            version: journal.version,
            revision: journal.revision,
            plan: MaterializationPlan {
                operation_id: journal.plan.operation_id,
                intent,
                edits,
            },
            root_identity: journal.root_identity,
            preparation_identity: journal.preparation_identity,
            preparation_progress: journal.preparation_progress,
            work: journal.work,
            preimages,
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
            custody: Some(custody),
        });
    }
    Ok(MaterializationJournal {
        version: journal.version,
        revision: journal.revision,
        plan: journal.plan.clone(),
        root_identity: journal.root_identity,
        preparation_identity: journal.preparation_identity,
        preparation_progress: journal.preparation_progress,
        work: journal.work,
        preimages: journal
            .preimages
            .iter()
            .map(|preimage| MaterializationPreimage {
                image: preimage.image.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            })
            .collect(),
        phase: journal.phase,
        applied: journal.applied,
        restored: journal.restored,
        #[cfg(not(target_arch = "wasm32"))]
        custody: None,
    })
}
