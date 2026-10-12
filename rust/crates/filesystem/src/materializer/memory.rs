use super::*;

/// Process-local journal adapter over the SDK record map.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: MemoryRecords<OperationId, MaterializationJournal>,
}
/// Acknowledgement prepared before native staging.
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
        let journals = self
            .journals
            .0
            .lock()
            .map_err(|_| Self::Error::Unavailable)?;
        let original = journals
            .get(&journal.plan.operation_id)
            .ok_or(Self::Error::Contended)?;
        if original != &journal {
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
        journal.revision = journal
            .revision
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)
            .map_err(NativeTreeMaterializationError::from)?;
        replacement.revision = journal.revision;
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
            .0
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
        let mut journals = self
            .journals
            .0
            .lock()
            .map_err(|_| Self::Error::Unavailable)?;
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
                    .0
                    .lock()
                    .map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
                let current = match journals.get_mut(&journal.plan.operation_id) {
                    Some(current) if current.revision == journal.revision - 1 => current,
                    _ => {
                        return Err(MemoryMaterializationJournalStoreError::ActionConflict(
                            journal,
                        ));
                    }
                };
                std::mem::replace(current, replacement)
            };
            drop(retired);
            Ok(journal)
        })())
    }
}
/// Record synchronization or native accounting failure.
#[derive(Debug, Error)]
pub enum MemoryMaterializationJournalStoreError {
    /// Record lock is poisoned.
    #[error("materialization store unavailable")]
    Unavailable,
    /// Original revision changed.
    #[error("materialization store contended")]
    Contended,
    /// Accepted staging lost its revision; retains the caller's journal.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("accepted action conflicts with its journal")]
    ActionConflict(MaterializationJournal),
    /// Original native accounting failed.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] NativeTreeMaterializationError),
}
pub(crate) fn duplicate_journal(
    journal: &MaterializationJournal,
) -> Result<MaterializationJournal, MemoryMaterializationJournalStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = journal
        .custody
        .as_ref()
        .or_else(|| {
            journal
                .preimages
                .iter()
                .find_map(|image| image.custody.as_ref())
        })
        .map(|owned| owned.work().claim(0, 0, crate::WorkCounters::UNCHARGED))
        .transpose()?;
    #[cfg(not(target_arch = "wasm32"))]
    let native = if let Some(custody) = &mut custody {
        let edits = copy_slice(&journal.plan.edits, custody, duplicate_edit)?;
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
                let original = copy_image(original, custody)?;
                let selected = copy_slice(selection, custody, |item, custody| {
                    Ok(MaterializationPreparationSelection {
                        provider_identity: copy_image(&item.provider_identity, custody)?,
                        volume: item.volume,
                        generation: item.generation,
                    })
                })?;
                MaterializationIntent::NativePreparation {
                    selection: selected,
                    original,
                }
            }
        };
        let preimages = copy_slice(&journal.preimages, custody, |image, custody| {
            Ok(MaterializationPreimage {
                image: copy_image(&image.image, custody)?,
                custody: None,
            })
        })?;
        Some((
            MaterializationPlan {
                operation_id: journal.plan.operation_id,
                intent,
                edits,
            },
            preimages,
        ))
    } else {
        None
    };
    #[cfg(target_arch = "wasm32")]
    let native: Option<(MaterializationPlan, Vec<MaterializationPreimage>)> = None;
    let (plan, preimages) = native.unwrap_or_else(|| {
        (
            journal.plan.clone(),
            journal
                .preimages
                .iter()
                .map(|image| MaterializationPreimage {
                    image: image.image.clone(),
                    #[cfg(not(target_arch = "wasm32"))]
                    custody: None,
                })
                .collect(),
        )
    });
    Ok(MaterializationJournal {
        version: journal.version,
        revision: journal.revision,
        plan,
        root_identity: journal.root_identity,
        preparation_identity: journal.preparation_identity,
        preparation_progress: journal.preparation_progress,
        work: journal.work,
        preimages,
        phase: journal.phase,
        applied: journal.applied,
        restored: journal.restored,
        #[cfg(not(target_arch = "wasm32"))]
        custody,
    })
}
