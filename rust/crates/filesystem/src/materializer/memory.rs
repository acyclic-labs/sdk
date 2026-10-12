use super::*;

/// Process-local journal store.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: std::sync::Mutex<std::collections::BTreeMap<OperationId, MaterializationJournal>>,
}
/// Acknowledgement prepaid before a physical mutation.
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
            (Some(original), Some(input)) => {
                if !original.work().shares_allocation_context(input.work()) {
                    return Err(NativeTreeMaterializationError::InvalidLayout.into());
                }
                if original.bytes()
                    < journal_owned_bytes(
                        journals
                            .get(&journal.plan.operation_id)
                            .expect("original journal"),
                    )?
                    || input.bytes() < journal_owned_bytes(&journal)?
                {
                    return Err(NativeTreeMaterializationError::MissingCustody.into());
                }
                original.work().admit(crate::WorkCounters::UNCHARGED)?;
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
        if let Some(allocation) = &replacement.custody {
            allocation.work().admit(crate::WorkCounters::UNCHARGED)?;
        }
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
        let journals = self.journals.lock().map_err(|_| Self::Error::Unavailable)?;
        journals.get(&operation).map(duplicate_journal).transpose()
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
                let stored = journals
                    .get_mut(&journal.plan.operation_id)
                    .ok_or(MemoryMaterializationJournalStoreError::Contended)?;
                if stored.revision != journal.revision - 1 {
                    return Err(MemoryMaterializationJournalStoreError::Contended);
                }
                std::mem::replace(stored, replacement)
            };
            drop(retired);
            Ok(journal)
        })())
    }
}
/// Journal synchronization or finite admission failure.
#[derive(Debug, Error)]
pub enum MemoryMaterializationJournalStoreError {
    /// Record lock was poisoned.
    #[error("materialization memory store unavailable")]
    Unavailable,
    /// Original revision changed before completion.
    #[error("materialization memory store contended")]
    Contended,
    /// Original native work failed.
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    NativeWork(#[from] NativeTreeMaterializationError),
}

/// Copies a journal on its existing ledger, when it has a native allocation.
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
        let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
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
                let original = copy_image(original, work, &mut allocation)?;
                let mut selected = Vec::new();
                for item in selection {
                    let provider_identity =
                        copy_image(&item.provider_identity, work, &mut allocation)?;
                    native_vec_push(
                        &mut selected,
                        MaterializationPreparationSelection {
                            provider_identity,
                            volume: item.volume,
                            generation: item.generation,
                        },
                        &mut allocation,
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
            let (edit, mut added) = duplicate_native_edit(edit, work)?;
            allocation.absorb(&mut added)?;
            native_vec_push(&mut edits, edit, &mut allocation)?;
        }
        let mut preimages = Vec::new();
        for preimage in &journal.preimages {
            let image = copy_image(&preimage.image, work, &mut allocation)?;
            native_vec_push(
                &mut preimages,
                MaterializationPreimage {
                    image,
                    custody: None,
                },
                &mut allocation,
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
            preimages,
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
            root_identity: journal.root_identity,
            preparation_identity: journal.preparation_identity,
            preparation_progress: journal.preparation_progress,
            work: journal.work,
            custody: Some(allocation),
        });
    }
    Ok(MaterializationJournal {
        version: journal.version,
        revision: journal.revision,
        plan: journal.plan.clone(),
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
        root_identity: journal.root_identity,
        preparation_identity: journal.preparation_identity,
        preparation_progress: journal.preparation_progress,
        work: journal.work,
        #[cfg(not(target_arch = "wasm32"))]
        custody: None,
    })
}
