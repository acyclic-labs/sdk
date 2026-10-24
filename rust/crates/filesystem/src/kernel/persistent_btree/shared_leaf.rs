use super::entries::{Entries, Entry, share};
use super::shared::SharedPage;
use super::*;
use crate::kernel::persistent_io::PageLease;
use std::sync::Arc;

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    pub(super) async fn rewrite_shared_leaf(
        &mut self,
        original: ObjectId,
        source: Owned<PageLease<F>>,
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let result = async {
            let PageLease::Shared { page, .. } = &source.value else {
                return Err(Self::error(Error::MutationContract));
            };
            let SharedPage::Leaf { entries, .. } = page.as_ref() else {
                return Err(Self::error(Error::MutationContract));
            };
            let rewritten = self.apply_shared_mutations(entries, mutations)?;
            let result = async {
                if rewritten.value.is_empty() {
                    return Owned::allocate(0, self.accounting);
                }
                if rewritten.value == *entries {
                    let mut result = Owned::allocate(1, self.accounting)?;
                    let entry = rewritten
                        .value
                        .first()
                        .ok_or_else(|| Self::error(Error::MutationContract))?;
                    let first = self.clone_key(F::key(entry))?;
                    push_summary(&mut result, first, original, self.accounting)?;
                    return Ok(result);
                }
                self.write_leaf_chunks(Entries::Shared(&rewritten.value))
                    .await
            }
            .await;
            rewritten.retire(self.accounting)?;
            result
        }
        .await;
        source.retire(self.accounting)?;
        result
    }

    fn apply_shared_mutations(
        &mut self,
        entries: &[Arc<Entry<F::Value>>],
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<Arc<Entry<F::Value>>>>, crate::FsError> {
        let capacity = entries
            .len()
            .checked_add(mutations.len())
            .ok_or_else(|| Self::error(Error::AllocationFailed))?;
        let mut output = Owned::allocate(capacity, self.accounting)?;
        let result = (|| {
            let mut entry_index = 0;
            let mut mutation_index = 0;
            while entry_index < entries.len() || mutation_index < mutations.len() {
                let entry = entries.get(entry_index);
                let mutation = mutations.get(mutation_index).map(|(_, mutation)| *mutation);
                let ordering = entry
                    .zip(mutation)
                    .map(|(entry, mutation)| F::key(entry).cmp(mutation.key()));
                self.charge_items(u64::from(ordering.is_some()))?;
                if mutation.is_none() || matches!(ordering, Some(std::cmp::Ordering::Less)) {
                    self.charge_items(u64::from(mutation.is_none()))?;
                    self.push_shared_entry(
                        &mut output,
                        entry.ok_or_else(|| Self::error(Error::MutationContract))?,
                    )?;
                    entry_index += 1;
                    continue;
                }
                let mutation = mutation.ok_or_else(|| Self::error(Error::MutationContract))?;
                let prior = if matches!(ordering, Some(std::cmp::Ordering::Equal)) {
                    entry_index += 1;
                    entry
                } else {
                    None
                };
                // A replacement borrows its mutation, never the current value.
                // Delay cloning until all same-key mutations have succeeded.
                let mut current = prior.map(|entry| &***entry);
                let key = mutation.key();
                while let Some((_, mutation)) = mutations
                    .get(mutation_index)
                    .filter(|(_, mutation)| mutation.key() == key)
                {
                    self.charge_items(1)?;
                    let next = mutation
                        .replacement(current)
                        .map_err(|error| Self::error(Error::Semantic(error)))?;
                    if !mutation.changes_cardinality() && current.is_some() != next.is_some() {
                        return Err(Self::error(Error::MutationContract));
                    }
                    current = next;
                    mutation_index += 1;
                }
                match (prior, current) {
                    (Some(prior), Some(current)) if &***prior == current => {
                        self.push_shared_entry(&mut output, prior)?;
                    }
                    (_, Some(current)) => {
                        let mut cloned = self.clone_value(current)?;
                        let value = cloned
                            .value
                            .take()
                            .ok_or_else(|| Self::error(Error::MutationContract))?;
                        let entry = share(
                            Owned::new(value, cloned.allocation, cloned.bytes),
                            self.accounting,
                        )?;
                        self.push_owned_entry(&mut output, entry)?;
                    }
                    (_, None) => {}
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            output.retire(self.accounting)?;
            return Err(error);
        }
        Ok(output)
    }

    fn push_shared_entry(
        &mut self,
        output: &mut Owned<Vec<Arc<Entry<F::Value>>>>,
        entry: &Arc<Entry<F::Value>>,
    ) -> Result<(), crate::FsError> {
        #[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
        if tests::restore_clones() {
            let mut cloned = self.clone_value(entry)?;
            let value = cloned
                .value
                .take()
                .ok_or_else(|| Self::error(Error::MutationContract))?;
            let owned = share(
                Owned::new(value, cloned.allocation, cloned.bytes),
                self.accounting,
            )?;
            return self.push_owned_entry(output, owned);
        }
        self.accounting.charge(WorkCounters {
            bytes_copied: usize_to_u64(std::mem::size_of::<Arc<Entry<F::Value>>>()),
            ..WorkCounters::UNCHARGED
        })?;
        output.value.push(Arc::clone(entry));
        Ok(())
    }

    fn push_owned_entry(
        &mut self,
        output: &mut Owned<Vec<Arc<Entry<F::Value>>>>,
        mut entry: Owned<Arc<Entry<F::Value>>>,
    ) -> Result<(), crate::FsError> {
        let result = (|| {
            let bytes = output
                .bytes
                .checked_add(entry.bytes)
                .ok_or(WorkError::Overflow)?;
            self.accounting.charge(WorkCounters {
                bytes_copied: usize_to_u64(std::mem::size_of::<Arc<Entry<F::Value>>>()),
                ..WorkCounters::UNCHARGED
            })?;
            output.allocation.absorb(&mut entry.allocation)?;
            output.bytes = bytes;
            Ok::<_, crate::FsError>(())
        })();
        if let Err(error) = result {
            entry.retire(self.accounting)?;
            return Err(error);
        }
        output.value.push(entry.value);
        Ok(())
    }
}

#[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
#[path = "../tests/persistent_btree/shared_leaf.rs"]
mod tests;
