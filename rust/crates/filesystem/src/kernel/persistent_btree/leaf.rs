use super::*;

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    pub(super) async fn rewrite_leaf(
        &mut self,
        original: ObjectId,
        mut entries: Owned<Vec<F::Value>>,
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let changed;
        if mutations
            .iter()
            .all(|(_, mutation)| !mutation.changes_cardinality())
        {
            changed = self.apply_point_mutations(&mut entries, mutations)?;
        } else {
            let rewritten = self.apply_structural_mutations(&entries.value, mutations)?;
            changed = rewritten.value != entries.value;
            entries.retire(self.accounting)?;
            entries = rewritten;
        }
        let result = if entries.value.is_empty() {
            Owned::allocate(0, self.accounting)?
        } else if !changed {
            let mut result = Owned::allocate(1, self.accounting)?;
            let first = self.clone_key(F::key(
                entries
                    .value
                    .first()
                    .ok_or_else(|| Self::error(Error::MutationContract))?,
            ))?;
            push_summary(&mut result, first, original, self.accounting)?;
            result
        } else {
            self.write_leaf_chunks(&entries.value).await?
        };
        entries.retire(self.accounting)?;
        Ok(result)
    }

    fn apply_mutation(
        &mut self,
        mutation: &M,
        current: &mut Owned<Option<F::Value>>,
    ) -> Result<(), crate::FsError> {
        let next = mutation
            .replacement(current.value.as_ref())
            .map_err(|error| Self::error(Error::Semantic(error)))?;
        let replacement = match next {
            Some(next) => self.clone_value(next)?,
            None => self.empty(None)?,
        };
        let prior = std::mem::replace(current, replacement);
        prior.retire(self.accounting)
    }

    fn apply_point_mutations(
        &mut self,
        entries: &mut Owned<Vec<F::Value>>,
        mutations: &[(usize, &M)],
    ) -> Result<bool, crate::FsError> {
        let mut changed = false;
        for (_, mutation) in mutations {
            let found = binary_search_with_accounting(&entries.value, self.accounting, |entry| {
                F::key(entry).cmp(mutation.key())
            })?;
            let prior = found.ok().and_then(|index| entries.value.get(index));
            let next = mutation
                .replacement(prior)
                .map_err(|error| Self::error(Error::Semantic(error)))?;
            if prior.is_some() != next.is_some() {
                return Err(Self::error(Error::MutationContract));
            }
            if prior == next {
                continue;
            }
            changed = true;
            if let (Ok(index), Some(next)) = (found, next) {
                let mut current = self.clone_value(next)?;
                let replacement = current
                    .value
                    .take()
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let slot = entries
                    .value
                    .get_mut(index)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let old_bytes = F::value_owned_bytes(slot);
                let old_allocation = split_allocation(&mut entries.allocation, old_bytes)?;
                drop(std::mem::replace(slot, replacement));
                retire_allocation(old_allocation, old_bytes, self.accounting)?;
                entries.allocation.absorb(&mut current.allocation)?;
                entries.bytes = entries
                    .bytes
                    .checked_sub(old_bytes)
                    .and_then(|bytes| bytes.checked_add(current.bytes))
                    .ok_or(WorkError::Overflow)?;
            }
        }
        Ok(changed)
    }

    fn apply_structural_mutations(
        &mut self,
        entries: &[F::Value],
        mutations: &[(usize, &M)],
    ) -> Result<Owned<Vec<F::Value>>, crate::FsError> {
        let capacity = entries
            .len()
            .checked_add(mutations.len())
            .ok_or_else(|| Self::error(Error::AllocationFailed))?;
        let mut output = Owned::allocate(capacity, self.accounting)?;
        let mut entry_index = 0;
        let mut mutation_index = 0;
        while entry_index < entries.len() || mutation_index < mutations.len() {
            let Some((_, mutation)) = mutations.get(mutation_index) else {
                self.charge_items(1)?;
                let entry = entries
                    .get(entry_index)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let cloned = self.clone_value(entry)?;
                push_value(&mut output, cloned)?;
                entry_index += 1;
                continue;
            };
            let entry = entries.get(entry_index);
            let ordering = entry.map(|entry| F::key(entry).cmp(mutation.key()));
            self.charge_items(u64::from(ordering.is_some()))?;
            if matches!(ordering, Some(std::cmp::Ordering::Less)) {
                let cloned =
                    self.clone_value(entry.ok_or_else(|| Self::error(Error::MutationContract))?)?;
                push_value(&mut output, cloned)?;
                entry_index += 1;
                continue;
            }
            let mut current = if matches!(ordering, Some(std::cmp::Ordering::Equal)) {
                entry_index += 1;
                self.clone_value(entry.ok_or_else(|| Self::error(Error::MutationContract))?)?
            } else {
                self.empty(None)?
            };
            let key = mutation.key();
            while let Some((_, mutation)) = mutations
                .get(mutation_index)
                .filter(|(_, mutation)| mutation.key() == key)
            {
                self.charge_items(1)?;
                self.apply_mutation(mutation, &mut current)?;
                mutation_index += 1;
            }
            if current.value.is_some() {
                push_value(&mut output, current)?;
            } else {
                current.retire(self.accounting)?;
            }
        }
        Ok(output)
    }
}
