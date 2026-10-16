use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    // One paged pass. Unique directories retain no name keys; legacy collisions
    // retain only ambiguous identities so unrelated siblings cost no cache space.
    pub(crate) async fn folded_collision_keys(
        &self,
        tree: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<Vec<Vec<u8>>>, FsError> {
        let mut keys = Owned::<Vec<Vec<u8>>>::allocate(0, accounting)?;
        // Entry count can exceed file count through hard links. Actual growth
        // is charged to the caller; this bound is only Vec's addressable size.
        let maximum = isize::MAX as usize / size_of::<Vec<u8>>();
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) = crate::kernel::list_tree_entries_async_with_accounting(
                &self.volume.fs.inner.objects,
                tree,
                (
                    after.as_ref().map_or(std::ops::Bound::Unbounded, |name| {
                        std::ops::Bound::Excluded(&name.0)
                    }),
                    super::names::CASE_FOLD_COLLISION_SCAN_PAGE,
                ),
                DecodeLimits::for_volume(self.volume.config),
                accounting,
                cancellation,
                Some(&self.dependencies.data_custody),
            )
            .await?;
            for entry in &page.entries {
                accounting.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                keys.ensure_for_push(maximum, accounting)?;
                let (key, mut owner) = super::names::name_fold_key(&entry.name, accounting)?;
                keys.bytes = keys
                    .bytes
                    .checked_add(key.capacity() as u64)
                    .ok_or(WorkError::Overflow)?;
                keys.allocation.absorb(&mut owner)?;
                keys.value.push(key);
            }
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                super::names::retire_name_owned(old, bytes, accounting)?;
            }
            let has_more = page.has_more;
            if has_more && let Some(entry) = page.entries.last() {
                after = Some(entry.name.clone_with_accounting(accounting, cancellation)?);
            }
            let bytes = (page.entries.capacity() * size_of::<crate::kernel::TreeEntry>()) as u64
                + page
                    .entries
                    .iter()
                    .map(|entry| entry.name.retained_bytes() as u64)
                    .sum::<u64>();
            super::names::retire_name_owned((page, allocation), bytes, accounting)?;
            if !has_more {
                break;
            }
        }
        keys.value.sort_unstable();
        retain_ambiguous_keys(keys, accounting)
    }
}

// Exactly one key is retained per ambiguous identity, in sorted order.
pub(crate) fn ambiguous_folded_name(
    keys: &[Vec<u8>],
    name: &LogicalName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    if keys.is_empty() {
        return Ok(false);
    }
    let key = super::names::name_fold_key(name, accounting)?;
    let ambiguous = keys.binary_search(&key.0).is_ok();
    let bytes = key.0.capacity() as u64;
    super::names::retire_name_owned(key, bytes, accounting)?;
    Ok(ambiguous)
}

fn retain_ambiguous_keys(
    mut keys: Owned<Vec<Vec<u8>>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<Vec<u8>>>, FsError> {
    accounting.charge(WorkCounters {
        items_examined: (keys.value.len() as u64)
            .checked_mul(2)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    })?;
    let mut cursor = 0;
    let mut retained_count = 0;
    while cursor < keys.value.len() {
        let mut end = cursor + 1;
        while end < keys.value.len() && keys.value[end] == keys.value[cursor] {
            end += 1;
        }
        if end > cursor + 1 {
            keys.value.swap(cursor, retained_count);
            retained_count += 1;
        }
        cursor = end;
    }
    if retained_count == 0 {
        keys.retire(accounting)?;
        return Owned::allocate(0, accounting);
    }
    keys.value.truncate(retained_count);
    let payload = keys
        .value
        .iter()
        .map(|key| key.capacity() as u64)
        .sum::<u64>();
    let backing = (keys.value.capacity() * size_of::<Vec<u8>>()) as u64;
    let discarded = keys
        .bytes
        .checked_sub(backing)
        .and_then(|bytes| bytes.checked_sub(payload))
        .ok_or(WorkError::Overflow)?;
    accounting.release_owned(&mut keys.allocation, discarded)?;
    keys.bytes -= discarded;
    let mut retained = Owned::<Vec<Vec<u8>>>::allocate(retained_count, accounting)?;
    retained.value.append(&mut keys.value);
    let mut owner = crate::kernel::split_allocation(&mut keys.allocation, payload)?;
    retained.allocation.absorb(&mut owner)?;
    retained.bytes = retained
        .bytes
        .checked_add(payload)
        .ok_or(WorkError::Overflow)?;
    keys.bytes -= payload;
    keys.retire(accounting)?;
    Ok(retained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Retained payload and backing capacity depend only on ambiguous identities,
    // and their custody exactly matches physical buffers, including after retire.
    fn check_retention(mut input: Vec<Vec<u8>>) -> Result<(), Box<dyn std::error::Error>> {
        input.sort_unstable();
        let expected = input
            .windows(2)
            .filter(|pair| pair[0] == pair[1])
            .map(|pair| pair[0].clone())
            .collect::<std::collections::BTreeSet<_>>();
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut keys = Owned::<Vec<Vec<u8>>>::allocate(input.len() * 2, &mut accounting)?;
        for key in input {
            let mut owned = Owned::<Vec<u8>>::allocate(key.len() + 7, &mut accounting)?;
            owned.value.extend_from_slice(&key);
            keys.bytes += owned.bytes;
            keys.allocation.absorb(&mut owned.allocation)?;
            keys.value.push(owned.value);
        }
        let keys = retain_ambiguous_keys(keys, &mut accounting)?;
        assert_eq!(
            keys.value
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert_eq!(keys.value.len(), expected.len());
        assert_eq!(keys.value.capacity(), expected.len());
        let physical = (keys.value.capacity() * size_of::<Vec<u8>>()
            + keys.value.iter().map(Vec::capacity).sum::<usize>()) as u64;
        assert_eq!(keys.bytes, physical);
        let KernelAccounting::Standalone { allocations, .. } = &accounting else {
            return Err("expected standalone custody".into());
        };
        assert_eq!(allocations.live_bytes(), physical);
        keys.retire(&mut accounting)?;
        let KernelAccounting::Standalone { allocations, .. } = &accounting else {
            return Err("expected standalone custody".into());
        };
        assert_eq!(allocations.live_bytes(), 0);
        Ok(())
    }

    #[test]
    fn ambiguous_keys_discard_large_unrelated_index() -> Result<(), Box<dyn std::error::Error>> {
        check_retention(
            [b"strasse".to_vec(), b"strasse".to_vec()]
                .into_iter()
                .chain((0..1024).map(|index| format!("sibling{index:04}").into_bytes()))
                .collect(),
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn ambiguous_keys_retain_only_collision_buffers(
            keys in prop::collection::vec(prop_oneof![
                prop::collection::vec(any::<u8>(), 0..24),
                prop::collection::vec(0_u8..4, 0..4),
            ], 0..64),
        ) {
            check_retention(keys).map_err(|error| TestCaseError::fail(error.to_string()))?;
        }
    }
}
