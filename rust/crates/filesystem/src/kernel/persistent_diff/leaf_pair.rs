use super::*;

fn shared_leaf<F: Format>(page: &OwnedPage<F>) -> Option<Entries<'_, F::Value>> {
    match &page.page {
        PageLease::Shared { .. } => match page.page.view() {
            PageRef::Leaf(entries) => Some(entries),
            PageRef::Internal(_) => None,
        },
        PageLease::Owned(_) => None,
    }
}

pub(super) fn can_compare<F: Format, const EARLY_EQUAL: bool>(
    left: &OwnedPage<F>,
    right: &OwnedPage<F>,
    maximum: usize,
) -> bool {
    #[cfg(all(
        test,
        feature = "memory",
        not(acyclic_fs_loom),
        not(target_arch = "wasm32")
    ))]
    if super::tests::borrowed::restore_clones() {
        return false;
    }
    let (Some(left), Some(right)) = (shared_leaf(left), shared_leaf(right)) else {
        return false;
    };
    // Closure may cancel equal pairs on every frontier. Public bounded diffs
    // retain the old intermediate union bound even before equal values cancel.
    EARLY_EQUAL
        || left
            .len()
            .checked_add(right.len())
            .is_some_and(|total| total <= maximum)
}

pub(super) fn compare<F: Format, const PREFIX: bool, const RESET: bool>(
    left: &OwnedPage<F>,
    right: &OwnedPage<F>,
    values: &mut Changes<F>,
    maximum: usize,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
) -> Result<bool, FsError> {
    let mut left = shared_leaf(left)
        .ok_or(FsError::InvalidDiff)?
        .iter()
        .peekable();
    let mut right = shared_leaf(right)
        .ok_or(FsError::InvalidDiff)?
        .iter()
        .peekable();
    let initial = if RESET {
        None
    } else {
        Some(values.value.len())
    };
    let (mut before_cursor, mut after_cursor) = (initial, initial);
    while left.peek().is_some() || right.peek().is_some() {
        if exceeds_bound(values.value.len(), maximum) {
            return Ok(true);
        }
        accounting.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let ordering = match (left.peek(), right.peek()) {
            (Some(left), Some(right)) => F::key(left).cmp(F::key(right)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => break,
        };
        let before = (ordering != std::cmp::Ordering::Greater)
            .then(|| left.next())
            .flatten();
        let after = (ordering != std::cmp::Ordering::Less)
            .then(|| right.next())
            .flatten();
        if before == after {
            continue;
        }
        for (value, before, cursor) in [
            (before, true, &mut before_cursor),
            (after, false, &mut after_cursor),
        ] {
            if let Some(value) = value {
                let (value, guard) = persistent_io::clone_value::<F>(
                    value,
                    limits.maximum_name_bytes,
                    accounting,
                    None,
                    0,
                )?;
                let bytes = F::value_owned_bytes(&value);
                insert_value::<F, PREFIX>(
                    values,
                    Owned::new(value, guard, bytes),
                    before,
                    maximum,
                    limits,
                    accounting,
                    cursor,
                )?;
            }
        }
    }
    Ok(values.value.len() > maximum)
}

fn exceeds_bound(count: usize, maximum: usize) -> bool {
    #[cfg(all(
        test,
        feature = "memory",
        not(acyclic_fs_loom),
        not(target_arch = "wasm32")
    ))]
    if super::tests::borrowed::restore_unbounded_pair() {
        return false;
    }
    count > maximum
}
