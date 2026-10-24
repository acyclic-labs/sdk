use super::super::allocation::{KernelAccounting, KernelOwnedAllocation, arc_control_bytes};
use crate::FsError;
use crate::foundation::usize_to_u64;
use crate::kernel::persistent_point::{Owned, split_allocation};
use crate::performance::{WorkCounters, WorkError};
use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) struct Entry<T> {
    value: T,
    pub(crate) bytes: u64,
    // The value retires before its original custody.
    _allocation: KernelOwnedAllocation,
}

impl<T> Deref for Entry<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T: PartialEq> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Eq> Eq for Entry<T> {}

pub(crate) fn share<T>(
    value: Owned<T>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Arc<Entry<T>>>, FsError> {
    share_with_control(value, accounting, arc_control_bytes::<Entry<T>>())
}

fn share_with_control<T>(
    mut value: Owned<T>,
    accounting: &mut KernelAccounting<'_>,
    control_bytes: Result<u64, WorkError>,
) -> Result<Owned<Arc<Entry<T>>>, FsError> {
    let result = (|| {
        let control_bytes = control_bytes?;
        let bytes = value
            .bytes
            .checked_add(control_bytes)
            .ok_or(WorkError::Overflow)?;
        let empty = accounting.claim_owned(0, 0)?;
        let mut controls = accounting.claim_owned(control_bytes, 1)?;
        value.allocation.absorb(&mut controls)?;
        value.bytes = bytes;
        accounting.charge(WorkCounters {
            bytes_copied: usize_to_u64(size_of::<T>()),
            ..WorkCounters::UNCHARGED
        })?;
        Ok((empty, bytes))
    })();
    let (empty, bytes) = match result {
        Ok(result) => result,
        Err(error) => {
            value.retire(accounting)?;
            return Err(error);
        }
    };
    let local = if matches!(value.allocation, KernelOwnedAllocation::Standalone) {
        bytes
    } else {
        0
    };
    let Owned {
        value, allocation, ..
    } = value;
    Ok(Owned::new(
        Arc::new(Entry {
            value,
            bytes,
            _allocation: allocation,
        }),
        empty,
        local,
    ))
}

pub(crate) fn copy_entries<T>(
    entries: &[Arc<Entry<T>>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Owned<Vec<Arc<Entry<T>>>>, u64), FsError> {
    let mut copied = Owned::allocate(entries.len(), accounting)?;
    let mut logical_bytes = copied.bytes;
    let result = (|| {
        for entry in entries {
            logical_bytes = logical_bytes
                .checked_add(entry.bytes)
                .ok_or(WorkError::Overflow)?;
            accounting.charge(WorkCounters {
                bytes_copied: usize_to_u64(size_of::<Arc<Entry<T>>>()),
                ..WorkCounters::UNCHARGED
            })?;
            copied.value.push(Arc::clone(entry));
        }
        Ok::<_, FsError>(())
    })();
    if let Err(error) = result {
        copied.retire(accounting)?;
        return Err(error);
    }
    Ok((copied, logical_bytes))
}

pub(crate) fn share_values<T>(
    mut values: Owned<Vec<T>>,
    nested_bytes: impl Fn(&T) -> u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Owned<Vec<Arc<Entry<T>>>>, u64), FsError> {
    let mut output = match Owned::allocate(values.value.len(), accounting) {
        Ok(output) => output,
        Err(error) => {
            values.retire(accounting)?;
            return Err(error);
        }
    };
    let mut logical_bytes = output.bytes;
    let result = (|| {
        for value in std::mem::take(&mut values.value) {
            let bytes = nested_bytes(&value);
            let remaining = values.bytes.checked_sub(bytes).ok_or(WorkError::Overflow)?;
            let allocation = split_allocation(&mut values.allocation, bytes)?;
            values.bytes = remaining;
            let mut entry = share(Owned::new(value, allocation, bytes), accounting)?;
            let Some(total) = output.bytes.checked_add(entry.bytes) else {
                entry.retire(accounting)?;
                return Err(WorkError::Overflow.into());
            };
            let Some(logical_total) = logical_bytes.checked_add(entry.value.bytes) else {
                entry.retire(accounting)?;
                return Err(WorkError::Overflow.into());
            };
            if let Err(error) = output.allocation.absorb(&mut entry.allocation) {
                entry.retire(accounting)?;
                return Err(error);
            }
            output.bytes = total;
            logical_bytes = logical_total;
            accounting.charge(WorkCounters {
                bytes_copied: usize_to_u64(size_of::<Arc<Entry<T>>>()),
                ..WorkCounters::UNCHARGED
            })?;
            output.value.push(entry.value);
        }
        Ok(())
    })();
    let retired = values.retire(accounting);
    if let Err(error) = result.and(retired) {
        output.retire(accounting)?;
        return Err(error);
    }
    Ok((output, logical_bytes))
}

pub(crate) enum Entries<'a, T> {
    Plain(&'a [T]),
    Shared(&'a [Arc<Entry<T>>]),
}
impl<T> Copy for Entries<'_, T> {}
impl<T> Clone for Entries<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Default for Entries<'_, T> {
    fn default() -> Self {
        Self::Plain(&[])
    }
}
impl<'a, T> From<&'a [T]> for Entries<'a, T> {
    fn from(values: &'a [T]) -> Self {
        Self::Plain(values)
    }
}
impl<'a, T> From<&'a [Arc<Entry<T>>]> for Entries<'a, T> {
    fn from(values: &'a [Arc<Entry<T>>]) -> Self {
        Self::Shared(values)
    }
}
impl<'a, T> Entries<'a, T> {
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Plain(values) => values.len(),
            Self::Shared(values) => values.len(),
        }
    }
    pub(crate) fn is_empty(self) -> bool {
        self.len() == 0
    }
    pub(crate) fn get(self, index: usize) -> Option<&'a T> {
        match self {
            Self::Plain(values) => values.get(index),
            Self::Shared(values) => values.get(index).map(|value| &value.value),
        }
    }
    pub(crate) fn slice(self, range: std::ops::Range<usize>) -> Option<Self> {
        match self {
            Self::Plain(values) => values.get(range).map(Self::Plain),
            Self::Shared(values) => values.get(range).map(Self::Shared),
        }
    }
    pub(crate) fn first(self) -> Option<&'a T> {
        self.get(0)
    }
    pub(crate) fn last(self) -> Option<&'a T> {
        self.len().checked_sub(1).and_then(|index| self.get(index))
    }
    pub(crate) fn iter(self) -> Iter<'a, T> {
        match self {
            Self::Plain(values) => Iter::Plain(values.iter()),
            Self::Shared(values) => Iter::Shared(values.iter()),
        }
    }
    pub(crate) fn search(
        self,
        known_before: usize,
        accounting: &mut KernelAccounting<'_>,
        mut compare: impl FnMut(&T) -> std::cmp::Ordering,
    ) -> Result<Result<usize, usize>, FsError> {
        use crate::kernel::search::binary_search_from_with_accounting;
        if self.is_empty() && known_before == 0 {
            return Ok(Err(0));
        }
        match self {
            Self::Plain(values) => {
                binary_search_from_with_accounting(values, known_before, accounting, compare)
            }
            Self::Shared(values) => {
                binary_search_from_with_accounting(values, known_before, accounting, |entry| {
                    compare(&entry.value)
                })
            }
        }
    }
    pub(crate) fn partition(
        self,
        accounting: &mut KernelAccounting<'_>,
        mut before: impl FnMut(&T) -> bool,
    ) -> Result<usize, FsError> {
        use crate::kernel::search::partition_point_with_accounting;
        match self {
            Self::Plain(values) => partition_point_with_accounting(values, accounting, before),
            Self::Shared(values) => {
                partition_point_with_accounting(values, accounting, |entry| before(&entry.value))
            }
        }
    }
}

pub(crate) enum Iter<'a, T> {
    Plain(std::slice::Iter<'a, T>),
    Shared(std::slice::Iter<'a, Arc<Entry<T>>>),
}
impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Plain(values) => Self::Plain(values.clone()),
            Self::Shared(values) => Self::Shared(values.clone()),
        }
    }
}
impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next(),
            Self::Shared(values) => values.next().map(|entry| &entry.value),
        }
    }
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.nth(n),
            Self::Shared(values) => values.nth(n).map(|entry| &entry.value),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
    fn count(self) -> usize {
        self.len()
    }
    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}
impl<T> DoubleEndedIterator for Iter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.next_back(),
            Self::Shared(values) => values.next_back().map(|entry| &entry.value),
        }
    }
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            Self::Plain(values) => values.nth_back(n),
            Self::Shared(values) => values.nth_back(n).map(|entry| &entry.value),
        }
    }
}
impl<T> ExactSizeIterator for Iter<'_, T> {
    fn len(&self) -> usize {
        match self {
            Self::Plain(values) => values.len(),
            Self::Shared(values) => values.len(),
        }
    }
}
impl<T> std::iter::FusedIterator for Iter<'_, T> {}

impl<'a, T> From<&'a Vec<T>> for Entries<'a, T> {
    fn from(values: &'a Vec<T>) -> Self {
        Self::Plain(values)
    }
}
impl<'a, T, const N: usize> From<&'a [T; N]> for Entries<'a, T> {
    fn from(values: &'a [T; N]) -> Self {
        Self::Plain(values)
    }
}
impl<'a, T> From<&Entries<'a, T>> for Entries<'a, T> {
    fn from(values: &Entries<'a, T>) -> Self {
        *values
    }
}
impl<'a, T> IntoIterator for Entries<'a, T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "../tests/persistent_btree/entry_custody.rs"]
mod custody_models;

#[cfg(all(test, not(acyclic_fs_loom)))]
#[path = "../tests/persistent_btree/entry_views.rs"]
mod view_properties;

#[cfg(all(test, not(acyclic_fs_loom)))]
#[path = "../tests/persistent_btree/entry_allocations.rs"]
mod allocation_properties;
