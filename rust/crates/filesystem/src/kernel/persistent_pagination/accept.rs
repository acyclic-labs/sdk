use super::*;
use crate::kernel::persistent_btree::{Page, PageRef};
use crate::kernel::persistent_io::OwnedPage;

impl<F: Format> Machine<'_, '_, F> {
    pub(super) fn accept(
        &mut self,
        decoded: OwnedPage<F>,
        allocation: KernelOwnedAllocation,
        mut pending: Pending<F::Key>,
    ) -> Result<(), Failure> {
        let result = {
            let mut allocation = allocation;
            let logical_bytes = decoded.logical_bytes;
            let page = decoded.page;
            let result = match page {
                persistent_io::PageLease::Owned(Page::Leaf(values)) => {
                    self.accept_leaf_owned(values, logical_bytes, &mut allocation, &pending)
                }
                persistent_io::PageLease::Owned(Page::Internal(children)) => self
                    .accept_internal_owned(children, logical_bytes, &mut allocation, &mut pending),
                shared @ persistent_io::PageLease::Shared { .. } => {
                    // Keep the decoded cache's original guard through the entire callback.
                    let result = match shared.view() {
                        PageRef::Leaf(values) => self.accept_shared_leaf(values, &pending),
                        PageRef::Internal(children) => {
                            self.accept_shared_internal_state(children, &mut pending)
                        }
                    };
                    drop(shared);
                    result
                }
            };
            result
        };
        pending
            .retire(self.accounting)
            .map_err(|error| self.failure(error))?;
        result?;
        Ok(())
    }

    fn accept_shared_leaf(
        &mut self,
        values: Entries<'_, F::Value>,
        pending: &Pending<F::Key>,
    ) -> Result<(), Failure> {
        if !self.leaf_bounds(values, pending)? {
            return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
        }
        let start = self.value_start(values)?;
        for value in values
            .iter()
            .skip(start)
            .take(self.target.saturating_sub(self.values.len()))
        {
            self.examine_value(value)?;
            let prefix = begin_custody_leaf(self.accounting, self.data_custody)
                .map_err(|error| self.failure(error))?;
            let result = persistent_io::clone_value::<F>(
                value,
                self.limits.maximum_name_bytes,
                self.accounting,
                None,
                0,
            );
            let completed = finish_custody_leaf(self.accounting, self.data_custody, prefix);
            let (cloned, allocation) = result.map_err(|error| self.failure(error))?;
            let nested = F::value_owned_bytes(&cloned);
            let cloned = Owned::new(cloned, allocation, nested);
            if let Err(error) = completed {
                cloned
                    .retire(self.accounting)
                    .map_err(|error| self.failure(error))?;
                return Err(self.failure(error));
            }
            self.push_value(cloned)?;
        }
        Ok(())
    }

    fn accept_shared_internal_state(
        &mut self,
        children: Entries<'_, Child<F::Key>>,
        pending: &mut Pending<F::Key>,
    ) -> Result<(), Failure> {
        if !self.child_bounds(children, pending)? {
            return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
        }
        let start = self.child_start(children)?;
        let height = self.next_height(pending.height)?;
        let end = start
            .saturating_add(
                self.target
                    .saturating_sub(self.values.len())
                    .saturating_add(1),
            )
            .min(children.len());
        if let Some(child) = children.get(end) {
            self.successor_page = Some(child.page);
        }
        let mut next_lower: Option<Owned<F::Key>> = None;
        let result = (|| {
            for (offset, child) in children.iter().enumerate().take(end).skip(start).rev() {
                let lower = self.clone_key(&child.first)?;
                let upper = next_lower.take().or_else(|| pending.upper.take());
                let previous = if offset == start {
                    Ok(None)
                } else {
                    self.clone_key(&child.first).map(Some)
                };
                let previous = match previous {
                    Ok(previous) => previous,
                    Err(failure) => {
                        self.retire_bounds(Some(lower), upper)?;
                        return Err(failure);
                    }
                };
                if let Err(failure) = self.push_pending(Pending {
                    page: child.page,
                    lower: Some(lower),
                    upper,
                    height,
                }) {
                    self.retire_bounds(previous, None)?;
                    return Err(failure);
                }
                next_lower = previous;
            }
            Ok(())
        })();
        self.retire_bounds(next_lower, None)?;
        result
    }

    fn next_height(&self, height: u16) -> Result<u16, Failure> {
        height
            .checked_add(1)
            .filter(|&next| next <= self.limits.maximum_page_height)
            .ok_or_else(|| failed(Error::HeightExceeded, self.accounting.spent()))
    }

    fn retire_bounds(
        &mut self,
        lower: Option<Owned<F::Key>>,
        upper: Option<Owned<F::Key>>,
    ) -> Result<(), Failure> {
        if let Some(lower) = lower {
            lower
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        if let Some(upper) = upper {
            upper
                .retire(self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        Ok(())
    }

    fn accept_leaf_owned(
        &mut self,
        values: Vec<F::Value>,
        decoded_bytes: u64,
        allocation: &mut KernelOwnedAllocation,
        pending: &Pending<F::Key>,
    ) -> Result<(), Failure> {
        let mut retained_nested = 0_u64;
        let result = (|| {
            if !self.leaf_bounds(&values, pending)? {
                return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
            }
            let start = self.value_start(&values)?;
            // Iterator destruction drops every skipped/unselected item and the source buffer.
            for value in values.into_iter().skip(start) {
                if self.values.len() == self.target {
                    break;
                }
                let nested = self.examine_value(&value)?;
                let tag =
                    split_allocation(allocation, nested).map_err(|error| self.failure(error))?;
                retained_nested = retained_nested
                    .checked_add(nested)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.accounting.spent()))?;
                self.push_value(Owned::new(value, tag, nested))?;
            }
            Ok(())
        })();
        let released = decoded_bytes
            .checked_sub(retained_nested)
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if matches!(allocation, KernelOwnedAllocation::Standalone) {
            retire_allocation(KernelOwnedAllocation::Standalone, released, self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        result
    }

    fn accept_internal_owned(
        &mut self,
        children: Vec<Child<F::Key>>,
        decoded_bytes: u64,
        allocation: &mut KernelOwnedAllocation,
        pending: &mut Pending<F::Key>,
    ) -> Result<(), Failure> {
        let mut moved_nested = 0_u64;
        let mut next_lower: Option<Owned<F::Key>> = None;
        let result = (|| {
            if !self.child_bounds(&children, pending)? {
                return Err(failed(Error::ChildBoundsMismatch, self.accounting.spent()));
            }
            let start = self.child_start(&children)?;
            let height = self.next_height(pending.height)?;
            let end = start
                .saturating_add(
                    self.target
                        .saturating_sub(self.values.len())
                        .saturating_add(1),
                )
                .min(children.len());
            if let Some(child) = children.get(end) {
                self.successor_page = Some(child.page);
            }
            let mut children = children;
            children.truncate(end);
            let mut tail = children.drain(start..);
            while let Some(child) = tail.next_back() {
                let previous = if tail.len() == 0 {
                    None
                } else {
                    Some(self.clone_key(&child.first)?)
                };
                let bytes = F::key_owned_bytes(&child.first);
                let tag = match split_allocation(allocation, bytes) {
                    Ok(tag) => tag,
                    Err(error) => {
                        self.retire_bounds(previous, None)?;
                        return Err(self.failure(error));
                    }
                };
                moved_nested = moved_nested
                    .checked_add(bytes)
                    .ok_or_else(|| failed(Error::AllocationFailed, self.accounting.spent()))?;
                let lower = Owned::new(child.first, tag, bytes);
                let upper = next_lower.take().or_else(|| pending.upper.take());
                if let Err(failure) = self.push_pending(Pending {
                    page: child.page,
                    lower: Some(lower),
                    upper,
                    height,
                }) {
                    self.retire_bounds(previous, None)?;
                    return Err(failure);
                }
                next_lower = previous;
            }
            drop(tail);
            drop(children);
            Ok(())
        })();
        self.retire_bounds(next_lower, None)?;
        let released = decoded_bytes
            .checked_sub(moved_nested)
            .ok_or_else(|| failed(Error::TraversalState, self.accounting.spent()))?;
        if matches!(allocation, KernelOwnedAllocation::Standalone) {
            retire_allocation(KernelOwnedAllocation::Standalone, released, self.accounting)
                .map_err(|error| self.failure(error))?;
        }
        result
    }
}
