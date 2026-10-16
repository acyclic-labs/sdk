use super::*;

impl<F: Format> Machine<'_, F> {
    pub(super) fn accept(
        &mut self,
        request: Request<F::Key>,
        decoded: persistent_io::OwnedPage<F>,
        allocation: KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if let persistent_io::PageLease::Shared { page, .. } = &decoded.page {
            let accepted = match page.as_ref() {
                Page::Leaf(entries) => self.accept_shared_leaf(&request, entries, accounting),
                Page::Internal(children) => self.accept_internal(&request, children, accounting),
            };
            let bytes = decoded.logical_bytes;
            drop(decoded);
            super::super::persistent_point::retire_allocation(allocation, bytes, accounting)?;
            Self::retire_request(request, accounting)?;
            return accepted;
        }
        let mut page = match decoded.into_owned_with_accounting(allocation, accounting) {
            Ok(((page, bytes), allocation)) => Owned::new(page, allocation, bytes),
            Err(error) => {
                Self::retire_request(request, accounting)?;
                return Err(error);
            }
        };
        let accepted = match &mut page.value {
            Page::Leaf(entries) => self.accept_leaf(
                &request,
                entries,
                &mut page.allocation,
                &mut page.bytes,
                accounting,
            ),
            Page::Internal(children) => self.accept_internal(&request, children, accounting),
        };
        page.retire(accounting)?;
        Self::retire_request(request, accounting)?;
        accepted
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Leaf selection and copying share the same admitted cursor and allocation guard."
    )]
    fn accept_leaf(
        &mut self,
        request: &Request<F::Key>,
        entries: &mut Vec<F::Value>,
        page_allocation: &mut KernelOwnedAllocation,
        page_bytes: &mut u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if !leaf_within::<F>(
            entries,
            request.lower.as_ref().map(|key| &key.value),
            request.upper.as_ref().map(|key| &key.value),
        ) {
            return Err(query_error(Error::ChildBoundsMismatch));
        }
        self.matches.value.clear();
        let mut query = request.queries.start;
        while query < request.queries.end {
            let group_end =
                equal_group(&self.ordered.value, query, request.queries.end, accounting)?;
            let key = self
                .ordered
                .value
                .get(query)
                .ok_or_else(|| query_error(Error::InvalidRouting))?
                .key;
            let found =
                binary_search_with_accounting(entries, accounting, |value| F::key(value).cmp(key))?;
            if let Ok(entry) = found {
                let copied = WorkCounters {
                    bytes_copied: usize_to_u64(std::mem::size_of::<Match>()),
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                self.matches.value.push(Match {
                    entry,
                    queries: query..group_end,
                });
                accounting.record_completed(copied)?;
            }
            query = group_end;
        }
        while let Some(found) = self.matches.value.pop() {
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(
                    std::mem::size_of::<F::Value>()
                        * (1 + usize::from(found.entry + 1 != entries.len())),
                ),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let value = entries.swap_remove(found.entry);
            let nested = F::value_owned_bytes(&value);
            let allocation = split_allocation(page_allocation, nested)?;
            *page_bytes = page_bytes
                .checked_sub(nested)
                .ok_or(crate::WorkError::Overflow)?;
            let value = Owned::new(value, allocation, nested);
            if let Err(error) = accounting.record_completed(copied) {
                value.retire(accounting)?;
                return Err(error);
            }
            let transferred = (|| {
                let mut queries = found.queries;
                let first = queries
                    .next()
                    .ok_or_else(|| query_error(Error::InvalidRouting))?;
                for query in queries {
                    self.clone_result(query, &value.value, accounting)?;
                }
                let ordinal = self
                    .ordered
                    .value
                    .get(first)
                    .ok_or_else(|| query_error(Error::InvalidRouting))?
                    .ordinal;
                self.values
                    .value
                    .get_mut(ordinal)
                    .ok_or_else(|| query_error(Error::InvalidRouting))?;
                accounting.admit(WorkCounters {
                    bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()),
                    ..WorkCounters::UNCHARGED
                })?;
                Ok(ordinal)
            })();
            match transferred {
                Ok(ordinal) => {
                    let Owned {
                        value,
                        mut allocation,
                        bytes,
                    } = value;
                    self.values.allocation.absorb(&mut allocation)?;
                    self.values.bytes = self
                        .values
                        .bytes
                        .checked_add(bytes)
                        .ok_or(crate::WorkError::Overflow)?;
                    *self
                        .values
                        .value
                        .get_mut(ordinal)
                        .ok_or_else(|| query_error(Error::InvalidRouting))? = Some(value);
                    accounting.record_completed(WorkCounters {
                        bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()),
                        ..WorkCounters::UNCHARGED
                    })?;
                }
                Err(error) => {
                    value.retire(accounting)?;
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn clone_result(
        &mut self,
        query: usize,
        value: &F::Value,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let ordinal = self
            .ordered
            .value
            .get(query)
            .ok_or_else(|| query_error(Error::InvalidRouting))?
            .ordinal;
        let destination = self
            .values
            .value
            .get_mut(ordinal)
            .ok_or_else(|| query_error(Error::InvalidRouting))?;
        let (cloned, allocation) = persistent_io::clone_value::<F>(
            value,
            self.limits.maximum_name_bytes,
            accounting,
            None,
            0,
        )?;
        let bytes = F::value_owned_bytes(&cloned);
        let cloned = Owned::new(cloned, allocation, bytes);
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(std::mem::size_of::<F::Value>()),
            ..WorkCounters::UNCHARGED
        };
        if let Err(error) = accounting.admit(copied) {
            cloned.retire(accounting)?;
            return Err(error);
        }
        let Owned {
            value: cloned,
            mut allocation,
            ..
        } = cloned;
        self.values.allocation.absorb(&mut allocation)?;
        self.values.bytes = self
            .values
            .bytes
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        *destination = Some(cloned);
        accounting.record_completed(copied)?;
        Ok(())
    }

    fn accept_internal(
        &mut self,
        request: &Request<F::Key>,
        children: &[Child<F::Key>],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if !children_within(
            children,
            request.lower.as_ref().map(|key| &key.value),
            request.upper.as_ref().map(|key| &key.value),
        ) {
            return Err(query_error(Error::ChildBoundsMismatch));
        }
        let next_height = request
            .height
            .checked_add(1)
            .ok_or_else(|| query_error(Error::HeightExceeded))?;
        admit_height(next_height, self.limits.maximum_page_height).map_err(query_error)?;
        let mut query = request.queries.start;
        while query < request.queries.end {
            let key = self
                .ordered
                .value
                .get(query)
                .ok_or_else(|| query_error(Error::InvalidRouting))?
                .key;
            let child_index =
                partition_point_with_accounting(children, accounting, |child| child.first <= *key)?
                    .saturating_sub(1);
            let child = children
                .get(child_index)
                .ok_or_else(|| query_error(Error::InvalidRouting))?;
            let upper_ref = children
                .get(child_index + 1)
                .map(|next| &next.first)
                .or(request.upper.as_ref().map(|key| &key.value));
            let mut end = next_index(query).map_err(query_error)?;
            while end < request.queries.end {
                let delta = WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(delta)?;
                let past_upper = upper_ref.is_some_and(|upper| {
                    self.ordered
                        .value
                        .get(end)
                        .is_some_and(|query| query.key >= upper)
                });
                accounting.record_completed(delta)?;
                if past_upper {
                    break;
                }
                end += 1;
            }
            let (lower, lower_allocation) = persistent_io::clone_key::<F>(
                &child.first,
                self.limits.maximum_name_bytes,
                accounting,
                None,
                0,
            )?;
            let lower_bytes = F::key_owned_bytes(&lower);
            let lower = Owned::new(lower, lower_allocation, lower_bytes);
            let upper = match upper_ref {
                Some(key) => persistent_io::clone_key::<F>(
                    key,
                    self.limits.maximum_name_bytes,
                    accounting,
                    None,
                    0,
                )
                .map(|(key, allocation)| {
                    let bytes = F::key_owned_bytes(&key);
                    Some(Owned::new(key, allocation, bytes))
                }),
                None => Ok(None),
            };
            let upper = match upper {
                Ok(upper) => upper,
                Err(error) => {
                    lower.retire(accounting)?;
                    return Err(error);
                }
            };
            self.push_request(
                Request {
                    page: child.page,
                    lower: Some(lower),
                    upper,
                    queries: query..end,
                    height: next_height,
                },
                accounting,
            )?;
            query = end;
        }
        Ok(())
    }

    fn accept_shared_leaf(
        &mut self,
        request: &Request<F::Key>,
        entries: &[F::Value],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        if !leaf_within::<F>(
            entries,
            request.lower.as_ref().map(|key| &key.value),
            request.upper.as_ref().map(|key| &key.value),
        ) {
            return Err(query_error(Error::ChildBoundsMismatch));
        }
        let mut query = request.queries.start;
        while query < request.queries.end {
            let end = equal_group(&self.ordered.value, query, request.queries.end, accounting)?;
            let key = self
                .ordered
                .value
                .get(query)
                .ok_or_else(|| query_error(Error::InvalidRouting))?
                .key;
            if let Ok(index) =
                binary_search_with_accounting(entries, accounting, |value| F::key(value).cmp(key))?
            {
                let value = entries
                    .get(index)
                    .ok_or_else(|| query_error(Error::InvalidRouting))?;
                for query in query..end {
                    self.clone_result(query, value, accounting)?;
                }
            }
            query = end;
        }
        Ok(())
    }
}
