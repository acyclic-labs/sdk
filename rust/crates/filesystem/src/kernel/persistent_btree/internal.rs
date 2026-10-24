use super::entries::{Entries, Entry, copy_entries, share_values};
use super::*;
use std::sync::Arc;

pub(super) enum InternalEntries<F: Format> {
    Owned {
        children: Owned<Vec<Child<F::Key>>>,
        rewritten: Owned<Vec<Child<F::Key>>>,
    },
    Shared {
        children: Owned<Vec<Arc<Entry<Child<F::Key>>>>>,
        rewritten: Owned<Vec<Arc<Entry<Child<F::Key>>>>>,
    },
}

#[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
#[path = "../tests/persistent_btree/shared_internal.rs"]
mod tests;

impl<F: Format> InternalEntries<F> {
    fn children(&self) -> Entries<'_, Child<F::Key>> {
        match self {
            Self::Owned { children, .. } => Entries::Plain(&children.value),
            Self::Shared { children, .. } => Entries::Shared(&children.value),
        }
    }

    pub(super) fn append(
        &mut self,
        result: Owned<Vec<Child<F::Key>>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        match self {
            Self::Owned { rewritten, .. } => append_owned(rewritten, result, accounting),
            Self::Shared { rewritten, .. } => {
                let (result, _) =
                    share_values(result, |child| F::key_owned_bytes(&child.first), accounting)?;
                append_owned(rewritten, result, accounting)
            }
        }
    }
}

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    pub(super) fn shared_internal_entries(
        &mut self,
        children: &[Arc<Entry<Child<F::Key>>>],
    ) -> Result<InternalEntries<F>, crate::FsError> {
        let (children, _) = copy_entries(children, self.accounting)?;
        let rewritten = match Owned::allocate(children.value.len(), self.accounting) {
            Ok(value) => value,
            Err(error) => {
                children.retire(self.accounting)?;
                return Err(error);
            }
        };
        Ok(InternalEntries::Shared {
            children,
            rewritten,
        })
    }

    fn preserve_child(
        &mut self,
        entries: &mut InternalEntries<F>,
        index: usize,
    ) -> Result<(), crate::FsError> {
        match entries {
            InternalEntries::Owned {
                children,
                rewritten,
            } => {
                let child = children
                    .value
                    .get(index)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                let first = self.clone_key(&child.first)?;
                push_summary(rewritten, first, child.page, self.accounting)
            }
            InternalEntries::Shared {
                children,
                rewritten,
            } => {
                let child = children
                    .value
                    .get(index)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                rewritten.ensure_for_push(usize::MAX, self.accounting)?;
                self.accounting.charge(WorkCounters {
                    bytes_copied: usize_to_u64(std::mem::size_of::<Arc<Entry<Child<F::Key>>>>()),
                    ..WorkCounters::UNCHARGED
                })?;
                rewritten.value.push(Arc::clone(child));
                Ok(())
            }
        }
    }

    pub(super) fn advance_frame(
        &mut self,
        frame: &mut InternalFrame<F>,
        mutations: &[(usize, &M)],
    ) -> Result<Option<NodeRequest<F::Key>>, crate::FsError> {
        while let Some(child) = frame.entries.children().get(frame.next_child) {
            let index = frame.next_child;
            frame.next_child += 1;
            let selected_start = frame.mutation_cursor;
            if let Some(next) = frame.entries.children().get(index + 1) {
                while frame.mutation_cursor < frame.mutation_end {
                    self.charge_items(1)?;
                    let mutation = mutations
                        .get(frame.mutation_cursor)
                        .ok_or_else(|| Self::error(Error::MutationContract))?;
                    if mutation.1.key() >= &next.first {
                        break;
                    }
                    frame.mutation_cursor += 1;
                }
            } else {
                frame.mutation_cursor = frame.mutation_end;
            }
            self.charge_items(1)?;
            if selected_start == frame.mutation_cursor {
                self.preserve_child(&mut frame.entries, index)?;
                continue;
            }
            let lower = self.clone_bound(Some(&child.first))?;
            let upper = self.clone_bound(
                frame
                    .entries
                    .children()
                    .get(index + 1)
                    .map(|next| &next.first)
                    .or(frame.inherited_upper.value.as_ref()),
            )?;
            return Ok(Some(NodeRequest {
                page: child.page,
                lower,
                upper,
                mutations: selected_start..frame.mutation_cursor,
                height: frame
                    .height
                    .checked_add(1)
                    .ok_or_else(|| Self::error(Error::HeightExceeded))?,
            }));
        }
        Ok(None)
    }

    pub(super) async fn finish_frame(
        &mut self,
        frame: InternalFrame<F>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let result = match frame.entries {
            InternalEntries::Owned {
                children,
                rewritten,
            } => {
                let result = if rewritten.value.is_empty() {
                    rewritten
                } else if rewritten.value == children.value {
                    let result =
                        self.original_summary(frame.original, children.value.as_slice())?;
                    rewritten.retire(self.accounting)?;
                    result
                } else if frame.height == 1 && rewritten.value.len() == 1 {
                    rewritten
                } else {
                    let result = self
                        .write_internal_chunks(rewritten.value.as_slice())
                        .await?;
                    rewritten.retire(self.accounting)?;
                    result
                };
                children.retire(self.accounting)?;
                result
            }
            InternalEntries::Shared {
                children,
                rewritten,
            } => {
                let result = if rewritten.value.is_empty() {
                    Owned::allocate(0, self.accounting)?
                } else if rewritten.value == children.value {
                    self.original_summary(frame.original, Entries::Shared(&children.value))?
                } else if frame.height == 1 && rewritten.value.len() == 1 {
                    let first = rewritten
                        .value
                        .first()
                        .ok_or_else(|| Self::error(Error::MutationContract))?;
                    self.original_summary(first.page, Entries::Shared(&rewritten.value))?
                } else {
                    self.write_internal_chunks(Entries::Shared(&rewritten.value))
                        .await?
                };
                rewritten.retire(self.accounting)?;
                children.retire(self.accounting)?;
                result
            }
        };
        frame.inherited_upper.retire(self.accounting)?;
        Ok(result)
    }

    fn original_summary<'a>(
        &mut self,
        original: ObjectId,
        children: impl Into<Entries<'a, Child<F::Key>>>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let mut result = Owned::allocate(1, self.accounting)?;
        let first = children
            .into()
            .first()
            .ok_or_else(|| Self::error(Error::MutationContract))?;
        let first = self.clone_key(&first.first)?;
        push_summary(&mut result, first, original, self.accounting)?;
        Ok(result)
    }
}
