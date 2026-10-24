use super::entries::Entries;
use super::shared::SharedPage;
use super::*;
use crate::kernel::persistent_io::{OwnedPage, PageLease};

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    pub(super) async fn enter_node(
        &mut self,
        request: NodeRequest<F::Key>,
        mutations: &[(usize, &M)],
    ) -> Result<EnteredNode<F>, crate::FsError> {
        if request.height > self.limits.maximum_page_height {
            return Err(Self::error(Error::HeightExceeded));
        }
        self.maximum_seen_height = self.maximum_seen_height.max(request.height);
        let inserted = VisitedObjectSet::insert_with_accounting(
            &mut self.visited,
            request.page,
            self.accounting,
        )?;
        if !inserted.inserted {
            return Err(Self::error(Error::CycleOrAlias));
        }
        let decoded = self.read_page(request.page).await?;
        if let PageLease::Shared { page, .. } = &decoded.value
            && let SharedPage::Internal { children, .. } = page.as_ref()
        {
            if !children_within(
                Entries::Shared(children),
                request.lower.value.as_ref(),
                request.upper.value.as_ref(),
            ) {
                decoded.retire(self.accounting)?;
                return Err(Self::error(Error::ChildBoundsMismatch));
            }
            let entries = self.shared_internal_entries(children);
            decoded.retire(self.accounting)?;
            let entries = entries?;
            request.lower.retire(self.accounting)?;
            return Ok(EnteredNode::Internal(InternalFrame {
                original: request.page,
                entries,
                inherited_upper: request.upper,
                next_child: 0,
                mutation_cursor: request.mutations.start,
                mutation_end: request.mutations.end,
                height: request.height,
            }));
        }
        if let PageLease::Shared { page, .. } = &decoded.value
            && let SharedPage::Leaf { entries, .. } = page.as_ref()
        {
            if !leaf_within::<F>(
                Entries::Shared(entries),
                request.lower.value.as_ref(),
                request.upper.value.as_ref(),
            ) {
                decoded.retire(self.accounting)?;
                return Err(Self::error(Error::ChildBoundsMismatch));
            }
            request.lower.retire(self.accounting)?;
            request.upper.retire(self.accounting)?;
            let selected = mutations
                .get(request.mutations)
                .ok_or_else(|| Self::error(Error::MutationContract))?;
            return Ok(EnteredNode::Complete(
                self.rewrite_shared_leaf(request.page, decoded, selected)
                    .await?,
            ));
        }
        let Owned {
            value,
            allocation,
            bytes,
        } = decoded;
        let ((page, bytes), allocation) = OwnedPage {
            page: value,
            logical_bytes: bytes,
        }
        .into_owned_with_accounting(allocation, self.accounting)?;
        match page {
            Page::Leaf(entries) => {
                let entries = Owned::new(entries, allocation, bytes);
                if !leaf_within::<F>(
                    &entries.value,
                    request.lower.value.as_ref(),
                    request.upper.value.as_ref(),
                ) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                request.upper.retire(self.accounting)?;
                let selected = mutations
                    .get(request.mutations)
                    .ok_or_else(|| Self::error(Error::MutationContract))?;
                Ok(EnteredNode::Complete(
                    self.rewrite_leaf(request.page, entries, selected).await?,
                ))
            }
            Page::Internal(children) => {
                let children = Owned::new(children, allocation, bytes);
                if !children_within(
                    &children.value,
                    request.lower.value.as_ref(),
                    request.upper.value.as_ref(),
                ) {
                    return Err(Self::error(Error::ChildBoundsMismatch));
                }
                request.lower.retire(self.accounting)?;
                Ok(EnteredNode::Internal(InternalFrame {
                    original: request.page,
                    entries: super::internal::InternalEntries::Owned {
                        children,
                        rewritten: Owned::allocate(0, self.accounting)?,
                    },
                    inherited_upper: request.upper,
                    next_child: 0,
                    mutation_cursor: request.mutations.start,
                    mutation_end: request.mutations.end,
                    height: request.height,
                }))
            }
        }
    }
}
