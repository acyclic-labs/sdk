use super::entries::Entries;
use super::*;
use crate::kernel::persistent_io::PageLease;
use crate::storage::{HashedObject, OBJECT_DIGEST_ENVELOPE_BYTES};

impl<S, F, M> Context<'_, '_, '_, S, F, M>
where
    S: crate::AsyncObjectStore,
    F: Format,
    M: Mutation<F>,
{
    pub(super) async fn write_leaf_chunks<'a>(
        &mut self,
        entries: impl Into<Entries<'a, F::Value>>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let entries = entries.into();
        let (mut chunks, examined) =
            PageChunks::new(entries, self.limits, F::leaf_item_encoded_length)
                .map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut result = Owned::allocate(0, self.accounting)?;
        while let Some((chunk, examined)) = chunks.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            let first = self.clone_key(F::key(
                chunk
                    .first()
                    .ok_or_else(|| Self::error(Error::MutationContract))?,
            ))?;
            result.ensure_for_push(entries.len(), self.accounting)?;
            let page = self.write_page(&PageRef::<F>::Leaf(chunk)).await?;
            if let Entries::Shared(entries) = chunk {
                persistent_io::retain_written_leaf::<S, F>(
                    self.store,
                    page,
                    self.limits,
                    entries,
                    self.accounting,
                )?;
            }
            push_summary(&mut result, first, page, self.accounting)?;
        }
        Ok(result)
    }

    pub(super) async fn write_internal_chunks<'a>(
        &mut self,
        children: impl Into<Entries<'a, Child<F::Key>>>,
    ) -> Result<Owned<Vec<Child<F::Key>>>, crate::FsError> {
        let children = children.into();
        let child_length = |child: &Child<F::Key>| F::internal_item_encoded_length(&child.first);
        let (mut counted, examined) =
            PageChunks::new(children, self.limits, child_length).map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut pages = 0;
        while let Some((_, examined)) = counted.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            pages += 1;
        }
        if children.len() > 1 && pages >= children.len() {
            return Err(Self::error(Error::PageItemTooLarge));
        }
        let (mut chunks, examined) =
            PageChunks::new(children, self.limits, child_length).map_err(Self::error)?;
        self.charge_items(examined)?;
        let mut result = Owned::allocate(pages, self.accounting)?;
        while let Some((chunk, examined)) = chunks.next().map_err(Self::error)? {
            self.charge_items(examined)?;
            let first = self.clone_key(
                &chunk
                    .first()
                    .ok_or_else(|| Self::error(Error::MutationContract))?
                    .first,
            )?;
            let page = self.write_page(&PageRef::<F>::Internal(chunk)).await?;
            if let Entries::Shared(children) = chunk {
                persistent_io::retain_written_internal::<S, F>(
                    self.store,
                    page,
                    self.limits,
                    children,
                    self.accounting,
                )?;
            }
            push_summary(&mut result, first, page, self.accounting)?;
        }
        Ok(result)
    }

    pub(super) async fn finish_root(
        &mut self,
        mut summaries: Owned<Vec<Child<F::Key>>>,
    ) -> Result<ObjectId, crate::FsError> {
        if summaries.value.is_empty() {
            summaries.retire(self.accounting)?;
            return self.write_page(&PageRef::<F>::Leaf((&[]).into())).await;
        }
        let mut height = self.maximum_seen_height;
        while summaries.value.len() > 1 {
            height = height
                .checked_add(1)
                .ok_or_else(|| Self::error(Error::HeightExceeded))?;
            if height > self.limits.maximum_page_height {
                return Err(Self::error(Error::HeightExceeded));
            }
            let next = self
                .write_internal_chunks(summaries.value.as_slice())
                .await?;
            summaries.retire(self.accounting)?;
            summaries = next;
        }
        let root = summaries
            .value
            .first()
            .ok_or_else(|| Self::error(Error::MutationContract))?
            .page;
        summaries.retire(self.accounting)?;
        Ok(root)
    }

    pub(super) async fn read_page(
        &mut self,
        page: ObjectId,
    ) -> Result<Owned<PageLease<F>>, crate::FsError> {
        let (decoded, allocation) = persistent_io::read_page_with_accounting::<S, F>(
            self.store,
            page,
            self.limits,
            self.accounting,
            self.cancellation,
            None,
        )
        .await?;
        Ok(Owned::new(decoded.page, allocation, decoded.logical_bytes))
    }

    async fn write_page(&mut self, page: &PageRef<'_, F>) -> Result<ObjectId, crate::FsError> {
        self.accounting.check()?;
        self.accounting.admit(PAGE_WRITE)?;
        let encoded_length = F::page_encoded_length(page, self.limits.maximum_page_items)?;
        if usize_to_u64(encoded_length) > self.limits.maximum_page_object_bytes() {
            return Err(Self::error(Error::PageItemTooLarge));
        }
        let (encoded, allocation) =
            F::encode_with_accounting(page, self.limits.maximum_page_items, self.accounting)?;
        let capacity = usize_to_u64(encoded.capacity());
        let mut encoded = Owned::new(encoded, allocation, capacity);
        if encoded.value.len() != encoded_length {
            return Err(Self::error(Error::MutationContract));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.accounting.native_work() {
            let KernelOwnedAllocation::NativeOriginal(allocation) = &mut encoded.allocation else {
                return Err(Self::error(Error::MutationContract));
            };
            let hashed = HashedObject::from_native_vec(
                F::kind(),
                &mut encoded.value,
                allocation,
                work,
                self.cancellation,
            )
            .map_err(storage_error)?;
            let object = hashed.object_id();
            self.accounting.charge(PAGE_WRITE)?;
            {
                let __object_args = (hashed, work, self.cancellation);
                let mut __object_work =
                    crate::AuthorityWork::native(__object_args.1, __object_args.2);

                crate::AsyncObjectStore::put_hashed(self.store, __object_args.0, &mut __object_work)
                    .await
            }
            .map_err(storage_error)?;
            drop(encoded);
            return Ok(object);
        }
        let hashing = WorkCounters {
            bytes_hashed: usize_to_u64(encoded.value.len())
                .checked_add(OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        self.accounting.admit(hashing)?;
        let hashed = HashedObject::new(F::kind(), std::mem::take(&mut encoded.value).into());
        let object = hashed.object_id();
        self.accounting.charge(hashing)?;
        self.accounting.charge(PAGE_WRITE)?;
        let remaining = self.accounting.remaining()?;
        let res = {
            let __object_args = (hashed, remaining, self.cancellation);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = crate::AsyncObjectStore::put_hashed(
                self.store,
                __object_args.0,
                &mut __object_work,
            )
            .await;
            __object_work.receipt(__object_result)
        };
        match res {
            Ok(receipt) => self.accounting.charge(receipt.work)?,
            Err(failure) => {
                self.accounting.charge(*failure.work)?;
                encoded.retire(self.accounting)?;
                return Err(storage_error(failure.error));
            }
        }
        encoded.retire(self.accounting)?;
        Ok(object)
    }
}

const PAGE_WRITE: WorkCounters = WorkCounters {
    page_writes: 1,
    ..WorkCounters::UNCHARGED
};

pub(crate) const TARGET_PAGE_BYTES: usize = 2 * 1024;
const PAGE_HEADER_BYTES: usize = 8 + 2 + 1 + 4;

pub(super) struct PageChunks<'a, T, L> {
    items: Entries<'a, T>,
    encoded_length: L,
    start: usize,
    remaining_bytes: usize,
    maximum_items: usize,
    maximum_bytes: usize,
}

impl<'a, T, L> PageChunks<'a, T, L>
where
    L: Fn(&T) -> Result<usize, CanonicalDecodeError>,
{
    pub(super) fn new<E: std::error::Error>(
        items: impl Into<Entries<'a, T>>,
        limits: DecodeLimits,
        encoded_length: L,
    ) -> Result<(Self, u64), Error<E>> {
        let items = items.into();
        let mut remaining_bytes = 0_usize;
        for item in items {
            remaining_bytes = remaining_bytes
                .checked_add(encoded_length(item)?)
                .ok_or(Error::PageItemTooLarge)?;
        }
        Ok((
            Self {
                items,
                encoded_length,
                start: 0,
                remaining_bytes,
                maximum_items: usize::try_from(limits.maximum_page_items)
                    .map_err(|_| Error::InvalidLimits)?,
                maximum_bytes: usize::try_from(limits.maximum_page_bytes)
                    .map_err(|_| Error::InvalidLimits)?,
            },
            usize_to_u64(items.len()),
        ))
    }

    pub(super) fn next<E: std::error::Error>(
        &mut self,
    ) -> Result<Option<(Entries<'a, T>, u64)>, Error<E>> {
        let items = self.items;
        let Some(rest) = items
            .slice(self.start..items.len())
            .filter(|rest| !rest.is_empty())
        else {
            return Ok(None);
        };
        let pages = self
            .remaining_bytes
            .div_ceil(TARGET_PAGE_BYTES)
            .max(rest.len().div_ceil(self.maximum_items.max(1)))
            .max(1);
        let share = self.remaining_bytes.div_ceil(pages);
        let mut count = 0_usize;
        let mut payload = 0_usize;
        for item in rest {
            if count == self.maximum_items || (count >= 2 && payload >= share) {
                break;
            }
            let item_bytes = (self.encoded_length)(item)?;
            let next = payload
                .checked_add(item_bytes)
                .ok_or(Error::PageItemTooLarge)?;
            if next.saturating_add(PAGE_HEADER_BYTES) > self.maximum_bytes {
                break;
            }
            payload = next;
            count += 1;
        }
        let page = rest
            .slice(0..count)
            .filter(|page| !page.is_empty())
            .ok_or(Error::PageItemTooLarge)?;
        self.start += count;
        self.remaining_bytes = self.remaining_bytes.saturating_sub(payload);
        Ok(Some((page, usize_to_u64(count))))
    }
}
