use super::*;

pub(super) struct BlobIndexBuilder {
    options: BlobBuildOptions,
    width: usize,
    leaf: ReadOwned<Vec<BlobChunkRef>>,
    levels: ReadOwned<Vec<ReadOwned<Vec<BlobChild>>>>,
}

impl BlobIndexBuilder {
    pub(super) fn new(options: BlobBuildOptions) -> Self {
        Self {
            options,
            width: effective_page_width(options).unwrap_or(0),
            leaf: empty_blob_vec(),
            levels: empty_blob_vec(),
        }
    }

    fn capacity(&self, level: usize) -> usize {
        let mut references = self
            .options
            .maximum_blob_bytes
            .div_ceil(u64::from(self.options.chunk_bytes));
        for _ in 0..level {
            references = references.div_ceil(self.width as u64);
        }
        usize::try_from(references)
            .unwrap_or(self.width)
            .min(self.width)
    }

    pub(super) async fn push_chunk<S: AsyncObjectStore>(
        &mut self,
        store: &mut BlobBatchStore<'_, S>,
        chunk: BlobChunkRef,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        let capacity = self.capacity(0);
        reserve_blob_vec(&mut self.leaf, capacity, accounting)?;
        self.leaf.value.push(chunk);
        if self.leaf.value.len() == self.width {
            self.flush_leaf(store, accounting, cancellation).await?;
        }
        Ok(())
    }

    async fn flush_leaf<S: AsyncObjectStore>(
        &mut self,
        store: &mut BlobBatchStore<'_, S>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        if self.leaf.value.is_empty() {
            return Ok(());
        }
        let items = std::mem::take(&mut self.leaf.value);
        let first_offset = items
            .first()
            .ok_or(BlobBuildError::InvalidIndexState)?
            .first_offset;
        let end_offset = items
            .last()
            .ok_or(BlobBuildError::InvalidIndexState)?
            .end_offset;
        let page = BlobPage {
            first_offset,
            end_offset,
            node: BlobNode::Leaf(items),
        };
        let result = put_blob_page(store, &page, self.options, accounting, cancellation).await;
        let BlobNode::Leaf(mut items) = page.node else {
            unreachable!()
        };
        items.clear();
        self.leaf.value = items;
        let page = result?;
        self.push_child(
            store,
            0,
            BlobChild {
                first_offset,
                end_offset,
                page,
            },
            accounting,
            cancellation,
        )
        .await
    }

    fn push_child<'a, 'store, 'accounting, S: AsyncObjectStore>(
        &'a mut self,
        store: &'a mut BlobBatchStore<'store, S>,
        mut level: usize,
        mut child: BlobChild,
        accounting: &'a mut KernelAccounting<'accounting>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<(), FsError>> + acyclic_stream::ProviderTask + 'a {
        let cancellation = cancellation.clone();
        async move {
            loop {
                while self.levels.value.len() <= level {
                    let capacity = self
                        .levels
                        .value
                        .len()
                        .checked_add(1)
                        .ok_or(WorkError::Overflow)?;
                    reserve_blob_vec(&mut self.levels, capacity, accounting)?;
                    self.levels.value.push(empty_blob_vec());
                }
                let capacity = self.capacity(level + 1);
                let items = self
                    .levels
                    .value
                    .get_mut(level)
                    .ok_or(BlobBuildError::InvalidIndexState)?;
                reserve_blob_vec(items, capacity, accounting)?;
                items.value.push(child);
                if items.value.len() < self.width {
                    return Ok(());
                }
                child = self
                    .flush_internal(store, level, accounting, &cancellation)
                    .await?;
                level = level.checked_add(1).ok_or(WorkError::Overflow)?;
            }
        }
    }

    async fn flush_internal<S: AsyncObjectStore>(
        &mut self,
        store: &mut BlobBatchStore<'_, S>,
        level: usize,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<BlobChild, FsError> {
        let owner = self
            .levels
            .value
            .get_mut(level)
            .ok_or(BlobBuildError::InvalidIndexState)?;
        let items = std::mem::take(&mut owner.value);
        let first_offset = items
            .first()
            .ok_or(BlobBuildError::InvalidIndexState)?
            .first_offset;
        let end_offset = items
            .last()
            .ok_or(BlobBuildError::InvalidIndexState)?
            .end_offset;
        let page = BlobPage {
            first_offset,
            end_offset,
            node: BlobNode::Internal(items),
        };
        let result = put_blob_page(store, &page, self.options, accounting, cancellation).await;
        let BlobNode::Internal(mut items) = page.node else {
            unreachable!()
        };
        items.clear();
        owner.value = items;
        Ok(BlobChild {
            first_offset,
            end_offset,
            page: result?,
        })
    }

    pub(super) async fn finish<S: AsyncObjectStore>(
        &mut self,
        store: &mut BlobBatchStore<'_, S>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        if self.leaf.value.is_empty() && self.levels.value.is_empty() {
            return put_blob_page(
                store,
                &BlobPage {
                    first_offset: 0,
                    end_offset: 0,
                    node: BlobNode::Leaf(Vec::new()),
                },
                self.options,
                accounting,
                cancellation,
            )
            .await;
        }
        self.flush_leaf(store, accounting, cancellation).await?;
        loop {
            let level = self
                .levels
                .value
                .iter()
                .position(|items| !items.value.is_empty())
                .ok_or(BlobBuildError::InvalidIndexState)?;
            let higher_empty = self
                .levels
                .value
                .iter()
                .skip(level + 1)
                .all(|items| items.value.is_empty());
            let items = self
                .levels
                .value
                .get(level)
                .ok_or(BlobBuildError::InvalidIndexState)?;
            if higher_empty && items.value.len() == 1 {
                return Ok(items
                    .value
                    .first()
                    .ok_or(BlobBuildError::InvalidIndexState)?
                    .page);
            }
            let child = self
                .flush_internal(store, level, accounting, cancellation)
                .await?;
            self.push_child(store, level + 1, child, accounting, cancellation)
                .await?;
        }
    }

    pub(super) fn release(mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        for level in self.levels.value.drain(..) {
            release_blob_vec(level, accounting)?;
        }
        release_blob_vec(self.leaf, accounting)?;
        release_blob_vec(self.levels, accounting)
    }
}

#[cfg(all(test, feature = "memory"))]
#[path = "index_tests.rs"]
mod tests;

pub(super) fn effective_page_width(options: BlobBuildOptions) -> Option<usize> {
    let item_bound = usize::try_from(options.page_items).ok()?;
    let byte_bound = usize::try_from(options.page_bytes)
        .ok()?
        .checked_sub(BLOB_PAGE_HEADER_BYTES)?
        / BLOB_PAGE_ITEM_BYTES;
    let width = item_bound.min(byte_bound);
    (width >= 2).then_some(width)
}

pub(super) async fn put_blob_page<S: AsyncObjectStore>(
    store: &mut BlobBatchStore<'_, S>,
    page: &BlobPage,
    options: BlobBuildOptions,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, FsError> {
    let length =
        blob_page_encoded_length(page, options.page_items).map_err(BlobBuildError::Encode)?;
    if usize_to_u64(length) > u64::from(options.page_bytes) {
        return Err(BlobBuildError::PageTooLarge {
            observed: usize_to_u64(length),
            maximum: u64::from(options.page_bytes),
        }
        .into());
    }
    let owner = encode_blob_page_with_accounting(page, options.page_items, accounting)?;
    let retained = usize_to_u64(owner.value.capacity());
    let bytes = owned_read_bytes(owner)?;
    let object = match hashed_blob_object(ObjectKind::Blob, bytes, accounting, cancellation) {
        Ok(object) => object,
        Err(error) => {
            let _ = release_blob_bytes(retained, accounting);
            return Err(error);
        }
    };
    let page = object.object_id();
    store
        .put(object, retained, accounting, cancellation)
        .await?;
    Ok(page)
}
