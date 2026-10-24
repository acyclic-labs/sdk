use super::*;

/// Resolves a logical byte to its authenticated content chunk, without reading
/// the body. Index identities are navigation only; the returned chunk digest
/// authenticates bytes independently of blob/extent layout.
pub(crate) async fn chunk_at<S: AsyncObjectStore>(
    store: &S,
    mut root: ObjectId,
    offset: u64,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<BlobChunkRef, FsError> {
    if !limits.page_limits_valid(1) {
        return Err(BlobReadError::InvalidLimits.into());
    }
    let mut first = 0;
    let mut end = None;
    for height in 0..=limits.maximum_page_height {
        accounting.check()?;
        if u32::from(height) >= limits.maximum_visited_pages {
            return Err(BlobReadError::CycleOrHeight.into());
        }
        if root.kind != ObjectKind::Blob {
            return Err(BlobReadError::WrongRootKind.into());
        }
        accounting.charge(WorkCounters {
            page_reads: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let key = DecodedCacheKey::new::<BlobPage>(root, limits);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            original.remaining()?;
        }
        let cached = store
            .decoded_cache_get(key)
            .map_err(crate::kernel::file_read::read_store_error)?;
        let (cached, allocated) = if let Some(cached) = cached {
            (cached, 0)
        } else {
            let (read, retained) = read_object_with_accounting(
                store,
                root,
                limits.maximum_page_object_bytes(),
                accounting,
                cancellation,
            )
            .await?;
            let decoded =
                ReadOwned::decoded(decode_blob_page_with_accounting(&read, limits, accounting)?);
            let decoded =
                crate::kernel::file_read::cache_decoded_read(store, key, decoded, accounting)?;
            drop(read);
            crate::kernel::file_read::release_standalone_read_bytes(retained, accounting)?;
            decoded
        };
        let result = (|| {
            let page = cached
                .value
                .downcast_ref::<BlobPage>()
                .ok_or_else(|| FsError::BlobRead(ObjectStoreError::Corrupt.into()))?;
            if page.first_offset != first || end.is_some_and(|end| page.end_offset != end) {
                return Err(BlobReadError::ChildBoundsMismatch.into());
            }
            if offset < first || offset >= page.end_offset {
                return Err(BlobReadError::InvalidRange.into());
            }
            match &page.node {
                BlobNode::Leaf(chunks) => {
                    for chunk in chunks {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        if offset >= chunk.first_offset && offset < chunk.end_offset {
                            return Ok(Ok(*chunk));
                        }
                    }
                }
                BlobNode::Internal(children) => {
                    for child in children {
                        accounting.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                        if offset >= child.first_offset && offset < child.end_offset {
                            return Ok(Err(*child));
                        }
                    }
                }
            }
            Err(FsError::BlobRead(BlobReadError::InvalidRange))
        })();
        drop(cached);
        crate::kernel::file_read::release_standalone_read_bytes(allocated, accounting)?;
        match result? {
            Ok(chunk) => return Ok(chunk),
            Err(child) => {
                root = child.page;
                first = child.first_offset;
                end = Some(child.end_offset);
            }
        }
    }
    Err(BlobReadError::CycleOrHeight.into())
}
