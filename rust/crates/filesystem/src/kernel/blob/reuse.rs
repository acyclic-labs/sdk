use super::*;

// A local index path is retired with the build. It never adds old pages to
// the checkout's persistent decoded cache or reads old chunk bodies.
pub(super) async fn matches_prior<S: AsyncObjectStore>(
    store: &S,
    (root, limits, length): (ObjectId, DecodeLimits, u64),
    pages: &mut ReadOwned<Vec<(ObjectId, ReadOwned<BlobPage>)>>,
    object: ObjectId,
    range: (u64, u64),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<bool, FsError> {
    if object.kind == ObjectKind::Blob && range == (0, length) {
        return Ok(object == root);
    }
    while pages.value.last().is_some_and(|(_, page)| {
        range.0 < page.value.first_offset || range.1 > page.value.end_offset
    }) {
        pop_page(pages, accounting)?;
    }
    if pages.value.is_empty() {
        push_page(
            store,
            (root, (0, length)),
            pages,
            limits,
            accounting,
            cancellation,
        )
        .await?;
    }
    loop {
        accounting.check()?;
        accounting.charge(WorkCounters {
            page_reads: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let (page_id, page) = pages.value.last().ok_or(BlobReadError::TraversalState)?;
        let bounds = (page.value.first_offset, page.value.end_offset);
        if object.kind == ObjectKind::Blob && range == bounds {
            return Ok(object == *page_id);
        }
        let next = match &page.value.node {
            BlobNode::Leaf(chunks) => {
                let (mut low, mut high) = (0, chunks.len());
                while low < high {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    let middle = low + (high - low) / 2;
                    let chunk = chunks[middle];
                    if chunk.first_offset < range.0 {
                        low = middle + 1;
                    } else if chunk.first_offset > range.0 {
                        high = middle;
                    } else {
                        return Ok(object == chunk.chunk
                            && range == (chunk.first_offset, chunk.end_offset));
                    }
                }
                return Ok(false);
            }
            BlobNode::Internal(children) => {
                let (mut low, mut high) = (0, children.len());
                let mut next = None;
                while low < high {
                    accounting.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    let middle = low + (high - low) / 2;
                    let child = children[middle];
                    if child.end_offset <= range.0 {
                        low = middle + 1;
                    } else if child.first_offset > range.0 {
                        high = middle;
                    } else {
                        next = (range.1 <= child.end_offset)
                            .then_some((child.page, (child.first_offset, child.end_offset)));
                        break;
                    }
                }
                next
            }
        };
        let Some((next, bounds)) = next else {
            return Ok(false);
        };
        if object.kind == ObjectKind::Blob && range == bounds {
            return Ok(object == next);
        }
        push_page(
            store,
            (next, bounds),
            pages,
            limits,
            accounting,
            cancellation,
        )
        .await?;
    }
}

async fn push_page<S: AsyncObjectStore>(
    store: &S,
    (id, bounds): (ObjectId, (u64, u64)),
    pages: &mut ReadOwned<Vec<(ObjectId, ReadOwned<BlobPage>)>>,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), FsError> {
    let length = pages.value.len();
    if length > usize::from(limits.maximum_page_height)
        || length >= limits.maximum_visited_pages as usize
    {
        return Err(BlobReadError::CycleOrHeight.into());
    }
    reserve_blob_vec(pages, length + 1, accounting)?;
    let (read, retained) = read_object_with_accounting(
        store,
        id,
        limits.maximum_page_object_bytes(),
        accounting,
        cancellation,
    )
    .await?;
    let decoded = decode_blob_page_with_accounting(&read, limits, accounting);
    drop(read);
    crate::kernel::file_read::release_standalone_read_bytes(retained, accounting)?;
    let (value, _bytes, custody) = decoded?;
    let page = ReadOwned { value, custody };
    let valid = (page.value.first_offset, page.value.end_offset) == bounds;
    pages.value.push((id, page));
    if !valid {
        return Err(BlobReadError::ChildBoundsMismatch.into());
    }
    Ok(())
}

fn pop_page(
    pages: &mut ReadOwned<Vec<(ObjectId, ReadOwned<BlobPage>)>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if let Some((_, page)) = pages.value.pop() {
        let bytes = match &page.value.node {
            BlobNode::Leaf(items) => vector_bytes::<BlobChunkRef>(items.capacity())?,
            BlobNode::Internal(items) => vector_bytes::<BlobChild>(items.capacity())?,
        };
        drop(page);
        release_blob_bytes(bytes, accounting)?;
    }
    Ok(())
}

pub(super) fn release_pages(
    mut pages: ReadOwned<Vec<(ObjectId, ReadOwned<BlobPage>)>>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    while !pages.value.is_empty() {
        pop_page(&mut pages, accounting)?;
    }
    release_blob_vec(pages, accounting)
}
