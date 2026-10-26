use crate::async_storage::AsyncObjectStore;
use crate::kernel::*;
use crate::memory::MemoryObjectStore;
use crate::storage::{ObjectId, ObjectKind, object_digest};
use crate::{AuthorityWork, CancellationToken, WorkBudget};
use acyclic_stream::OwnedBytes;

pub(crate) async fn put_attribute_page(
    store: &MemoryObjectStore,
    page: &AttributePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = attribute_page_id(page, maximum_items)?;
    store
        .put(
            id,
            OwnedBytes::from(encode_attribute_page(
                page,
                maximum_items,
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed,
            )?),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(id)
}

pub(crate) async fn put_extent_page(
    store: &MemoryObjectStore,
    page: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = extent_page_id(page, maximum_items)?;
    store
        .put(
            id,
            OwnedBytes::from(encode_extent_page(
                page,
                maximum_items,
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed,
            )?),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(id)
}

pub(crate) async fn put_file_table_page(
    store: &MemoryObjectStore,
    page: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = file_table_page_id(page, maximum_items)?;
    store
        .put(
            id,
            OwnedBytes::from(encode_file_table_page(
                page,
                maximum_items,
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed,
            )?),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(id)
}

pub(crate) async fn put_tree_page(
    store: &MemoryObjectStore,
    page: &TreePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = tree_page_id(page, maximum_items)?;
    store
        .put(
            id,
            OwnedBytes::from(encode_tree_page(
                page,
                maximum_items,
                |_| Ok::<_, CanonicalDecodeError>(()),
                |_| CanonicalDecodeError::AllocationFailed,
            )?),
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(id)
}

pub(crate) async fn put_object<S: AsyncObjectStore>(
    store: &S,
    kind: ObjectKind,
    bytes: impl Into<OwnedBytes>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = bytes.into();
    let id = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store
        .put(
            id,
            bytes,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(id)
}
