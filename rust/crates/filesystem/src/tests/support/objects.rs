use crate::async_storage::AsyncObjectStore;
use crate::kernel::*;
use crate::storage::{ObjectId, ObjectKind};

#[path = "../../../tests/support/extent_objects.rs"]
mod extent_objects;
pub(crate) use extent_objects::{put_extent_page, put_object};

pub(crate) async fn put_attribute_page<S: AsyncObjectStore>(
    store: &S,
    value: &AttributePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_attribute_page(
        value,
        maximum_items,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::AttributePage, bytes).await
}

pub(crate) async fn put_file_table_page<S: AsyncObjectStore>(
    store: &S,
    value: &FileTablePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_file_table_page(
        value,
        maximum_items,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::FileTablePage, bytes).await
}

pub(crate) async fn put_tree_page<S: AsyncObjectStore>(
    store: &S,
    value: &TreePage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_tree_page(
        value,
        maximum_items,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::TreePage, bytes).await
}

pub(crate) async fn put_blob_page<S: AsyncObjectStore>(
    store: &S,
    value: &BlobPage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_blob_page(
        value,
        maximum_items,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::Blob, bytes).await
}

pub(crate) async fn put_file_metadata<S: AsyncObjectStore>(
    store: &S,
    value: FileMetadata,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_file_metadata(
        value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::Metadata, bytes).await
}

pub(crate) async fn put_generation_root<S: AsyncObjectStore>(
    store: &S,
    value: &GenerationRoot,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_generation_root(
        value,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::GenerationRoot, bytes).await
}
