use crate::async_storage::AsyncObjectStore;
use crate::kernel::*;
use crate::storage::{ObjectId, ObjectKind};

#[path = "../../../tests/support/extent_objects.rs"]
mod extent_objects;
pub(crate) use extent_objects::{put_extent_page, put_object};

#[path = "../../../tests/support/generation_objects.rs"]
mod generation_objects;
pub(crate) use generation_objects::{
    put_file_metadata, put_file_table_page, put_generation_root, put_tree_page,
};

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
