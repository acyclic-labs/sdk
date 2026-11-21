use acyclic_fs::async_storage::AsyncObjectStore;
use acyclic_fs::kernel::*;
use acyclic_fs::storage::{ObjectId, ObjectKind};

#[path = "../../../tests/support/extent_objects.rs"]
mod extent_objects;
pub(crate) use extent_objects::put_extent_page;
#[path = "../../../tests/support/put_object.rs"]
mod object;
pub(crate) use object::put_object;

#[path = "../../../tests/support/generation_objects.rs"]
mod generation_objects;
pub(crate) use generation_objects::{put_file_metadata, put_file_table_page, put_generation_root};

#[path = "../../../tests/support/tree_objects.rs"]
mod tree_objects;
pub(crate) use tree_objects::put_tree_page;

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

#[path = "../../../tests/support/blob_objects.rs"]
mod blob_objects;
pub(crate) use blob_objects::put_blob_page;
