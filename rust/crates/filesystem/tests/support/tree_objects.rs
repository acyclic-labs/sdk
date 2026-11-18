use acyclic_fs::AsyncObjectStore;
use acyclic_fs::kernel::{CanonicalDecodeError, TreePage, encode_tree_page};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};

use super::put_object;

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
