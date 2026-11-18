use acyclic_fs::AsyncObjectStore;
use acyclic_fs::kernel::{CanonicalDecodeError, FileTablePage, encode_file_table_page};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};

use super::put_object;

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
