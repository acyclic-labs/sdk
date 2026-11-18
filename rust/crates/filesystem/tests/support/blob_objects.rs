use acyclic_fs::AsyncObjectStore;
use acyclic_fs::kernel::{BlobPage, CanonicalDecodeError, encode_blob_page};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};

use super::put_object;

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
