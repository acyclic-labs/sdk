use acyclic_fs::AsyncObjectStore;
use acyclic_fs::kernel::{
    CanonicalDecodeError, FileTablePage, GenerationRoot, encode_file_metadata,
    encode_file_table_page, encode_generation_root,
};
use acyclic_fs_format::{
    metadata::FileMetadata,
    object_reference::{ObjectId, ObjectKind},
};

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
