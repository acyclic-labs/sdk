use acyclic_fs::kernel::{CanonicalDecodeError, ExtentPage, encode_extent_page};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, object_digest};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

pub(crate) async fn put_extent_page<S: AsyncObjectStore>(
    store: &S,
    value: &ExtentPage,
    maximum_items: u32,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = encode_extent_page(
        value,
        maximum_items,
        |_| Ok::<_, CanonicalDecodeError>(()),
        |_| CanonicalDecodeError::AllocationFailed,
    )?;
    put_object(store, ObjectKind::ExtentPage, bytes).await
}

pub(crate) async fn put_object<S: AsyncObjectStore>(
    store: &S,
    kind: ObjectKind,
    bytes: impl Into<OwnedBytes>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let bytes = bytes.into();
    let id = ObjectId {
        storage: None,
        kind,
        digest: object_digest(&bytes),
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
