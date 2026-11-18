use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, object_digest};
use acyclic_fs_format::object_reference::{ObjectId, ObjectKind};
use acyclic_fs_work::performance::WorkBudget;
use acyclic_stream::OwnedBytes;

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
