use super::*;
use crate::LogicalObjectStore;
use crate::kernel::DecodeLimits;
use crate::storage::{ObjectKind, object_digest};
use acyclic_objects::v1::ObjectsProvider as _;
use std::sync::Arc;

type LocalStaged = StagedObjects<LogicalObjectStore<acyclic_objects::v1::local::LocalObjects>>;

async fn open_staged(
    directory: &Path,
) -> Result<(LocalStaged, Arc<acyclic_objects::v1::local::LocalObjects>), Box<dyn std::error::Error>>
{
    let provider = Arc::new(
        acyclic_objects::v1::local::LocalObjects::open(
            directory.join("objects"),
            acyclic_objects::LocalObjectsLimits::default(),
            None,
        )
        .await?,
    );
    let bucket = provider
        .create_bucket(acyclic_objects::v1::wire::CreateBucketRequest {
            name: "staged-test".to_owned(),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or("bucket creation returned no bucket")?;
    let store = StagedObjects::open(
        LogicalObjectStore::new(Arc::clone(&provider), bucket),
        directory.to_path_buf(),
    )
    .await?;
    Ok((store, provider))
}

async fn reopen_contains(
    directory: &Path,
    objects: &[ObjectId],
) -> Result<bool, Box<dyn std::error::Error>> {
    let reopened = acyclic_objects::v1::local::LocalObjects::open(
        directory.join("objects"),
        acyclic_objects::LocalObjectsLimits::default(),
        None,
    )
    .await?;
    let bucket = acyclic_objects::v1::wire::BucketRef {
        name: "staged-test".to_owned(),
    };
    reopened
        .head_bucket(acyclic_objects::v1::wire::HeadBucketRequest {
            bucket: Some(bucket.clone()),
        })
        .await?;
    let reopened = LogicalObjectStore::new(Arc::new(reopened), bucket);
    let token = CancellationToken::new();
    for &object_id in objects {
        if !{
            let __object_args = (object_id, WorkBudget::UNBOUNDED, &token);
            let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2);
            let __object_result = reopened.contains(__object_args.0, &mut __object_work).await;
            __object_work.receipt(__object_result)
        }?
        .value
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn blob(bytes: OwnedBytes) -> (ObjectId, OwnedBytes) {
    let object_id = ObjectId {
        kind: ObjectKind::Blob,
        digest: object_digest(ObjectKind::Blob, &bytes),
    };
    (object_id, bytes)
}

fn native_read_budget() -> WorkBudget {
    WorkBudget {
        backend_read_operations: 8,
        object_probes: 64,
        object_bytes_read: 65_536,
        bytes_hashed: 65_536,
        bytes_copied: 65_536,
        source_bytes_read: 65_536,
        items_examined: 4_096,
        items_returned: 256,
        allocation_operations: 256,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

async fn spill_fixture(
    store: &LocalStaged,
    token: &CancellationToken,
) -> Result<(ObjectId, OwnedBytes), Box<dyn std::error::Error>> {
    let (id, bytes) = blob(OwnedBytes::from_static(b"real spill bytes"));
    {
        let __object_args = (id, bytes.clone(), WorkBudget::UNBOUNDED, token);
        let mut __object_work = crate::AuthorityWork::new(__object_args.2, __object_args.3);
        let __object_result = store
            .put(__object_args.0, __object_args.1, &mut __object_work)
            .await;
        __object_work.receipt(__object_result)
    }?;
    let _spill_file = store.spill_io.write().await;
    let spill_bytes = bytes.clone();
    store
        .spill
        .control_async(
            move |file, _| {
                acyclic_native_runtime::write_all_at(file, 0, spill_bytes.as_slice(), None)
            },
            None,
        )
        .expect("unmetered control construction cannot fail")
        .await?;
    let mut index = store.index_mut();
    if let Some(Staged::Resident { order, .. }) = index.objects.get(&id) {
        let order = *order;
        index.order.remove(&order);
    }
    index.mark_spilled(&[(id, bytes.clone())], 0);
    Ok((id, bytes))
}

mod native_batch;
mod native_read;
mod native_write;
mod ordinary;
