use super::*;
use crate::kernel::allocation::retained::{Retained, RetainedAllocation};
use std::sync::Mutex;

struct RetainingOpening {
    held: Mutex<Option<Retained<Vec<u8>>>>,
    reject: bool,
}

impl ObjectSealer for RetainingOpening {
    fn prepare(
        &self,
        object: crate::HashedObject,
        _: &mut AuthorityWork<'_>,
    ) -> Result<crate::HashedObject, ObjectStoreError> {
        Ok(object)
    }

    fn stored_limit(&self, id: ObjectId, maximum: u64) -> Result<u64, ObjectStoreError> {
        PlainObjects.stored_limit(id, maximum)
    }

    fn open(
        &self,
        id: ObjectId,
        stored: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<OwnedBytes, ObjectStoreError> {
        // A detached remaining-budget frame loses this completed prefix.
        assert_eq!(context.spent().items_examined, 1);
        let mut work = context.accounting();
        let mut copy = crate::kernel::Owned::<Vec<u8>>::allocate(stored.len(), &mut work)
            .map_err(crate::async_storage::object_error)?;
        work.charge(WorkCounters {
            bytes_copied: stored.len() as u64,
            ..WorkCounters::UNCHARGED
        })
        .map_err(crate::async_storage::object_error)?;
        copy.value.extend_from_slice(&stored);
        *self.held.lock().unwrap() = Some(
            Retained::from_owned(copy, &mut work).map_err(crate::async_storage::object_error)?,
        );
        work.charge_items(1)
            .map_err(crate::async_storage::object_error)?;
        if self.reject {
            return Err(ObjectStoreError::Rejected("opening refused".into()));
        }
        PlainObjects.open(id, stored, context)
    }
}

#[tokio::test]
async fn ordinary_opening_retains_original_work_and_live_backing_on_every_exit()
-> Result<(), Box<dyn std::error::Error>> {
    #[cfg(not(target_arch = "wasm32"))]
    crate::initialize_test_native_io();
    let cancellation = crate::CancellationToken::new();
    let object = crate::HashedObject::new(
        crate::ObjectKind::BlobChunk,
        OwnedBytes::from_static(b"opening-body"),
    );
    for (reject, items) in [(false, 2), (true, 2), (false, 1)] {
        let store = LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory()))
            .with_sealer(RetainingOpening {
                held: Mutex::new(None),
                reject,
            });
        store
            .put_hashed(
                object.clone(),
                &mut AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation),
            )
            .await?;
        let budget = crate::WorkBudget {
            items_examined: items,
            ..crate::performance::BOUNDARY_BUDGET
        };
        let mut work = KernelAccounting::standalone(budget, &cancellation);
        work.charge_items(1)?;
        let marker = RetainedAllocation::claim(16, 1, &mut work)?;
        let result = store
            .fetch::<false>(object.object_id(), object.length(), &mut work)
            .await;
        match (reject, items, result) {
            (false, 2, Ok((read, mut custody))) => {
                assert_eq!(read.bytes, *object.bytes());
                let bytes = read.bytes.len() as u64;
                drop(read);
                work.release_owned(&mut custody, bytes)?;
            }
            (true, 2, Err(crate::FsError::Object(ObjectStoreError::Rejected(cause)))) => {
                assert_eq!(cause, "opening refused");
            }
            (
                false,
                1,
                Err(crate::FsError::Object(ObjectStoreError::Work(WorkError::BudgetExceeded {
                    counter,
                    ..
                }))),
            ) => assert_eq!(counter, "items_examined"),
            _ => return Err("opening lost its original result".into()),
        }
        assert_eq!(work.spent().items_examined, items);
        assert_eq!(work.spent().backend_read_operations, 1);
        work.spent().verify(budget)?;
        let held = store
            .sealer
            .held
            .lock()
            .unwrap()
            .take()
            .ok_or("no retained body")?;
        assert_eq!(held.value.as_slice(), object.bytes().as_ref());
        let capacity = held.value.capacity() as u64;
        let live = work
            .standalone_parts()
            .ok_or("not ordinary work")?
            .0
            .live_bytes();
        drop(held);
        assert_eq!(
            work.standalone_parts()
                .ok_or("not ordinary work")?
                .0
                .live_bytes(),
            live - capacity,
            "sealer backing must retire on the original caller ledger"
        );
        drop(marker);
    }
    Ok(())
}
