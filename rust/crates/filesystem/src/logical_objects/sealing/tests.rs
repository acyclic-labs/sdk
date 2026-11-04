use super::*;
use crate::kernel::allocation::retained::{Retained, RetainedAllocation};
use std::sync::Mutex;

struct RetainingOpening {
    held: Mutex<Option<Retained<Vec<u8>>>>,
    reject: bool,
    replace: bool,
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
        stored: ObjectRead,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
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
        if self.replace {
            let mut output = crate::kernel::Owned::<Vec<u8>>::allocate(stored.len(), &mut work)
                .map_err(crate::async_storage::object_error)?;
            work.charge(WorkCounters {
                bytes_copied: stored.len() as u64,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
            output.value.extend_from_slice(&stored);
            let logical_bytes = output.value.capacity() as u64;
            return Ok(ObjectRead {
                bytes: output.value.into(),
                retention: ObjectReadRetention::Owned { logical_bytes },
            });
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
    let mut discarded_input = 0_u8;
    for (mode, replace, reject, items) in [0, 1, 2].into_iter().flat_map(|mode| {
        [false, true].into_iter().flat_map(move |replace| {
            [(false, 2), (true, 2), (false, 1)]
                .map(|(reject, items)| (mode, replace, reject, items))
        })
    }) {
        let public = mode != 0;
        let store = LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory()))
            .with_sealer(RetainingOpening {
                held: Mutex::new(None),
                reject,
                replace,
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
        let mut work = AuthorityWork::new(budget, &cancellation);
        work.accounting().charge_items(1)?;
        let mut earlier = work.vec_owned::<u8>(31)?;
        earlier.value.resize(31, 7);
        let marker = RetainedAllocation::claim(16, 1, &mut work.accounting())?;
        let baseline = work
            .accounting()
            .standalone_parts()
            .ok_or("not ordinary work")?
            .0
            .live_bytes();
        let result = if mode == 2 {
            async {
                let (mut reads, allocation) = store
                    .read_many(
                        &[ObjectReadRequest {
                            object_id: object.object_id(),
                            maximum_bytes: object.length(),
                        }],
                        &mut work,
                    )
                    .await?;
                let read = reads.pop().ok_or(ObjectStoreError::Missing)?;
                let bytes = (reads.capacity() * std::mem::size_of::<ObjectRead>()) as u64;
                drop(reads);
                work.allocations.release_owned(bytes)?;
                assert!(allocation.is_none());
                Ok::<_, crate::FsError>((read, KernelOwnedAllocation::Standalone))
            }
            .await
        } else if public {
            store
                .read(object.object_id(), object.length(), &mut work)
                .await
                .map(|read| (read, KernelOwnedAllocation::Standalone))
                .map_err(crate::FsError::Object)
        } else {
            store
                .fetch::<false>(object.object_id(), object.length(), &mut work.accounting())
                .await
        };
        match (reject, items, result) {
            (false, 2, Ok((read, mut custody))) => {
                assert_eq!(read.bytes, *object.bytes());
                let bytes = read.retention.owned_bytes();
                let alias = read.bytes.clone();
                drop(read);
                assert_eq!(alias.as_ref(), object.bytes().as_ref());
                drop(alias);
                work.accounting().release_owned(&mut custody, bytes)?;
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
            .accounting()
            .standalone_parts()
            .ok_or("not ordinary work")?
            .0
            .live_bytes();
        drop(held);
        assert_eq!(
            work.accounting()
                .standalone_parts()
                .ok_or("not ordinary work")?
                .0
                .live_bytes(),
            live - capacity,
            "sealer backing must retire on the original caller ledger"
        );
        if public && live - capacity != baseline {
            discarded_input |= 1 << mode;
        }
        drop(marker);
        assert_eq!(earlier.value, [7; 31]);
        earlier.retire(&mut work.accounting())?;
    }
    assert_eq!(
        discarded_input, 0,
        "opening scratch did not retire (scalar=2, batch=4)"
    );
    Ok(())
}
