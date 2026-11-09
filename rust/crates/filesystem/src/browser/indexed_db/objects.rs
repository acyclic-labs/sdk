use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::{Retained, RetainedAllocation};
use crate::storage::ObjectWriteSource;

impl AsyncObjectStore for IndexedDbObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_many(&[ObjectWrite { object_id, bytes }], context)
            .await
    }
    async fn put_hashed(
        &self,
        object: crate::HashedObject,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_batch(std::slice::from_ref(&object), context).await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_batch(writes, context).await
    }
    async fn put_many_hashed(
        &self,
        objects: &[crate::HashedObject],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_batch(objects, context).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let io = context;
        io.check().map_err(object_error)?;
        work::charge(
            io,
            WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                ..Default::default()
            },
        )
        .map_err(object_error)?;
        let tx = transaction(&self.database, &[OBJECTS], TransactionMode::Readonly)
            .map_err(|e| object_error(backend(e)))?;
        let key = RetainedAllocation::claim(object_key_bytes(id), 1, &mut io.accounting())
            .map_err(crate::async_storage::object_error)?
            .into_value(Self::key(id));
        let blob = Self::object(io, &tx, &key.value)
            .await?
            .ok_or_else(|| ObjectStoreError::Missing)?;
        let length = work::blob_length(&blob).map_err(object_error)?;
        self.bound(length, maximum)?;
        work::charge(
            io,
            WorkCounters {
                bytes_hashed: length,
                ..Default::default()
            },
        )
        .map_err(object_error)?;
        drop(tx);
        let value = Self::read_blob(io, id, &blob, length).await?;
        Ok(value)
    }
    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let io = &mut *context;
        io.check().map_err(object_error)?;
        if requests.is_empty() {
            return Err(ObjectStoreError::Rejected(
                "empty object read batch".to_owned(),
            ));
        }
        let count = requests.len() as u64;
        work::admit(
            io,
            WorkCounters {
                items_examined: count,
                items_returned: count,
                ..Default::default()
            },
        )
        .map_err(object_error)?;
        work::charge(
            io,
            WorkCounters {
                items_examined: count,
                ..Default::default()
            },
        )
        .map_err(object_error)?;
        let mut output = Retained::allocate(requests.len(), &mut io.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut handles = Retained::allocate(requests.len(), &mut io.accounting())
            .map_err(crate::async_storage::object_error)?;
        let tx = transaction(&self.database, &[OBJECTS], TransactionMode::Readonly)
            .map_err(|e| object_error(backend(e)))?;
        for request in requests {
            work::charge(
                io,
                WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    ..Default::default()
                },
            )
            .map_err(object_error)?;
            let key = RetainedAllocation::claim(
                object_key_bytes(request.object_id),
                1,
                &mut io.accounting(),
            )
            .map_err(crate::async_storage::object_error)?
            .into_value(Self::key(request.object_id));
            let blob = Self::object(io, &tx, &key.value)
                .await?
                .ok_or_else(|| ObjectStoreError::Missing)?;
            let length = work::blob_length(&blob).map_err(object_error)?;
            self.bound(length, request.maximum_bytes)?;
            handles.value.push((request.object_id, blob, length));
        }
        drop(tx);
        for (id, blob, length) in handles.value.drain(..) {
            work::charge(
                io,
                WorkCounters {
                    bytes_hashed: length,
                    ..Default::default()
                },
            )
            .map_err(object_error)?;
            output
                .value
                .push(Self::read_blob(io, id, &blob, length).await?);
        }
        work::charge(
            io,
            WorkCounters {
                items_returned: count,
                ..Default::default()
            },
        )
        .map_err(object_error)?;
        Ok((output.value, Some(output.allocation)))
    }
    async fn contains(
        &self,
        id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        match self.read(id, self.maximum_object_bytes, context).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl IndexedDbObjectStore {
    fn admit_batch<T: ObjectWriteSource>(
        &self,
        writes: &[T],
        io: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        io.check().map_err(object_error)?;
        if writes.is_empty() {
            return Err(ObjectStoreError::Rejected(
                "empty object write batch".to_owned(),
            ));
        }
        for write in writes {
            let (id, _, bytes) = write.parts();
            let length = bytes.len() as u64;
            let authenticate = write.sealed().is_none();
            self.bound(length, self.maximum_object_bytes)?;
            work::charge(
                io,
                WorkCounters {
                    bytes_hashed: if authenticate { length } else { 0 },
                    ..Default::default()
                },
            )
            .map_err(object_error)?;
            if authenticate && object_digest(bytes) != id.digest {
                return Err(ObjectStoreError::DigestMismatch);
            }
        }
        Ok(())
    }
    async fn put_batch<T: ObjectWriteSource>(
        &self,
        writes: &[T],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let io = context;
        self.admit_batch(writes, io)?;
        let mut observed = Retained::allocate(writes.len(), &mut io.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut unique = FixedIndex::<(), { ObjectId::ENCODED_LEN }>::default();
        let tx = transaction(&self.database, &[OBJECTS], TransactionMode::Readonly)
            .map_err(|e| object_error(backend(e)))?;
        for write in writes {
            let (id, _, bytes) = write.parts();
            let mut inserted = false;
            unique
                .get_or_try_insert_with(id.index_key(), &mut io.accounting(), |_| {
                    inserted = true;
                    Ok(())
                })
                .map_err(crate::async_storage::object_error)?;
            if !inserted {
                continue;
            }
            work::charge(
                io,
                WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    ..Default::default()
                },
            )
            .map_err(object_error)?;
            let key = RetainedAllocation::claim(object_key_bytes(id), 1, &mut io.accounting())
                .map_err(crate::async_storage::object_error)?
                .into_value(Self::key(id));
            let stored = Self::object(io, &tx, &key.value).await?;
            if let Some(stored) = &stored {
                if work::blob_length(stored).map_err(object_error)? != bytes.len() as u64 {
                    return Err(ObjectStoreError::Corrupt);
                }
            }
            observed.value.push((id, stored, bytes));
        }
        drop(tx);
        drop(unique);
        // Blob materialization may suspend beyond an IndexedDB transaction's
        // active turn. Complete every required check before submitting writes.
        for (_, stored, bytes) in &observed.value {
            if let Some(blob) = stored
                && io.blob_bytes(blob, bytes.len() as u64).await?.as_ref() != bytes.as_ref()
            {
                return Err(ObjectStoreError::Corrupt);
            }
        }
        let missing = observed
            .value
            .iter()
            .filter(|(_, blob, _)| blob.is_none())
            .count();
        if missing == 0 {
            return Ok(());
        }
        let mut records = Retained::allocate(missing, &mut io.accounting())
            .map_err(crate::async_storage::object_error)?;
        for (id, stored, bytes) in observed.value.drain(..) {
            if stored.is_some() {
                continue;
            }
            let length = bytes.len() as u64;
            let allocation = RetainedAllocation::claim(
                object_key_bytes(id)
                    .checked_add(length.checked_mul(2).ok_or(crate::WorkError::Overflow)?)
                    .ok_or(crate::WorkError::Overflow)?,
                1 + 2 * u64::from(length != 0),
                &mut io.accounting(),
            )
            .map_err(crate::async_storage::object_error)?;
            work::charge(
                io,
                WorkCounters {
                    backend_write_operations: 1,
                    object_bytes_written: length,
                    bytes_copied: length * 2,
                    ..Default::default()
                },
            )
            .map_err(object_error)?;
            let blob = create_blob(bytes).map_err(|e| object_error(backend(js_message(&e))))?;
            records
                .value
                .push(allocation.into_value((Self::key(id), blob)));
        }
        drop(observed);
        let database = self.database.clone();
        let cancellation = io.cancellation;
        crate::browser::complete(
            || {
                Ok(async move {
                    let tx = transaction(&database, &[OBJECTS], TransactionMode::Readwrite)
                        .map_err(backend)?;
                    let result = async {
                        let store = tx.object_store(OBJECTS).map_err(backend)?;
                        for record in &records.value {
                            let (key, blob) = &record.value;
                            // Only authenticated immutable content is submitted.
                            // Concurrent equal publication writes the same value,
                            // without a second read round.
                            store
                                .put(JsValue::from(blob.clone()))
                                .with_key(key.as_str())
                                .primitive()
                                .map_err(backend)?
                                .await
                                .map_err(backend)?;
                        }
                        Ok::<_, AuthorityStoreError>(())
                    }
                    .await;
                    if let Err(error) = result {
                        // The producer retains records through terminal abort even
                        // when delivery has already been withdrawn.
                        let _aborted = tx.abort().await;
                        return Err(error);
                    }
                    let result = tx.commit().await.map_err(backend);
                    drop(records);
                    result
                })
            },
            WorkCounters {
                durability_operations: 1,
                ..Default::default()
            },
            &std::cell::RefCell::new(io.accounting()),
            cancellation,
        )
        .await?
        .map_err(object_error)
    }
}
