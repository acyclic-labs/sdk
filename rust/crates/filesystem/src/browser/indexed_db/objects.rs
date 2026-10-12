use super::*;

impl AsyncObjectStore for IndexedDbObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_many(&[ObjectWrite { object_id, bytes }], budget, cancellation)
            .await
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut context = crate::AuthorityWork::new(budget, cancellation);
        let result: Result<(), ObjectStoreError> = async {
            let io = &mut context;
            io.check().map_err(object_error)?;
            if writes.is_empty() {
                return Err((ObjectStoreError::Rejected("empty object write batch".to_owned())));
            }
            for write in writes {
                let length = write.bytes.len() as u64;
                self.bound(length, self.maximum_object_bytes)?;
                io.charge(WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                    allocation_operations: 1,
                    peak_allocation_bytes: OBJECT_KEY_BYTES,
                    ..Default::default()
                })
                .map_err(object_error)?;
                if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
                    return Err((ObjectStoreError::DigestMismatch));
                }
            }
            let retained = writes.len() as u64 * size_of::<(Blob, &OwnedBytes)>() as u64;
            io.charge(WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: retained,
                ..Default::default()
            })
            .map_err(object_error)?;
            let mut existing = Vec::new();
            existing
                .try_reserve_exact(writes.len())
                .map_err(|e| object_error(backend(e)))?;
            let tx = transaction(
                &self.database,
                &[OBJECTS, OBJECT_METADATA],
                TransactionMode::Readwrite,
            )
            .map_err(|e| object_error(backend(e)))?;
            let mut added = false;
            for write in writes {
                let key = Self::key(write.object_id);
                let length = write.bytes.len() as u64;
                let stored = Self::length(&io, &tx, &key).await?;
                let copy = WorkCounters {
                    bytes_copied: length * 2,
                    allocation_operations: 2 * u64::from(length != 0),
                    peak_allocation_bytes: OBJECT_KEY_BYTES + length * 2 + retained,
                    ..Default::default()
                };
                if let Some(stored) = stored {
                    if stored != length {
                        return Err((ObjectStoreError::Corrupt));
                    }
                    io.charge(WorkCounters {
                        backend_read_operations: 1,
                        object_bytes_read: length,
                        ..copy
                    })
                    .map_err(object_error)?;
                    let blob = io.blob(&tx, OBJECTS, &key).await.map_err(object_error)?;
                    existing.push((blob, &write.bytes));
                } else {
                    io.charge(WorkCounters {
                        backend_write_operations: 2,
                        object_bytes_written: length,
                        ..copy
                    })
                    .map_err(object_error)?;
                    cancellation
                        .check()
                        .map_err(|_| (ObjectStoreError::Cancelled))?;
                    let blob = create_blob(&write.bytes)
                        .map_err(|e| object_error(backend(js_message(&e))))?;
                    io.insert(&tx, OBJECTS, &key, blob.into())
                        .await
                        .map_err(object_error)?;
                    io.insert(
                        &tx,
                        OBJECT_METADATA,
                        &key,
                        JsValue::from_str(&length.to_string()),
                    )
                    .await
                    .map_err(object_error)?;
                    added = true;
                }
            }
            if added {
                io.commit(tx).await.map_err(object_error)?;
            } else {
                drop(tx);
            }
            for (blob, bytes) in existing {
                if io
                    .bytes(&blob, bytes.len() as u64)
                    .await
                    .map_err(object_error)?
                    .as_slice()
                    != bytes.as_ref()
                {
                    return Err((ObjectStoreError::Corrupt));
                }
            }
            Ok(())
        }
        .await;
        context.receipt(result)
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut context = crate::AuthorityWork::new(budget, cancellation);
        let result: Result<ObjectRead, ObjectStoreError> = async {
            let io = &mut context;
            io.check().map_err(object_error)?;
            io.charge(WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                allocation_operations: 1,
                peak_allocation_bytes: OBJECT_KEY_BYTES,
                ..Default::default()
            })
            .map_err(object_error)?;
            let tx = transaction(
                &self.database,
                &[OBJECTS, OBJECT_METADATA],
                TransactionMode::Readonly,
            )
            .map_err(|e| object_error(backend(e)))?;
            let key = Self::key(id);
            let length = Self::length(&io, &tx, &key)
                .await?
                .ok_or_else(|| (ObjectStoreError::Missing))?;
            self.bound(length, maximum)?;
            io.charge(WorkCounters {
                backend_read_operations: 1,
                object_bytes_read: length,
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                bytes_copied: length * 2,
                allocation_operations: 2 * u64::from(length != 0),
                peak_allocation_bytes: OBJECT_KEY_BYTES + length * 2,
                ..Default::default()
            })
            .map_err(object_error)?;
            let blob = io.blob(&tx, OBJECTS, &key).await.map_err(object_error)?;
            drop(tx);
            let value = Self::read_blob(&io, id, &blob, length).await?;
            Ok(value)
        }
        .await;
        context.receipt(result)
    }
    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut context = crate::AuthorityWork::new(budget, cancellation);
        let result: Result<Vec<ObjectRead>, ObjectStoreError> = async {
            let io = &mut context;
            io.check().map_err(object_error)?;
            if requests.is_empty() {
                return Err((ObjectStoreError::Rejected("empty object read batch".to_owned())));
            }
            let count = requests.len() as u64;
            let mut retained =
                count * (size_of::<ObjectRead>() + size_of::<(ObjectId, Blob, u64)>()) as u64;
            io.admit(WorkCounters {
                items_examined: count,
                items_returned: count,
                allocation_operations: 2,
                peak_allocation_bytes: retained,
                ..Default::default()
            })
            .map_err(object_error)?;
            io.charge(WorkCounters {
                items_examined: count,
                allocation_operations: 2,
                peak_allocation_bytes: retained,
                ..Default::default()
            })
            .map_err(object_error)?;
            let mut output = Vec::new();
            output
                .try_reserve_exact(requests.len())
                .map_err(|e| object_error(backend(e)))?;
            let mut handles = Vec::new();
            handles
                .try_reserve_exact(requests.len())
                .map_err(|e| object_error(backend(e)))?;
            let tx = transaction(
                &self.database,
                &[OBJECTS, OBJECT_METADATA],
                TransactionMode::Readonly,
            )
            .map_err(|e| object_error(backend(e)))?;
            for request in requests {
                io.charge(WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    allocation_operations: 1,
                    peak_allocation_bytes: retained + OBJECT_KEY_BYTES,
                    ..Default::default()
                })
                .map_err(object_error)?;
                let key = Self::key(request.object_id);
                let length = Self::length(&io, &tx, &key)
                    .await?
                    .ok_or_else(|| (ObjectStoreError::Missing))?;
                self.bound(length, request.maximum_bytes)?;
                io.charge(authority_fixed_read_work(0))
                    .map_err(object_error)?;
                handles.push((
                    request.object_id,
                    io.blob(&tx, OBJECTS, &key).await.map_err(object_error)?,
                    length,
                ));
            }
            drop(tx);
            for (id, blob, length) in handles {
                io.charge(WorkCounters {
                    object_bytes_read: length,
                    bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                    bytes_copied: length * 2,
                    allocation_operations: 2 * u64::from(length != 0),
                    peak_allocation_bytes: retained + OBJECT_KEY_BYTES + length * 2,
                    ..Default::default()
                })
                .map_err(object_error)?;
                output.push(Self::read_blob(&io, id, &blob, length).await?);
                retained += length;
            }
            io.charge(WorkCounters {
                items_returned: count,
                ..Default::default()
            })
            .map_err(object_error)?;
            Ok(output)
        }
        .await;
        context.receipt(result)
    }
    async fn contains(
        &self,
        id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        match self
            .read(id, self.maximum_object_bytes, budget, cancellation)
            .await
        {
            Ok(receipt) => Ok(crate::OperationReceipt {
                value: true,
                work: receipt.work,
            }),
            Err(ObjectFailure {
                error: ObjectStoreError::Missing,
                work,
            }) => Ok(crate::OperationReceipt {
                value: false,
                work: *work,
            }),
            Err(error) => Err(error),
        }
    }
}
