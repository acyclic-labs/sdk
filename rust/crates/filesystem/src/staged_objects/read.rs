use super::*;

pub(super) fn resident_read(
    bytes: OwnedBytes,
    maximum_bytes: u64,
    work: &mut AuthorityWork<'_>,
) -> Result<ObjectRead, ObjectStoreError> {
    let length = byte_length(&bytes);
    if length > maximum_bytes {
        return Err(ObjectStoreError::TooLarge {
            observed: length,
            maximum: maximum_bytes,
        });
    }
    work.accounting()
        .charge(WorkCounters {
            object_bytes_read: length,
            ..WorkCounters::UNCHARGED
        })
        .map_err(crate::async_storage::object_error)?;
    Ok(ObjectRead {
        bytes,
        retention: ObjectReadRetention::Shared,
    })
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    pub(super) async fn read_private(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        // The shared lock keeps a drain from reusing this spill offset.
        let spill_file = self.spill_io.read().await;
        let (offset, length) = match self.lookup(object_id) {
            Lookup::Spilled { offset, length } => (offset, length),
            Lookup::Resident(bytes) => return resident_read(bytes, maximum_bytes, work),
            Lookup::Absent => {
                drop(spill_file);
                return self.inner.read(object_id, maximum_bytes, work).await;
            }
        };
        if length > maximum_bytes {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum: maximum_bytes,
            });
        }
        let bytes = if let Some(native) = work.original {
            native.admit(WorkCounters {
                object_bytes_read: length,
                bytes_hashed: length,
                ..WorkCounters::UNCHARGED
            })?;
            let requested = usize::try_from(length).map_err(|_| ObjectStoreError::Corrupt)?;
            let spill = spill_file.as_ref().ok_or(ObjectStoreError::Corrupt)?;
            let bytes = staged_native_read(spill, offset, requested, native).await?;
            let observed = byte_length(&bytes);
            native.record_admitted(WorkCounters {
                object_bytes_read: observed,
                ..WorkCounters::UNCHARGED
            })?;
            native.charge(WorkCounters {
                bytes_hashed: observed,
                ..WorkCounters::UNCHARGED
            })?;
            if observed != length
                || object_id.digest != crate::storage::object_digest(object_id.kind, &bytes)
            {
                return Err(ObjectStoreError::Corrupt);
            }
            bytes
        } else {
            work.accounting()
                .charge(WorkCounters {
                    backend_read_operations: 1,
                    object_bytes_read: length,
                    bytes_hashed: length,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(crate::async_storage::object_error)?;
            read_spilled(
                spill_file.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                &[(object_id, offset, length)],
            )
            .await?
            .pop()
            .ok_or(ObjectStoreError::Corrupt)?
            .into_parts()
            .1
        };
        let mut index = self.index_mut();
        if index.resident_bytes.saturating_add(length) <= MAXIMUM_RESIDENT_BYTES
            && matches!(index.objects.get(&object_id), Some(&Staged::Spilled { offset: current, .. }) if current == offset)
        {
            index.insert_resident(object_id, bytes.clone());
        }
        Ok(ObjectRead {
            bytes,
            retention: ObjectReadRetention::Shared,
        })
    }
}
