use super::*;

impl<S: AsyncObjectStore> StagedObjects<S> {
    pub(super) async fn flush(
        &self,
        scope: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let _spill_file = self.spill_io.write().await;
        let (objects, proven_at) = match scope {
            PublicationScope::Closure { objects, proven_at } => (objects, proven_at),
            PublicationScope::Everything => (&[][..], self.collection.sweeps()),
        };
        let unswept = self
            .collection
            .admit(
                objects,
                |id| self.index().objects.contains_key(id),
                proven_at,
            )
            .await?;
        let (_allocation, targets) = {
            let index = self.index();
            let capacity = match scope {
                PublicationScope::Closure { objects, .. } => objects.len(),
                PublicationScope::Everything => index.objects.len(),
            };
            let (allocation, mut targets) = staged_vector(capacity, work.original)?;
            let mut visit = |id: ObjectId| -> Result<(), ObjectStoreError> {
                if let Some(native) = work.original {
                    native.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                }
                if index.objects.contains_key(&id) {
                    targets.push(id);
                }
                Ok(())
            };
            match scope {
                PublicationScope::Closure { objects, .. } => {
                    for &id in objects {
                        visit(id)?;
                    }
                }
                PublicationScope::Everything => {
                    for &id in index.objects.keys() {
                        visit(id)?;
                    }
                }
            }
            (allocation, targets)
        };
        self.drain_locked(&unswept, &targets, work).await?;
        Ok(unswept.into_hold())
    }

    pub(super) async fn drain_locked(
        &self,
        unswept: &crate::collection::Unswept,
        targets: &[ObjectId],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        {
            let mut index = self.index_mut();
            let sweeps = self.collection.sweeps();
            if index.drained_at != sweeps {
                index.drained.clear();
                index.drained_at = sweeps;
            }
        }
        let mut position = 0;
        loop {
            work.accounting()
                .check()
                .map_err(crate::async_storage::object_error)?;
            let mut end = position;
            let mut count = 0;
            {
                let index = self.index();
                let mut bytes = 0_u64;
                for &id in &targets[position..] {
                    if let Some(native) = work.original {
                        native.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                    }
                    if let Some(staged) = index.objects.get(&id)
                        && !index.drained.contains(&id)
                    {
                        if count != 0 && bytes.saturating_add(staged.length()) > MAXIMUM_DRAIN_BYTES
                        {
                            break;
                        }
                        bytes = bytes.saturating_add(staged.length());
                        count += 1;
                    }
                    end += 1;
                }
            }
            let (_write_allocation, mut writes) =
                staged_vector::<ObjectWrite>(count, work.original)?;
            let (_spill_allocation, mut spilled) =
                staged_vector::<(ObjectId, u64, u64)>(count, work.original)?;
            {
                let mut index = self.index_mut();
                for &id in &targets[position..end] {
                    if let Some(native) = work.original {
                        native.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })?;
                    }
                    if index.drained.contains(&id) {
                        self.collection.writing(unswept, [id]);
                        index.remove(id);
                        continue;
                    }
                    match index.objects.get(&id) {
                        Some(Staged::Resident { bytes, .. }) => writes.push(ObjectWrite {
                            object_id: id,
                            bytes: bytes.clone(),
                        }),
                        Some(&Staged::Spilled { offset, length }) => {
                            spilled.push((id, offset, length));
                        }
                        None => {}
                    }
                }
            }
            position = end;
            if writes.is_empty() && spilled.is_empty() {
                break;
            }
            if let Some(native) = work.original {
                for (id, offset, length) in spilled {
                    work.cancellation
                        .check()
                        .map_err(|_| ObjectStoreError::Cancelled)?;
                    let hashed = length
                        .checked_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES)
                        .ok_or(crate::WorkError::Overflow)?;
                    native.admit(WorkCounters {
                        object_bytes_read: length,
                        bytes_hashed: hashed,
                        ..WorkCounters::UNCHARGED
                    })?;
                    let bytes = staged_native_read(
                        &self.spill,
                        offset,
                        usize::try_from(length).map_err(|_| ObjectStoreError::Corrupt)?,
                        native,
                    )
                    .await?;
                    let observed = byte_length(&bytes);
                    native.record_admitted(WorkCounters {
                        object_bytes_read: observed,
                        ..WorkCounters::UNCHARGED
                    })?;
                    if observed != length {
                        return Err(ObjectStoreError::Corrupt);
                    }
                    native.charge(WorkCounters {
                        bytes_hashed: hashed,
                        ..WorkCounters::UNCHARGED
                    })?;
                    if crate::storage::object_digest(id.kind, &bytes) != id.digest {
                        return Err(ObjectStoreError::Corrupt);
                    }
                    writes.push(ObjectWrite {
                        object_id: id,
                        bytes,
                    });
                }
            } else if !spilled.is_empty() {
                let bytes = spilled
                    .iter()
                    .fold(0_u64, |sum, &(_, _, length)| sum.saturating_add(length));
                work.accounting()
                    .charge(WorkCounters {
                        backend_read_operations: 1,
                        object_bytes_read: bytes,
                        bytes_hashed: bytes,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(crate::async_storage::object_error)?;
                writes.extend(
                    self.read_spilled(&spilled)
                        .await?
                        .into_iter()
                        .map(|object| {
                            let (object_id, bytes) = object.into_parts();
                            ObjectWrite { object_id, bytes }
                        }),
                );
            }
            self.collection
                .writing(unswept, writes.iter().map(|write| write.object_id));
            // A failed batch may have written a prefix. Keep every member staged
            // until the provider acknowledges the batch; immutable retries are safe.
            self.inner.put_many(&writes, work).await?;
            let mut index = self.index_mut();
            for write in &writes {
                index.acknowledge(write.object_id);
            }
        }
        let truncate = {
            let index = self.index();
            index.spilled_objects == 0 && index.spill_end != 0
        };
        if truncate {
            if let Some(native) = work.original {
                self.spill
                    .set_len_async(0, Some(native.runtime_meter()))
                    .map_err(|e| native.runtime_error(e))?
                    .await
                    .map_err(|e| native.runtime_error(e))?;
            } else {
                self.spill
                    .set_len_async(0, None)
                    .expect("unmetered unit construction cannot fail")
                    .await?;
            }
            self.index_mut().spill_end = 0;
        }
        Ok(())
    }
}
