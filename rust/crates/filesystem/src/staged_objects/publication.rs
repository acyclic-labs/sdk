use super::*;
use crate::kernel::allocation::retained::Retained;

impl<S: AsyncObjectStore> StagedObjects<S> {
    pub(super) async fn flush(
        &self,
        scope: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        let mut spill = self.spill_io.write().await;
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let PublicationScope {
            objects, proven_at, ..
        } = scope;
        let unswept = self
            .collection
            .admit(
                objects,
                |id| self.index().objects.contains_key(id),
                proven_at,
            )
            .await?;
        let targets = {
            let index = self.index();
            let mut targets = Retained::allocate(objects.len(), &mut work.accounting())
                .map_err(crate::async_storage::object_error)?;
            let mut visit = |id: ObjectId| -> Result<(), ObjectStoreError> {
                work.accounting()
                    .charge_items(1)
                    .map_err(crate::async_storage::object_error)?;
                if index.objects.contains_key(&id) {
                    targets.value.push(id);
                }
                Ok(())
            };
            for &id in objects {
                visit(id)?;
            }
            targets
        };
        self.drain_locked(&mut spill, &unswept, &targets.value, work)
            .await?;
        drop(targets);
        // Only the drain uses the spill file; other publications drain while
        // this one's closure becomes durable.
        drop(spill);
        // Draining only stores the objects; the inner store makes the
        // record's closure durable, so the record is appended only after.
        let inner = self.inner.flush_before_publish(scope, work).await?;
        Ok(unswept.into_hold().with(inner))
    }

    pub(super) async fn drain_locked(
        &self,
        spill: &mut Option<NativeFile>,
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
        let ordered = if self.inner.requires_child_first_publication()
            && targets
                .iter()
                .any(|id| id.kind != crate::ObjectKind::BlobChunk)
        {
            Some(self.child_first(spill, targets, work).await?)
        } else {
            None
        };
        let targets = ordered.as_ref().map_or(targets, |ids| ids.value.as_slice());
        let mut position = 0;
        while position < targets.len() {
            work.accounting()
                .check()
                .map_err(crate::async_storage::object_error)?;
            let mut end = position;
            let mut count = 0;
            let mut spilled_count = 0;
            {
                let index = self.index();
                let mut bytes = 0_u64;
                for &id in &targets[position..] {
                    work.accounting()
                        .charge_items(1)
                        .map_err(crate::async_storage::object_error)?;
                    if let Some(staged) = index.objects.get(&id)
                        && !index.drained.contains(&id)
                    {
                        if count != 0 && bytes.saturating_add(staged.length()) > MAXIMUM_DRAIN_BYTES
                        {
                            break;
                        }
                        bytes = bytes.saturating_add(staged.length());
                        count += 1;
                        spilled_count += usize::from(matches!(staged, Staged::Spilled { .. }));
                    }
                    end += 1;
                }
            }
            let mut writes = Retained::allocate(count, &mut work.accounting())
                .map_err(crate::async_storage::object_error)?;
            let mut spilled = Retained::allocate(
                if work.original.is_none() {
                    spilled_count
                } else {
                    0
                },
                &mut work.accounting(),
            )
            .map_err(crate::async_storage::object_error)?;
            if work.original.is_none() {
                let index = self.index();
                for &id in &targets[position..end] {
                    work.accounting()
                        .charge_items(1)
                        .map_err(crate::async_storage::object_error)?;
                    if !index.drained.contains(&id)
                        && let Some(&Staged::Spilled { offset, length }) = index.objects.get(&id)
                    {
                        spilled.value.push((id, offset, length));
                    }
                }
            }
            let recovered = if spilled.value.is_empty() {
                None
            } else {
                let bytes = spilled
                    .value
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
                Some(
                    read_spilled(
                        spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                        &spilled.value,
                    )
                    .await?,
                )
            };
            let mut recovered = recovered.map(Vec::into_iter);
            for &id in &targets[position..end] {
                work.accounting()
                    .charge_items(1)
                    .map_err(crate::async_storage::object_error)?;
                let staged = {
                    let index = self.index();
                    if index.drained.contains(&id) {
                        drop(index);
                        self.collection.writing(unswept, [id]);
                        self.index_mut().remove(id);
                        continue;
                    }
                    match index.objects.get(&id) {
                        Some(Staged::Resident { object, .. }) => Ok(object.clone()),
                        Some(&Staged::Spilled { offset, length }) => Err((offset, length)),
                        None => continue,
                    }
                };
                let object = match staged {
                    Ok(object) => object,
                    Err((offset, length)) => {
                        if let Some(native) = work.original {
                            work.cancellation
                                .check()
                                .map_err(|_| ObjectStoreError::Cancelled)?;
                            read_spilled_native(
                                spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                                id,
                                offset,
                                length,
                                native,
                            )
                            .await?
                        } else {
                            let object = recovered
                                .as_mut()
                                .and_then(Iterator::next)
                                .ok_or(ObjectStoreError::Corrupt)?;
                            if object.object_id() != id {
                                return Err(ObjectStoreError::Corrupt);
                            }
                            object
                        }
                    }
                };
                writes.value.push(object);
            }
            position = end;
            drop(recovered);
            drop(spilled);
            if writes.value.is_empty() {
                continue;
            }
            self.collection
                .writing(unswept, writes.value.iter().map(HashedObject::object_id));
            // A failed batch may have written a prefix. Keep every member staged
            // until the provider acknowledges the batch; immutable retries are safe.
            self.inner.put_many_hashed(&writes.value, work).await?;
            let mut index = self.index_mut();
            for write in &writes.value {
                index.acknowledge(write.object_id());
            }
        }
        let (empty, truncate) = {
            let index = self.index();
            (
                index.spilled_objects == 0,
                index.spilled_objects == 0 && index.spill_end != 0,
            )
        };
        if let Some(file) = spill.as_ref().filter(|_| truncate) {
            if let Some(native) = work.original {
                file.set_len_async(0, Some(native.runtime_meter()))
                    .map_err(|e| native.runtime_error(e))?
                    .await
                    .map_err(|e| native.runtime_error(e))?;
            } else {
                file.set_len_async(0, None)
                    .expect("unmetered unit construction cannot fail")
                    .await?;
            }
            self.index_mut().spill_end = 0;
        }
        if empty {
            // Nothing is spilled: the unlinked file closes, holding no
            // descriptor until the next spill creates another.
            *spill = None;
        }
        Ok(())
    }
}
