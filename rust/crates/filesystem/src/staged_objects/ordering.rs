use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::allocation::retained::Retained;

fn push<T>(
    values: &mut Retained<Vec<T>>,
    value: T,
    work: &mut AuthorityWork<'_>,
) -> Result<(), ObjectStoreError> {
    if values.value.len() == values.value.capacity() {
        let capacity = values
            .value
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        let mut replacement = Retained::allocate(capacity, &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        work.accounting()
            .charge(WorkCounters {
                bytes_copied: std::mem::size_of_val(values.value.as_slice()) as u64,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        replacement.value.append(&mut values.value);
        *values = replacement;
    }
    values.value.push(value);
    Ok(())
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    /// Visits only selected staged objects and their staged dependencies.
    /// Explicit entry/exit frames avoid recursion for arbitrarily deep DAGs.
    pub(super) async fn child_first(
        &self,
        spill: &Option<NativeFile>,
        targets: &[ObjectId],
        work: &mut AuthorityWork<'_>,
    ) -> Result<Retained<Vec<ObjectId>>, ObjectStoreError> {
        let mut output = Retained::allocate(0, &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut pending = Retained::allocate(0, &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut visited = FixedIndex::<bool, { ObjectId::ENCODED_LEN }>::default();
        for &target in targets {
            push(&mut pending, (target, false), work)?;
            while let Some((id, exiting)) = pending.value.pop() {
                let key = id.index_key();
                if exiting {
                    *visited
                        .get_or_try_insert_with(key, &mut work.accounting(), |_| Ok(true))
                        .map_err(crate::async_storage::object_error)? = true;
                    push(&mut output, id, work)?;
                    continue;
                }
                match visited
                    .get(key, &mut work.accounting())
                    .map_err(crate::async_storage::object_error)?
                {
                    Some(true) => continue,
                    Some(false) => return Err(ObjectStoreError::Corrupt),
                    None => {}
                }
                // Chunks have no edges: their spill payload need only be read by the drain.
                if id.kind == crate::ObjectKind::BlobChunk {
                    if self.index().objects.contains_key(&id) {
                        visited
                            .get_or_try_insert_with(key, &mut work.accounting(), |_| Ok(true))
                            .map_err(crate::async_storage::object_error)?;
                        push(&mut output, id, work)?;
                    }
                    continue;
                }
                let bytes = match self.lookup(id) {
                    Lookup::Absent => continue,
                    Lookup::Resident { bytes, .. } => bytes,
                    Lookup::Spilled {
                        offset,
                        length,
                        stored_length,
                    } => {
                        let stored = if let Some(native) = work.original {
                            read_spilled_native(
                                spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                                id,
                                offset,
                                stored_length,
                                length,
                                native,
                            )
                            .await?
                        } else {
                            work.accounting()
                                .admit(WorkCounters {
                                    bytes_hashed: stored_length.saturating_add(
                                        if id.storage.is_some() { length } else { 0 },
                                    ),
                                    ..WorkCounters::UNCHARGED
                                })
                                .map_err(crate::async_storage::object_error)?;
                            work.accounting()
                                .charge(WorkCounters {
                                    backend_read_operations: 1,
                                    object_bytes_read: stored_length,
                                    ..WorkCounters::UNCHARGED
                                })
                                .map_err(crate::async_storage::object_error)?;
                            read_spilled(
                                spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                                &[(id, offset, stored_length)],
                            )
                            .await?
                            .pop()
                            .ok_or(ObjectStoreError::Corrupt)?
                        };
                        self.inner
                            .restore_hashed(id, stored, length, work)?
                            .bytes()
                            .clone()
                    }
                };
                visited
                    .get_or_try_insert_with(key, &mut work.accounting(), |_| Ok(false))
                    .map_err(crate::async_storage::object_error)?;
                push(&mut pending, (id, true), work)?;
                let mut children = Retained::allocate(bytes.len() / 33, &mut work.accounting())
                    .map_err(crate::async_storage::object_error)?;
                crate::kernel::visit_object_references(
                    id,
                    &bytes,
                    crate::kernel::DecodeLimits::for_object_bytes(bytes.len()),
                    &mut work.accounting(),
                    |child| {
                        if children.value.len() == children.value.capacity() {
                            return Err(ObjectStoreError::Corrupt.into());
                        }
                        children.value.push(child);
                        Ok(())
                    },
                )
                .map_err(crate::async_storage::object_error)?;
                for &child in &children.value {
                    push(&mut pending, (child, false), work)?;
                }
            }
        }
        Ok(output)
    }
}
