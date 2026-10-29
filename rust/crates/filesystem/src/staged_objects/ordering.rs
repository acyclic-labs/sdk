use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;

fn push<T>(
    values: &mut Vec<T>,
    allocation: &mut Option<NativeAllocation>,
    value: T,
    native: Option<&NativeMaterializationWork>,
) -> Result<(), ObjectStoreError> {
    if values.len() == values.capacity() {
        let capacity = values
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .ok_or(crate::WorkError::Overflow)?;
        let (funding, mut replacement) = staged_vector(capacity, native)?;
        if let Some(native) = native {
            native.charge(WorkCounters {
                bytes_copied: (values.len() * size_of::<T>()) as u64,
                ..WorkCounters::UNCHARGED
            })?;
        }
        replacement.append(values);
        *values = replacement;
        *allocation = funding;
    }
    values.push(value);
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
    ) -> Result<(Option<NativeAllocation>, Vec<ObjectId>), ObjectStoreError> {
        let (mut output_allocation, mut output) = staged_vector(0, work.original)?;
        let (mut pending_allocation, mut pending) = staged_vector(0, work.original)?;
        let mut visited = FixedIndex::<bool, { ObjectId::ENCODED_LEN }>::default();
        for &target in targets {
            push(
                &mut pending,
                &mut pending_allocation,
                (target, false),
                work.original,
            )?;
            while let Some((id, exiting)) = pending.pop() {
                let key = id.index_key();
                if exiting {
                    *visited
                        .get_or_try_insert_with(key, &mut work.accounting(), |_| Ok(true))
                        .map_err(crate::async_storage::object_error)? = true;
                    push(&mut output, &mut output_allocation, id, work.original)?;
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
                        push(&mut output, &mut output_allocation, id, work.original)?;
                    }
                    continue;
                }
                let bytes = match self.lookup(id) {
                    Lookup::Absent => continue,
                    Lookup::Resident(bytes) => bytes,
                    Lookup::Spilled { offset, length } => {
                        if let Some(native) = work.original {
                            native.admit(WorkCounters {
                                object_bytes_read: length,
                                ..WorkCounters::UNCHARGED
                            })?;
                            let bytes = staged_native_read(
                                spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                                offset,
                                usize::try_from(length).map_err(|_| ObjectStoreError::Corrupt)?,
                                native,
                            )
                            .await?;
                            native.record_admitted(WorkCounters {
                                object_bytes_read: bytes.len() as u64,
                                ..WorkCounters::UNCHARGED
                            })?;
                            if bytes.len() as u64 != length {
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
                            let mut read = read_spilled(
                                spill.as_ref().ok_or(ObjectStoreError::Corrupt)?,
                                &[(id, offset, length)],
                            )
                            .await?;
                            read.pop().ok_or(ObjectStoreError::Corrupt)?.into_parts().1
                        }
                    }
                };
                visited
                    .get_or_try_insert_with(key, &mut work.accounting(), |_| Ok(false))
                    .map_err(crate::async_storage::object_error)?;
                push(
                    &mut pending,
                    &mut pending_allocation,
                    (id, true),
                    work.original,
                )?;
                let (_children_allocation, mut children) =
                    staged_vector(bytes.len() / 33, work.original)?;
                crate::kernel::visit_object_references(
                    id,
                    &bytes,
                    &mut work.accounting(),
                    |child| {
                        if children.len() == children.capacity() {
                            return Err(ObjectStoreError::Corrupt.into());
                        }
                        children.push(child);
                        Ok(())
                    },
                )
                .map_err(crate::async_storage::object_error)?;
                for child in children {
                    push(
                        &mut pending,
                        &mut pending_allocation,
                        (child, false),
                        work.original,
                    )?;
                }
            }
        }
        Ok((output_allocation, output))
    }
}
