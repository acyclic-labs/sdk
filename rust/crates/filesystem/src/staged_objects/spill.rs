use super::*;

impl<S> StagedObjects<S> {
    pub(super) async fn spill_locked(
        &self,
        spill: &mut Option<NativeFile>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let (_victim_allocation, victims, offset) = {
            let mut index = self.index_mut();
            if index.resident_bytes <= MAXIMUM_RESIDENT_BYTES {
                return Ok(());
            }
            let (allocation, mut victims) = staged_vector(index.order.len(), work.original)?;
            index.spill_victims(&mut victims);
            (allocation, victims, index.spill_end)
        };
        let spilled = victims.iter().fold(0_u64, |sum, (_, bytes)| {
            sum.saturating_add(byte_length(bytes))
        });
        let result = async {
            if work.original.is_none() {
                work.accounting()
                    .admit(WorkCounters {
                        backend_write_operations: 1,
                        object_bytes_written: spilled,
                        bytes_copied: spilled,
                        allocation_operations: 1,
                        peak_allocation_bytes: spilled,
                        ..WorkCounters::UNCHARGED
                    })
                    .map_err(crate::async_storage::object_error)?;
            }
            let copy = work
                .original
                .map(|native| {
                    native.reserve_work(WorkCounters {
                        bytes_copied: spilled,
                        ..WorkCounters::UNCHARGED
                    })
                })
                .transpose()?;
            let (buffer_allocation, mut buffer) = staged_vector(
                usize::try_from(spilled).map_err(|_| crate::WorkError::Overflow)?,
                work.original,
            )?;
            for (_, bytes) in &victims {
                buffer.extend_from_slice(bytes);
            }
            if let Some(copy) = copy {
                copy.record_completed(WorkCounters {
                    bytes_copied: spilled,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            if let Some(native) = work.original {
                native.admit(WorkCounters {
                    object_bytes_written: spilled,
                    ..WorkCounters::UNCHARGED
                })?;
                let bytes = staged_owned_bytes(
                    buffer,
                    buffer_allocation.expect("native spill buffer is funded"),
                    native,
                )?;
                self.spill_file(spill, Some(native))
                    .await?
                    .control_async(
                        move |file, meter| {
                            acyclic_native_runtime::write_all_at(
                                file,
                                offset,
                                &bytes,
                                Some((meter.expect("metered control"), true)),
                            )
                        },
                        Some(native.runtime_meter()),
                    )
                    .map_err(|e| native.runtime_error(e))?
                    .await
                    .map_err(|e| native.runtime_error(e))?;
                native.record_admitted(WorkCounters {
                    object_bytes_written: spilled,
                    ..WorkCounters::UNCHARGED
                })?;
            } else {
                let delta = WorkCounters {
                    backend_write_operations: 1,
                    object_bytes_written: spilled,
                    bytes_copied: spilled,
                    allocation_operations: 1,
                    peak_allocation_bytes: spilled,
                    ..WorkCounters::UNCHARGED
                };
                let result = match self.spill_file(spill, None).await {
                    Ok(file) => {
                        file.write_all_batch_async(vec![OwnedWrite {
                            offset,
                            bytes: bytes::Bytes::from(buffer),
                        }])
                        .await
                    }
                    Err(error) => return Err(error),
                };
                work.accounting()
                    .record_completed(delta)
                    .map_err(crate::async_storage::object_error)?;
                result?;
            }
            Ok::<_, ObjectStoreError>(())
        }
        .await;
        if result.is_err() {
            let mut index = self.index_mut();
            if index.spilled_objects == 0 {
                // A refused first spill leaves no file behind.
                *spill = None;
            }
            for (id, _) in &victims {
                if let Some(Staged::Resident { order, .. }) = index.objects.get(id) {
                    let order = *order;
                    index.order.insert(order, *id);
                }
            }
        }
        result?;
        self.index_mut().mark_spilled(&victims, offset);
        Ok(())
    }
}

impl<S: AsyncObjectStore> StagedObjects<S> {
    pub(super) async fn relieve_window(
        &self,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let mut spill = self.spill_io.write().await;
        self.spill_locked(&mut spill, work).await?;
        let (_target_allocation, targets) = {
            let index = self.index();
            if index.spill_end <= MAXIMUM_SPILL_BYTES {
                return Ok(());
            }
            let mut bytes = 0_u64;
            let mut count = 0;
            for staged in index.objects.values() {
                if let Some(native) = work.original {
                    native.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                }
                if let Staged::Spilled { length, .. } = staged {
                    if count != 0 && bytes.saturating_add(*length) > MAXIMUM_DRAIN_BYTES {
                        break;
                    }
                    bytes = bytes.saturating_add(*length);
                    count += 1;
                }
            }
            let (allocation, mut targets) = staged_vector(count, work.original)?;
            for (&id, staged) in &index.objects {
                if targets.len() == count {
                    break;
                }
                if let Some(native) = work.original {
                    native.charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                }
                if matches!(staged, Staged::Spilled { .. }) {
                    targets.push(id);
                }
            }
            (allocation, targets)
        };
        let unswept = self.collection.unswept().await;
        if work.is_native() {
            self.drain_locked(&mut spill, &unswept, &targets, work)
                .await?;
        } else {
            let cancellation = CancellationToken::new();
            let mut housekeeping = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let _ = self
                .drain_locked(&mut spill, &unswept, &targets, &mut housekeeping)
                .await;
        }
        Ok(())
    }
}
