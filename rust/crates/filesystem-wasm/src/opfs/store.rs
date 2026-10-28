use super::*;

impl AsyncObjectStore for OpfsAcceleratedObjectStore {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
            if length > self.maximum_object_bytes {
                return Err(ObjectFailure::before_work(ObjectStoreError::TooLarge {
                    observed: length,
                    maximum: self.maximum_object_bytes,
                }));
            }
            let work = put_work(&bytes);
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            if object_digest(&bytes) != object_id.digest {
                return Err(ObjectFailure::new(ObjectStoreError::DigestMismatch, work));
            }
            self.write(object_id, &bytes, cancellation)
                .await
                .map_err(|error| ObjectFailure::new(error, work))?;
            Ok(acyclic_fs::OperationReceipt { value: (), work })
        }
        .await;
        context.accept(result)
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(), acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            if writes.is_empty() {
                return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "object write batch is empty".to_owned(),
                )));
            }
            let mut total = WorkCounters::default();
            for window in writes.chunks(MAX_BATCH_CONCURRENCY) {
                let mut admitted = WorkCounters::default();
                for write in window {
                    admitted = combine_parallel_window(admitted, put_work(&write.bytes))
                        .map_err(|error| ObjectFailure::new(error.into(), total))?;
                }
                let mut futures = window.iter().map(|write| async {
                    let __object_args = (
                        write.object_id,
                        write.bytes.clone(),
                        put_work(&write.bytes),
                        cancellation,
                    );
                    let mut __object_work =
                        acyclic_fs::AuthorityWork::new(__object_args.2, __object_args.3);
                    let __object_result = self
                        .put(__object_args.0, __object_args.1, &mut __object_work)
                        .await;
                    __object_work.receipt(__object_result)
                });
                let first = futures.next().ok_or_else(|| {
                    ObjectFailure::new(
                        ObjectStoreError::Rejected("OPFS write window is empty".to_owned()),
                        total,
                    )
                })?;
                let frames = join_frames_bytes(&first, window.len())
                    .and_then(|bytes| {
                        bytes
                            .checked_add(allocation_bytes::<ObjectResult<()>>(window.len())?)
                            .ok_or(acyclic_fs::WorkError::Overflow)
                    })
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                admitted.peak_allocation_bytes = admitted
                    .peak_allocation_bytes
                    .checked_add(frames)
                    .ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                let frame_work = WorkCounters {
                    allocation_operations: 2,
                    peak_allocation_bytes: admitted.peak_allocation_bytes,
                    ..WorkCounters::default()
                };
                let next = total
                    .checked_add(frame_work)
                    .and_then(|work| work.checked_add(admitted))
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                next.verify(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                total = total
                    .checked_add(frame_work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                let results = join_all(std::iter::once(first).chain(futures)).await;
                total = fold_unit_results(total, results, budget)?;
            }
            Ok(acyclic_fs::OperationReceipt {
                value: (),
                work: total,
            })
        }
        .await;
        context.accept(result)
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<acyclic_fs::ObjectRead, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.read_budget()?;
        let cancellation = context.cancellation;
        let result = async {
            self.read_exact(object_id, maximum_bytes, budget, cancellation)
                .await
        }
        .await;
        context.accept(result)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<
        (Vec<acyclic_fs::ObjectRead>, acyclic_fs::AuthorityAllocation),
        acyclic_fs::ObjectStoreError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            if requests.is_empty() {
                return Err(ObjectFailure::before_work(ObjectStoreError::Rejected(
                    "object read batch is empty".to_owned(),
                )));
            }
            let vector_bytes = allocation_bytes::<ObjectRead>(requests.len())
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            let mut total = WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: vector_bytes,
                ..WorkCounters::default()
            };
            total
                .verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            let mut values = Vec::new();
            values.try_reserve_exact(requests.len()).map_err(|_| {
                ObjectFailure::new(
                    ObjectStoreError::Rejected("object batch result allocation failed".to_owned()),
                    total,
                )
            })?;
            let mut retained = vector_bytes;
            for window in requests.chunks(MAX_BATCH_CONCURRENCY) {
                // Fetch file handles and lengths before any ArrayBuffer allocation.
                // Prior output carriers remain live throughout the next window.
                let handles_bytes = allocation_bytes::<(ObjectId, Blob, u64)>(window.len())
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                let results_bytes = allocation_bytes::<ObjectResult<ObjectRead>>(window.len())
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                let live = retained
                    .checked_add(handles_bytes)
                    .and_then(|value| value.checked_add(results_bytes))
                    .ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                let prospective = total
                    .checked_add(WorkCounters {
                        allocation_operations: 2,
                        peak_allocation_bytes: live,
                        ..WorkCounters::default()
                    })
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                prospective
                    .verify(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                total = prospective;
                let mut handles = Vec::new();
                handles.try_reserve_exact(window.len()).map_err(|_| {
                    ObjectFailure::new(
                        ObjectStoreError::Rejected(
                            "OPFS batch handle allocation failed".to_owned(),
                        ),
                        total,
                    )
                })?;
                for request in window {
                    let mut remaining = total
                        .remaining(budget)
                        .map_err(|error| ObjectFailure::new(error.into(), total))?;
                    remaining.peak_allocation_bytes =
                        remaining.peak_allocation_bytes.saturating_sub(live);
                    let prepared = self
                        .prepare_read(
                            request.object_id,
                            request.maximum_bytes,
                            remaining,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| {
                            let mut failed = *failure.work;
                            failed.peak_allocation_bytes =
                                live.saturating_add(failed.peak_allocation_bytes);
                            let spent = total.checked_add(failed).unwrap_or(total);
                            ObjectFailure::new(failure.error, spent)
                        })?;
                    let mut work = prepared.work;
                    work.peak_allocation_bytes = live
                        .checked_add(work.peak_allocation_bytes)
                        .ok_or_else(|| {
                            ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                        })?;
                    total = total
                        .checked_add(work)
                        .map_err(|error| ObjectFailure::new(error.into(), total))?;
                    let (blob, length) = prepared.value;
                    handles.push((request.object_id, blob, length));
                }
                let mut window_work = WorkCounters::default();
                let mut window_retained = 0_u64;
                for (_, _, length) in &handles {
                    window_work = combine_parallel_window(
                        window_work,
                        materialize_work(*length)
                            .map_err(|error| ObjectFailure::new(error.into(), total))?,
                    )
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                    window_retained = window_retained.checked_add(*length).ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                }
                let mut futures = handles.into_iter().map(|(object_id, blob, length)| {
                    Self::materialize_read(
                        object_id,
                        blob,
                        length,
                        WorkCounters::default(),
                        cancellation,
                    )
                });
                let first = futures.next().ok_or_else(|| {
                    ObjectFailure::new(
                        ObjectStoreError::Rejected("OPFS read window is empty".to_owned()),
                        total,
                    )
                })?;
                let frames = join_frames_bytes(&first, window.len())
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                window_work.allocation_operations = window_work
                    .allocation_operations
                    .checked_add(1)
                    .ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                window_work.peak_allocation_bytes = window_work
                    .peak_allocation_bytes
                    .checked_add(frames)
                    .ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                window_work.peak_allocation_bytes = live
                    .checked_add(window_work.peak_allocation_bytes)
                    .ok_or_else(|| {
                        ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                    })?;
                let admitted = total
                    .checked_add(window_work)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                admitted
                    .verify(budget)
                    .map_err(|error| ObjectFailure::new(error.into(), total))?;
                let results = join_all(std::iter::once(first).chain(futures)).await;
                // Every future has completed, including after a corrupt/failed blob.
                // Preserve the admitted window's live buffers and all performed work.
                total = admitted;
                let mut first_error = None;
                for result in results {
                    match result {
                        Ok(receipt) => values.push(receipt.value),
                        Err(failure) if first_error.is_none() => first_error = Some(failure.error),
                        Err(_) => {}
                    }
                }
                if let Some(error) = first_error {
                    return Err(ObjectFailure::new(error, total));
                }
                retained = retained.checked_add(window_retained).ok_or_else(|| {
                    ObjectFailure::new(acyclic_fs::WorkError::Overflow.into(), total)
                })?;
            }
            Ok(acyclic_fs::OperationReceipt {
                value: values,
                work: total,
            })
        }
        .await;
        context.accept(result).map(|values| (values, None))
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<bool, acyclic_fs::ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(acyclic_fs::ObjectStoreError::NativeUnavailable);
        }
        let budget = context.work.remaining(context.budget)?;
        let cancellation = context.cancellation;
        let result = async {
            cancellation
                .check()
                .map_err(|_| ObjectFailure::before_work(ObjectStoreError::Cancelled))?;
            let work = WorkCounters {
                object_probes: 1,
                backend_read_operations: 1,
                allocation_operations: 1,
                peak_allocation_bytes: 66,
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            let value =
                match JsFuture::from(self.directory.get_file_handle(&Self::key(object_id))).await {
                    Ok(handle) => handle.dyn_into::<FileSystemFileHandle>().is_ok(),
                    Err(error) if is_not_found(&error) => false,
                    Err(error) => {
                        return Err(ObjectFailure::new(rejected(&error), work));
                    }
                };
            cancellation
                .check()
                .map_err(|_| ObjectFailure::new(ObjectStoreError::Cancelled, work))?;
            Ok(acyclic_fs::OperationReceipt { value, work })
        }
        .await;
        context.accept(result)
    }
}
