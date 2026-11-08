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
    ) -> Result<ObjectRead, ObjectStoreError> {
        acyclic_fs::browser::read_one(
            self,
            &ObjectReadRequest {
                object_id,
                maximum_bytes,
            },
            context,
        )
        .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, acyclic_fs::AuthorityAllocation), ObjectStoreError> {
        acyclic_fs::browser::read_many(self, requests, MAX_BATCH_CONCURRENCY, context).await
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
                ..WorkCounters::default()
            };
            work.verify(budget)
                .map_err(|error| ObjectFailure::before_work(error.into()))?;
            let value = match JsFuture::from(
                Self::with_key(object_id, |key| self.directory.get_file_handle(key))
                    .map_err(ObjectFailure::before_work)?,
            )
            .await
            {
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
