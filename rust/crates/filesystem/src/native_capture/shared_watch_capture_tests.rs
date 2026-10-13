use super::*;

#[cfg(test)]
mod shared_watch_capture_tests {
    use super::*;
    use crate::demand::NativeDemandReadMeter;
    use crate::storage::{ObjectId, ObjectRead, ObjectReadRequest, ObjectResult, ObjectWrite};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct ReenteringObjects {
        inner: crate::memory::MemoryObjectStore,
        meter: Arc<Mutex<Option<Arc<crate::demand::NativeDemandReadMeter>>>>,
        calls: Arc<AtomicUsize>,
    }

    impl ReenteringObjects {
        async fn reenter(&self) {
            for _ in 0..2 {
                {
                    let binding = self.meter.lock().expect("fixture meter binding");
                    if let Some(meter) = binding.as_ref() {
                        let original = meter.0.try_lock().expect(
                            "watch capture must release its original meter before backend entry",
                        );
                        original.check().expect("fixture invocation remains active");
                        self.calls.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    }

    impl AsyncObjectStore for ReenteringObjects {
        async fn put(
            &self,
            object_id: ObjectId,
            bytes: acyclic_stream::OwnedBytes,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<(), crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = async {
                self.reenter().await;
                ({
                    let __object_args = (object_id, bytes, budget, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.2, __object_args.3);
                    let __object_result = AsyncObjectStore::put(
                        &self.inner,
                        __object_args.0,
                        __object_args.1,
                        &mut __object_work,
                    )
                    .await;
                    __object_work.receipt(__object_result)
                })
            }
            .await;
            context.accept(result)
        }

        async fn put_many(
            &self,
            writes: &[ObjectWrite],
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<(), crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = async {
                self.reenter().await;
                ({
                    let __object_args = (writes, budget, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.1, __object_args.2);
                    let __object_result = AsyncObjectStore::put_many(
                        &self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await;
                    __object_work.receipt(__object_result)
                })
            }
            .await;
            context.accept(result)
        }

        async fn read(
            &self,
            object_id: ObjectId,
            maximum_bytes: u64,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.read_budget()?;
            let cancellation = context.cancellation;
            let result = async {
                self.reenter().await;
                ({
                    let __object_args = (object_id, maximum_bytes, budget, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.2, __object_args.3);
                    let __object_result = AsyncObjectStore::read(
                        &self.inner,
                        __object_args.0,
                        __object_args.1,
                        &mut __object_work,
                    )
                    .await;
                    __object_work.receipt(__object_result)
                })
            }
            .await;
            context.accept(result)
        }

        async fn read_many(
            &self,
            requests: &[ObjectReadRequest],
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError>
        {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = async {
                self.reenter().await;
                ({
                    let __object_args = (requests, budget, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.1, __object_args.2);
                    let __object_result = AsyncObjectStore::read_many(
                        &self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await
                    .map(|(values, _allocation)| values);
                    __object_work.receipt(__object_result)
                })
            }
            .await;
            context.accept(result).map(|values| (values, None))
        }

        async fn contains(
            &self,
            object_id: ObjectId,
            context: &mut crate::AuthorityWork<'_>,
        ) -> Result<bool, crate::ObjectStoreError> {
            #[cfg(not(target_arch = "wasm32"))]
            if context.original.is_some() {
                return Err(crate::ObjectStoreError::NativeUnavailable);
            }
            let budget = context.work.remaining(context.budget)?;
            let cancellation = context.cancellation;
            let result = async {
                self.reenter().await;
                ({
                    let __object_args = (object_id, budget, cancellation);
                    let mut __object_work =
                        crate::AuthorityWork::new(__object_args.1, __object_args.2);
                    let __object_result = AsyncObjectStore::contains(
                        &self.inner,
                        __object_args.0,
                        &mut __object_work,
                    )
                    .await;
                    __object_work.receipt(__object_result)
                })
            }
            .await;
            context.accept(result)
        }
    }

    #[tokio::test]
    async fn shared_watch_reenters_original_meter() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("file"), vec![b'x'; 256 * 1024])?;
        let binding = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let fs = crate::Fs::new(
            crate::memory::MemoryAuthorityStore::default(),
            ReenteringObjects {
                inner: crate::memory::MemoryObjectStore::default(),
                meter: Arc::clone(&binding),
                calls: Arc::clone(&calls),
            },
            crate::EmbeddedCapabilities::MEMORY,
        );
        let workspace = fs
            .create_workspace(
                "shared-watch-reentry",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        workspace.write_text("/file", "prior watch value").await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let cancellation = CancellationToken::new();
        let budget = WorkBudget::UNBOUNDED;
        let native = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let meter = Arc::clone(&native.state);
        *binding.lock().expect("fixture binding") = Some(Arc::clone(&meter));
        let limits = checkout.volume_config().limits;
        let path = NamespacePath::from_portable(
            &crate::path::PortablePath::parse("/file", limits)?,
            limits,
        )?;
        let options = CaptureOptions {
            source_root: directory.path().to_path_buf(),
            expected_root_identity: capture_root_identity(directory.path())?,
            maximum_paths: 8,
            maximum_extent_spans: 8,
        };
        let captured = capture_watch_batch(
            &mut checkout,
            WatchBatch::Changes {
                original: Default::default(),
                epoch: 9,
                first_sequence: 3,
                next_sequence: 4,
                changes: vec![WatchChange::Created(path)],
            },
            &options,
            &CapturePolicy::default(),
            &native,
            &cancellation,
        )
        .await?;
        assert!(calls.load(Ordering::Relaxed) > 0);
        assert_eq!(captured.value.epoch, 9);
        assert_eq!(captured.value.next_sequence, 4);
        Ok(())
    }
}
