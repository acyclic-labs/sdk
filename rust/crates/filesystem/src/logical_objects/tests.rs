use super::*;
    use crate::distributed::object_key;
    use crate::storage::ObjectKind;
    #[cfg(not(target_arch = "wasm32"))]
    use acyclic_objects::v1::ObjectsProvider as _;

    #[test]
    fn original_request_accounts_canonical_textual_object_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let bucket = wire::BucketRef { name: "filesystem".to_owned(), ..Default::default() };
        let cancellation = CancellationToken::new();
        for tag in 0..=u8::MAX {
            let Ok(kind) = ObjectKind::from_canonical_tag(tag) else { continue };
            let object_id = ObjectId { kind, digest: object_digest(kind, b"key") };
            let expected = object_key(object_id);
            let copied = (bucket.name.len() + expected.len()) as u64;
            let mut accounting = KernelAccounting::standalone(
                WorkBudget { bytes_copied: copied, ..WorkBudget::UNBOUNDED }, &cancellation);
            let ((request, maximum), custody) =
                read_request(&bucket, object_id, 42, &mut accounting)?;
            assert_eq!(request.object_key, expected);
            assert_eq!(maximum, 42);
            assert_eq!(accounting.spent().bytes_copied, copied);
            let retained = request.object_key.capacity() as u64
                + request.bucket.as_ref().expect("original bucket").name.capacity() as u64;
            drop(request);
            release_read_custody(&mut accounting, custody, retained)?;

            let mut denied = KernelAccounting::standalone(
                WorkBudget { bytes_copied: copied - 1, ..WorkBudget::UNBOUNDED }, &cancellation);
            assert!(matches!(
                read_request(&bucket, object_id, 42, &mut denied),
                Err(crate::FsError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }))
            ));
            assert_eq!(denied.spent().bytes_copied, bucket.name.len() as u64);
        }
        Ok(())
    }
    #[tokio::test]
    async fn provider_object_batch_reads_once_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = OwnedBytes::from_static(b"first");
        let second_bytes = OwnedBytes::from_static(b"second");
        let first = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
        };
        let second = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
        };
        let cancellation = CancellationToken::new();
        store
            .put(
                first,
                first_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        store
            .put(
                second,
                second_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert!(store.contains(first, WorkBudget::UNBOUNDED, &cancellation).await?.value);
        let missing = ObjectId { kind: first.kind, digest: crate::Digest::from_bytes([99; 32]) };
        assert!(!store.contains(missing, WorkBudget::UNBOUNDED, &cancellation).await?.value);
        let denied = WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED };
        let failure = store.contains(first, denied, &cancellation).await.expect_err("unfunded presence read");
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
        let receipt = store
            .read_many(
                &[
                    ObjectReadRequest {
                        object_id: second,
                        maximum_bytes: 6,
                    },
                    ObjectReadRequest {
                        object_id: first,
                        maximum_bytes: 5,
                    },
                ],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(
            receipt.value.first().map(|value| &value.bytes),
            Some(&second_bytes)
        );
        assert_eq!(
            receipt.value.get(1).map(|value| &value.bytes),
            Some(&first_bytes)
        );
        assert_eq!(receipt.work.object_probes, 2);
        Ok(())
    }


    #[tokio::test]
    async fn provider_put_batch_rejects_unadmitted_request_allocation() {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let bytes = OwnedBytes::from_static(b"body");
        let writes = [ObjectWrite {
            object_id: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, &bytes),
            },
            bytes,
        }];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let failure = store.put_many(&writes, budget, &CancellationToken::new()).await
            .err().unwrap_or_else(|| panic!("unadmitted allocation unexpectedly succeeded"));
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct ObservedProvider {
        inner: acyclic_objects::v1::MemoryObjects,
        original: crate::NativeMaterializationWork,
        calls: std::sync::atomic::AtomicU64,
        maximum: std::sync::atomic::AtomicU64,
        cancel: bool,
        panic: bool,
        error: bool,
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_observed_body(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>, acyclic_objects::v1::NativeObjectsError> {
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, 4)?;
        let mut data = Vec::new();
        data.try_reserve_exact(4).map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        data.resize(data.capacity(), 0);
        acyclic_stream::PreparedOwnedBytes::new(data, allocation, Arc::clone(meter))
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => error.into(),
                _ => acyclic_objects::v1::NativeObjectsError::from(std::io::Error::from(std::io::ErrorKind::InvalidData)),
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_body(
        mut prepared: acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Object, acyclic_objects::v1::NativeObjectsError> {
        meter.admit_codec(0, 4, 0)?;
        prepared.writable()[..4].copy_from_slice(b"body");
        meter.record_codec(0, 4, 0)?;
        Ok(Object::new_native(wire::GetObjectHeader::default(),
            prepared.freeze(4).expect("fixture prefix fits its prefunded storage"), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_text(
        value: &str, meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        custody: &mut acyclic_native_runtime::ReadAllocation,
    ) -> Result<String, acyclic_objects::v1::NativeObjectsError> {
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, value.len())?;
        let mut text = String::new();
        text.try_reserve_exact(value.len()).map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(text.capacity())?;
        meter.admit_codec(0, value.len() as u64, 0)?;
        text.push_str(value);
        meter.record_codec(0, value.len() as u64, 0)?;
        custody.absorb(&mut allocation)?;
        Ok(text)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait::async_trait]
    impl acyclic_objects::v1::ObjectsProvider for ObservedProvider {
        async fn put_batch(&self, q: Vec<PutRequest>) -> Vec<Result<wire::ObjectInfo, Error>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            assert!(self.original.receipt().backend_write_operations > 0);
            let mut results = Vec::new();
            for (index, (header, body)) in q.into_iter().enumerate() {
                if self.error && index == 1 { results.push(Err(wire::ErrorCode::Unavailable.into())); }
                else { results.push(self.inner.put(header, body).await); }
            }
            let header_bytes = self.maximum.load(SeqCst) as usize;
            if header_bytes != 0 && let Some(Ok(info)) = results.first_mut() {
                info.etag = "e".repeat(header_bytes);
            }
            if self.cancel { self.original.cancellation().cancel(); }
            results
        }
        async fn get_batch(&self, q: Vec<GetRequest>) -> Vec<Result<Object, Error>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            let maximum = q.iter().map(|request| request.1).sum();
            self.maximum.store(maximum, SeqCst);
            assert!(self.original.receipt().backend_read_operations > 0);
            assert_eq!(self.original.receipt().object_probes, q.len() as u64);
            assert!(self.original.receipt().peak_allocation_bytes >=
                crate::NativeMaterializationWork::accounting_state_bytes() + maximum);
            assert!(!self.panic, "provider batch boundary panic");
            if self.cancel { self.original.cancellation().cancel(); }
            q.into_iter().map(|request| {
                assert_eq!(request.0.object_key, object_key(native_test_id()));
                if self.error { Err(wire::ErrorCode::Unavailable.into()) }
                else { Ok(Object::new(wire::GetObjectHeader::default(), OwnedBytes::from_static(b"body"))) }
            }).collect()
        }

        fn get_batch_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<Vec<GetRequest>>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsBatch<Object>>,
            acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let count = request.data().len();
                let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<Result<Object, acyclic_objects::v1::NativeObjectsError>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut results = Vec::new();
                results.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                allocation.reconcile_capacity(results.capacity() *
                    size_of::<Result<Object, acyclic_objects::v1::NativeObjectsError>>())?;
                let mut prepared_allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut prepared = Vec::new();
                prepared.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                prepared_allocation.reconcile_capacity(prepared.capacity() *
                    size_of::<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>())?;
                for _ in 0..count { prepared.push(prepare_observed_body(&meter)?); }
                meter.admit_read(0)?;
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.maximum.store(request.data().iter().map(|request| request.1).sum(), std::sync::atomic::Ordering::SeqCst);
                assert_eq!(self.original.receipt().object_probes, count as u64);
                assert!(!self.panic, "provider batch boundary panic");
                for (query, prepared) in request.data().iter().zip(prepared.into_iter()) {
                    assert_eq!(query.0.object_key, object_key(native_test_id()));
                    results.push(if self.error { Err(Error::from(wire::ErrorCode::Unavailable).into()) }
                        else { observed_body(prepared, &meter) });
                }
                drop(prepared_allocation);
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                acyclic_objects::v1::ObjectsBatch::new(results, allocation)
            })
        }

        fn put_batch_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<Vec<PutRequest>>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsBatch<acyclic_objects::v1::ObjectsPutReceipt>>,
            acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let count = request.data().len();
                let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<Result<acyclic_objects::v1::ObjectsPutReceipt,
                        acyclic_objects::v1::NativeObjectsError>>()).ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut results = Vec::new();
                results.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                allocation.reconcile_capacity(results.capacity() * size_of::<Result<acyclic_objects::v1::ObjectsPutReceipt,
                    acyclic_objects::v1::NativeObjectsError>>())?;
                let mut inputs_allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<acyclic_objects::v1::ObjectsNativeRequest<PutRequest>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut inputs = Vec::new();
                inputs.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                inputs_allocation.reconcile_capacity(inputs.capacity() *
                    size_of::<acyclic_objects::v1::ObjectsNativeRequest<PutRequest>>())?;
                for (header, body) in request.data() {
                    assert!(header.mutation.is_none());
                    assert!(matches!(header.preconditions.as_ref().and_then(|value| value.condition.as_ref()),
                        Some(wire::preconditions::Condition::IfAbsent(true))));
                    let mut custody = acyclic_native_runtime::ReadAllocation::empty(&meter);
                    let name = observed_text(&header.bucket.as_ref().expect("fixture bucket").name, &meter, &mut custody)?;
                    let object_key = observed_text(&header.object_key, &meter, &mut custody)?;
                    let metadata = header.metadata.as_ref().expect("fixture content type");
                    assert!(metadata.user.is_empty() && metadata.content_encoding.is_empty() &&
                        metadata.cache_control.is_empty() && metadata.content_disposition.is_empty() && metadata.content_language.is_empty());
                    let content_type = observed_text(&metadata.content_type, &meter, &mut custody)?;
                    let header = wire::PutObjectHeader {
                        bucket: Some(wire::BucketRef { name }), object_key,
                        metadata: Some(wire::ObjectMetadata { content_type, ..Default::default() }),
                        preconditions: Some(wire::Preconditions { condition: Some(wire::preconditions::Condition::IfAbsent(true)) }),
                        mutation: None,
                    };
                    inputs.push(acyclic_objects::v1::ObjectsNativeRequest::new((header, body.clone()), custody, &meter)?);
                }
                // Even injected failure response metadata is admitted before mutation.
                let response_bytes = self.maximum.load(std::sync::atomic::Ordering::SeqCst) as usize;
                let mut response_allocation = acyclic_native_runtime::ReadAllocation::new(&meter, response_bytes)?;
                let mut response_etag = String::new();
                response_etag.try_reserve_exact(response_bytes).map_err(|error| meter.allocation_failed(error))?;
                response_allocation.reconcile_capacity(response_etag.capacity())?;
                meter.admit_codec(0, response_bytes as u64, 0)?;
                for _ in 0..response_bytes { response_etag.push('e'); }
                meter.record_codec(0, response_bytes as u64, 0)?;
                let mut response = (response_bytes != 0).then_some((response_etag, response_allocation));
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for (index, input) in inputs.into_iter().enumerate() {
                    results.push(if self.error && index == 1 {
                        Err(Error::from(wire::ErrorCode::Unavailable).into())
                    } else {
                        match self.inner.put_native(input, Arc::clone(&meter)) {
                            Ok(future) => match future.await {
                                Ok(receipt) if index == 0 && response.is_some() => {
                                    let (etag, custody) = response.take().expect("prepared fixture response");
                                    Ok(acyclic_objects::v1::ObjectsPutReceipt::new(wire::ObjectInfo {
                                        size: receipt.info().size, etag, ..Default::default()
                                    }, Some(custody)))
                                }
                                result => result,
                            },
                            Err(error) => Err(error),
                        }
                    });
                }
                drop(inputs_allocation);
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                acyclic_objects::v1::ObjectsBatch::new(results, allocation)
            })
        }

        async fn create_bucket(&self, q: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> { self.inner.create_bucket(q).await }
        async fn head_bucket(&self, q: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> { self.inner.head_bucket(q).await }
        async fn delete_bucket(&self, q: wire::DeleteBucketRequest) -> Result<wire::DeleteBucketResponse, Error> { self.inner.delete_bucket(q).await }
        async fn put(&self, q: wire::PutObjectHeader, b: OwnedBytes) -> Result<wire::ObjectInfo, Error> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            assert!(self.original.receipt().backend_write_operations > 0);
            assert!(self.original.receipt().bytes_copied > 0);
            let result = self.inner.put(q, b).await;
            if self.cancel { self.original.cancellation().cancel(); }
            if self.error { Err(wire::ErrorCode::Unavailable.into()) } else { result }
        }

        fn put_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<PutRequest>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsPutReceipt>,
            acyclic_objects::v1::NativeObjectsError> {
            let future = self.inner.put_native(request, Arc::clone(&meter))?;
            acyclic_objects::v1::native_future(meter, async move {
                let result = future.await;
                if self.original.receipt().backend_write_operations != 0 {
                    self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if self.cancel { self.original.cancellation().cancel(); }
                if self.error { Err(Error::from(wire::ErrorCode::Unavailable).into()) } else { result }
            })
        }
        async fn get(&self, q: wire::GetObjectRequest, maximum: u64) -> Result<Object, Error> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            self.maximum.store(maximum, SeqCst);
            assert!(self.original.receipt().backend_read_operations > 0);
            assert!(self.original.receipt().peak_allocation_bytes >=
                crate::NativeMaterializationWork::accounting_state_bytes()
                + q.bucket.as_ref().map_or(0, |bucket| bucket.name.len() as u64)
                + q.object_key.len() as u64 + maximum);
            assert_eq!(q.object_key, object_key(native_test_id()));
            assert!(!self.panic, "provider boundary panic");
            if self.error { return Err(wire::ErrorCode::Unavailable.into()); }
            if self.cancel { self.original.cancellation().cancel(); }
            Ok(Object::new(wire::GetObjectHeader::default(), OwnedBytes::from_static(b"body")))
        }

        fn get_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<wire::GetObjectRequest>,
            maximum: u64, meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, Object>, acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let prepared = prepare_observed_body(&meter)?;
                meter.admit_read(0)?;
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.maximum.store(maximum, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(request.data().object_key, object_key(native_test_id()));
                assert!(!self.panic, "provider boundary panic");
                if self.error { return Err(Error::from(wire::ErrorCode::Unavailable).into()); }
                let value = observed_body(prepared, &meter)?;
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                Ok(value)
            })
        }
        async fn head(&self, q: wire::HeadObjectRequest) -> Result<wire::HeadObjectResponse, Error> { self.inner.head(q).await }
        async fn delete(&self, q: wire::DeleteObjectRequest) -> Result<wire::DeleteObjectResponse, Error> { self.inner.delete(q).await }
        async fn list(&self, q: wire::ListObjectsRequest) -> Result<wire::ListObjectsResponse, Error> { self.inner.list(q).await }
        async fn create_multipart(&self, q: wire::CreateMultipartRequest) -> Result<wire::MultipartUpload, Error> { self.inner.create_multipart(q).await }
        async fn upload_part(&self, q: wire::UploadPartHeader, b: OwnedBytes) -> Result<wire::UploadedPart, Error> { self.inner.upload_part(q, b).await }
        async fn list_parts(&self, q: wire::ListPartsRequest) -> Result<wire::ListPartsResponse, Error> { self.inner.list_parts(q).await }
        async fn complete_multipart(&self, q: wire::CompleteMultipartRequest) -> Result<wire::ObjectInfo, Error> { self.inner.complete_multipart(q).await }
        async fn abort_multipart(&self, q: wire::AbortMultipartRequest) -> Result<wire::AbortMultipartResponse, Error> { self.inner.abort_multipart(q).await }
    }



    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_write_denies_request_and_call_before_effects() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        for deny_copy in [true, false] {
            let mut budget = WorkBudget::UNBOUNDED;
            if deny_copy { budget.bytes_copied = 0; } else { budget.backend_write_operations = 0; }
            let (store, work, cancellation) = observed_native_store(budget, false, false, false);
            assert!(store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await.is_err());
            assert_eq!(store.provider.calls.load(SeqCst), 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert_eq!(work.receipt().object_bytes_written, 0);
            let fs_hash_bytes = 4 + crate::OBJECT_DIGEST_ENVELOPE_BYTES;
            if deny_copy { assert_eq!(work.receipt().bytes_hashed, fs_hash_bytes); }
            else { assert!(work.receipt().bytes_hashed >= fs_hash_bytes + 4); }
            if deny_copy { assert_eq!(work.receipt().bytes_copied, 0); }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_keeps_all_acknowledged_prefix_and_original_error() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, true);
        let writes = [b"first".as_slice(), b"second".as_slice(), b"third".as_slice()].map(|body| {
            ObjectWrite { object_id: ObjectId { kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, body) }, bytes: OwnedBytes::copy_from_slice(body) }
        });
        let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
            .err().ok_or("partial batch unexpectedly succeeded")?;
        assert!(matches!(error, ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })));
        assert_eq!(work.receipt().object_bytes_written, 10);
        assert!(work.receipt().bytes_hashed >= 26 + 3 * crate::OBJECT_DIGEST_ENVELOPE_BYTES);
        for index in [0, 2] {
            assert_eq!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(writes[index].object_id), ..Default::default() }, 10)
                .await?.body().as_ref(), writes[index].bytes.as_ref());
        }
        assert!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(writes[1].object_id), ..Default::default() }, 10).await.is_err());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_response_controls_deny_before_mutation() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        for partial_failure in [false, true] {
            let mut budget = WorkBudget::UNBOUNDED;
            budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes() + 4096;
            let (store, work, cancellation) = observed_native_store(budget, false, false, partial_failure);
            store.provider.maximum.store(8192, SeqCst);
            let writes = [b"body".as_slice(), b"other".as_slice()].map(|body| ObjectWrite {
                object_id: ObjectId { kind: ObjectKind::BlobChunk, digest: object_digest(ObjectKind::BlobChunk, body) },
                bytes: OwnedBytes::copy_from_slice(body),
            });
            let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
                .err().ok_or("unbounded response was accepted")?;
            assert!(matches!(error, ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError { cause: acyclic_objects::v1::NativeObjectsCause::Admission(_), .. })
                | ObjectStoreError::Work(_) | ObjectStoreError::Native(_)));
            assert_eq!(work.receipt().object_bytes_written, 0);
            assert_eq!(store.provider.calls.load(SeqCst), 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert!(work.receipt().peak_allocation_bytes <= budget.peak_allocation_bytes);
            for write in &writes {
                assert!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(write.object_id), ..Default::default() }, 10).await.is_err());
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_provider_alias_retains_original_vec_custody() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let mut custody = work.claim(4, 1, crate::WorkCounters::UNCHARGED)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(4).map_err(crate::NativeTreeMaterializationError::AllocationFailed)?;
        custody.reconcile_capacity(bytes.capacity() as u64)?;
        let copy = WorkCounters { bytes_copied: 4, ..WorkCounters::UNCHARGED };
        work.admit(copy)?;
        bytes.extend_from_slice(b"body");
        work.record_admitted(copy)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let object = crate::storage::HashedObject::from_native_vec(ObjectKind::BlobChunk,
            &mut bytes, &mut custody, &work, &cancellation)?;
        let id = object.object_id();
        let caller_alias = object.bytes().slice(1..);
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        store.put_hashed_with_native_work(object, &work, &cancellation).await?;
        drop(custody);
        let provider_alias = store.provider.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(id), ..Default::default() }, 4).await?.into_body();
        drop(caller_alias);
        let retained = work.remaining()?.peak_allocation_bytes;
        assert!(retained < baseline);
        drop(store);
        assert!(work.remaining()?.peak_allocation_bytes >= retained);
        assert!(work.remaining()?.peak_allocation_bytes < baseline);
        drop(provider_alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_post_mutation_cancellation_keeps_acknowledgement() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, true, false, false);
        assert!(store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await.is_err());
        assert_eq!(work.receipt().object_bytes_written, 4);
        assert_eq!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(native_test_id()), ..Default::default() }, 4).await?.body(),
            &OwnedBytes::from_static(b"body"));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_lost_ack_retry_inspects_existing_with_original_work() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
        store.provider.inner.put(wire::PutObjectHeader { bucket: Some(store.bucket().clone()),
            object_key: object_key(native_test_id()), ..Default::default() }, OwnedBytes::from_static(b"body")).await?;
        store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await?;
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(work.receipt().object_bytes_written, 4);
        assert_eq!(work.receipt().object_bytes_read, 4);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_test_id() -> ObjectId {
        ObjectId { kind: ObjectKind::BlobChunk, digest: object_digest(ObjectKind::BlobChunk, b"body") }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_native_store(
        budget: WorkBudget, cancel: bool, panic: bool, error: bool,
    ) -> (LogicalObjectStore<ObservedProvider>, crate::NativeMaterializationWork, CancellationToken) {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let provider = ObservedProvider { inner, original: work.clone(),
            calls: 0.into(), maximum: 0.into(), cancel, panic, error };
        (LogicalObjectStore::new(Arc::new(provider), bucket), work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_original_quota_and_all_byte_aliases() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 4;
        budget.peak_allocation_bytes = 100_000;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let baseline = work.remaining()?.peak_allocation_bytes;
        let initial_operations = work.receipt().allocation_operations;
        let read = store.read_with_native_work(native_test_id(), u64::MAX, &work, &cancellation).await?;
        assert_eq!(store.provider.maximum.load(SeqCst), 4);
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        // Original request, future and prepared-byte controls are real pre-I/O allocations.
        assert!(work.receipt().allocation_operations - initial_operations >= 7);
        assert_eq!(work.receipt().object_bytes_read, 4);
        assert_eq!(work.receipt().bytes_hashed, 4);
        assert_eq!(work.receipt().bytes_copied,
            store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64 + 4);
        let read = read;
        let alias = read.bytes.slice(1..);
        let retained = work.remaining()?.peak_allocation_bytes;
        assert!(retained < baseline - 4);
        drop(read);
        assert_eq!(work.remaining()?.peak_allocation_bytes, retained);
        drop(alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_denies_named_data_and_hash_before_their_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let before = work.receipt();
        assert!(store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt(), before);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_hashed = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().object_bytes_read, 4);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_error_cancellation_and_panic_keep_attempted_prefix() -> Result<(), Box<dyn std::error::Error>> {
        use futures::FutureExt as _;
        for (cancel, panic, error) in [(true, false, false), (false, true, false),
            (false, false, true), (true, false, true)] {
            let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
            let future = store.read_with_native_work(native_test_id(), 4, &work, &cancellation);
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if panic { assert!(result.is_err()); }
            else if error {
                assert!(matches!(result, Ok(Err(ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })))));
            } else {
                assert!(matches!(result, Ok(Err(ObjectStoreError::Cancelled | ObjectStoreError::Native(_)))));
                assert_eq!(work.receipt().object_bytes_read, 4);
            }
            assert_eq!(work.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_opaque_dto_is_not_known_memory_shared_authority() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let memory = crate::MemoryObjectStore::default();
        memory.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let shared = memory.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(shared.retention, ObjectReadRetention::Shared);
        assert_eq!(work.receipt().allocation_operations, before);
        let (logical, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
        let opaque = logical.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(opaque.retention, ObjectReadRetention::Owned { logical_bytes: 4 });
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_live_control_ceiling_denies_before_provider() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes()
            + bucket.name.len() as u64 + object_key(native_test_id()).len() as u64 + 2;
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let provider = ObservedProvider { inner, original: work.clone(),
            calls: 0.into(), maximum: 0.into(), cancel: false, panic: false, error: false };
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let error = store.read_with_native_work(native_test_id(), 100, &work, &cancellation).await
            .err().ok_or("unfunded native controls reached the provider")?;
        assert!(matches!(error, ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError { cause: acyclic_objects::v1::NativeObjectsCause::Admission(_), .. })
            | ObjectStoreError::Native(_) | ObjectStoreError::Work(_)));
        assert_eq!(store.provider.maximum.load(SeqCst), 0);
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn ordinary_and_native_logical_reads_share_request_and_payload_accounting() -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancellation = CancellationToken::new();
        store.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let ordinary = store.read(native_test_id(), 4, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(native.bytes, ordinary.value.bytes);
        assert_eq!(native.retention, ObjectReadRetention::Shared);
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert!(work.receipt().bytes_copied >= ordinary.work.bytes_copied);
        assert!(work.receipt().allocation_operations - before > ordinary.work.allocation_operations);
        Ok(())
    }


    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_is_one_original_provider_call_with_retained_vector_and_aliases()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 8;
        budget.peak_allocation_bytes = 100_000;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let initial_operations = work.receipt().allocation_operations;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let batch = store.read_many_with_native_work(&requests, &work, &cancellation).await?;
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(store.provider.maximum.load(SeqCst), 8);
        assert_eq!(work.receipt().object_bytes_read, 8);
        assert_eq!(work.receipt().bytes_hashed, 8);
        // Both vector frames, original request controls and both prepared owners are funded.
        assert!(work.receipt().allocation_operations - initial_operations >= 16);
        assert_eq!(work.receipt().bytes_copied,
            2 * (store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64) + 8);
        let alias = batch.0[0].bytes.clone();
        let (reads, vector_custody) = batch;
        assert!(vector_custody.work().shares_ledger(&work));
        assert_eq!(vector_custody.bytes(), reads.capacity() as u64 * size_of::<ObjectRead>() as u64);
        drop(reads);
        let with_vector = work.remaining()?.peak_allocation_bytes;
        drop(vector_custody);
        assert!(work.remaining()?.peak_allocation_bytes > with_vector);
        assert!(work.remaining()?.peak_allocation_bytes < baseline);
        drop(alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_finite_data_and_budget_denial_precede_provider_and_hash()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_hashed = 4;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().object_bytes_read, 8);
        assert_eq!(work.receipt().bytes_hashed, 4);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 7;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.maximum.load(SeqCst), 7);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_error_cancellation_and_panic_keep_one_call_prefix()
        -> Result<(), Box<dyn std::error::Error>> {
        use futures::FutureExt as _;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        for (cancel, panic, error) in [(true, false, false), (false, true, false),
            (false, false, true), (true, false, true)] {
            let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
            let future = store.read_many_with_native_work(&requests, &work, &cancellation);
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if panic { assert!(result.is_err()); }
            else if error {
                assert!(matches!(result, Ok(Err(ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })))));
            } else {
                assert!(matches!(result, Ok(Err(ObjectStoreError::Cancelled | ObjectStoreError::Native(_)))));
                assert_eq!(work.receipt().object_bytes_read, 8);
            }
            assert_eq!(work.receipt().object_probes, 2);
            assert_eq!(work.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn ordinary_and_native_logical_batches_share_the_genuine_batch_engine()
        -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancellation = CancellationToken::new();
        store.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let ordinary = store.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_many_with_native_work(&requests, &work, &cancellation).await?;
        assert_eq!(native.0.len(), ordinary.value.len());
        for (native, ordinary) in native.0.iter().zip(&ordinary.value) {
            assert_eq!(native.bytes, ordinary.bytes);
            assert_eq!(native.retention, ObjectReadRetention::Shared);
        }
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert!(work.receipt().bytes_copied >= ordinary.work.bytes_copied);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert!(work.receipt().allocation_operations - before > ordinary.work.allocation_operations);
        Ok(())
    }
