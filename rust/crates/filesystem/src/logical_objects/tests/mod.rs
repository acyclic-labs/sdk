use super::*;
use crate::distributed::object_key;
use crate::storage::ObjectKind;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_objects::v1::ObjectsProvider as _;

#[test]
fn original_request_accounts_canonical_textual_object_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let bucket = wire::BucketRef {
        name: "filesystem".to_owned(),
        ..Default::default()
    };
    let cancellation = CancellationToken::new();
    for tag in 0..=u8::MAX {
        let Ok(kind) = ObjectKind::from_canonical_tag(tag) else {
            continue;
        };
        let object_id = ObjectId {
            kind,
            digest: object_digest(kind, b"key"),
        };
        let expected = object_key(object_id);
        let copied = (bucket.name.len() + expected.len()) as u64;
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                bytes_copied: copied,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        let ((request, maximum), custody) = read_request(&bucket, object_id, 42, &mut accounting)?;
        assert_eq!(request.object_key, expected);
        assert_eq!(maximum, 42);
        assert_eq!(accounting.spent().bytes_copied, copied);
        let retained = request.object_key.capacity() as u64
            + request
                .bucket
                .as_ref()
                .expect("original bucket")
                .name
                .capacity() as u64;
        drop(request);
        release_read_custody(&mut accounting, custody, retained)?;

        let mut denied = KernelAccounting::standalone(
            WorkBudget {
                bytes_copied: copied - 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        assert!(matches!(
            read_request(&bucket, object_id, 42, &mut denied),
            Err(crate::FsError::Work(WorkError::BudgetExceeded {
                counter: "bytes_copied",
                ..
            }))
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
    ({
        store
            .put(
                first,
                first_bytes.clone(),
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .await
    })?;
    ({
        store
            .put(
                second,
                second_bytes.clone(),
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
            )
            .await
    })?;
    assert!(
        (store
            .contains(
                first,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            )
            .await)?
    );
    let missing = ObjectId {
        kind: first.kind,
        digest: crate::Digest::from_bytes([99; 32]),
    };
    assert!(
        !(store
            .contains(
                missing,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            )
            .await)?
    );
    let denied = WorkBudget {
        allocation_operations: 0,
        ..WorkBudget::UNBOUNDED
    };
    let failure = ({
        let mut __object_work = crate::AuthorityWork::new(denied, &cancellation);
        let __object_result = store.contains(first, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .expect_err("unfunded presence read");
    assert!(matches!(failure.error, ObjectStoreError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    let receipt = ({
        let authority_input_0 = &[
            ObjectReadRequest {
                object_id: second,
                maximum_bytes: 6,
            },
            ObjectReadRequest {
                object_id: first,
                maximum_bytes: 5,
            },
        ];
        let mut __object_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let __object_result = store
            .read_many(authority_input_0, &mut __object_work)
            .await
            .map(|(values, _allocation)| values);
        __object_work.receipt(__object_result)
    })?;
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
    let failure = ({
        let authority_cancellation = &CancellationToken::new();
        let mut __object_work = crate::AuthorityWork::new(budget, authority_cancellation);
        let __object_result = store.put_many(&writes, &mut __object_work).await;
        __object_work.receipt(__object_result)
    })
    .err()
    .unwrap_or_else(|| panic!("unadmitted allocation unexpectedly succeeded"));
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
) -> Result<
    acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
    acyclic_objects::v1::NativeObjectsError,
> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, 4)?;
    let mut data = Vec::new();
    data.try_reserve_exact(4)
        .map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    data.resize(data.capacity(), 0);
    acyclic_stream::PreparedOwnedBytes::new(data, allocation, Arc::clone(meter)).map_err(
        |failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => error.into(),
            _ => acyclic_objects::v1::NativeObjectsError::from(std::io::Error::from(
                std::io::ErrorKind::InvalidData,
            )),
        },
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn observed_body(
    mut prepared: acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> Result<Object, acyclic_objects::v1::NativeObjectsError> {
    meter.admit_codec(0, 4, 0)?;
    prepared.writable()[..4].copy_from_slice(b"body");
    meter.record_codec(0, 4, 0)?;
    Ok(Object::new_native(
        wire::GetObjectHeader::default(),
        prepared
            .freeze(4)
            .expect("fixture prefix fits its prefunded storage"),
        None,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn observed_text(
    value: &str,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    custody: &mut acyclic_native_runtime::ReadAllocation,
) -> Result<String, acyclic_objects::v1::NativeObjectsError> {
    let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, value.len())?;
    let mut text = String::new();
    text.try_reserve_exact(value.len())
        .map_err(|error| meter.allocation_failed(error))?;
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
            if self.error && index == 1 {
                results.push(Err(wire::ErrorCode::Unavailable.into()));
            } else {
                results.push(self.inner.put(header, body).await);
            }
        }
        let header_bytes = self.maximum.load(SeqCst) as usize;
        if header_bytes != 0
            && let Some(Ok(info)) = results.first_mut()
        {
            info.etag = "e".repeat(header_bytes);
        }
        if self.cancel {
            self.original.cancellation().cancel();
        }
        results
    }
    async fn get_batch(&self, q: Vec<GetRequest>) -> Vec<Result<Object, Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        self.calls.fetch_add(1, SeqCst);
        let maximum = q.iter().map(|request| request.1).sum();
        self.maximum.store(maximum, SeqCst);
        assert!(self.original.receipt().backend_read_operations > 0);
        assert_eq!(self.original.receipt().object_probes, q.len() as u64);
        assert!(
            self.original.receipt().peak_allocation_bytes
                >= crate::NativeMaterializationWork::accounting_state_bytes() + maximum
        );
        assert!(!self.panic, "provider batch boundary panic");
        if self.cancel {
            self.original.cancellation().cancel();
        }
        q.into_iter()
            .map(|request| {
                assert_eq!(request.0.object_key, object_key(native_test_id()));
                if self.error {
                    Err(wire::ErrorCode::Unavailable.into())
                } else {
                    Ok(Object::new(
                        wire::GetObjectHeader::default(),
                        OwnedBytes::from_static(b"body"),
                    ))
                }
            })
            .collect()
    }

    fn get_batch_native(
        &self,
        request: acyclic_objects::v1::ObjectsNativeRequest<Vec<GetRequest>>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsBatch<Object>>,
        acyclic_objects::v1::NativeObjectsError,
    > {
        acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
            let count = request.data().len();
            let mut allocation = acyclic_native_runtime::ReadAllocation::new(
                &meter,
                count
                    .checked_mul(size_of::<
                        Result<Object, acyclic_objects::v1::NativeObjectsError>,
                    >())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?,
            )?;
            let mut results = Vec::new();
            results
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            allocation.reconcile_capacity(
                results.capacity()
                    * size_of::<Result<Object, acyclic_objects::v1::NativeObjectsError>>(),
            )?;
            let mut prepared_allocation = acyclic_native_runtime::ReadAllocation::new(
                &meter,
                count
                    .checked_mul(size_of::<
                        acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
                    >())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?,
            )?;
            let mut prepared = Vec::new();
            prepared
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            prepared_allocation.reconcile_capacity(
                prepared.capacity()
                    * size_of::<
                        acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
                    >(),
            )?;
            for _ in 0..count {
                prepared.push(prepare_observed_body(&meter)?);
            }
            meter.admit_read(0)?;
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.maximum.store(
                request.data().iter().map(|request| request.1).sum(),
                std::sync::atomic::Ordering::SeqCst,
            );
            assert_eq!(self.original.receipt().object_probes, count as u64);
            assert!(!self.panic, "provider batch boundary panic");
            for (query, prepared) in request.data().iter().zip(prepared.into_iter()) {
                assert_eq!(query.0.object_key, object_key(native_test_id()));
                results.push(if self.error {
                    Err(Error::from(wire::ErrorCode::Unavailable).into())
                } else {
                    observed_body(prepared, &meter)
                });
            }
            drop(prepared_allocation);
            if self.cancel {
                self.original.cancellation().cancel();
            }
            drop(request);
            acyclic_objects::v1::ObjectsBatch::new(results, allocation)
        })
    }

    fn put_batch_native(
        &self,
        request: acyclic_objects::v1::ObjectsNativeRequest<Vec<PutRequest>>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        acyclic_objects::v1::ObjectsNativeFuture<
            '_,
            acyclic_objects::v1::ObjectsBatch<acyclic_objects::v1::ObjectsPutReceipt>,
        >,
        acyclic_objects::v1::NativeObjectsError,
    > {
        acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
            let count = request.data().len();
            let mut allocation = acyclic_native_runtime::ReadAllocation::new(
                &meter,
                count
                    .checked_mul(size_of::<
                        Result<
                            acyclic_objects::v1::ObjectsPutReceipt,
                            acyclic_objects::v1::NativeObjectsError,
                        >,
                    >())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?,
            )?;
            let mut results = Vec::new();
            results
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            allocation.reconcile_capacity(
                results.capacity()
                    * size_of::<
                        Result<
                            acyclic_objects::v1::ObjectsPutReceipt,
                            acyclic_objects::v1::NativeObjectsError,
                        >,
                    >(),
            )?;
            let mut inputs_allocation = acyclic_native_runtime::ReadAllocation::new(
                &meter,
                count
                    .checked_mul(size_of::<
                        acyclic_objects::v1::ObjectsNativeRequest<PutRequest>,
                    >())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?,
            )?;
            let mut inputs = Vec::new();
            inputs
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            inputs_allocation.reconcile_capacity(
                inputs.capacity()
                    * size_of::<acyclic_objects::v1::ObjectsNativeRequest<PutRequest>>(),
            )?;
            for (header, body) in request.data() {
                assert!(header.mutation.is_none());
                assert!(matches!(
                    header
                        .preconditions
                        .as_ref()
                        .and_then(|value| value.condition.as_ref()),
                    Some(wire::preconditions::Condition::IfAbsent(true))
                ));
                let mut custody = acyclic_native_runtime::ReadAllocation::empty(&meter);
                let name = observed_text(
                    &header.bucket.as_ref().expect("fixture bucket").name,
                    &meter,
                    &mut custody,
                )?;
                let object_key = observed_text(&header.object_key, &meter, &mut custody)?;
                let metadata = header.metadata.as_ref().expect("fixture content type");
                assert!(
                    metadata.user.is_empty()
                        && metadata.content_encoding.is_empty()
                        && metadata.cache_control.is_empty()
                        && metadata.content_disposition.is_empty()
                        && metadata.content_language.is_empty()
                );
                let content_type = observed_text(&metadata.content_type, &meter, &mut custody)?;
                let header = wire::PutObjectHeader {
                    bucket: Some(wire::BucketRef { name }),
                    object_key,
                    metadata: Some(wire::ObjectMetadata {
                        content_type,
                        ..Default::default()
                    }),
                    preconditions: Some(wire::Preconditions {
                        condition: Some(wire::preconditions::Condition::IfAbsent(true)),
                    }),
                    mutation: None,
                };
                inputs.push(acyclic_objects::v1::ObjectsNativeRequest::new(
                    (header, body.clone()),
                    custody,
                    &meter,
                )?);
            }
            // Even injected failure response metadata is admitted before mutation.
            let response_bytes = self.maximum.load(std::sync::atomic::Ordering::SeqCst) as usize;
            let mut response_allocation =
                acyclic_native_runtime::ReadAllocation::new(&meter, response_bytes)?;
            let mut response_etag = String::new();
            response_etag
                .try_reserve_exact(response_bytes)
                .map_err(|error| meter.allocation_failed(error))?;
            response_allocation.reconcile_capacity(response_etag.capacity())?;
            meter.admit_codec(0, response_bytes as u64, 0)?;
            for _ in 0..response_bytes {
                response_etag.push('e');
            }
            meter.record_codec(0, response_bytes as u64, 0)?;
            let mut response =
                (response_bytes != 0).then_some((response_etag, response_allocation));
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            for (index, input) in inputs.into_iter().enumerate() {
                results.push(if self.error && index == 1 {
                    Err(Error::from(wire::ErrorCode::Unavailable).into())
                } else {
                    match self.inner.put_native(input, Arc::clone(&meter)) {
                        Ok(future) => match future.await {
                            Ok(receipt) if index == 0 && response.is_some() => {
                                let (etag, custody) =
                                    response.take().expect("prepared fixture response");
                                Ok(acyclic_objects::v1::ObjectsPutReceipt::new(
                                    wire::ObjectInfo {
                                        size: receipt.info().size,
                                        etag,
                                        ..Default::default()
                                    },
                                    Some(custody),
                                ))
                            }
                            result => result,
                        },
                        Err(error) => Err(error),
                    }
                });
            }
            drop(inputs_allocation);
            if self.cancel {
                self.original.cancellation().cancel();
            }
            drop(request);
            acyclic_objects::v1::ObjectsBatch::new(results, allocation)
        })
    }

    async fn create_bucket(&self, q: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        self.inner.create_bucket(q).await
    }
    async fn head_bucket(&self, q: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        self.inner.head_bucket(q).await
    }
    async fn delete_bucket(
        &self,
        q: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        self.inner.delete_bucket(q).await
    }
    async fn put(
        &self,
        q: wire::PutObjectHeader,
        b: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        use std::sync::atomic::Ordering::SeqCst;
        self.calls.fetch_add(1, SeqCst);
        assert!(self.original.receipt().backend_write_operations > 0);
        assert!(self.original.receipt().bytes_copied > 0);
        let result = self.inner.put(q, b).await;
        if self.cancel {
            self.original.cancellation().cancel();
        }
        if self.error {
            Err(wire::ErrorCode::Unavailable.into())
        } else {
            result
        }
    }

    fn put_native(
        &self,
        request: acyclic_objects::v1::ObjectsNativeRequest<PutRequest>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsPutReceipt>,
        acyclic_objects::v1::NativeObjectsError,
    > {
        let future = self.inner.put_native(request, Arc::clone(&meter))?;
        acyclic_objects::v1::native_future(meter, async move {
            let result = future.await;
            if self.original.receipt().backend_write_operations != 0 {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if self.cancel {
                self.original.cancellation().cancel();
            }
            if self.error {
                Err(Error::from(wire::ErrorCode::Unavailable).into())
            } else {
                result
            }
        })
    }
    async fn get(&self, q: wire::GetObjectRequest, maximum: u64) -> Result<Object, Error> {
        use std::sync::atomic::Ordering::SeqCst;
        self.calls.fetch_add(1, SeqCst);
        self.maximum.store(maximum, SeqCst);
        assert!(self.original.receipt().backend_read_operations > 0);
        assert!(
            self.original.receipt().peak_allocation_bytes
                >= crate::NativeMaterializationWork::accounting_state_bytes()
                    + q.bucket
                        .as_ref()
                        .map_or(0, |bucket| bucket.name.len() as u64)
                    + q.object_key.len() as u64
                    + maximum
        );
        assert_eq!(q.object_key, object_key(native_test_id()));
        assert!(!self.panic, "provider boundary panic");
        if self.error {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        if self.cancel {
            self.original.cancellation().cancel();
        }
        Ok(Object::new(
            wire::GetObjectHeader::default(),
            OwnedBytes::from_static(b"body"),
        ))
    }

    fn get_native(
        &self,
        request: acyclic_objects::v1::ObjectsNativeRequest<wire::GetObjectRequest>,
        maximum: u64,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        acyclic_objects::v1::ObjectsNativeFuture<'_, Object>,
        acyclic_objects::v1::NativeObjectsError,
    > {
        acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
            let prepared = prepare_observed_body(&meter)?;
            meter.admit_read(0)?;
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.maximum
                .store(maximum, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(request.data().object_key, object_key(native_test_id()));
            assert!(!self.panic, "provider boundary panic");
            if self.error {
                return Err(Error::from(wire::ErrorCode::Unavailable).into());
            }
            let value = observed_body(prepared, &meter)?;
            if self.cancel {
                self.original.cancellation().cancel();
            }
            drop(request);
            Ok(value)
        })
    }
    async fn head(&self, q: wire::HeadObjectRequest) -> Result<wire::HeadObjectResponse, Error> {
        self.inner.head(q).await
    }
    async fn delete(
        &self,
        q: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        self.inner.delete(q).await
    }
    async fn list(&self, q: wire::ListObjectsRequest) -> Result<wire::ListObjectsResponse, Error> {
        self.inner.list(q).await
    }
    async fn create_multipart(
        &self,
        q: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        self.inner.create_multipart(q).await
    }
    async fn upload_part(
        &self,
        q: wire::UploadPartHeader,
        b: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.inner.upload_part(q, b).await
    }
    async fn list_parts(
        &self,
        q: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        self.inner.list_parts(q).await
    }
    async fn complete_multipart(
        &self,
        q: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        self.inner.complete_multipart(q).await
    }
    async fn abort_multipart(
        &self,
        q: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        self.inner.abort_multipart(q).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_write_denies_request_and_call_before_effects()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering::SeqCst;
    for deny_copy in [true, false] {
        let mut budget = WorkBudget::UNBOUNDED;
        if deny_copy {
            budget.bytes_copied = 0;
        } else {
            budget.backend_write_operations = 0;
        }
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(
            ({
                store
                    .put(
                        native_test_id(),
                        OwnedBytes::from_static(b"body"),
                        &mut crate::AuthorityWork::native(&work, &cancellation),
                    )
                    .await
            })
            .is_err()
        );
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(work.receipt().object_bytes_written, 0);
        let fs_hash_bytes = 4 + crate::OBJECT_DIGEST_ENVELOPE_BYTES;
        if deny_copy {
            assert_eq!(work.receipt().bytes_hashed, fs_hash_bytes);
        } else {
            assert!(work.receipt().bytes_hashed >= fs_hash_bytes + 4);
        }
        if deny_copy {
            assert_eq!(work.receipt().bytes_copied, 0);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_post_mutation_cancellation_keeps_acknowledgement()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, work, cancellation) =
        observed_native_store(WorkBudget::UNBOUNDED, true, false, false);
    assert!(
        ({
            store
                .put(
                    native_test_id(),
                    OwnedBytes::from_static(b"body"),
                    &mut crate::AuthorityWork::native(&work, &cancellation),
                )
                .await
        })
        .is_err()
    );
    assert_eq!(work.receipt().object_bytes_written, 4);
    assert_eq!(
        store
            .provider
            .inner
            .get(
                wire::GetObjectRequest {
                    bucket: Some(store.bucket().clone()),
                    object_key: object_key(native_test_id()),
                    ..Default::default()
                },
                4
            )
            .await?
            .body(),
        &OwnedBytes::from_static(b"body")
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_logical_lost_ack_retry_inspects_existing_with_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, work, cancellation) =
        observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
    store
        .provider
        .inner
        .put(
            wire::PutObjectHeader {
                bucket: Some(store.bucket().clone()),
                object_key: object_key(native_test_id()),
                ..Default::default()
            },
            OwnedBytes::from_static(b"body"),
        )
        .await?;
    ({
        store
            .put(
                native_test_id(),
                OwnedBytes::from_static(b"body"),
                &mut crate::AuthorityWork::native(&work, &cancellation),
            )
            .await
    })?;
    assert_eq!(work.receipt().backend_write_operations, 0);
    assert_eq!(work.receipt().object_bytes_written, 4);
    assert_eq!(work.receipt().object_bytes_read, 4);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_test_id() -> ObjectId {
    ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, b"body"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn observed_native_store(
    budget: WorkBudget,
    cancel: bool,
    panic: bool,
    error: bool,
) -> (
    LogicalObjectStore<ObservedProvider>,
    crate::NativeMaterializationWork,
    CancellationToken,
) {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation)
        .unwrap_or_else(|_| panic!("test accounting construction failed"));
    let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let provider = ObservedProvider {
        inner,
        original: work.clone(),
        calls: 0.into(),
        maximum: 0.into(),
        cancel,
        panic,
        error,
    };
    (
        LogicalObjectStore::new(Arc::new(provider), bucket),
        work,
        cancellation,
    )
}

mod batches;
mod reads;
