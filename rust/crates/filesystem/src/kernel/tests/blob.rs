use super::*;
use crate::Digest;
use crate::ObjectFailure;
use crate::ObjectStoreError;
use crate::memory::MemoryObjectStore;
use crate::speculation::{ResidencyHint, ResidencyReason};
use crate::storage::ObjectReadRequest;
use crate::test_support::OwnedReadObjectStore;
use crate::{CachedObjectStore, ObjectCacheOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

struct PeakRecordingStore {
    calls: AtomicU64,
    peak: u64,
}

impl AsyncObjectStore for PeakRecordingStore {
    async fn put(
        &self,
        _object_id: ObjectId,
        _bytes: Bytes,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        unreachable!()
    }

    async fn put_many(
        &self,
        _writes: &[crate::storage::ObjectWrite],
        budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let work = WorkCounters {
            peak_allocation_bytes: self.peak,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| ObjectFailure::before_work(error.into()))?;
        Ok(crate::storage::ObjectReceipt { value: (), work })
    }

    async fn read(
        &self,
        _object_id: ObjectId,
        _maximum_bytes: u64,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        unreachable!()
    }

    async fn read_many(
        &self,
        _requests: &[ObjectReadRequest],
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        unreachable!()
    }

    async fn contains(
        &self,
        _object_id: ObjectId,
        _budget: WorkBudget,
        _cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<bool> {
        unreachable!()
    }
}

struct ScriptedSource {
    bytes: Bytes,
    position: usize,
    maximum_per_read: usize,
    fail_after: Option<usize>,
    cancel_at: Option<usize>,
}

impl AsyncBlobSource for ScriptedSource {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        if self.fail_after.is_some_and(|limit| self.position >= limit) {
            return Err(std::io::Error::other("scripted source failure"));
        }
        if self.position == self.bytes.len() {
            return Ok(0);
        }
        let count = self
            .maximum_per_read
            .min(destination.len())
            .min(self.bytes.len() - self.position);
        destination[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        if self.cancel_at.is_some_and(|offset| self.position >= offset) {
            cancellation.cancel();
        }
        Ok(count)
    }
}

fn put<S: ObjectStore>(
    store: &S,
    kind: ObjectKind,
    bytes: Bytes,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let id = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    store.put(id, bytes, WorkBudget::UNBOUNDED)?;
    Ok(id)
}

#[test]
fn blob_leaf_encoding_has_a_locked_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
    let page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([7; 32]),
            },
        }]),
    };
    let expected = hex::decode(concat!(
        "4143594653424c420100",
        "0000000000000000",
        "0400000000000000",
        "01",
        "01000000",
        "0000000000000000",
        "0400000000000000",
        "07070707070707070707070707070707",
        "07070707070707070707070707070707"
    ))?;
    assert_eq!(encode_blob_page(&page, 1)?, expected);
    assert_eq!(decode_blob_page(&expected, DecodeLimits::default())?, page);
    let page_id = blob_page_id(&page, 1)?;
    assert_eq!(page_id.kind, ObjectKind::Blob);
    assert_eq!(page_id.digest, object_digest(ObjectKind::Blob, &expected));

    let internal = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 0,
            end_offset: 4,
            page: page_id,
        }]),
    };
    let internal_bytes = encode_blob_page(&internal, 1)?;
    assert_eq!(
        decode_blob_page(&internal_bytes, DecodeLimits::default())?,
        internal
    );
    assert_eq!(
        blob_page_id(&internal, 1)?.digest,
        object_digest(ObjectKind::Blob, &internal_bytes)
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn blob_page_codec_rejects_every_noncanonical_shape() -> Result<(), Box<dyn std::error::Error>> {
    let chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([7; 32]),
    };
    let valid = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk,
        }]),
    };
    for invalid in [
        BlobPage {
            first_offset: 1,
            end_offset: 1,
            node: BlobNode::Leaf(Vec::new()),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 0,
                chunk,
            }]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![
                BlobChunkRef {
                    first_offset: 0,
                    end_offset: 2,
                    chunk,
                },
                BlobChunkRef {
                    first_offset: 3,
                    end_offset: 4,
                    chunk,
                },
            ]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: chunk.digest,
                },
            }]),
        },
        BlobPage {
            first_offset: 0,
            end_offset: 4,
            node: BlobNode::Internal(vec![BlobChild {
                first_offset: 0,
                end_offset: 4,
                page: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: chunk.digest,
                },
            }]),
        },
    ] {
        assert!(encode_blob_page(&invalid, 8).is_err());
        assert!(blob_page_id(&invalid, 8).is_err());
    }
    assert!(matches!(
        encode_blob_page(&valid, 0),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));

    let encoded = encode_blob_page(&valid, 1)?;
    let mut unknown_tag = encoded.clone();
    unknown_tag[26] = 99;
    assert!(matches!(
        decode_blob_page(&unknown_tag, DecodeLimits::default()),
        Err(CanonicalDecodeError::UnknownTag {
            field: "blob_page",
            tag: 99,
        })
    ));
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_blob_page(&trailing, DecodeLimits::default()).is_err());
    assert!(decode_blob_page(&encoded[..encoded.len() - 1], DecodeLimits::default()).is_err());
    let limited = DecodeLimits {
        maximum_page_items: 0,
        ..DecodeLimits::default()
    };
    assert!(matches!(
        decode_blob_page(&encoded, limited),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));
    Ok(())
}

#[test]
fn tiny_range_reads_only_one_intersecting_chunk() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"abcd"))?;
    let second = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"efgh"))?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: first,
            },
            BlobChunkRef {
                first_offset: 4,
                end_offset: 8,
                chunk: second,
            },
        ]),
    };
    let bytes = encode_blob_page(&page, 8)?;
    let root = put(&store, ObjectKind::Blob, Bytes::from(bytes))?;
    let result = read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 5,
            length: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(result.bytes, Bytes::from_static(b"fg"));
    assert_eq!(result.work.page_reads, 1);
    assert_eq!(result.work.object_probes, 2);

    let cancellation = CancellationToken::new();
    let mut future = std::pin::pin!(read_blob_range_async(
        &store,
        root,
        ByteRange {
            offset: 5,
            length: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let Poll::Ready(asynchronous) = future.as_mut().poll(&mut context) else {
        return Err("memory-backed asynchronous blob read remained pending".into());
    };
    assert_eq!(asynchronous?, result);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut future = std::pin::pin!(read_blob_range_async(
        &store,
        root,
        ByteRange {
            offset: 5,
            length: 2,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancelled,
    ));
    let Poll::Ready(cancelled_result) = future.as_mut().poll(&mut context) else {
        return Err("pre-cancelled asynchronous blob read remained pending".into());
    };
    let failure = cancelled_result
        .err()
        .ok_or("pre-cancelled asynchronous blob read unexpectedly succeeded")?;
    assert!(matches!(failure.error, BlobReadError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn owned_blob_pages_and_chunks_preserve_exact_range_results()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let chunk = put(
        &store.inner,
        ObjectKind::BlobChunk,
        Bytes::from_static(b"owned-range"),
    )?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 11,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 11,
            chunk,
        }]),
    };
    let root = put(
        &store.inner,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&page, 8)?),
    )?;
    let read = crate::async_storage::poll_ready(read_blob_range_async(
        &store,
        root,
        ByteRange {
            offset: 1,
            length: 5,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("owned blob range read blocked")??;
    assert_eq!(&read.bytes[..], b"wned-");
    assert_eq!(read.work.backend_read_operations, 2);
    assert!(read.work.bytes_copied >= 5);
    Ok(())
}

#[test]
fn asynchronous_blob_ranges_batch_intersecting_leaf_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunks = [b"abc".as_slice(), b"def".as_slice(), b"ghi".as_slice()]
        .into_iter()
        .map(|bytes| put(&store, ObjectKind::BlobChunk, Bytes::copy_from_slice(bytes)))
        .collect::<Result<Vec<_>, _>>()?;
    let [first, second, third] = chunks.as_slice() else {
        return Err("expected exactly three chunks".into());
    };
    let page = BlobPage {
        first_offset: 0,
        end_offset: 9,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 3,
                chunk: *first,
            },
            BlobChunkRef {
                first_offset: 3,
                end_offset: 6,
                chunk: *second,
            },
            BlobChunkRef {
                first_offset: 6,
                end_offset: 9,
                chunk: *third,
            },
        ]),
    };
    let root = put(
        &store,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&page, 8)?),
    )?;
    let read = crate::async_storage::poll_ready(read_blob_range_async(
        &store,
        root,
        ByteRange {
            offset: 1,
            length: 7,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory blob batch blocked")??;
    assert_eq!(read.bytes, Bytes::from_static(b"bcdefgh"));
    assert_eq!(read.work.backend_read_operations, 2);
    assert_eq!(read.work.object_probes, 4);
    Ok(())
}

#[test]
fn blob_page_publication_rejects_the_exact_encoded_byte_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([91; 32]),
    };
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk,
        }]),
    };
    let options = BlobBuildOptions {
        chunk_bytes: 1,
        page_items: 2,
        page_bytes: 1,
        maximum_blob_bytes: 1,
    };
    let (mut batch, batch_work) = BlobBatchStore::new(&store, WorkBudget::UNBOUNDED)?;
    assert_eq!(batch_work.allocation_operations, 1);
    let mut work = WorkCounters::default();
    let failure = crate::async_storage::poll_ready(put_blob_page(
        &mut batch,
        &page,
        options,
        0,
        WorkBudget::UNBOUNDED,
        &mut work,
        &CancellationToken::new(),
    ))
    .ok_or("blob page publication blocked")?
    .err()
    .ok_or("oversized blob page unexpectedly published")?;
    assert!(matches!(
        failure.error,
        BlobBuildError::PageTooLarge {
            observed,
            maximum: 1,
        } if observed > 1
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(work, WorkCounters::default());
    Ok(())
}

#[test]
fn blob_batch_passes_only_the_residual_peak_budget_to_backends()
-> Result<(), Box<dyn std::error::Error>> {
    let store = PeakRecordingStore {
        calls: AtomicU64::new(0),
        peak: 1,
    };
    let retained_per_object = 3_u64;
    let vector_bytes = u64::try_from(BLOB_BATCH_OBJECTS)?
        * u64::try_from(size_of::<crate::storage::ObjectWrite>())?;
    let total_live = vector_bytes + retained_per_object * u64::try_from(BLOB_BATCH_OBJECTS)?;
    let total_budget = WorkBudget {
        peak_allocation_bytes: total_live + store.peak,
        ..WorkBudget::UNBOUNDED
    };
    let (mut batch, _) = BlobBatchStore::new(&store, total_budget)?;
    let cancellation = CancellationToken::new();
    for value in 0..BLOB_BATCH_OBJECTS {
        let bytes = Bytes::from(vec![
            u8::try_from(value)?;
            usize::try_from(retained_per_object)?
        ]);
        let id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let residual = WorkBudget {
            peak_allocation_bytes: total_budget
                .peak_allocation_bytes
                .checked_sub(vector_bytes + retained_per_object * u64::try_from(value + 1)?)
                .ok_or("invalid residual peak")?,
            ..WorkBudget::UNBOUNDED
        };
        crate::async_storage::poll_ready(batch.put_with_retained(
            id,
            bytes,
            retained_per_object,
            residual,
            &cancellation,
        ))
        .ok_or("batch write blocked")??;
    }
    assert_eq!(store.calls.load(Ordering::Relaxed), 1);

    let final_store = PeakRecordingStore {
        calls: AtomicU64::new(0),
        peak: 1,
    };
    let final_live = vector_bytes + retained_per_object;
    let final_budget = WorkBudget {
        peak_allocation_bytes: final_live + final_store.peak,
        ..WorkBudget::UNBOUNDED
    };
    let (mut final_batch, _) = BlobBatchStore::new(&final_store, final_budget)?;
    let bytes = Bytes::from_static(b"one");
    let id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &bytes),
    };
    crate::async_storage::poll_ready(final_batch.put_with_retained(
        id,
        bytes,
        retained_per_object,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("retained batch write blocked")??;
    assert_eq!(final_store.calls.load(Ordering::Relaxed), 0);
    crate::async_storage::poll_ready(final_batch.finish(
        WorkBudget {
            peak_allocation_bytes: final_store.peak,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    ))
    .ok_or("final batch flush blocked")??;
    assert_eq!(final_store.calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn owned_blob_page_releases_retention_when_decoded_cache_rejects_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let chunk = put(
        &store.inner,
        ObjectKind::BlobChunk,
        Bytes::from_static(b"x"),
    )?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk,
        }]),
    };
    let root = put(
        &store.inner,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&page, 8)?),
    )?;
    store
        .reject_decoded_admission
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let failure = crate::async_storage::poll_ready(read_blob_range_async(
        &store,
        root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("owned blob range read blocked")?
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        BlobReadError::Storage(ObjectStoreError::Corrupt)
    ));
    assert!(failure.work.backend_read_operations > 0);
    Ok(())
}

#[test]
fn repeated_blob_reads_reuse_decoded_indexes_without_backend_reads()
-> Result<(), Box<dyn std::error::Error>> {
    let backend = MemoryObjectStore::new(1024 * 1024)?;
    let chunk_bytes = Bytes::from_static(b"abcdefgh");
    let chunk = put(&backend, ObjectKind::BlobChunk, chunk_bytes.clone())?;
    let leaf_page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 8,
            chunk,
        }]),
    };
    let leaf = put(
        &backend,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&leaf_page, 8)?),
    )?;
    let root_page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 0,
            end_offset: 8,
            page: leaf,
        }]),
    };
    let root = put(
        &backend,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&root_page, 8)?),
    )?;
    let store = CachedObjectStore::new(
        backend,
        ObjectCacheOptions {
            maximum_entries: 8,
            maximum_bytes: 1024 * 1024,
            maximum_in_flight: 2,
            maximum_waiters_per_object: 2,
        },
    )?;
    let range = ByteRange {
        offset: 2,
        length: 4,
    };
    let cancellation = CancellationToken::new();
    let first = crate::async_storage::poll_ready(read_blob_range_async(
        &store,
        root,
        range,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("cached blob read blocked")??;
    assert_eq!(first.bytes, Bytes::from_static(b"cdef"));
    assert_eq!(first.work.backend_read_operations, 3);
    assert_eq!(store.stats()?.resident_decoded_pages, 2);

    let second = crate::async_storage::poll_ready(read_blob_range_async(
        &store,
        root,
        range,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("warm cached blob read blocked")??;
    assert_eq!(second.bytes, first.bytes);
    assert_eq!(second.work.page_reads, 2);
    assert_eq!(second.work.backend_read_operations, 0);
    assert_eq!(second.work.object_bytes_read, 0);
    assert_eq!(store.stats()?.decoded_hits, 2);
    Ok(())
}

#[test]
fn blob_read_exposes_nearest_authenticated_unvisited_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"abcd"))?;
    let second = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"efgh"))?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: first,
            },
            BlobChunkRef {
                first_offset: 4,
                end_offset: 8,
                chunk: second,
            },
        ]),
    };
    let root = put(
        &store,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&page, 8)?),
    )?;
    let read = read_blob_range(
        &store,
        root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(read.bytes, Bytes::from_static(b"abcd"));
    assert_eq!(read.work.object_probes, 2);
    assert_eq!(
        read.next_residency,
        Some(ResidencyHint {
            request: ObjectReadRequest {
                object_id: second,
                maximum_bytes: 4,
            },
            reason: ResidencyReason::SequentialRange,
        })
    );
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn blob_range_rejects_missing_short_invalid_and_wrongly_typed_inputs()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let missing_chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([88; 32]),
    };
    let missing_page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: missing_chunk,
        }]),
    };
    let missing_root = put(
        &store,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&missing_page, 8)?),
    )?;
    let missing = read_blob_range(
        &store,
        missing_root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("missing blob chunk unexpectedly succeeded")?;
    assert!(matches!(
        missing.error,
        BlobReadError::Storage(ObjectStoreError::Missing)
    ));
    assert_eq!(missing.work.page_reads, 1);

    let short_chunk = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"ab"))?;
    let short_page = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk: short_chunk,
        }]),
    };
    let short_root = put(
        &store,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&short_page, 8)?),
    )?;
    let short = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 4,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("short blob chunk unexpectedly succeeded")?;
    assert!(matches!(short.error, BlobReadError::ChunkLengthMismatch));
    assert_eq!(short.work.object_probes, 2);

    for range in [
        ByteRange {
            offset: 4,
            length: 1,
        },
        ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        let failure = read_blob_range(
            &store,
            short_root,
            range,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("invalid blob range unexpectedly succeeded")?;
        assert!(matches!(failure.error, BlobReadError::InvalidRange));
    }
    let empty = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 0,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(empty.bytes.is_empty());
    assert_eq!(empty.work.page_reads, 1);
    let wrong = read_blob_range(
        &store,
        short_chunk,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("wrong blob root kind unexpectedly succeeded")?;
    assert!(matches!(wrong.error, BlobReadError::WrongRootKind));
    assert_eq!(*wrong.work, WorkCounters::default());
    let invalid_limits = DecodeLimits {
        maximum_page_height: 0,
        ..DecodeLimits::default()
    };
    let invalid = read_blob_range(
        &store,
        short_root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        invalid_limits,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid blob limits unexpectedly succeeded")?;
    assert!(matches!(invalid.error, BlobReadError::InvalidLimits));
    assert_eq!(*invalid.work, WorkCounters::default());
    Ok(())
}

#[test]
fn blob_read_allocations_are_admitted_before_decode_queue_and_output_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let first = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"abcd"))?;
    let second = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"efgh"))?;
    let page = BlobPage {
        first_offset: 0,
        end_offset: 8,
        node: BlobNode::Leaf(vec![
            BlobChunkRef {
                first_offset: 0,
                end_offset: 4,
                chunk: first,
            },
            BlobChunkRef {
                first_offset: 4,
                end_offset: 8,
                chunk: second,
            },
        ]),
    };
    let root = put(
        &store,
        ObjectKind::Blob,
        Bytes::from(encode_blob_page(&page, 2)?),
    )?;
    let limits = DecodeLimits {
        maximum_page_items: 2,
        maximum_page_height: 1,
        maximum_visited_pages: 2,
        ..DecodeLimits::default()
    };
    let range = ByteRange {
        offset: 1,
        length: 6,
    };
    let baseline = read_blob_range(&store, root, range, limits, WorkBudget::UNBOUNDED)?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = read_blob_range(
        &store,
        root,
        range,
        limits,
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
    )
    .err()
    .ok_or("one-byte-short blob read allocation budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        BlobReadError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed == baseline.work.peak_allocation_bytes && observed_maximum == maximum
    ));
    assert_eq!(failure.work.backend_read_operations, 1);
    assert_eq!(failure.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn parent_child_bound_forgery_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let chunk = put(&store, ObjectKind::BlobChunk, Bytes::from_static(b"abcd"))?;
    let leaf = BlobPage {
        first_offset: 0,
        end_offset: 4,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 4,
            chunk,
        }]),
    };
    let leaf_bytes = encode_blob_page(&leaf, 8)?;
    let leaf_id = put(&store, ObjectKind::Blob, Bytes::from(leaf_bytes))?;
    let root = BlobPage {
        first_offset: 1,
        end_offset: 4,
        node: BlobNode::Internal(vec![BlobChild {
            first_offset: 1,
            end_offset: 4,
            page: leaf_id,
        }]),
    };
    let root_bytes = encode_blob_page(&root, 8)?;
    let root_id = put(&store, ObjectKind::Blob, Bytes::from(root_bytes))?;
    assert!(matches!(
        read_blob_range(
            &store,
            root_id,
            ByteRange {
                offset: 1,
                length: 1
            },
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED
        ),
        Err(BlobReadFailure {
            error: BlobReadError::ChildBoundsMismatch,
            ..
        })
    ));
    Ok(())
}

#[test]
fn streaming_builder_round_trips_a_multilevel_index() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(b"abcdefghi");
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 2,
            page_items: 2,
            page_bytes: 127,
            maximum_blob_bytes: 9,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(built.logical_bytes, 9);
    assert_eq!(built.work.source_bytes_read, 9);
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 1,
            length: 7,
        },
        DecodeLimits {
            maximum_page_items: 2,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(read.bytes, Bytes::from_static(b"bcdefgh"));
    assert!(read.work.page_reads >= 3);
    let narrow = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 4,
            length: 1,
        },
        DecodeLimits {
            maximum_page_items: 2,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(narrow.bytes, Bytes::from_static(b"e"));
    assert!(narrow.work.object_probes < read.work.object_probes);

    let shallow = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits {
            maximum_page_items: 2,
            maximum_page_height: 1,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("multilevel blob unexpectedly fit a one-page height bound")?;
    assert!(matches!(shallow.error, BlobReadError::CycleOrHeight));
    assert!(shallow.work.page_reads > 0);
    Ok(())
}

#[test]
fn async_and_sync_blob_build_share_exact_results_and_work() -> Result<(), Box<dyn std::error::Error>>
{
    let sync_store = MemoryObjectStore::default();
    let async_store = MemoryObjectStore::default();
    let mut sync_source = std::io::Cursor::new(b"abcdefghi");
    let mut async_source = std::io::Cursor::new(b"abcdefghi");
    let options = BlobBuildOptions {
        chunk_bytes: 2,
        page_items: 2,
        page_bytes: 127,
        maximum_blob_bytes: 9,
    };
    let synchronous = build_blob(
        &sync_store,
        &mut sync_source,
        options,
        WorkBudget::UNBOUNDED,
    )?;
    let asynchronous = crate::async_storage::poll_ready(build_blob_async(
        &async_store,
        &mut async_source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("memory-backed asynchronous blob build unexpectedly blocked")??;
    assert_eq!(asynchronous, synchronous);
    Ok(())
}

#[test]
fn measured_async_source_work_replaces_inferred_bytes_and_obeys_budget()
-> Result<(), Box<dyn std::error::Error>> {
    struct MeasuredSource {
        returned: bool,
        work: Option<WorkCounters>,
    }

    impl AsyncBlobSource for MeasuredSource {
        async fn read<'a>(
            &'a mut self,
            _destination: &'a mut [u8],
            _cancellation: &'a CancellationToken,
        ) -> std::io::Result<usize> {
            Err(std::io::Error::other("owned source should not be copied"))
        }

        async fn read_owned(
            &mut self,
            _maximum: usize,
            _cancellation: &CancellationToken,
        ) -> std::io::Result<Option<Bytes>> {
            if self.returned {
                self.work = Some(WorkCounters::default());
                return Ok(Some(Bytes::new()));
            }
            self.returned = true;
            self.work = Some(WorkCounters {
                source_bytes_read: 3,
                source_path_components: 7,
                ..WorkCounters::default()
            });
            Ok(Some(Bytes::from_static(b"abc")))
        }

        fn take_work(&mut self) -> Option<WorkCounters> {
            self.work.take()
        }
    }

    let options = BlobBuildOptions {
        chunk_bytes: 3,
        page_items: 2,
        page_bytes: 127,
        maximum_blob_bytes: 3,
    };
    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let built = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("measured source build blocked")??;
    assert_eq!(built.work.source_bytes_read, 3);
    assert_eq!(built.work.source_path_components, 7);

    let mut source = MeasuredSource {
        returned: false,
        work: None,
    };
    let mut budget = WorkBudget::UNBOUNDED;
    budget.source_path_components = 6;
    let failed = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut source,
        options,
        budget,
        &CancellationToken::new(),
    ))
    .ok_or("measured source budget check blocked")?
    .err()
    .ok_or("underbudgeted measured source unexpectedly built")?;
    assert!(matches!(failed.error, BlobBuildError::Work(_)));
    assert_eq!(failed.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn partial_async_source_failure_and_midstream_cancellation_preserve_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let options = BlobBuildOptions {
        chunk_bytes: 4,
        page_items: 2,
        page_bytes: 127,
        maximum_blob_bytes: 6,
    };
    let store = MemoryObjectStore::default();
    let mut partial = ScriptedSource {
        bytes: Bytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: None,
    };
    let built = crate::async_storage::poll_ready(build_blob_async(
        &store,
        &mut partial,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("partial source build blocked")??;
    assert_eq!(built.logical_bytes, 6);
    assert_eq!(built.work.source_bytes_read, 6);
    assert_eq!(
        read_blob_range(
            &store,
            built.root,
            ByteRange {
                offset: 0,
                length: 6,
            },
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )?
        .bytes,
        Bytes::from_static(b"abcdef")
    );

    let mut failing = ScriptedSource {
        bytes: Bytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: Some(1),
        cancel_at: None,
    };
    let failure = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut failing,
        options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .ok_or("failing source build blocked")?
    .err()
    .ok_or("failing source unexpectedly built")?;
    assert!(matches!(failure.error, BlobBuildError::Source(_)));
    assert_eq!(failure.work.source_bytes_read, 1);
    assert_eq!(failure.work.backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut cancelling = ScriptedSource {
        bytes: Bytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(1),
    };
    let cancelled = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut cancelling,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("midstream-cancelled source blocked")?
    .err()
    .ok_or("midstream-cancelled source unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work.source_bytes_read, 1);
    assert_eq!(cancelled.work.backend_write_operations, 0);

    let cancellation = CancellationToken::new();
    let mut queued = ScriptedSource {
        bytes: Bytes::from_static(b"abcdef"),
        position: 0,
        maximum_per_read: 1,
        fail_after: None,
        cancel_at: Some(5),
    };
    let cancelled = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut queued,
        options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("queued midstream cancellation blocked")?
    .err()
    .ok_or("queued midstream cancellation unexpectedly built")?;
    assert!(matches!(cancelled.error, BlobBuildError::Cancelled));
    assert_eq!(cancelled.work.source_bytes_read, 5);
    assert_eq!(cancelled.work.backend_write_operations, 0);

    let mut tight_budget = WorkBudget::UNBOUNDED;
    tight_budget.peak_allocation_bytes = cancelled.work.peak_allocation_bytes.saturating_sub(1);
    let mut bounded = std::io::Cursor::new(Bytes::from_static(b"abcdef"));
    let rejected = crate::async_storage::poll_ready(build_blob_async(
        &MemoryObjectStore::default(),
        &mut bounded,
        options,
        tight_budget,
        &CancellationToken::new(),
    ))
    .ok_or("peak-bounded source build blocked")?
    .err()
    .ok_or("one-byte-short retained-buffer budget unexpectedly built")?;
    assert!(matches!(rejected.error, BlobBuildError::Work(_)));
    assert_eq!(rejected.work.source_bytes_read, 4);
    assert_eq!(rejected.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn empty_source_builds_one_authenticated_empty_root() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(Bytes::new());
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 4,
            page_items: 2,
            page_bytes: 127,
            maximum_blob_bytes: 1,
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(built.logical_bytes, 0);
    assert_eq!(built.work.source_bytes_read, 0);
    assert_eq!(built.work.backend_write_operations, 1);
    let encoded = ObjectStore::read(&store, built.root, 127, WorkBudget::UNBOUNDED)?.value;
    assert_eq!(
        decode_blob_page(&encoded, DecodeLimits::default())?,
        BlobPage {
            first_offset: 0,
            end_offset: 0,
            node: BlobNode::Leaf(Vec::new()),
        }
    );
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 0,
            length: 0,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(read.bytes.is_empty());
    Ok(())
}

#[test]
fn pre_cancelled_async_blob_build_performs_zero_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new(b"abc");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = crate::async_storage::poll_ready(build_blob_async(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 2,
            page_items: 2,
            page_bytes: 127,
            maximum_blob_bytes: 3,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("memory-backed asynchronous blob build unexpectedly blocked")?
    .err()
    .ok_or("cancelled blob build unexpectedly succeeded")?;
    assert!(matches!(failure.error, BlobBuildError::Cancelled));
    assert_eq!(*failure.work, WorkCounters::default());
    assert_eq!(source.position(), 0);
    Ok(())
}

#[test]
fn oversized_source_consumes_only_one_detection_byte_past_bound() {
    let store = MemoryObjectStore::default();
    let mut source = std::io::Cursor::new([7_u8; 100]);
    let result = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 4,
            page_bytes: 223,
            maximum_blob_bytes: 3,
        },
        WorkBudget::UNBOUNDED,
    );
    assert!(matches!(
        result,
        Err(OperationFailure {
            error: BlobBuildError::TooLarge,
            ..
        })
    ));
    assert_eq!(source.position(), 4);
}

#[test]
fn streaming_index_memory_is_logarithmic_in_chunk_count() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let content = vec![3_u8; 4_096];
    let mut source = std::io::Cursor::new(&content);
    let built = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 1_024,
            page_bytes: 127,
            maximum_blob_bytes: 4_096,
        },
        WorkBudget::UNBOUNDED,
    )?;
    let linear_reference_bytes = u64::try_from(4_096 * size_of::<BlobChunkRef>())?;
    assert!(built.work.peak_allocation_bytes < linear_reference_bytes / 8);
    let read = read_blob_range(
        &store,
        built.root,
        ByteRange {
            offset: 2_047,
            length: 3,
        },
        DecodeLimits {
            maximum_page_items: 1_024,
            maximum_page_bytes: 127,
            ..DecodeLimits::default()
        },
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(read.bytes, Bytes::from_static(&[3, 3, 3]));
    Ok(())
}

#[test]
fn page_byte_bound_and_peak_budget_reject_before_unadmitted_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    for options in [
        BlobBuildOptions {
            chunk_bytes: 0,
            page_items: 2,
            page_bytes: 127,
            maximum_blob_bytes: 1,
        },
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 1,
            page_bytes: 127,
            maximum_blob_bytes: 1,
        },
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 2,
            page_bytes: 127,
            maximum_blob_bytes: 0,
        },
    ] {
        let mut source = std::io::Cursor::new(b"x");
        let failure = build_blob(&store, &mut source, options, WorkBudget::UNBOUNDED)
            .err()
            .ok_or("invalid blob options unexpectedly succeeded")?;
        assert!(matches!(failure.error, BlobBuildError::InvalidOptions));
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(source.position(), 0);
    }
    let mut invalid_source = std::io::Cursor::new(b"abc");
    let invalid = build_blob(
        &store,
        &mut invalid_source,
        BlobBuildOptions {
            chunk_bytes: 1,
            page_items: 2,
            page_bytes: 126,
            maximum_blob_bytes: 3,
        },
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("undersized blob page unexpectedly succeeded")?;
    assert!(matches!(invalid.error, BlobBuildError::InvalidOptions));
    assert_eq!(invalid_source.position(), 0);
    assert_eq!(*invalid.work, WorkCounters::default());

    let options = BlobBuildOptions {
        chunk_bytes: 2,
        page_items: 2,
        page_bytes: 127,
        maximum_blob_bytes: 8,
    };
    let mut baseline_source = std::io::Cursor::new(b"abcdefgh");
    let baseline = build_blob(&store, &mut baseline_source, options, WorkBudget::UNBOUNDED)?;
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let mut limited_source = std::io::Cursor::new(b"abcdefgh");
    let failure = build_blob(
        &MemoryObjectStore::default(),
        &mut limited_source,
        options,
        WorkBudget {
            peak_allocation_bytes: maximum,
            ..WorkBudget::UNBOUNDED
        },
    )
    .err()
    .ok_or("one-byte-short blob peak budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        BlobBuildError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            maximum: observed_maximum,
            ..
        }) if observed_maximum == maximum
    ));
    Ok(())
}

#[test]
fn decoded_blob_shape_and_sync_source_cancellation_fail_before_hidden_work()
-> Result<(), Box<dyn std::error::Error>> {
    let page = BlobPage {
        first_offset: 0,
        end_offset: 1,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: Digest::from_bytes([77; 32]),
            },
        }]),
    };
    let encoded = encode_blob_page(&page, 1)?;
    let limits = DecodeLimits {
        maximum_page_items: 1,
        ..DecodeLimits::default()
    };
    let shape = blob_page_decode_shape(&encoded, limits)?;
    assert_eq!(shape.kind, DecodedPageKind::Leaf);
    assert_eq!(shape.items, 1);

    let zero_items = DecodeLimits {
        maximum_page_items: 0,
        ..limits
    };
    assert!(matches!(
        blob_page_decode_shape(&encoded, zero_items),
        Err(CanonicalDecodeError::FieldTooLarge {
            observed: 1,
            maximum: 0,
        })
    ));
    let mut unknown_tag = encoded;
    unknown_tag[26] = 99;
    assert!(matches!(
        blob_page_decode_shape(&unknown_tag, limits),
        Err(CanonicalDecodeError::UnknownTag {
            field: "blob_page",
            tag: 99,
        })
    ));

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let mut source = std::io::Cursor::new(b"x");
    let mut destination = [0_u8; 1];
    let failure = crate::async_storage::poll_ready(AsyncBlobSource::read(
        &mut source,
        &mut destination,
        &cancellation,
    ))
    .ok_or("cancelled synchronous blob source suspended")?
    .err()
    .ok_or("cancelled synchronous blob source read succeeded")?;
    assert_eq!(failure.kind(), std::io::ErrorKind::Interrupted);
    assert_eq!(source.position(), 0);
    Ok(())
}

#[test]
fn allocation_and_backend_work_error_translation_is_total() {
    for error in [AllocationError::Overflow, AllocationError::ReleaseInvariant] {
        assert!(matches!(
            BlobReadError::from(error),
            BlobReadError::Work(WorkError::Overflow)
        ));
    }
    assert!(matches!(
        BlobReadError::from(AllocationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 2,
            maximum: 1,
        })),
        BlobReadError::Work(WorkError::BudgetExceeded {
            counter: "bytes_copied",
            observed: 2,
            maximum: 1,
        })
    ));
    for error in [
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            BlobReadError::from(error),
            BlobReadError::AllocationFailed
        ));
    }

    let overflow = merge_blob_backend_work(
        WorkCounters {
            object_probes: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            object_probes: 1,
            ..WorkCounters::default()
        },
        0,
    );
    assert_eq!(overflow, Err(WorkError::Overflow));
    assert_eq!(
        merge_blob_backend_work(
            WorkCounters {
                peak_allocation_bytes: 7,
                ..WorkCounters::default()
            },
            WorkCounters {
                backend_read_operations: 1,
                peak_allocation_bytes: 5,
                ..WorkCounters::default()
            },
            11,
        ),
        Ok(WorkCounters {
            backend_read_operations: 1,
            peak_allocation_bytes: 16,
            ..WorkCounters::default()
        })
    );
}

#[test]
fn defensive_blob_machine_states_fail_closed_with_exact_work()
-> Result<(), Box<dyn std::error::Error>> {
    let root = ObjectId {
        kind: ObjectKind::Blob,
        digest: Digest::ZERO,
    };
    let mut machine = BlobRangeMachine::new(
        root,
        ByteRange {
            offset: 0,
            length: 1,
        },
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
    )?;
    let incomplete = machine
        .finish()
        .err()
        .ok_or("incomplete blob traversal unexpectedly finished")?;
    assert!(matches!(
        incomplete.error,
        BlobReadError::IncompleteCoverage
    ));

    machine.pending.clear();
    let empty = machine
        .prepare_read()
        .err()
        .ok_or("empty blob frontier unexpectedly prepared a read")?;
    assert!(matches!(empty.error, BlobReadError::TraversalState));

    let chunk = BlobChunkRef {
        first_offset: 0,
        end_offset: 1,
        chunk: ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: Digest::ZERO,
        },
    };
    machine.awaiting = Some(PendingBlobRead::Chunk(chunk));
    let cached = DecodedCacheValue {
        value: Arc::new(BlobPage {
            first_offset: 0,
            end_offset: 1,
            node: BlobNode::Leaf(vec![chunk]),
        }),
        logical_bytes: 0,
    };
    let wrong_cached = machine
        .accept_cached_page(cached)
        .err()
        .ok_or("cached page accepted a pending chunk")?;
    assert!(matches!(wrong_cached.error, BlobReadError::TraversalState));

    let short = ObjectReceipt {
        value: ObjectRead {
            bytes: Bytes::new(),
            retention: ObjectReadRetention::Shared,
        },
        work: WorkCounters::default(),
    };
    let mismatch = machine
        .accept_chunk(&short, chunk)
        .err()
        .ok_or("short chunk unexpectedly satisfied its authenticated range")?;
    assert!(matches!(mismatch.error, BlobReadError::ChunkLengthMismatch));

    let cancelled = frontier::Machine::cancelled(&machine);
    assert!(matches!(cancelled.error, BlobReadError::Cancelled));
    let storage = frontier::Machine::storage_failure(
        &machine,
        WorkCounters {
            object_probes: u64::MAX,
            ..WorkCounters::default()
        },
        ObjectFailure::new(
            ObjectStoreError::Missing,
            WorkCounters {
                object_probes: 1,
                ..WorkCounters::default()
            },
        ),
    );
    assert!(matches!(
        storage.error,
        BlobReadError::Work(WorkError::Overflow)
    ));
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn impossible_blob_buffers_fail_before_allocation_and_preserve_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    let chunk = allocate_chunk_buffer(
        usize::MAX,
        0,
        0,
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("impossible chunk allocation succeeded")?;
    assert!(matches!(chunk.error, BlobBuildError::AllocationFailed));

    let mut items = Vec::<u8>::new();
    let mut live = 0;
    let mut work = WorkCounters::default();
    let page = reserve_page_items(
        &mut items,
        usize::MAX,
        &mut live,
        &mut work,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("impossible page item allocation succeeded")?;
    assert!(matches!(page.error, BlobBuildError::AllocationFailed));
    assert_eq!(live, 0);
    Ok(())
}
