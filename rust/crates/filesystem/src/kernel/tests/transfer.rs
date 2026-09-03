use super::*;
use crate::async_storage::{AsyncObjectStore, ImmediateObjectStore};
use crate::foundation::{FileId, GenerationId, VolumeId};
use crate::kernel::{
    DecodeLimits, FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    MetadataField, TreePage, encode_file_metadata, encode_file_table_page, encode_generation_root,
    encode_tree_page,
};
use crate::memory::MemoryObjectStore;
use crate::model::{FilesystemProfile, Lifecycle, VolumeConfig};
use crate::storage::{ObjectKind, ObjectStore, object_digest};
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn ready<T>(future: impl Future<Output = T>) -> Result<T, &'static str> {
    let mut future = std::pin::pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => Err("immediate transfer store unexpectedly blocked"),
    }
}

fn fixture() -> (GenerationExportManifest, [Bytes; 2]) {
    let chunk = Bytes::from_static(b"transfer-chunk");
    let root_body = Bytes::from_static(b"transfer-root");
    let chunk_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &chunk),
    };
    let root_id = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: object_digest(ObjectKind::GenerationRoot, &root_body),
    };
    (
        GenerationExportManifest {
            volume_id: VolumeId::from_bytes([7; 16]),
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            generation_root: root_id,
            generation_id: GenerationId::new(root_id.digest),
            objects: vec![chunk_id, root_id],
            file_count: 1,
        },
        [chunk, root_body],
    )
}

fn empty_metadata() -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: MetadataField::Unavailable,
        accessed_ns: MetadataField::Unavailable,
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn put(
    store: &MemoryObjectStore,
    kind: ObjectKind,
    bytes: Vec<u8>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object_id = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    ObjectStore::put(store, object_id, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(object_id)
}

fn minimal_generation(
    store: &MemoryObjectStore,
    volume_id: VolumeId,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(empty_metadata())?,
    )?;
    let entries = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(&TreePage::Leaf(Vec::new()), 8)?,
    )?;
    let root_file_id = FileId::from_bytes([41; 16]);
    let table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![FileRecord {
                file_id: root_file_id,
                kind: FileKind::Directory,
                link_count: 1,
                metadata,
                payload: FilePayload::Directory { entries },
            }]),
            8,
        )?,
    )?;
    put(
        store,
        ObjectKind::GenerationRoot,
        encode_generation_root(&GenerationRoot {
            volume_id,
            root_file_id,
            file_table: table,
            parents: Vec::new(),
            required_features: 0,
        })?,
    )
}

fn closure_limits() -> ClosureLimits {
    ClosureLimits {
        decode: DecodeLimits::default(),
        maximum_objects: 16,
        maximum_files: 8,
        maximum_object_bytes: 1024 * 1024,
        profile: FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    }
}

#[test]
fn resumable_batches_preserve_manifest_order_and_idempotent_import()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, bodies) = fixture();
    let source = MemoryObjectStore::default();
    let destination = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    for (object_id, body) in manifest.objects.iter().copied().zip(bodies.iter().cloned()) {
        ready(AsyncObjectStore::put(
            &source,
            object_id,
            body,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))??;
    }

    let first = ready(export_generation_batch_async(
        &source,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(first.value.first_object, TransferCursor::START);
    assert_eq!(first.value.next, Some(TransferCursor::new(1)));
    assert_eq!(first.value.objects.len(), 1);
    assert_eq!(first.value.objects[0].bytes, bodies[0]);
    assert_eq!(first.work.backend_read_operations, 1);
    assert!(first.work.peak_allocation_bytes > 0);

    let imported = ready(import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::START,
        &[first.value.objects[0].bytes.clone()],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(imported.value, TransferCursor::new(1));
    let repeated = ready(import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::START,
        &[bodies[0].clone()],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(repeated.value, TransferCursor::new(1));

    let second = ready(export_generation_batch_async(
        &source,
        &manifest,
        TransferCursor::new(1),
        8,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(second.value.next, None);
    assert_eq!(second.value.objects[0].bytes, bodies[1]);
    ready(import_generation_batch_async(
        &destination,
        &manifest,
        TransferCursor::new(1),
        &[second.value.objects[0].bytes.clone()],
        8,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    for (object_id, body) in manifest.objects.iter().copied().zip(bodies) {
        let read = ObjectStore::read(
            &destination,
            object_id,
            manifest.config.limits.maximum_object_bytes,
            WorkBudget::UNBOUNDED,
        )?;
        assert_eq!(read.value.bytes, body);
    }
    Ok(())
}

#[test]
fn terminal_invalid_and_cancelled_batches_do_no_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    fn assert_immediate<T: ImmediateObjectStore>(_: &T) {}
    let (manifest, _) = fixture();
    let store = MemoryObjectStore::default();
    assert_immediate(&store);
    let cancellation = CancellationToken::new();
    let terminal = ready(export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(2),
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert!(terminal.value.objects.is_empty());
    assert_eq!(terminal.work, WorkCounters::default());

    let invalid = ready(export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(3),
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .err()
    .ok_or("invalid transfer cursor unexpectedly succeeded")?;
    assert!(matches!(
        invalid.error,
        GenerationTransferError::InvalidCursor
    ));
    assert_eq!(*invalid.work, WorkCounters::default());

    cancellation.cancel();
    let cancelled = ready(import_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        &[Bytes::from_static(b"ignored")],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .err()
    .ok_or("cancelled transfer unexpectedly succeeded")?;
    assert!(matches!(
        cancelled.error,
        GenerationTransferError::Cancelled(_)
    ));
    assert_eq!(*cancelled.work, WorkCounters::default());
    Ok(())
}

#[test]
fn transfer_page_bounds_are_total_and_terminal_import_is_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, _) = fixture();
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();

    for (maximum_objects, maximum_object_bytes) in
        [(0, manifest.config.limits.maximum_object_bytes), (1, 0)]
    {
        let failure = ready(export_generation_batch_async(
            &store,
            &manifest,
            TransferCursor::START,
            maximum_objects,
            maximum_object_bytes,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .err()
        .ok_or("zero export bound was accepted")?;
        assert!(matches!(failure.error, GenerationTransferError::EmptyBatch));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    let terminal = ready(import_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::new(2),
        &[],
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(terminal.value, TransferCursor::new(2));
    assert_eq!(terminal.work, WorkCounters::default());

    let tiny_budget = WorkBudget {
        bytes_copied: 0,
        ..WorkBudget::UNBOUNDED
    };
    let budget_failure = ready(export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        tiny_budget,
        &cancellation,
    ))?
    .err()
    .ok_or("transfer request-vector copy escaped its work budget")?;
    assert!(matches!(
        budget_failure.error,
        GenerationTransferError::Work(_)
    ));
    assert_eq!(*budget_failure.work, WorkCounters::default());

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_failure = ready(export_generation_batch_async(
        &store,
        &manifest,
        TransferCursor::START,
        1,
        manifest.config.limits.maximum_object_bytes,
        WorkBudget::UNBOUNDED,
        &cancelled,
    ))?
    .err()
    .ok_or("cancelled export succeeded")?;
    assert!(matches!(
        cancellation_failure.error,
        GenerationTransferError::Cancelled(_)
    ));
    assert_eq!(*cancellation_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn import_rejects_every_noncanonical_page_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let (manifest, fixture_bodies) = fixture();
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    for (cursor, bodies, maximum_objects, expected) in [
        (
            TransferCursor::START,
            Vec::<Bytes>::new(),
            1,
            GenerationTransferError::EmptyBatch,
        ),
        (
            TransferCursor::START,
            vec![fixture_bodies[0].clone()],
            0,
            GenerationTransferError::EmptyBatch,
        ),
        (
            TransferCursor::new(3),
            vec![fixture_bodies[0].clone()],
            1,
            GenerationTransferError::InvalidCursor,
        ),
        (
            TransferCursor::new(1),
            vec![fixture_bodies[0].clone(), fixture_bodies[1].clone()],
            2,
            GenerationTransferError::TooManyObjects,
        ),
        (
            TransferCursor::START,
            vec![fixture_bodies[0].clone(), fixture_bodies[1].clone()],
            1,
            GenerationTransferError::TooManyObjects,
        ),
    ] {
        let failure = ready(import_generation_batch_async(
            &store,
            &manifest,
            cursor,
            &bodies,
            maximum_objects,
            WorkBudget::UNBOUNDED,
            &cancellation,
        ))?
        .err()
        .ok_or("invalid import page was accepted")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(*failure.work, WorkCounters::default());
    }
    Ok(())
}

#[test]
fn manifest_construction_rejects_foreign_volume_after_complete_authentication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let actual_volume = VolumeId::from_bytes([42; 16]);
    let generation = minimal_generation(&store, actual_volume)?;
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);

    let mismatch = ready(build_generation_export_manifest_async(
        &store,
        VolumeId::from_bytes([43; 16]),
        config,
        generation,
        closure_limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))?
    .err()
    .ok_or("foreign-volume generation unexpectedly produced a manifest")?;
    assert!(matches!(
        mismatch.error,
        GenerationTransferError::ManifestMismatch
    ));
    assert!(mismatch.work.backend_read_operations > 0);
    assert!(mismatch.work.page_reads > 0);

    let manifest = ready(build_generation_export_manifest_async(
        &store,
        actual_volume,
        config,
        generation,
        closure_limits(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))??;
    assert_eq!(manifest.value.volume_id, actual_volume);
    assert_eq!(manifest.value.generation_root, generation);
    assert_eq!(manifest.value.file_count, 1);
    Ok(())
}
