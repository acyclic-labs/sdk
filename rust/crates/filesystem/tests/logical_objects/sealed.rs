use super::*;
use acyclic_fs::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot, TreePage,
};
use acyclic_fs::{AuthorityWork, ObjectSealer, WorkCounters};
use acyclic_fs::{
    Digest, FileId, GenerationId, HashedObject, NativeMaterializationWork, ObjectKind, WorkBudget,
};

#[derive(Clone)]
struct Envelope {
    lineage: FileId,
    generation: GenerationId,
}

impl ObjectSealer for Envelope {
    fn prepare(
        &self,
        object: HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<HashedObject, ObjectStoreError> {
        let original = context
            .original
            .ok_or(ObjectStoreError::NativeUnavailable)?;
        let length = 81 + object.bytes().len();
        let mut allocation = original.claim(length as u64, 1, WorkCounters::UNCHARGED)?;
        original.admit(WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(&self.lineage.into_bytes());
        bytes.extend_from_slice(self.generation.digest().as_bytes());
        bytes.extend_from_slice(&object.object_id().to_bytes());
        bytes.extend_from_slice(object.bytes());
        original.charge(WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let bytes = HashedObject::from_native_vec(
            ObjectKind::BlobChunk,
            &mut bytes,
            &mut allocation,
            original,
            context.cancellation,
        )?
        .into_sealed()
        .1;
        Ok(object.seal(bytes, self.lineage, self.generation))
    }
    fn stored_limit(&self, id: ObjectId, maximum: u64) -> Result<u64, ObjectStoreError> {
        id.storage.ok_or(ObjectStoreError::Corrupt)?;
        Ok(maximum.saturating_add(81))
    }
    fn open(
        &self,
        id: ObjectId,
        stored: OwnedBytes,
        _: &mut AuthorityWork<'_>,
    ) -> Result<OwnedBytes, ObjectStoreError> {
        let association = id.storage.ok_or(ObjectStoreError::Corrupt)?;
        let plain_id = ObjectId {
            storage: None,
            ..id
        };
        if stored.get(..16) != Some(association.root_file_id.into_bytes().as_slice())
            || stored.get(16..48) != Some(association.origin_generation.as_bytes().as_slice())
            || stored.get(48..81) != Some(plain_id.to_bytes().as_slice())
        {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(stored.slice(81..))
    }
}

#[tokio::test]
async fn required_sealing_refuses_every_unprepared_write_before_provider_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let store = LogicalObjectStore::new(provider.clone()).with_sealer(Envelope {
        lineage: FileId::from_bytes([27; 16]),
        generation: GenerationId::new(Digest::from_bytes([28; 32])),
    });
    let object = HashedObject::new(ObjectKind::BlobChunk, OwnedBytes::from_static(b"secret"));
    let writes = [ObjectWrite {
        object_id: object.object_id(),
        bytes: object.bytes().clone(),
    }];
    for native in [false, true] {
        let mut work = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        assert!(matches!(
            store
                .put(object.object_id(), object.bytes().clone(), &mut work)
                .await,
            Err(ObjectStoreError::Corrupt)
        ));
        assert!(matches!(
            store.put_many(&writes, &mut work).await,
            Err(ObjectStoreError::Corrupt)
        ));
        assert!(matches!(
            store.put_hashed(object.clone(), &mut work).await,
            Err(ObjectStoreError::Corrupt)
        ));
        assert!(matches!(
            store
                .put_many_hashed(std::slice::from_ref(&object), &mut work)
                .await,
            Err(ObjectStoreError::Corrupt)
        ));
        assert_eq!(work.spent().backend_write_operations, 0);
        assert_eq!(work.spent().object_bytes_written, 0);
        assert!(matches!(provider.head(blob(object.object_id())).await,
            Err(error) if error.code == acyclic_objects::wire::ErrorCode::NotFound));
    }
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.receipt().object_bytes_written, 0);
    Ok(())
}

#[tokio::test]
async fn sealing_preserves_each_registered_roots_retention()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let a = acyclic_objects::RootId::new(b"a".to_vec())?;
    let b = acyclic_objects::RootId::new(b"b".to_vec())?;
    let first = LogicalObjectStore::new(provider.clone()).with_publication_root(a.clone());
    let second = first
        .clone()
        .with_publication_root(b.clone())
        .with_sealer(Envelope {
            lineage: FileId::from_bytes([31; 16]),
            generation: GenerationId::new(Digest::from_bytes([32; 32])),
        });
    let mut work = AuthorityWork::native(&original, &cancellation);
    let payload = OwnedBytes::from(vec![42; 1 << 16]);
    let hashed = acyclic_stream::hashed_on_this_thread;
    let before = hashed();
    let object = second
        .prepare_hashed(
            HashedObject::new_with_native_work(
                ObjectKind::BlobChunk,
                payload.clone(),
                &original,
                &cancellation,
            )?,
            &mut work,
        )
        .await?;
    assert_eq!(hashed() - before, 2 * payload.len() as u64 + 81);
    let id = object.object_id();
    let before = hashed();
    second.put_hashed(object, &mut work).await?;
    assert!(hashed() - before < payload.len() as u64);
    // Negative control: a provider receiving raw bytes must hash them.
    let before = hashed();
    provider.put(payload.clone(), Vec::new()).await?;
    assert!(hashed() - before >= payload.len() as u64);
    let _first = first
        .flush_before_publish(
            acyclic_fs::PublicationScope::Everything,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation),
        )
        .await?;
    let _second = second
        .flush_before_publish(acyclic_fs::PublicationScope::Everything, &mut work)
        .await?;
    provider.release(&a).await?;
    for _ in 0..3 {
        provider.collect().await?;
    }
    assert_eq!(second.read(id, u64::MAX, &mut work).await?.bytes, payload);
    // The second root is the only remaining retention of the physical locator.
    provider.release(&b).await?;
    for _ in 0..3 {
        provider.collect().await?;
    }
    assert!(second.read(id, u64::MAX, &mut work).await.is_err());
    Ok(())
}

#[tokio::test]
async fn sealed_references_reopen_across_generations_with_real_cas_closure()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut work = AuthorityWork::native(&original, &cancellation);
    let lineage = FileId::from_bytes([17; 16]);
    let generation = GenerationId::new(Digest::from_bytes([18; 32]));
    let store = LogicalObjectStore::new(provider.clone()).with_sealer(Envelope {
        lineage,
        generation,
    });
    #[cfg(feature = "conformance")]
    let store = acyclic_stream::testing::Counted::new(store);
    let encode_error = |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed;
    let observe = |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(());
    let metadata =
        acyclic_fs::kernel::encode_file_metadata(FileMetadata::default(), observe, encode_error)?;
    let tree = acyclic_fs::kernel::encode_tree_page(
        &TreePage::Leaf(Vec::new()),
        8,
        observe,
        encode_error,
    )?;
    let mut objects = Vec::new();
    for (kind, bytes) in [
        (ObjectKind::Metadata, metadata),
        (ObjectKind::TreePage, tree),
    ] {
        let object =
            HashedObject::new_with_native_work(kind, bytes.into(), &original, &cancellation)?;
        objects.push(store.prepare_hashed(object, &mut work).await?);
    }
    store.put_many_hashed(&objects, &mut work).await?;
    let record = FileRecord {
        file_id: lineage,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: objects[0].object_id(),
        payload: FilePayload::Directory {
            entries: objects[1].object_id(),
        },
    };
    let table = acyclic_fs::kernel::encode_file_table_page(
        &FileTablePage::Leaf(vec![record]),
        8,
        observe,
        encode_error,
    )?;
    let table = store
        .prepare_hashed(
            HashedObject::new_with_native_work(
                ObjectKind::FileTablePage,
                table.into(),
                &original,
                &cancellation,
            )?,
            &mut work,
        )
        .await?;
    let table_id = table.object_id();
    store.put_hashed(table, &mut work).await?;
    drop(store);
    let next = LogicalObjectStore::new(provider.clone()).with_sealer(Envelope {
        lineage,
        generation: GenerationId::new(Digest::from_bytes([19; 32])),
    });
    let mut root = GenerationRoot {
        volume_id: acyclic_fs::VolumeId::from_bytes([20; 16]),
        root_file_id: lineage,
        file_table: table_id,
        parents: Vec::new(),
        required_features: 0,
    };
    // A transferred generation authenticates its lineage without requiring
    // the destination to import and retain every ancestor's contents.
    let parent_bytes = acyclic_fs::kernel::encode_generation_root(&root, observe, encode_error)?;
    let parent = next
        .prepare_hashed(
            HashedObject::new_with_native_work(
                ObjectKind::GenerationRoot,
                parent_bytes.into(),
                &original,
                &cancellation,
            )?,
            &mut work,
        )
        .await?;
    let parent_id = parent.object_id();
    root.parents.push(GenerationId::from_root(parent_id));
    drop(parent);
    let bytes = acyclic_fs::kernel::encode_generation_root(&root, observe, encode_error)?;
    let root_object = next
        .prepare_hashed(
            HashedObject::new_with_native_work(
                ObjectKind::GenerationRoot,
                bytes.into(),
                &original,
                &cancellation,
            )?,
            &mut work,
        )
        .await?;
    let root_id = root_object.object_id();
    next.put_hashed(root_object, &mut work).await?;
    let retained_reference = serde_json::to_vec(&GenerationId::from_root(root_id))?;
    drop(next);
    let cold = LogicalObjectStore::new(provider.clone()).with_sealer(Envelope {
        lineage,
        generation: GenerationId::new(Digest::from_bytes([21; 32])),
    });
    let historical: GenerationId = serde_json::from_slice(&retained_reference)?;
    let recovered = historical.object_id();
    let ids = [recovered];
    let scope = acyclic_fs::PublicationScope::Closure {
        objects: &ids,
        proven_at: 0,
    };
    // A cold sealed root uses the same commit boundary as an unsealed root.
    // Refusal must precede the durability call and its locator allocation.
    let mut denied = AuthorityWork::new(
        WorkBudget {
            durability_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(matches!(
        cold.flush_before_publish(scope, &mut denied).await,
        Err(ObjectStoreError::Work(_))
    ));
    assert_eq!(denied.spent(), WorkCounters::UNCHARGED);
    let mut commit = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let _hold = cold.flush_before_publish(scope, &mut commit).await?;
    assert_eq!(commit.spent().durability_operations, 1);
    let before = original.receipt().durability_operations;
    let live = original.live_allocation_bytes();
    let _hold = cold.flush_before_publish(scope, &mut work).await?;
    assert_eq!(original.receipt().durability_operations, before + 1);
    assert_eq!(original.live_allocation_bytes(), live);
    let read = cold.read(recovered, 1024, &mut work).await?;
    assert!(
        cold.put(recovered, read.bytes.clone(), &mut work)
            .await
            .is_err()
    );
    assert!(
        matches!(provider.head(acyclic_objects::Digest::from_bytes(*recovered.digest.as_bytes())).await,
        Err(error) if error.code == acyclic_objects::wire::ErrorCode::NotFound)
    );
    let decoded = acyclic_fs::kernel::decode_generation_root::<
        acyclic_fs::kernel::CanonicalDecodeError,
    >(&read.bytes, Default::default(), |_| Ok(()))?;
    assert_eq!(decoded, root);
    assert_eq!(
        decoded
            .file_table
            .storage
            .ok_or("missing table association")?
            .origin_generation,
        generation.digest()
    );
    let read = cold.read(decoded.file_table, 1024, &mut work).await?;
    assert_eq!(
        acyclic_fs::kernel::decode_file_table_page::<acyclic_fs::kernel::CanonicalDecodeError>(
            &read.bytes,
            Default::default(),
            &mut |_| Ok(())
        )?,
        FileTablePage::Leaf(vec![record])
    );
    let head = provider.head(blob(recovered)).await?;
    assert_eq!(head.references, vec![blob(table_id)]);
    assert!(matches!(provider.head(blob(parent_id)).await,
        Err(error) if error.code == acyclic_objects::wire::ErrorCode::NotFound));
    assert_ne!(blob(recovered).as_bytes(), recovered.digest.as_bytes());
    // Transfer exact CAS bytes across a different writer origin and cold reopen.
    let limits = acyclic_fs::kernel::ClosureLimits {
        maximum_objects: 16,
        maximum_files: 8,
        maximum_object_bytes: 1024 * 1024,
        decode: Default::default(),
        profile: acyclic_fs::FilesystemProfile::Portable,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
    };
    let source = acyclic_fs::CachedObjectStore::new(cold, Default::default())?;
    let manifest = acyclic_fs::kernel::build_generation_export_manifest_async(
        &source,
        root.volume_id,
        acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
        recovered,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?
    .value;
    let batch = acyclic_fs::kernel::export_generation_batch_async(
        &source,
        &manifest,
        acyclic_fs::kernel::TransferCursor::START,
        16,
        1024,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(batch.work.backend_read_operations, 1);
    let destination_provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let destination = acyclic_fs::CachedObjectStore::new(
        LogicalObjectStore::new(destination_provider.clone()).with_sealer(Envelope {
            lineage,
            generation: GenerationId::new(Digest::from_bytes([23; 32])),
        }),
        Default::default(),
    )?;
    let bodies = batch
        .value
        .objects
        .iter()
        .map(|object| object.bytes.clone())
        .collect::<Vec<_>>();
    for (&id, body) in manifest.objects.iter().zip(&bodies) {
        assert_eq!(
            provider
                .get(blob(id), acyclic_objects::ByteRange::FULL, 2048)
                .await?
                .body(),
            body
        );
    }
    // A body from another physical reference cannot enter the destination.
    assert!(
        acyclic_fs::kernel::import_generation_batch_async(
            &destination,
            &manifest,
            acyclic_fs::kernel::TransferCursor::START,
            &[bodies.last().ok_or("missing transfer root")?.clone()],
            16,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .is_err()
    );
    assert!(
        destination_provider
            .head(blob(manifest.objects[0]))
            .await
            .is_err()
    );
    for change_origin in [true, false] {
        let mut wrong = manifest.clone();
        let storage = wrong.objects[0]
            .storage
            .as_mut()
            .ok_or("missing transfer association")?;
        if change_origin {
            storage.origin_generation = Digest::from_bytes([25; 32]);
        } else {
            storage.root_file_id = FileId::from_bytes([26; 16]);
        }
        assert!(
            acyclic_fs::kernel::import_generation_batch_async(
                &destination,
                &wrong,
                acyclic_fs::kernel::TransferCursor::START,
                &bodies[..1],
                16,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .is_err()
        );
        assert!(
            destination_provider
                .head(blob(manifest.objects[0]))
                .await
                .is_err()
        );
    }
    acyclic_fs::kernel::import_generation_batch_async(
        &destination,
        &manifest,
        acyclic_fs::kernel::TransferCursor::START,
        &bodies,
        16,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    drop(destination);
    let cold = LogicalObjectStore::new(destination_provider.clone()).with_sealer(Envelope {
        lineage,
        generation: GenerationId::new(Digest::from_bytes([24; 32])),
    });
    acyclic_fs::kernel::authenticate_generation_export_manifest_async(
        &cold,
        &manifest,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    for &id in &manifest.objects {
        assert_eq!(
            destination_provider.head(blob(id)).await?.references,
            provider.head(blob(id)).await?.references
        );
    }
    for wrong_origin in [true, false] {
        let mut wrong = recovered;
        let association = wrong.storage.as_mut().ok_or("missing root association")?;
        if wrong_origin {
            association.origin_generation = generation.digest();
        } else {
            association.root_file_id = FileId::from_bytes([22; 16]);
        }
        assert!(cold.read(wrong, 1024, &mut work).await.is_err());
    }
    Ok(())
}

fn blob(id: ObjectId) -> acyclic_objects::Digest {
    acyclic_objects::Digest::from_bytes(
        *id.storage
            .map_or(id.digest, |storage| storage.locator)
            .as_bytes(),
    )
}
