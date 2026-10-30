use super::*;
use acyclic_fs::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot, TreePage,
};
use acyclic_fs::{AuthorityWork, ObjectSealer, WorkCounters, WorkError};
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
        maximum.checked_add(81).ok_or(WorkError::Overflow.into())
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
async fn sealing_preserves_the_epoch_roots_retention() -> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    // A fresh store retains its publications under epoch 0's root.
    let epoch =
        acyclic_objects::RootId::new([b"fs-epoch/".as_slice(), &0_u64.to_be_bytes()].concat())?;
    let second = LogicalObjectStore::new(provider.clone()).with_sealer(Envelope {
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
    let _second = second
        .flush_before_publish(
            acyclic_fs::PublicationScope {
                objects: &[id],
                proven_at: 0,
            },
            &mut work,
        )
        .await?;
    for _ in 0..3 {
        provider.collect().await?;
    }
    assert_eq!(
        second
            .read(id, payload.len() as u64, &mut work)
            .await?
            .bytes,
        payload
    );
    // The epoch root is the only retention of the physical locator.
    provider.release(&epoch).await?;
    for _ in 0..3 {
        provider.collect().await?;
    }
    assert!(
        second
            .read(id, payload.len() as u64, &mut work)
            .await
            .is_err()
    );
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
        bindings: None,
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
    let scope = acyclic_fs::PublicationScope {
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

#[cfg(feature = "memory")]
#[tokio::test]
async fn cold_sealed_changed_paths_follow_the_original_hard_link_bindings()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{
        CreationOperation, EmbeddedCapabilities, Fs, IdempotencyKey, Lifecycle,
        MemoryAuthorityBackend, OperationId, PublicationPermit, TransactionCommit, VolumeConfig,
        VolumeInitialization, WorkspaceError,
    };

    acyclic_native_runtime::initialize_native_io_services()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let authority = Arc::new(MemoryAuthorityBackend::new(Arc::new(
        acyclic_stream::MemoryStream::default(),
    )));
    let cancellation = CancellationToken::new();
    let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let lineage = FileId::from_bytes([51; 16]);
    let writer_origin = GenerationId::new(Digest::from_bytes([52; 32]));
    let fs = Fs::new(
        Arc::clone(&authority),
        LogicalObjectStore::new(Arc::clone(&provider)).with_sealer(Envelope {
            lineage,
            generation: writer_origin,
        }),
        EmbeddedCapabilities::MEMORY,
    );
    let files = [
        ("/entry", OwnedBytes::from_static(b"before")),
        ("/unrelated", OwnedBytes::from_static(b"unchanged")),
    ];
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            "sealed-changed-paths",
            VolumeInitialization {
                config: VolumeConfig::portable(Lifecycle::Ephemeral),
                access_policy: None,
            },
            CreationOperation::populated(OperationId::new(), &files),
            &original,
            &cancellation,
            None,
        )
        .await?;
    let workspace = created.workspace();
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &original, &cancellation)
        .await?;
    transaction
        .hard_link("/entry", "/alias", &original, &cancellation)
        .await?;
    let mut allow = |_: &NativeMaterializationWork| std::future::ready(Ok::<_, WorkspaceError>(()));
    let TransactionCommit::Committed(before) = transaction
        .commit_with_native_work(
            None,
            PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            &mut allow,
        )
        .await?
    else {
        return Err("hard-link publication did not commit".into());
    };
    drop(transaction);
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &original, &cancellation)
        .await?;
    transaction
        .write(
            "/entry",
            OwnedBytes::from_static(b"after"),
            &original,
            &cancellation,
        )
        .await?;
    let TransactionCommit::Committed(after) = transaction
        .commit_with_native_work(
            None,
            PublicationPermit::Unrestricted,
            &original,
            &cancellation,
            &mut allow,
        )
        .await?
    else {
        return Err("content publication did not commit".into());
    };
    let endpoints = [before.id(), after.id()];
    drop((transaction, before, after, created, workspace, fs));

    // A fresh store and filesystem have no retained decoded namespace pages.
    // Their writer origin differs; every old reference must keep its own seal.
    let cold_objects = LogicalObjectStore::new(provider).with_sealer(Envelope {
        lineage,
        generation: GenerationId::new(Digest::from_bytes([53; 32])),
    });
    let cold = Fs::new(
        authority,
        cold_objects.clone(),
        EmbeddedCapabilities::MEMORY,
    );
    let workspace = cold
        .open_workspace("sealed-changed-paths", &original, &cancellation)
        .await?;
    let before = workspace
        .generation(endpoints[0], &original, &cancellation)
        .await?;
    let after = workspace
        .generation(endpoints[1], &original, &cancellation)
        .await?;
    let mut reader = before
        .changed_paths_to(&after, &original, &cancellation)
        .await?
        .value;
    let mut names = Vec::new();
    let mut original_binding = None;
    while let Some(row) = reader.next().await?.value {
        let record = row.value().before.ok_or("missing baseline record")?;
        let current = row.value().after.ok_or("missing current record")?;
        assert_eq!(record.file_id, current.file_id);
        assert_eq!(record.link_count, 2);
        assert_eq!(record.bindings, current.bindings);
        let binding = record.bindings.ok_or("missing authenticated bindings")?;
        original_binding = Some(binding);
        assert_eq!(binding.kind, ObjectKind::BindingPage);
        assert_eq!(
            binding
                .storage
                .ok_or("unsealed bindings")?
                .origin_generation,
            writer_origin.digest()
        );
        let components = row.value().path.components();
        assert_eq!(components.len(), 1);
        names.push(components[0].as_bytes().to_vec());
    }
    names.sort();
    assert_eq!(names, [b"alias".to_vec(), b"entry".to_vec()]);
    let mut substituted = original_binding.ok_or("no emitted binding reference")?;
    substituted
        .storage
        .as_mut()
        .ok_or("unsealed bindings")?
        .origin_generation = GenerationId::new(Digest::from_bytes([53; 32])).digest();
    assert!(matches!(
        cold_objects
            .read(
                substituted,
                4096,
                &mut AuthorityWork::native(&original, &cancellation),
            )
            .await,
        Err(ObjectStoreError::Corrupt)
    ));
    Ok(())
}
