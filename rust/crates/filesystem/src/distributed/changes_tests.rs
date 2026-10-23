use super::*;
use crate::{EmbeddedCapabilities, Fs, MemoryObjectStore};
use acyclic_stream::{FollowFilter, MemoryStream, ReadRequest, StreamProvider};
use std::error::Error;

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn published_changes_share_the_authority_commit_and_filter_before_follow() -> TestResult {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams.clone());
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace("atomic-changes", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|error| error.error)?
        .value;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let authority = AuthorityId::from_bytes(workspace.id().into_bytes());
    let path = changes_path(workspace.id().volume_id(), &mut work)?;
    assert_eq!(streams.tail(path.clone()).await?, 1);
    let initial = streams
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 64,
        })
        .await?
        .next()
        .await
        .ok_or("initial changes missing")??;
    let initial_envelope = streams.read_commit(initial.commit_id).await?;
    let authority_path = stream_path(authority, "/records")?;
    assert!(
        initial_envelope
            .mutations
            .iter()
            .any(|mutation| matches!(mutation,
        acyclic_stream::CommittedMutation::Append(append) if append.path == authority_path))
    );
    let filter = FollowFilter::new(acyclic_stream::wire::SessionFilter {
        keys: vec![acyclic_stream::wire::KeyRange {
            start: Bytes::from_static(b"public-"),
            end: Bytes::from_static(b"public."),
        }],
        attributes: vec![],
    })?;
    let mut follow = streams.follow(path.clone(), 1, filter).await?;
    workspace.write_text("/private", "undisclosed body").await?;
    assert!(futures::poll!(follow.next()).is_pending());
    workspace
        .write_text("/public-visible", "separately authorized content")
        .await?;
    let record = follow.next().await.ok_or("visible change missing")??;
    assert_eq!(record.headers.keys().len(), 1);
    assert_eq!(
        record
            .headers
            .keys()
            .first()
            .ok_or("indexed key missing")?
            .as_ref(),
        b"public-visible"
    );
    let decoded = acyclic_stream::wire::ChangeRecord::decode(record.value.as_ref())?;
    assert_eq!(decoded.entries.len(), 1);
    assert!(
        !record
            .value
            .windows(b"undisclosed body".len())
            .any(|part| part == b"undisclosed body")
    );
    assert!(
        !record
            .value
            .windows(b"separately authorized content".len())
            .any(|part| part == b"separately authorized content")
    );
    let envelope = streams.read_commit(record.commit_id).await?;
    assert!(envelope.mutations.iter().any(|mutation| matches!(mutation,
        acyclic_stream::CommittedMutation::Append(append) if append.path == authority_path
            && append.records.iter().all(|authority_record| authority_record.commit_id == record.commit_id))));
    Ok(())
}

#[tokio::test]
async fn creation_rejects_missing_and_foreign_namespace_seals_without_side_effects() -> TestResult {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams.clone());
    let objects = MemoryObjectStore::default();
    let fs = Fs::new(store.clone(), objects, EmbeddedCapabilities::MEMORY);
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let volume = fs
        .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|error| error.error)?
        .value;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let authority = volume_authority_id(volume.id());
    let ((_, payload), _) = store
        .head_payload(authority, STREAM_RECORD_LIMIT, &mut work)
        .await?;
    let creation = crate::kernel::decode_volume_created(
        payload.as_ref().ok_or("creation missing")?,
        STREAM_RECORD_LIMIT,
        &mut |_| Ok::<_, crate::FsError>(()),
    )?;
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let seal =
        PublicationChanges::initial(fs.objects(), creation, &mut accounting, &cancellation).await?;
    for native in [false, true] {
        for publication in [None, Some(seal.clone())] {
            let foreign = crate::kernel::VolumeCreated {
                volume_id: crate::VolumeId::new(),
                ..creation
            };
            let payload = crate::kernel::encode_volume_created::<crate::FsError>(
                foreign,
                |_| Ok(()),
                |_| crate::FsError::HandleAllocationFailed,
            )?;
            let proposed = ProposedCommit {
                publication,
                operation_id: OperationId::from_bytes([99; 16]),
                fingerprint: Digest::from_bytes([99; 32]),
                payload: payload.into(),
            };
            let original =
                crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &cancellation)
            } else {
                AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
            };
            assert!(
                store
                    .create_first_record(
                        volume_authority_id(foreign.volume_id),
                        proposed.clone(),
                        &mut work,
                        None::<&mut ()>
                    )
                    .await
                    .is_err()
            );
            assert!(
                matches!(store.head(volume_authority_id(foreign.volume_id), &mut work).await, Err(error) if authority_missing(&error))
            );
            let retention = AuthorityId::from_bytes(crate::VolumeId::new().into_bytes());
            let fork = WorkspaceForkCommit {
                lineage: None,
                destination: volume_authority_id(foreign.volume_id),
                creation: proposed,
                retention,
                retained: ProposedCommit {
                    publication: None,
                    operation_id: OperationId::from_bytes([101; 16]),
                    fingerprint: Digest::from_bytes([101; 32]),
                    payload: Bytes::from_static(b"retention without publication facts"),
                },
            };
            assert!(store.fork_workspace(
                fork, PublicationPermit::Unrestricted, &mut work, None::<&mut ()>,
            ).await.is_err());
            assert!(matches!(store.head(retention, &mut work).await,
                Err(error) if authority_missing(&error)));
            let path = changes_path(foreign.volume_id, &mut work)?;
            assert!(matches!(
                streams.tail(path).await,
                Err(acyclic_stream::StreamError::NotFound)
            ));
        }
    }
    let proposed = ProposedCommit {
        publication: Some(seal.clone()),
        operation_id: OperationId::from_bytes([100; 16]),
        fingerprint: Digest::ZERO,
        payload: Bytes::new(),
    };
    let generation = GenerationId::new(creation.initial_generation_root.digest);
    assert!(
        store
            .publication_changes(
                authority,
                &proposed,
                generation,
                Some(Head::genesis(Epoch::GENESIS)),
                Sequence::new(2),
                &mut work
            )
            .is_err()
    );
    assert!(
        store
            .publication_changes(
                authority,
                &proposed,
                GenerationId::new(Digest::ZERO),
                None,
                Sequence::new(1),
                &mut work
            )
            .is_err()
    );
    Ok(())
}

#[test]
fn bounded_index_prefix_never_authorizes_an_extension_outside_a_range() -> TestResult {
    let config = VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let mut components = Vec::new();
    for _ in 0..6 {
        components.push(LogicalName::new(NameEncoding::Utf8, vec![b'x'; 240], 255)?);
    }
    let path = NamespacePath::new(components, config.limits)?;
    let key = path_key(&path, config);
    assert_eq!(key.slice().len(), acyclic_stream::MAX_RECORD_KEY_BYTES);
    let full = path
        .components()
        .iter()
        .map(|name| name.as_bytes())
        .collect::<Vec<_>>()
        .join(&b'/');
    assert!(full.starts_with(key.slice()));
    // Every possible bounded endpoint is either <= the indexed prefix, or
    // compares strictly above every extension. Equality at end excludes both.
    for cut in 0..=key.slice().len() {
        let endpoint = key.slice().get(..cut).ok_or("endpoint out of range")?;
        for last in [b'w', b'x', b'y'] {
            let mut endpoint = endpoint.to_vec();
            if let Some(byte) = endpoint.last_mut() {
                *byte = last;
            }
            if endpoint.as_slice() > key.slice() {
                assert!(endpoint > full);
            }
            if endpoint.as_slice() <= key.slice() {
                assert!(endpoint <= full);
            }
        }
    }
    Ok(())
}

#[test]
fn change_chunks_preserve_every_entry_and_obey_both_wire_bounds() -> TestResult {
    let cancellation = CancellationToken::new();
    let context = [7; 56];
    for native in [false, true] {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut work = if native { AuthorityWork::native(&original, &cancellation) }
            else { AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation) };
        let entries: Vec<_> = (0..130).map(|index| PreparedEntry {
            key: Bytes::from(format!("key-{index:03}")),
            operation: "update", metadata: vec![index],
        }).collect();
        let encoded = encode_chunks(&context, &entries, &mut work)?;
        assert_eq!(encoded.len(), 3, "one entry per record adds write fan-out");
        let mut decoded = Vec::new();
        for record in encoded {
            assert!(record.value.len() + record.headers.payload_bytes()
                <= acyclic_stream::MAX_RECORD_BYTES);
            assert!(record.headers.keys().len() <= acyclic_stream::MAX_RECORD_KEYS);
            let chunk = acyclic_stream::wire::ChangeRecord::decode(record.value.as_ref())?;
            assert_eq!(chunk.context.as_ref(), context);
            decoded.extend(chunk.entries);
        }
        assert_eq!(decoded.len(), entries.len());
        for (actual, expected) in decoded.iter().zip(&entries) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.operation, expected.operation);
            assert_eq!(actual.metadata.as_ref(), expected.metadata);
        }
        let large = [PreparedEntry { key: Bytes::from_static(b"a"), operation: "create",
            metadata: vec![0; 33_000] },
            PreparedEntry { key: Bytes::from_static(b"b"), operation: "create",
            metadata: vec![1; 33_000] }];
        assert_eq!(encode_chunks(&context, &large, &mut work)?.len(), 2);
        let duplicate = [PreparedEntry { key: Bytes::from_static(b"clipped"),
            operation: "create", metadata: vec![0] },
            PreparedEntry { key: Bytes::from_static(b"clipped"),
            operation: "create", metadata: vec![1] }];
        assert_eq!(encode_chunks(&context, &duplicate, &mut work)?.len(), 2);
        let oversized = [PreparedEntry { key: Bytes::new(), operation: "update",
            metadata: vec![0; acyclic_stream::MAX_RECORD_BYTES] }];
        assert!(encode_chunks(&context, &oversized, &mut work).is_err());
    }
    Ok(())
}

#[cfg(feature = "local")]
#[tokio::test]
async fn local_change_records_survive_reopen_with_their_authority_commit() -> TestResult {
    let directory = tempfile::tempdir()?;
    let options = crate::LocalOptions::new(directory.path());
    let cancellation = CancellationToken::new();
    let fs = Fs::local(options.clone()).await?;
    let workspace = fs.create_workspace("durable-changes", WorkBudget::UNBOUNDED, &cancellation)
        .await.map_err(|error| error.error)?.value;
    workspace.write_text("/visible", "content remains separately authorized").await?;
    let streams = fs.authority().provider();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = changes_path(workspace.id().volume_id(), &mut work)?;
    let tail = streams.tail(path.clone()).await?;
    let before: Vec<_> = streams.read(ReadRequest {
        path: path.clone(), from: 0, limit: 64,
    }).await?.collect::<Vec<_>>().await.into_iter().collect::<Result<_, _>>()?;
    assert_eq!(before.len() as u64, tail);
    for record in &before {
        let envelope = streams.read_commit(record.commit_id).await?;
        let authority_path = stream_path(volume_authority_id(workspace.id().volume_id()), "/records")?;
        assert!(envelope.mutations.iter().any(|mutation| matches!(mutation,
            acyclic_stream::CommittedMutation::Append(append) if append.path == authority_path)));
    }
    drop(streams);
    drop(workspace);
    drop(fs);
    let reopened = Fs::local(options).await?;
    let streams = reopened.authority().provider();
    let after: Vec<_> = streams.read(ReadRequest { path, from: 0, limit: 64 })
        .await?.collect::<Vec<_>>().await.into_iter().collect::<Result<_, _>>()?;
    assert_eq!(before, after);
    Ok(())
}

#[tokio::test]
async fn append_rejects_unsealed_publications_before_changing_either_stream() -> TestResult {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams.clone());
    let fs = Fs::new(store.clone(), MemoryObjectStore::default(), EmbeddedCapabilities::MEMORY);
    let cancellation = CancellationToken::new();
    let workspace = fs.create_workspace("reject-unsealed-append", WorkBudget::UNBOUNDED, &cancellation)
        .await.map_err(|error| error.error)?.value;
    workspace.write_text("/original", "separate content").await?;
    let authority = volume_authority_id(workspace.id().volume_id());
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let ((head, payload), _) = store.head_payload(authority, STREAM_RECORD_LIMIT, &mut work).await?;
    let payload = payload.ok_or("publication payload missing")?;
    let path = changes_path(workspace.id().volume_id(), &mut work)?;
    let tail = streams.tail(path.clone()).await?;
    for native in [false, true] {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut work = if native { AuthorityWork::native(&original, &cancellation) }
            else { AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation) };
        let request = crate::GuardedAppend {
            authority_id: authority, epoch: head.epoch, expected: head,
            permit: PublicationPermit::Unrestricted,
            commit: ProposedCommit {
                publication: None, operation_id: OperationId::from_bytes([102; 16]),
                fingerprint: Digest::from_bytes([102; 32]), payload: payload.clone(),
            },
        };
        assert!(store.compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await.is_err());
        assert_eq!(store.head(authority, &mut work).await?, head);
        assert_eq!(streams.tail(path.clone()).await?, tail);
        // A stale attempt must still report CAS conflict before inspecting a
        // seal that could never become visible under this expected head.
        let stale = crate::GuardedAppend { expected: Head::genesis(head.epoch), ..request };
        assert!(matches!(store.compare_and_append(stale, &mut work, None::<&mut ()>).await?,
            FsAppendOutcome::Conflict { actual } if actual == head));
    }
    Ok(())
}
