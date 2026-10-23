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
                        proposed,
                        &mut work,
                        None::<&mut ()>
                    )
                    .await
                    .is_err()
            );
            assert!(
                matches!(store.head(volume_authority_id(foreign.volume_id), &mut work).await, Err(error) if authority_missing(&error))
            );
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
