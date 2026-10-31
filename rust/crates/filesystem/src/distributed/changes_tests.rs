use super::*;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::{EmbeddedCapabilities, Fs, MemoryObjectStore};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{FollowFilter, MemoryStream, ReadRequest, StreamProvider};
use std::error::Error;

type TestResult = Result<(), Box<dyn Error>>;

#[tokio::test]
async fn proposal_normalization_reuses_receipt_owned_publication_roots() -> TestResult {
    crate::initialize_test_native_io();
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("owned-roots", crate::performance::BOUNDARY_BUDGET, &cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let authority = AuthorityId::from_bytes(workspace.id().into_bytes());
    for published in [false, true] {
        if published {
            workspace.write("/file", Bytes::from_static(b"new")).await?;
        }
        let (head, root) = fixture_publication_root(&store, authority, &cancel).await?;
        let request = crate::kernel::PublishGenerationRequest {
            authority_id: authority,
            volume_id: workspace.id().volume_id(),
            epoch: head.epoch,
            expected: head,
            operation_id: OperationId::new(),
            generation_root: root,
        };
        for native in [false, true] {
            let original =
                crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancel)
                    .map_err(|(error, _)| error)?;
            let mut accounting = if native {
                crate::kernel::KernelAccounting::native(&original, &cancel)
            } else {
                crate::kernel::KernelAccounting::standalone(
                    crate::performance::BOUNDARY_BUDGET,
                    &cancel,
                )
            };
            assert_eq!(
                PublicationChanges::current_root(&store, request, &mut accounting, &cancel).await?,
                (head, root)
            );
            assert_eq!(
                accounting.spent().authority_records_read,
                0,
                "proposal normalization resampled its receipt-owned publication root"
            );
            let actor = crate::kernel::authority_actor(
                store.operation_claims(),
                store.actor_scope(),
                authority,
                &mut accounting,
            )?;
            assert!(
                actor
                    .publication_root(crate::kernel::PublishGenerationRequest {
                        volume_id: crate::VolumeId::new(),
                        ..request
                    })
                    .is_none(),
                "a publication root escaped its volume identity"
            );
            let mut future = request;
            future.expected.sequence = head.sequence.checked_next()?;
            assert!(
                actor.publication_root(future).is_none(),
                "an older projection replaced the caller's newer observation"
            );
        }
    }
    Ok(())
}

async fn fixture_publication_root(
    store: &StreamAuthorityStore<MemoryStream>,
    authority: AuthorityId,
    cancel: &CancellationToken,
) -> Result<(Head, ObjectId), Box<dyn Error>> {
    let mut setup = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, cancel);
    let ((head, payload), _custody) = store
        .head_payload(authority, STREAM_RECORD_LIMIT, &mut setup)
        .await?;
    let payload = payload.ok_or("publication payload missing")?;
    let root = if head.sequence == Sequence::new(1) {
        crate::kernel::decode_volume_created(&payload, STREAM_RECORD_LIMIT, &mut |_| {
            Ok::<_, crate::FsError>(())
        })?
        .initial_generation_root
    } else {
        crate::kernel::decode_published_generation(&payload, STREAM_RECORD_LIMIT, &mut |_| {
            Ok::<_, crate::FsError>(())
        })?
        .generation_root
    };
    Ok((head, root))
}

#[tokio::test]
async fn publication_changes_keep_original_credit_until_the_last_retained_seal_drops() -> TestResult
{
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let volume = fs
        .create_volume(
            VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|error| error.error)?
        .value;
    let mut backend = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let ((_, payload), _) = store
        .head_payload(
            volume_authority_id(volume.id()),
            STREAM_RECORD_LIMIT,
            &mut backend,
        )
        .await?;
    let creation = crate::kernel::decode_volume_created(
        payload.as_ref().ok_or("creation missing")?,
        STREAM_RECORD_LIMIT,
        &mut |_| Ok::<_, crate::FsError>(()),
    )?;
    let mut ledger = crate::kernel::allocation::AllocationLedger::default();
    let mut work = WorkCounters::UNCHARGED;
    let seal = {
        let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
            &mut ledger,
            &mut work,
            WorkBudget::UNBOUNDED,
            &cancel,
        );
        PublicationChanges::initial(fs.objects(), creation, &mut accounting, &cancel).await?
    };
    let retained = ledger.live_bytes();
    assert!(retained > 0);
    let second = seal.clone();
    drop(seal);
    assert_eq!(
        ledger.live_bytes(),
        retained,
        "one clone retired shared publication backing"
    );
    drop(second);
    assert!(
        ledger.live_bytes() < retained,
        "last seal leaked its original standalone credit"
    );
    Ok(())
}

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
    let path = changes_path(
        &AuthorityPlacement::default(),
        workspace.id().volume_id(),
        &mut work,
    )?;
    assert_eq!(streams.tail(path.clone()).await?, 1);
    let initial = streams
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 64,
            at_least: Default::default(),
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
        all_of: vec![],
        keys: vec![acyclic_stream::wire::KeyRange {
            start: Bytes::from_static(b"public-"),
            end: Bytes::from_static(b"public."),
        }],
        attributes: vec![],
    })?;
    let mut follow = acyclic_stream::records_only(
        streams
            .follow(acyclic_stream::FollowRequest {
                when_absent: Default::default(),
                path: path.clone(),
                from: 1,
                preceding: None,
                filter,
                progress: false,
                at_least: Default::default(),
            })
            .await?,
    );
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
    let decoded = acyclic_stream::decode_changes(record.value.as_ref())?;
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
            assert!(
                store
                    .fork_workspace(
                        fork,
                        PublicationPermit::Unrestricted,
                        &mut work,
                        None::<&mut ()>,
                    )
                    .await
                    .is_err()
            );
            assert!(matches!(store.head(retention, &mut work).await,
                Err(error) if authority_missing(&error)));
            let path = changes_path(&AuthorityPlacement::default(), foreign.volume_id, &mut work)?;
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
    let generation = GenerationId::from_root(creation.initial_generation_root);
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
    let key = crate::publication_changes::entry::key(&path, config);
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
        for last in *b"wxy" {
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
        let mut work = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        let entries: Vec<_> = (0..130)
            .map(|index| PreparedEntry {
                key: Bytes::from(format!("key-{index:03}")),
                operation: "update",
                metadata: vec![index],
            })
            .collect();
        let encoded = encode_chunks(&context, &entries, &mut work)?;
        assert_eq!(encoded.len(), 3, "one entry per record adds write fan-out");
        let mut decoded = Vec::new();
        for record in encoded {
            assert!(
                record.value.len() + record.headers.payload_bytes()
                    <= acyclic_stream::MAX_RECORD_BYTES
            );
            assert!(record.headers.keys().len() <= acyclic_stream::MAX_RECORD_KEYS);
            let chunk = acyclic_stream::decode_changes(record.value.as_ref())?;
            assert_eq!(chunk.context.as_ref(), context);
            decoded.extend(chunk.entries);
        }
        assert_eq!(decoded.len(), entries.len());
        for (actual, expected) in decoded.iter().zip(&entries) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.operation, expected.operation);
            assert_eq!(actual.metadata.as_ref(), expected.metadata);
        }
        let large = [
            PreparedEntry {
                key: Bytes::from_static(b"a"),
                operation: "create",
                metadata: vec![0; 33_000],
            },
            PreparedEntry {
                key: Bytes::from_static(b"b"),
                operation: "create",
                metadata: vec![1; 33_000],
            },
        ];
        assert_eq!(encode_chunks(&context, &large, &mut work)?.len(), 2);
        let duplicate = [
            PreparedEntry {
                key: Bytes::from_static(b"clipped"),
                operation: "create",
                metadata: vec![0],
            },
            PreparedEntry {
                key: Bytes::from_static(b"clipped"),
                operation: "create",
                metadata: vec![1],
            },
        ];
        assert_eq!(encode_chunks(&context, &duplicate, &mut work)?.len(), 2);
        let oversized = [PreparedEntry {
            key: Bytes::new(),
            operation: "update",
            metadata: vec![0; acyclic_stream::MAX_RECORD_BYTES],
        }];
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
    let workspace = fs
        .create_workspace("durable-changes", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|error| error.error)?
        .value;
    workspace
        .write_text("/visible", "content remains separately authorized")
        .await?;
    let streams = fs.authority().provider();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = changes_path(
        &AuthorityPlacement::default(),
        workspace.id().volume_id(),
        &mut work,
    )?;
    let tail = streams.tail(path.clone()).await?;
    let before: Vec<_> = streams
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 64,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert_eq!(before.len() as u64, tail);
    for record in &before {
        let envelope = streams.read_commit(record.commit_id).await?;
        let authority_path =
            stream_path(volume_authority_id(workspace.id().volume_id()), "/records")?;
        assert!(envelope.mutations.iter().any(|mutation| matches!(mutation,
            acyclic_stream::CommittedMutation::Append(append) if append.path == authority_path)));
    }
    drop(streams);
    drop(workspace);
    drop(fs);
    let reopened = Fs::local(options).await?;
    let streams = reopened.authority().provider();
    let after: Vec<_> = streams
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 64,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
    assert_eq!(before, after);
    Ok(())
}

#[tokio::test]
async fn append_rejects_unsealed_publications_before_changing_either_stream() -> TestResult {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams.clone());
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace(
            "reject-unsealed-append",
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.error)?
        .value;
    workspace
        .write_text("/original", "separate content")
        .await?;
    let authority = volume_authority_id(workspace.id().volume_id());
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let ((head, payload), _) = store
        .head_payload(authority, STREAM_RECORD_LIMIT, &mut work)
        .await?;
    let payload = payload.ok_or("publication payload missing")?;
    let path = changes_path(
        &AuthorityPlacement::default(),
        workspace.id().volume_id(),
        &mut work,
    )?;
    let tail = streams.tail(path.clone()).await?;
    for native in [false, true] {
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut work = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        let request = crate::GuardedAppend {
            authority_id: authority,
            epoch: head.epoch,
            expected: head,
            permit: PublicationPermit::Unrestricted,
            commit: ProposedCommit {
                publication: None,
                operation_id: OperationId::from_bytes([102; 16]),
                fingerprint: Digest::from_bytes([102; 32]),
                payload: payload.clone(),
            },
        };
        assert!(
            store
                .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                .await
                .is_err()
        );
        assert_eq!(store.head(authority, &mut work).await?, head);
        assert_eq!(streams.tail(path.clone()).await?, tail);
        // A stale attempt must still report CAS conflict before inspecting a
        // seal that could never become visible under this expected head.
        let stale = crate::GuardedAppend {
            expected: Head::genesis(head.epoch),
            ..request
        };
        assert!(
            matches!(store.compare_and_append(stale, &mut work, None::<&mut ()>).await?,
            FsAppendOutcome::Conflict { actual } if actual == head)
        );
    }
    Ok(())
}

/// One fork of a volume holding `files` files, from its head or, when
/// `from_head` is false, from the generation before it. Returns the change
/// entries the fork's creation appended itself, the records it shares with
/// its source, its changes tail and its source's changes tail.
async fn fork_change_set(
    files: usize,
    from_head: bool,
) -> Result<(usize, u64, u64, u64), Box<dyn Error>> {
    let streams = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(streams.clone());
    let fs = Fs::new(
        store,
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let workspace = fs
        .create_workspace("fork-changes", WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    for file in 0..files {
        workspace.write_text(&format!("/{file}"), "content").await?;
    }
    let older = workspace
        .head(WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| failure.error)?
        .value;
    workspace.write_text("/tip", "tip").await?;
    let source = if from_head {
        workspace
            .head(WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| failure.error)?
            .value
    } else {
        older
    };
    let fork = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "fork-changes-child",
            crate::ForkOptions::from_generation(
                source,
                crate::IdempotencyKey::from_bytes([94; 16]),
            ),
            PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancel);
    let placement = AuthorityPlacement::default();
    let own = changes_path(&placement, fork.id().volume_id(), &mut work)?;
    let shared = changes_path(&placement, workspace.id().volume_id(), &mut work)?;
    let tail = streams.tail(own.clone()).await?;
    let source_tail = streams.tail(shared.clone()).await?;
    let records = streams
        .read(ReadRequest {
            key: None,
            path: own.clone(),
            from: 0,
            limit: u32::try_from(tail)?,
            at_least: Default::default(),
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let (mut listed, mut inherited) = (0, 0);
    for record in &records {
        let changes = acyclic_stream::decode_changes(record.value.as_ref())?;
        let appender = changes.context.get(..16).ok_or("context missing")?;
        if record.origin == own {
            assert_eq!(appender, fork.id().volume_id().into_bytes());
            listed += changes
                .entries
                .iter()
                .filter(|entry| !entry.key.is_empty())
                .count();
        } else {
            // A shared record names its source's stream and volume.
            assert_eq!(record.origin, shared);
            assert_eq!(appender, workspace.id().volume_id().into_bytes());
            inherited += 1;
        }
    }
    // The fork's next publication continues its own stream after the cut.
    fork.write_text("/after-fork", "later").await?;
    assert_eq!(streams.tail(own).await?, tail + 1);
    Ok((listed, inherited, tail, source_tail))
}

#[tokio::test]
async fn an_unfiltered_fork_shares_its_source_changes_instead_of_listing_them() -> TestResult {
    let (few, many) = (
        fork_change_set(4, true).await?,
        fork_change_set(32, true).await?,
    );
    // The fork lists nothing itself: it shares every source record up to
    // the forked head and appends one record marking its own root, whatever
    // the volume's size.
    for (listed, inherited, tail, source_tail) in [few, many] {
        assert_eq!(listed, 0, "a fork listed its namespace");
        assert_eq!(inherited, source_tail);
        assert_eq!(tail, inherited + 1);
    }
    // Negative control: a fork of an older generation shares no published
    // prefix, so it lists every path, and that grows with the volume.
    let (few_listed, many_listed) = (
        fork_change_set(4, false).await?,
        fork_change_set(32, false).await?,
    );
    assert_eq!((few_listed.1, many_listed.1), (0, 0));
    assert!(
        few_listed.0 >= 4 && many_listed.0 >= 32,
        "{few_listed:?} {many_listed:?}"
    );
    Ok(())
}
