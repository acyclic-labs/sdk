use super::*;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::{EmbeddedCapabilities, Fs, MemoryAuthorityStore, MemoryObjectStore};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

async fn first_occurrence<A: AsyncAuthorityStore + 'static>(
    store: A,
    stream_backend: bool,
) -> TestResult {
    let store = Arc::new(store);
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let workspace = fs.create_workspace("indexed", BUDGET, &cancel).await?.value;
    workspace
        .write("/file", Bytes::from_static(b"original"))
        .await?;
    let authority = volume_authority_id(workspace.id().volume_id());
    let mut setup = AuthorityWork::new(BUDGET, &cancel);
    let ((mut head, payload), _allocation) = store
        .head_payload(authority, STREAM_RECORD_LIMIT, &mut setup)
        .await?;
    let payload = payload.ok_or("publication missing")?;
    let generation = crate::async_storage::generation_fact(
        authority,
        head.sequence,
        &payload,
        &mut setup.accounting(),
    )?
    .ok_or("generation missing")?;
    let first = head.sequence;
    assert_eq!(first, Sequence::new(2));
    let old = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            BUDGET,
            &cancel,
        )
        .await?
        .value;

    // Actual original appends publish the same complete root again. More than
    // the former cache's capacity must not change which occurrence is returned.
    for history in 0..=65 {
        if history != 0 {
            let mut work = AuthorityWork::new(BUDGET, &cancel);
            let outcome = store
                .compare_and_append(
                    crate::GuardedAppend {
                        authority_id: authority,
                        epoch: head.epoch,
                        expected: head,
                        permit: PublicationPermit::Unrestricted,
                        commit: ProposedCommit {
                            publication: None,
                            operation_id: OperationId::new(),
                            fingerprint: Digest::from_bytes([history; 32]),
                            payload: payload.clone(),
                        },
                    },
                    &mut work,
                    None::<&mut ()>,
                )
                .await?;
            let FsAppendOutcome::Committed(record) = outcome else {
                panic!("append refused");
            };
            head = Head {
                epoch: record.epoch,
                sequence: record.sequence,
                digest: record.digest,
            };
        }
        if ![0, 1, 65].contains(&history) {
            continue;
        }
        for native in [false, true] {
            let original = crate::NativeMaterializationWork::new(BUDGET, &cancel)
                .map_err(|(error, _)| error)?;
            let mut work = if native {
                AuthorityWork::native(&original, &cancel)
            } else {
                AuthorityWork::new(BUDGET, &cancel)
            };
            assert_eq!(
                store
                    .first_generation(authority, generation, head.sequence, &mut work)
                    .await?,
                Some(first)
            );
            if !native {
                let counts = (
                    work.spent().backend_read_operations,
                    work.spent().authority_records_read,
                );
                assert_eq!(counts, if stream_backend { (3, 1) } else { (1, 0) });
            }
            let mut absent = generation.object_id();
            absent.storage = Some(crate::ObjectStorage {
                locator: Digest::from_bytes([91; 32]),
                root_file_id: crate::FileId::from_bytes([92; 16]),
                origin_generation: Digest::from_bytes([93; 32]),
            });
            assert_eq!(
                store
                    .first_generation(
                        authority,
                        crate::GenerationId::from_root(absent),
                        head.sequence,
                        &mut work
                    )
                    .await?,
                None
            );
            assert_eq!(
                store
                    .first_generation(authority, generation, Sequence::new(1), &mut work)
                    .await?,
                None
            );
            assert!(
                store
                    .first_generation(
                        authority,
                        generation,
                        head.sequence.checked_next()?,
                        &mut work
                    )
                    .await
                    .is_err()
            );
            drop(work);
        }
    }
    // The facade's own later head must not bypass the first-occurrence index.
    for checkout in [
        old,
        workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                BUDGET,
                &cancel,
            )
            .await?
            .value,
    ] {
        let position = checkout
            .generation_position(generation, BUDGET, &cancel)
            .await?;
        assert_eq!(position.value.0, first);
    }
    let stopped = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(BUDGET, &stopped).map_err(|(error, _)| error)?;
    stopped.cancel();
    let mut work = AuthorityWork::native(&original, &stopped);
    assert!(
        store
            .first_generation(authority, generation, head.sequence, &mut work)
            .await
            .is_err()
    );
    Ok(())
}

async fn forged_header_is_not_publication_evidence() -> TestResult {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let fs = Fs::new(
        store.clone(),
        MemoryObjectStore::default(),
        EmbeddedCapabilities::MEMORY,
    );
    let cancel = CancellationToken::new();
    let workspace = fs.create_workspace("forged", BUDGET, &cancel).await?.value;
    workspace
        .write("/file", Bytes::from_static(b"actual"))
        .await?;
    let authority = volume_authority_id(workspace.id().volume_id());
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    let ((head, payload), _allocation) = store
        .head_payload(authority, STREAM_RECORD_LIMIT, &mut work)
        .await?;
    let payload = payload.ok_or("publication missing")?;
    let fake = GenerationId::new(Digest::from_bytes([79; 32]));
    let mut durable = crate::streams_record::BorrowedStreamsDurableRecord {
        epoch: head.epoch,
        sequence: head.sequence.checked_next()?,
        operation_id: OperationId::new(),
        fingerprint: Digest::from_bytes([81; 32]),
        previous_digest: head.digest,
        digest: Digest::ZERO,
        payload: &payload,
    };
    durable.digest = work.durable_digest(authority, &durable)?;
    let record = work.publication_record(&durable, Some(fake))?;
    provider
        .append(acyclic_stream::AppendRequest {
            path: stream_path(authority, "/records")?,
            records: vec![record],
            if_tail: Some(durable.sequence.get()),
            idempotency_key: work.key(&[], b"forged-generation", &[&authority.into_bytes()])?,
        })
        .await?;
    assert!(matches!(
        store
            .first_generation(authority, fake, durable.sequence, &mut work)
            .await,
        Err(AuthorityStoreError::Corrupt(_) | AuthorityStoreError::CorruptStatic(_))
    ));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn first_generation_seek_retains_identity_prefix_and_original_funding() -> TestResult {
    crate::initialize_test_native_io();
    first_occurrence(MemoryAuthorityStore::default(), false).await?;
    first_occurrence(
        StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default())),
        true,
    )
    .await?;
    forged_header_is_not_publication_evidence().await?;
    #[cfg(feature = "local")]
    {
        let directory = tempfile::tempdir()?;
        let provider =
            acyclic_stream::LocalStream::open(directory.path(), Default::default()).await?;
        first_occurrence(StreamAuthorityStore::new(Arc::new(provider)), true).await?;
    }
    Ok(())
}
