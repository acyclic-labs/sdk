use super::*;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    IdempotencyKey, MemoryLimits, MemoryStream, OwnedBytes, StreamError, StreamProvider,
};
use state_record::{CompanionKind, PortableRoot, StateRecord};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn state(payload: &'static [u8]) -> StateRecord {
    StateRecord::Volume {
        root: PortableRoot {
            root_file_id: crate::FileId::from_bytes([7; 16]),
            file_table: ObjectId {
                storage: None,
                kind: crate::ObjectKind::FileTablePage,
                digest: Digest::from_bytes([8; 32]),
            },
        },
        companion: OwnedBytes::from_static(payload),
    }
}

fn commit(key: &'static [u8]) -> Result<FilesystemCommit, StreamError> {
    Ok(FilesystemCommit::new(CommitRequest {
        conditions: Vec::new(),
        mutations: Vec::new(),
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(key))?,
        deadline_unix_millis: None,
    }))
}

async fn publish(
    store: &StreamAuthorityStore<MemoryStream>,
    path: &StreamPath,
    key: &'static [u8],
    value: &StateRecord,
    work: &mut AuthorityWork<'_>,
) -> Result<acyclic_stream::CommitReceipt, Box<dyn std::error::Error>> {
    let observed = store.observe_record(path.clone(), work).await?;
    let mut publication = commit(key)?;
    store.contribute_record(
        &mut publication,
        &observed.observation,
        value,
        crate::PublicationHold::none(),
        work,
    )?;
    store.seal_observed(&mut publication, &[], None, work)?;
    match store.submit(publication, work).await? {
        FilesystemCommitOutcome::Committed(receipt) => Ok(receipt),
        _ => Err("publication did not commit".into()),
    }
}

#[tokio::test]
async fn portable_root_and_companion_share_one_record_and_exact_receipt() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = StreamPath::new("agent/one/files/root")?;
    let committed = publish(
        &store,
        &path,
        b"create",
        &state(b"overlay+shadows"),
        &mut work,
    )
    .await?;
    assert_eq!(committed.appends.len() + committed.forks.len(), 1);
    assert!(RecordReceipt::from_receipt(&committed, &path, 1).is_none());
    let receipt = RecordReceipt::from_receipt(&committed, &path, 0).ok_or("missing receipt")?;
    let recorded = store.read_receipt(&receipt, &mut work).await?;
    let record = recorded.record.as_ref().ok_or("missing record")?;
    let decoded = StateRecord::decode(record.value.clone())?;
    assert_eq!(decoded.state, state(b"overlay+shadows"));
    assert_eq!(decoded.operation.as_bytes(), b"create");
    assert_eq!(decoded.published_at.as_ref(), path.as_str().as_bytes());
    assert_eq!(provider.tail(path).await?, 1);
    let mut wrong = receipt;
    wrong.commit_id = acyclic_stream::CommitId::from_bytes([255; 32]);
    assert!(store.read_receipt(&wrong, &mut work).await.is_err());
    Ok(())
}

#[tokio::test]
async fn stale_root_or_authorization_publishes_no_other_root() -> TestResult {
    for stale_policy in [false, true] {
        let provider = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(provider.clone());
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let left = StreamPath::new("agent/files/left")?;
        let right = StreamPath::new("agent/files/right")?;
        let policy = StreamPath::new("agent/policy")?;
        publish(&store, &left, b"left", &state(b"old"), &mut work).await?;
        let left_cut = store.observe_record(left.clone(), &mut work).await?;
        let right_cut = store.observe_record(right.clone(), &mut work).await?;
        let policy_cut = store.observe_record(policy.clone(), &mut work).await?;
        let mut publication = commit(b"both")?;
        for cut in [&left_cut, &right_cut] {
            store.contribute_record(
                &mut publication,
                &cut.observation,
                &state(b"new"),
                crate::PublicationHold::none(),
                &mut work,
            )?;
        }
        store.seal_observed(&mut publication, &[policy_cut.observation], None, &mut work)?;
        publish(
            &store,
            if stale_policy { &policy } else { &left },
            b"advance",
            &state(b"advanced"),
            &mut work,
        )
        .await?;
        assert!(matches!(
            store.submit(publication, &mut work).await?,
            FilesystemCommitOutcome::Rejected(_)
        ));
        assert_eq!(provider.tail(right).await, Err(StreamError::NotFound));
        assert_eq!(provider.tail(left).await?, if stale_policy { 1 } else { 2 });
    }
    Ok(())
}

#[tokio::test]
async fn cross_provider_observation_and_oversize_total_refuse_before_submit() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let other = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = StreamPath::new("agent/files/root")?;
    let foreign = other.observe_record(path.clone(), &mut work).await?;
    let mut publication = commit(b"foreign")?;
    assert!(
        store
            .contribute_record(
                &mut publication,
                &foreign.observation,
                &state(b"new"),
                crate::PublicationHold::none(),
                &mut work
            )
            .is_err()
    );
    assert!(publication.request().mutations.is_empty());
    assert!(
        store
            .seal_observed(&mut publication, &[foreign.observation], None, &mut work)
            .is_err()
    );
    let record = StateRecord::Companion {
        kind: CompanionKind::OperationOutcome,
        payload: vec![1; acyclic_stream::MAX_RECORD_BYTES - 128].into(),
    };
    for index in 0..32 {
        let path = StreamPath::new(format!("agent/files/root{index}"))?;
        let cut = store.observe_record(path, &mut work).await?;
        store.contribute_record(
            &mut publication,
            &cut.observation,
            &record,
            crate::PublicationHold::none(),
            &mut work,
        )?;
    }
    let before = publication.request().clone();
    assert!(matches!(
        store.seal_observed(&mut publication, &[], None, &mut work),
        Err(AuthorityStoreError::NativeStream(
            StreamError::LimitExceeded
        ))
    ));
    assert_eq!(publication.request(), &before);
    assert!(publication.request_mut().is_some());
    assert_eq!(provider.tail(path).await, Err(StreamError::NotFound));
    Ok(())
}

#[tokio::test]
async fn permanent_record_reconciles_without_idempotency_lookup_and_binds_bytes() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = StreamPath::new("agent/files/root")?;
    // Discard the reply; permanent recovery reads the predicted record only.
    drop(publish(&store, &path, b"lost-reply", &state(b"accepted"), &mut work).await?);
    let operation = IdempotencyKey::new(OwnedBytes::from_static(b"lost-reply"))?;
    let before = work.spent().backend_read_operations;
    let receipt = store
        .reconcile_record(path.clone(), 0, &operation, &state(b"accepted"), &mut work)
        .await?;
    assert_eq!(work.spent().backend_read_operations - before, 1);
    assert_eq!(receipt.sequence, 0);
    assert!(
        store
            .reconcile_record(path, 0, &operation, &state(b"different"), &mut work)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn prepared_commit_cannot_mix_or_switch_provider_owners() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let foreign_provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let other = StreamAuthorityStore::new(foreign_provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = StreamPath::new("agent/files/root")?;
    let cut = store.observe_record(path.clone(), &mut work).await?;
    let foreign = other
        .observe_record(StreamPath::new("agent/files/other")?, &mut work)
        .await?;
    let mut publication = commit(b"bound")?;
    store.contribute_record(
        &mut publication,
        &cut.observation,
        &state(b"new"),
        crate::PublicationHold::none(),
        &mut work,
    )?;
    assert!(
        other
            .contribute_record(
                &mut publication,
                &foreign.observation,
                &state(b"foreign"),
                crate::PublicationHold::none(),
                &mut work,
            )
            .is_err()
    );
    assert!(
        other
            .seal_observed(&mut publication, &[], None, &mut work)
            .is_err()
    );
    store.seal_observed(&mut publication, &[], None, &mut work)?;
    assert!(matches!(
        other.submit(publication, &mut work).await?,
        FilesystemCommitOutcome::Unknown { .. }
    ));
    assert_eq!(
        provider.tail(path.clone()).await,
        Err(StreamError::NotFound)
    );
    assert_eq!(
        foreign_provider.tail(path).await,
        Err(StreamError::NotFound)
    );
    Ok(())
}

struct Clock;
impl acyclic_stream::UnixMillisClock for Clock {
    fn now_unix_millis(&self) -> u64 {
        100
    }
}

#[tokio::test]
async fn exact_expiry_is_retained_with_the_unknown_request() -> TestResult {
    let provider = Arc::new(MemoryStream::new_with_clock(
        MemoryLimits::default(),
        Arc::new(Clock),
    ));
    let store = StreamAuthorityStore::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let path = StreamPath::new("agent/files/root")?;
    let cut = store.observe_record(path.clone(), &mut work).await?;
    let mut publication = commit(b"expired")?;
    store.contribute_record(
        &mut publication,
        &cut.observation,
        &state(b"new"),
        crate::PublicationHold::none(),
        &mut work,
    )?;
    store.seal_observed(&mut publication, &[], Some(100), &mut work)?;
    match store.submit(publication, &mut work).await? {
        FilesystemCommitOutcome::Unknown { commit, .. } => {
            assert_eq!(commit.deadline(), Some(100));
            assert!(commit.request().idempotency_key.as_bytes() == b"expired");
        }
        _ => return Err("expired publication was admitted".into()),
    }
    assert_eq!(provider.tail(path).await, Err(StreamError::NotFound));
    Ok(())
}

#[test]
fn malformed_record_framing_is_rejected_without_panics() {
    for bytes in [
        OwnedBytes::new(),
        OwnedBytes::from_static(b"acyclic-fs-state-v1\0"),
        OwnedBytes::from_static(b"acyclic-fs-state-v1\0\xff\xff"),
    ] {
        assert!(StateRecord::decode(bytes).is_err());
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn sealed_request_keeps_original_native_custody_after_preparation_work_drops() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider);
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut work = AuthorityWork::native(&original, &cancellation);
    let path = StreamPath::new("agent/files/root")?;
    assert!(
        store
            .authorized_observation(CommitCondition::Tail {
                path: path.clone(),
                expected: 0
            })
            .is_err()
    );
    let observation = store.authorized_observation(CommitCondition::Absent { path })?;
    let mut publication = commit(b"native-custody")?;
    store.contribute_record(
        &mut publication,
        &observation,
        &state(b"native"),
        crate::PublicationHold::none(),
        &mut work,
    )?;
    store.seal_observed(&mut publication, &[], None, &mut work)?;
    let retained = original.live_allocation_bytes();
    assert!(retained > baseline);
    drop(work);
    assert_eq!(
        original.live_allocation_bytes(),
        retained,
        "sealed request lost its vector funding"
    );
    drop(publication);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn local_retry_refusals_preserve_lost_ack_commit_and_collection_hold() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let spy = Arc::new(super::tests::ReleaseSpy::losing_ack(provider.clone()));
    let store = StreamAuthorityStore::new(spy.clone());
    let other = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let collection = Arc::new(crate::collection::Collection::default());
    let hold = collection.unswept().await.into_hold();
    let path = StreamPath::new("agent/files/lost")?;
    let mut preparation = AuthorityWork::native(&original, &cancellation);
    let cut = store.authorized_observation(CommitCondition::Absent { path: path.clone() })?;
    let mut publication = commit(b"held-lost-reply")?;
    store.contribute_record(
        &mut publication,
        &cut,
        &state(b"accepted"),
        hold,
        &mut preparation,
    )?;
    store.seal_observed(&mut publication, &[], Some(u64::MAX), &mut preparation)?;
    let request = publication.request().clone();
    drop(preparation);
    let retained = original.live_allocation_bytes();
    assert!(retained > baseline);

    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let mut publication = unknown(store.submit(publication, &mut work).await?)?;
    assert_eq!(
        provider.tail(path).await?,
        1,
        "the lost reply followed a real publication"
    );
    assert_eq!(spy.commit_attempts(), 1);
    let observed = provider
        .inspect_idempotency(request.idempotency_key.clone())
        .await?
        .ok_or("missing actual outcome")?;
    let acyclic_stream::IdempotencyOutcome::Commit(envelope) = observed.outcome else {
        return Err("wrong outcome family".into());
    };
    let outcome = CommitOutcome::Committed(acyclic_stream::CommitReceipt::project(
        &envelope,
        observed.revision.ok_or("missing revision")?,
        Vec::with_capacity(envelope.mutations.len()),
        Vec::with_capacity(envelope.mutations.len()),
        |_, _| Ok::<(), StreamError>(()),
    )?);
    drop(envelope);
    let mut collecting = Box::pin(collection.begin());
    assert!(futures::poll!(collecting.as_mut()).is_pending());

    let denied = original.native_volume_work(WorkBudget {
        items_examined: 0,
        ..crate::performance::BOUNDARY_BUDGET
    })?;
    let mut exhausted = AuthorityWork::native(&denied, &cancellation);
    publication = unknown(store.submit(publication, &mut exhausted).await?)?;
    assert_eq!(publication.request(), &request);
    drop(exhausted);
    drop(denied);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut stopped = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancelled);
    publication = unknown(store.submit(publication, &mut stopped).await?)?;
    assert_eq!(publication.request(), &request);
    publication = unknown(other.submit(publication, &mut work).await?)?;
    publication = unknown(
        other
            .settle(publication, outcome.clone(), &mut work)
            .await?,
    )?;
    assert_eq!(publication.request(), &request);
    assert_eq!(publication.deadline(), Some(u64::MAX));
    assert_eq!(spy.commit_attempts(), 1, "local refusals must not resubmit");
    assert_eq!(original.live_allocation_bytes(), retained);
    assert!(
        futures::poll!(collecting.as_mut()).is_pending(),
        "uncertain closure became collectible"
    );

    assert!(matches!(
        store.settle(publication, outcome, &mut work).await?,
        FilesystemCommitOutcome::Committed(_)
    ));
    assert!(futures::poll!(collecting.as_mut()).is_ready());
    drop(request);
    drop(work);
    drop(store);
    drop(spy);
    drop(provider);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn unknown(
    outcome: FilesystemCommitOutcome,
) -> Result<FilesystemCommit, Box<dyn std::error::Error>> {
    match outcome {
        FilesystemCommitOutcome::Unknown { commit, .. } => Ok(commit),
        _ => Err("lost-ack owner was not preserved".into()),
    }
}

#[tokio::test]
async fn prepared_state_batches_reconcile_all_roots_and_refs_from_exact_records() -> TestResult {
    for count in [1, 8, 64] {
        let provider = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(provider.clone());
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let mut updates = Vec::new();
        let first = StreamPath::new("actor/files/0")?;
        for index in 0..count {
            let path = StreamPath::new(format!("actor/files/{index}"))?;
            let observed = store.observe_record(path, &mut work).await?;
            updates.push((
                observed.observation,
                state(b"root+metadata"),
                crate::PublicationHold::none(),
            ));
        }
        let refs = store
            .observe_record(StreamPath::new("actor/refs")?, &mut work)
            .await?;
        updates.push((
            refs.observation,
            StateRecord::Companion {
                kind: CompanionKind::GitRefs,
                payload: OwnedBytes::from_static(b"refs-snapshot"),
            },
            crate::PublicationHold::none(),
        ));
        let mut publication = commit(b"all-roots")?;
        store.prepare_records(&mut publication, updates, &[], Some(u64::MAX), &mut work)?;
        assert!(publication.request_mut().is_none());
        assert_eq!(publication.request().mutations.len(), count + 1);
        assert_eq!(
            provider.tail(first.clone()).await,
            Err(StreamError::NotFound)
        );

        // Capture a real provider decision, then discard the reply. Recovery
        // never consults the decision cache or republishes the command.
        let CommitOutcome::Committed(envelope) =
            provider.commit(publication.request().clone()).await?
        else {
            return Err("prepared state batch conflicted".into());
        };
        let recorded_id = envelope.commit_id;
        drop(envelope);
        publish(
            &store,
            &first,
            b"later-operation",
            &state(b"later"),
            &mut work,
        )
        .await?;
        let before = work.spent();
        assert!(
            matches!(store.reconcile_states(publication, &mut work).await,
            StatePublicationOutcome::Recorded(id) if id == recorded_id)
        );
        assert_eq!(
            work.spent().backend_read_operations - before.backend_read_operations,
            (count + 1) as u64
        );
        assert_eq!(
            work.spent().backend_write_operations,
            before.backend_write_operations
        );
        assert_eq!(provider.tail(first).await?, 2);
    }
    Ok(())
}

#[tokio::test]
async fn failed_state_batch_restores_caller_conditions_and_publishes_nothing() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let foreign = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let path = StreamPath::new("actor/files/root")?;
    publish(&store, &path, b"initial", &state(b"old"), &mut work).await?;
    let observed = store.observe_record(path.clone(), &mut work).await?;
    let other = foreign
        .observe_record(StreamPath::new("actor/files/foreign")?, &mut work)
        .await?;
    let mut publication = commit(b"refused-batch")?;
    publication
        .request_mut()
        .ok_or("unexpected seal")?
        .conditions
        .push(CommitCondition::Tail {
            path: path.clone(),
            expected: 1,
        });
    let before = publication.request().clone();
    assert!(
        store
            .prepare_records(
                &mut publication,
                [
                    (
                        observed.observation,
                        state(b"new"),
                        crate::PublicationHold::none()
                    ),
                    (
                        other.observation,
                        state(b"foreign"),
                        crate::PublicationHold::none()
                    ),
                ],
                &[],
                None,
                &mut work
            )
            .is_err()
    );
    // The first contribution strengthened Tail -> Head in the draft. It must
    // not leak into the original request when the second contribution refuses.
    assert_eq!(publication.request(), &before);
    assert!(publication.request_mut().is_some());
    assert_eq!(provider.tail(path).await?, 1);
    Ok(())
}

#[tokio::test]
async fn inherited_state_is_readable_but_cannot_settle_a_destination_append() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let source = StreamPath::new("actor/source")?;
    let destination = StreamPath::new("actor/child")?;
    let key = IdempotencyKey::new(OwnedBytes::from_static(b"same-operation"))?;
    let observed = store.observe_record(destination.clone(), &mut work).await?;
    let mut publication = commit(b"same-operation")?;
    store.prepare_records(
        &mut publication,
        [(
            observed.observation,
            state(b"same"),
            crate::PublicationHold::none(),
        )],
        &[],
        None,
        &mut work,
    )?;
    let original = publication.request().clone();
    let envelope = publish(
        &store,
        &source,
        b"same-operation",
        &state(b"same"),
        &mut work,
    )
    .await?;
    provider
        .fork(acyclic_stream::ForkRequest {
            exclude: Vec::new(),
            source: source.clone(),
            destination: destination.clone(),
            at_tail: Some(1),
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"inherit-source"))?,
        })
        .await?;
    let inherited = store
        .read_receipt(
            &RecordReceipt {
                path: destination.clone(),
                sequence: 0,
                commit_id: envelope.commit_id,
            },
            &mut work,
        )
        .await?;
    let decoded = StateRecord::decode(inherited.record.ok_or("missing inherited state")?.value)?;
    assert_eq!(decoded.state, state(b"same"));
    assert_eq!(decoded.published_at.as_ref(), source.as_str().as_bytes());
    assert!(
        store
            .reconcile_record(destination.clone(), 0, &key, &state(b"same"), &mut work)
            .await
            .is_err()
    );
    match store.reconcile_states(publication, &mut work).await {
        StatePublicationOutcome::Unknown { commit, .. } => assert_eq!(commit.request(), &original),
        _ => {
            return Err(
                "an inherited source record settled an unsubmitted destination append".into(),
            );
        }
    }
    assert_eq!(provider.tail(destination).await?, 1);
    Ok(())
}

#[tokio::test]
async fn stale_batch_root_metadata_policy_or_actor_publishes_no_root_or_ref() -> TestResult {
    for changed in ["left", "metadata", "policy", "actor"] {
        let provider = Arc::new(MemoryStream::default());
        let store = StreamAuthorityStore::new(provider.clone());
        let cancellation = CancellationToken::new();
        let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
        let mut updates = Vec::new();
        let mut observations = Vec::new();
        for suffix in ["left", "right", "refs", "metadata", "policy", "actor"] {
            let path = StreamPath::new(format!("batch/{suffix}"))?;
            publish(
                &store,
                &path,
                match suffix {
                    "left" => b"initial-left",
                    "right" => b"initial-right",
                    "refs" => b"initial-refs",
                    "metadata" => b"initial-metadata",
                    "policy" => b"initial-policy",
                    _ => b"initial-actor",
                },
                &state(b"old"),
                &mut work,
            )
            .await?;
            let observed = store.observe_record(path, &mut work).await?;
            if matches!(suffix, "left" | "right" | "refs") {
                let next = if suffix == "refs" {
                    StateRecord::Companion {
                        kind: CompanionKind::GitRefs,
                        payload: OwnedBytes::from_static(b"new-refs"),
                    }
                } else {
                    state(b"new-root+metadata")
                };
                updates.push((observed.observation, next, crate::PublicationHold::none()));
            } else {
                observations.push(observed.observation);
            }
        }
        let mut publication = commit(b"batch-with-authorized-observations")?;
        store.prepare_records(&mut publication, updates, &observations, None, &mut work)?;
        publish(
            &store,
            &StreamPath::new(format!("batch/{changed}"))?,
            b"advance",
            &state(b"advanced"),
            &mut work,
        )
        .await?;
        assert!(matches!(
            store.submit(publication, &mut work).await?,
            FilesystemCommitOutcome::Rejected(_)
        ));
        for suffix in ["left", "right", "refs", "metadata", "policy", "actor"] {
            assert_eq!(
                provider
                    .tail(StreamPath::new(format!("batch/{suffix}"))?)
                    .await?,
                if suffix == changed { 2 } else { 1 }
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn oversized_state_batch_leaves_the_original_request_and_no_published_prefix() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let record = StateRecord::Companion {
        kind: CompanionKind::OperationOutcome,
        payload: vec![1; acyclic_stream::MAX_RECORD_BYTES - 128].into(),
    };
    let mut updates = Vec::new();
    for index in 0..17 {
        let cut = store
            .observe_record(StreamPath::new(format!("large/root{index}"))?, &mut work)
            .await?;
        updates.push((
            cut.observation,
            record.clone(),
            crate::PublicationHold::none(),
        ));
    }
    let mut publication = commit(b"oversized-atomic-batch")?;
    publication
        .request_mut()
        .ok_or("unexpected seal")?
        .conditions
        .push(CommitCondition::Absent {
            path: StreamPath::new("caller/condition")?,
        });
    let original = publication.request().clone();
    assert!(matches!(
        store.prepare_records(&mut publication, updates, &[], None, &mut work),
        Err(AuthorityStoreError::NativeStream(
            StreamError::LimitExceeded
        ))
    ));
    assert_eq!(publication.request(), &original);
    assert!(publication.request_mut().is_some());
    for index in 0..17 {
        assert_eq!(
            provider
                .tail(StreamPath::new(format!("large/root{index}"))?)
                .await,
            Err(StreamError::NotFound)
        );
    }
    Ok(())
}

#[tokio::test]
async fn identical_state_bytes_from_separate_commits_are_not_one_publication() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let mut publication = commit(b"original-operation")?;
    let mut updates = Vec::new();
    for suffix in ["left", "right"] {
        let observed = store
            .observe_record(StreamPath::new(format!("actor/files/{suffix}"))?, &mut work)
            .await?;
        updates.push((
            observed.observation,
            state(b"same"),
            crate::PublicationHold::none(),
        ));
    }
    store.prepare_records(&mut publication, updates, &[], None, &mut work)?;
    let original = publication.request().clone();
    for (index, mutation) in original.mutations.iter().enumerate() {
        let CommitMutation::Append { path, .. } = mutation else {
            return Err("unexpected fork".into());
        };
        let decision = provider
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutations: vec![mutation.clone()],
                idempotency_key: IdempotencyKey::new(format!("separate-{index}").into_bytes())?,
                deadline_unix_millis: None,
            })
            .await?;
        assert!(matches!(decision, CommitOutcome::Committed(_)));
    }
    match store.reconcile_states(publication, &mut work).await {
        StatePublicationOutcome::Unknown {
            commit,
            error: AuthorityStoreError::NativeStream(StreamError::IdempotencyMismatch),
        } => assert_eq!(commit.request(), &original),
        _ => return Err("separate commits were accepted as one publication".into()),
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn state_reconciliation_refusals_keep_actual_lost_ack_custody() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let spy = Arc::new(super::tests::ReleaseSpy::losing_ack(provider.clone()));
    let store = StreamAuthorityStore::new(spy.clone());
    let foreign = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let collection = Arc::new(crate::collection::Collection::default());
    let hold = collection.unswept().await.into_hold();
    let mut preparation = AuthorityWork::native(&original, &cancellation);
    let observed = store.authorized_observation(CommitCondition::Absent {
        path: StreamPath::new("actor/files/held")?,
    })?;
    let mut publication = commit(b"held-state-batch")?;
    store.prepare_records(
        &mut publication,
        [(observed, state(b"accepted"), hold)],
        &[],
        Some(u64::MAX),
        &mut preparation,
    )?;
    let request = publication.request().clone();
    drop(preparation);
    let retained = original.live_allocation_bytes();
    assert!(retained > baseline);
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    let mut publication = unknown(store.submit(publication, &mut work).await?)?;
    let mut collecting = Box::pin(collection.begin());
    assert!(futures::poll!(collecting.as_mut()).is_pending());
    let stopped = CancellationToken::new();
    stopped.cancel();
    let mut cancelled = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &stopped);
    publication = match store.reconcile_states(publication, &mut cancelled).await {
        StatePublicationOutcome::Unknown { commit, .. } => commit,
        _ => return Err("cancelled reconciliation lost its unresolved owner".into()),
    };
    let mut exhausted = AuthorityWork::new(
        WorkBudget {
            items_examined: 0,
            ..crate::performance::BOUNDARY_BUDGET
        },
        &cancellation,
    );
    publication = match store.reconcile_states(publication, &mut exhausted).await {
        StatePublicationOutcome::Unknown { commit, .. } => commit,
        _ => return Err("finite reconciliation refusal lost its unresolved owner".into()),
    };
    publication = match foreign.reconcile_states(publication, &mut work).await {
        StatePublicationOutcome::Unknown { commit, .. } => commit,
        _ => return Err("foreign reconciliation settled another owner's request".into()),
    };
    assert_eq!(publication.request(), &request);
    assert_eq!(publication.deadline(), Some(u64::MAX));
    assert_eq!(original.live_allocation_bytes(), retained);
    assert!(futures::poll!(collecting.as_mut()).is_pending());
    assert!(matches!(
        store.reconcile_states(publication, &mut work).await,
        StatePublicationOutcome::Recorded(_)
    ));
    assert_eq!(spy.commit_attempts(), 1);
    assert!(futures::poll!(collecting.as_mut()).is_ready());
    drop(request);
    drop(work);
    drop(store);
    drop(spy);
    drop(provider);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}
