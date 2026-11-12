use super::*;
use crate::guest::CodecIdentity;
use acyclic_stream::{AppendRequest, LocalStream, MemoryStream, ReadRequest};
use bytes::Bytes;
use futures::TryStreamExt;
use proptest::prelude::*;

mod disposition;
mod hot_path;
mod overlay;
mod subscriptions;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn indexed_delivery_requires_original_cursor_and_source_key() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(provider.clone(), state_root()?);
    let address = actor("indexed")?;
    place(&store, &address).await?;
    let before = store.load(&address).await?;
    let input = before.state().ok_or("missing state")?.subscriptions()["inbox"]
        .path
        .clone();
    provider
        .append(AppendRequest {
            path: input.clone(),
            records: (0..4)
                .map(|sequence| {
                    Ok(acyclic_stream::AppendRecord {
                        value: Bytes::from_static(b"emit").into(),
                        headers: acyclic_stream::RecordHeaders::new(
                            vec![
                                Bytes::from_static(if sequence == 3 {
                                    b"control"
                                } else {
                                    b"output"
                                })
                                .into(),
                            ],
                            vec![],
                        )?,
                    })
                })
                .collect::<Result<_, StreamError>>()?,
            if_tail: Some(0),
            idempotency_key: key("indexed/input")?,
        })
        .await?;
    let delivered = provider
        .read(ReadRequest {
            path: input,
            key: Some(acyclic_stream::ReadKeySeek::exact(
                Bytes::from_static(b"control"),
                u64::MAX,
            )),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("missing selected record")?;
    let selected = |from, sequence, key: &'static [u8]| Trigger::KeyedRecord {
        subscription: "inbox".into(),
        from,
        sequence,
        key: Bytes::from_static(key),
    };
    for trigger in [
        record(3),
        selected(1, 3, b"control"),
        selected(0, 3, b"output"),
        selected(0, 2, b"control"),
    ] {
        assert!(
            store
                .prepare_publish(
                    &before,
                    1,
                    &trigger,
                    Some(&delivered),
                    transition(7, AlarmChange::Keep),
                    key("indexed/refused")?
                )
                .is_err()
        );
    }
    assert!(
        store
            .prepare_publish(
                &before,
                1,
                &selected(0, 3, b"control"),
                None,
                transition(7, AlarmChange::Keep),
                key("indexed/no-source")?
            )
            .is_err()
    );
    let trigger = selected(0, 3, b"control");
    let publication = store.prepare_publish(
        &before,
        1,
        &trigger,
        Some(&delivered),
        transition(7, AlarmChange::Keep),
        key("indexed/accepted")?,
    )?;
    store.submit(publication).await?;
    let after = store.load(&address).await?;
    assert_eq!(
        after.state().and_then(|state| state.cursor("inbox")),
        Some(4)
    );
    assert!(
        store
            .prepare_publish(
                &after,
                1,
                &trigger,
                Some(&delivered),
                transition(8, AlarmChange::Keep),
                key("indexed/replayed")?
            )
            .is_err()
    );
    let delivery = crate::guest::Delivery {
        actor: address,
        epoch: 1,
        trigger,
        now_unix_ms: 1,
        subscriptions: crate::guest::SubscriptionCuts::new(
            before.path.clone(),
            before.anchor.ok_or("missing state anchor")?,
            before
                .state()
                .ok_or("missing state")?
                .subscriptions()
                .clone(),
        ),
        record: Some(delivered),
    };
    assert_eq!(
        crate::guest::wire::decode_delivery(&crate::guest::wire::encode_delivery(&delivery, None))?
            .0,
        delivery
    );
    Ok(())
}

#[tokio::test]
async fn fork_replay_retains_original_cut_after_parent_and_input_progress() -> TestResult {
    use acyclic_stream::testing::{Counted, RequestClass};
    let provider = Arc::new(Counted::new(MemoryStream::default()));
    let store = Store::new(provider.clone(), state_root()?);
    let parent = actor("fork-parent")?;
    let child = actor("fork-child")?;
    place(&store, &parent).await?;
    let input = StreamPath::new("inputs/fork-child")?;
    let append = |value: &'static [u8], name: &str| -> Result<_, StreamError> {
        Ok(acyclic_stream::AppendRequest {
            path: input.clone(),
            records: vec![value.to_vec().into()],
            if_tail: None,
            idempotency_key: key(name)?,
        })
    };
    provider
        .append(append(b"before", "fork/input-before")?)
        .await?;
    let request = crate::wire::ForkActorRequest {
        actor_id: parent.id().to_owned(),
        child_actor_id: child.id().to_owned(),
        subscriptions: vec![crate::wire::SubscriptionSpec {
            subscription_id: "inbox".into(),
            stream_path: input.as_str().into(),
            start: Some(crate::wire::SubscriptionStart {
                start: Some(crate::wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        }],
        idempotency_key: "original-fork".into(),
        namespace: Some(parent.namespace().as_str().into()),
        account_id: parent.account().as_bytes().to_vec(),
    };
    // The original operation commits, but its caller loses the reply.
    store.fork_actor(&[], &request).await?;
    // The former acceptance identity must not select this generation's
    // changed overlay semantics. A restored earlier key fails this real retained
    // envelope oracle, without a migration reader or synthetic provider.
    {
        use prost::Message as _;
        use sha2::Digest as _;
        for format in [
            &b"acyclic.actors.fork.v1\0"[..],
            b"acyclic.actors.fork.v2\0",
            b"acyclic.actors.fork.v3\0",
        ] {
            let mut old = sha2::Sha256::new();
            old.update(format);
            old.update(store.path(&child)?.as_str().as_bytes());
            old.update([0]);
            old.update(request.encode_to_vec());
            assert!(
                provider
                    .inspect_idempotency(IdempotencyKey::new(old.finalize().to_vec())?)
                    .await?
                    .is_none()
            );
        }
        let current = provider
            .inspect_idempotency(fork_key(&store.path(&child)?, &request)?)
            .await?
            .ok_or("missing versioned acceptance")?;
        let acyclic_stream::IdempotencyOutcome::Commit(envelope) = current.outcome else {
            return Err("fork acceptance is not a commit".into());
        };
        assert!(envelope.mutations.iter().any(|mutation| matches!(mutation,
            acyclic_stream::CommittedMutation::Fork(fork)
                if super::disposition::is_fork_exclusion(&fork.exclude))));
    }
    let accepted = store.load(&child).await?;
    assert_eq!(
        accepted.state().ok_or("missing child")?.cursor("inbox"),
        Some(1)
    );
    let parent_cut = store.load(&parent).await?;
    store
        .set_paused(&parent_cut, 1, true, key("fork/parent-progress")?)
        .await?;
    provider
        .append(append(b"after", "fork/input-after")?)
        .await?;
    let reopened = Store::new(provider.clone(), state_root()?);
    for count in [1, 100, 1_000] {
        let before = provider.requests();
        for _ in 0..count {
            assert_eq!(reopened.fork_actor(&[], &request).await?, child);
        }
        let work = provider.requests().since(before);
        assert_eq!(work.data(), 0, "replay must not redispatch a mutation/read");
        assert_eq!(work.control(), count);
        assert_eq!(work.class(RequestClass::Metadata), count);
        assert_eq!(work.class(RequestClass::Head), 0);
    }
    let after = reopened.load(&child).await?;
    assert_eq!(after.tail, accepted.tail);
    assert_eq!(after.anchor, accepted.anchor);
    assert_eq!(
        after.state().ok_or("missing child")?.cursor("inbox"),
        Some(1)
    );
    // Changing the original caller facts cannot borrow the settled result.
    let mut changed = request.clone();
    changed.idempotency_key = "changed-fork".into();
    assert!(matches!(
        reopened.fork_actor(&[], &changed).await,
        Err(Error::Conflict)
    ));
    assert_eq!(reopened.load(&child).await?.anchor, accepted.anchor);
    Ok(())
}

/// Runtime delivery keeps the confirmed publication cut without reopening it.
/// Either missing provenance handoff must fail before a later load can mask it.
#[tokio::test]
async fn forked_prepared_publication_retains_original_provenance() -> TestResult {
    crate::conformance::fork_uniform(
        Arc::new(MemoryStream::default()),
        StreamPath::new("fork-placement")?,
    )
    .await?;
    let store = Store::new(Arc::new(MemoryStream::default()), state_root()?);
    let parent = actor("parent")?;
    let child = actor("child")?;
    place(&store, &parent).await?;
    let parent_cut = store.load(&parent).await?;
    let mut publication = PreparedPublish::foreign(key("fork")?);
    publication.join(store.branch(&parent_cut, &child, Branch::unfunded(inbox(&child)?))?)?;
    publication.join(store.activation(&child)?)?;
    store.submit(publication).await?;
    let forked = store.load(&child).await?;
    let original = forked.provenance().cloned().ok_or("missing fork origin")?;
    assert_eq!(original.source, *parent_cut.path());
    // Born paused: the forker's resume is the branch's first placement.
    store
        .claim_with_pause(&forked, 2, Some(false), key("claim")?)
        .await?;
    let mut retained = store.load(&child).await?;
    for sequence in 0..2 {
        let publication = store.prepare_publish(
            &retained,
            2,
            &record(sequence),
            None,
            transition(7, AlarmChange::Keep),
            key(&format!("publish/{sequence}"))?,
        )?;
        let next = store
            .submit_retained(publication)
            .await?
            .ok_or("missing retained publication")?;
        assert_eq!(next.provenance(), Some(&original));
        assert_eq!(next.tail, retained.tail + 1);
        assert_ne!(next.anchor, retained.anchor);
        retained = next;
    }
    Ok(())
}

/// The original publication owns response, cursor, checkpoint and retry. A
/// missing/foreign response cannot consume input; an empty response is real.
#[tokio::test]
async fn invocation_response_is_one_fenced_publication_and_reconciles_after_reopen() -> TestResult {
    use crate::invocation::Invocation;
    use acyclic_stream::testing::Counted;
    let provider = Arc::new(Counted::new(MemoryStream::default()));
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let address = actor("invoked")?;
    place(&store, &address).await?;
    let invocation = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"op"),
        Bytes::from_static(b"request"),
    )?;
    let ticket = acyclic_stream::assert_requests!(
        provider,
        store.enqueue_invoke(&invocation).await,
        data = 1,
        control = 1
    )?;
    assert_eq!(
        acyclic_stream::assert_requests!(
            provider,
            store.enqueue_invoke(&invocation).await,
            data = 1,
            control = 1
        )?,
        ticket
    );
    // The original creation follow is the only pending observation. Dropping
    // its caller cannot withdraw accepted input or require an external wake.
    let before_wait = provider.requests();
    let mut abandoned = Box::pin(store.wait_invocation(ticket.clone()));
    assert!(futures::poll!(abandoned.as_mut()).is_pending());
    let waiting = provider.requests().since(before_wait);
    assert_eq!(waiting.data(), 1);
    assert_eq!(waiting.control(), 0);
    drop(abandoned);
    let mut waiting = Box::pin(store.wait_invocation(ticket.clone()));
    let before_wait = provider.requests();
    assert!(futures::poll!(waiting.as_mut()).is_pending());
    let registered = provider.requests().since(before_wait);
    assert_eq!(registered.data(), 1);
    assert_eq!(registered.control(), 0);
    let before = store.load(&address).await?;
    // A host guard refusal cannot become a guest terminal or consume this
    // accepted input. The same pending waiter completes on the valid retry.
    let failure_key = key("invoked/precondition")?;
    let trigger = record(0);
    let before_refusal = provider.requests();
    let refusal = acyclic_test_support::measure(|| {
        assert!(matches!(
            store.prepare_failed_input(
                &before,
                1,
                &trigger,
                ticket.record(),
                &crate::guest::GuestError::PreconditionChanged,
                failure_key,
            ),
            Err(Error::PreconditionChanged(_))
        ));
    });
    assert_eq!(refusal.count_total, 0);
    assert_eq!(refusal.bytes_total, 0);
    let requests = provider.requests().since(before_refusal);
    assert_eq!(requests.data(), 0);
    assert_eq!(requests.control(), 0);
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            transition(7, AlarmChange::Keep),
            key("invoked/missing")?
        ),
        Err(Error::Invalid("invocation has no response"))
    ));
    let mut result = transition(7, AlarmChange::Keep);
    result.response = Some(Bytes::new());
    // Mutate the original accepted record's version. Unsupported formats must
    // fail before either success or failure can consume input or mint a caller.
    for version in [b"0", b"2", b"3"] {
        let mut obsolete = ticket.record().clone();
        obsolete.headers = acyclic_stream::RecordHeaders::new(
            obsolete.headers.keys().to_vec(),
            vec![acyclic_stream::Attribute {
                name: "acyclic.actor.invoke".into(),
                value: Bytes::copy_from_slice(version).into(),
            }],
        )?;
        assert!(matches!(
            Invocation::decode(&obsolete),
            Err(crate::guest::GuestError::UnsupportedFormat)
        ));
        assert!(matches!(
            store.prepare_publish(
                &before,
                1,
                &record(0),
                Some(&obsolete),
                result.clone(),
                key("invoked/unsupported")?
            ),
            Err(Error::Handler(crate::guest::GuestError::UnsupportedFormat))
        ));
        assert!(matches!(
            store.prepare_failed_input(
                &before,
                1,
                &record(0),
                &obsolete,
                &crate::guest::GuestError::Decode("failed".into()),
                key("invoked/unsupported-failure")?
            ),
            Err(Error::Handler(crate::guest::GuestError::UnsupportedFormat))
        ));
    }
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            None,
            result.clone(),
            key("invoked/no-input")?
        ),
        Err(Error::Invalid("response without an invocation"))
    ));
    let foreign = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        actor("different")?,
        "inbox".into(),
        Bytes::from_static(b"op"),
        Bytes::from_static(b"request"),
    )?
    .record(before.path())?;
    let mut different = ticket.record().clone();
    different.value = foreign.value;
    different.headers = foreign.headers;
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(&different),
            result.clone(),
            key("invoked/foreign")?
        ),
        Err(Error::Invalid(
            "invocation input targets another actor or subscription"
        ))
    ));
    let other_root = Store::new(Arc::clone(&provider), StreamPath::new("foreign/actors")?);
    let foreign = invocation.record(&other_root.path(&address)?)?;
    different.value = foreign.value;
    different.headers = foreign.headers;
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(&different),
            result.clone(),
            key("invoked/foreign-root")?
        ),
        Err(Error::Invalid(
            "invocation input targets another actor or subscription"
        ))
    ));
    assert_eq!(store.load(&address).await?.tail, before.tail);
    let mut forked = result.clone();
    let reply_parent = invocation.caller().clone();
    let reply_fork = acyclic_stream::CommitEffect::hard(
        Vec::new(),
        vec![CommitMutation::Fork {
            exclude: Vec::new(),
            source: before.path().clone(),
            destination: reply_parent,
            at_tail: 0,
            records: Vec::new(),
        }],
    );
    forked.effects.push(reply_fork.clone());
    assert!(matches!(
        store.prepare_publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            forked,
            key("invoked/fork-parent")?
        ),
        Err(Error::Invalid(
            "guest effect targets the host's invocation reply"
        ))
    ));
    let mut publication = store.prepare_publish(
        &before,
        1,
        &record(0),
        Some(ticket.record()),
        result.clone(),
        key("invoked/publish")?,
    )?;
    let original_request = publication.request.clone();
    assert!(matches!(
        publication.join(reply_fork),
        Err(Error::Invalid("effect changes the host's invocation reply"))
    ));
    assert_eq!(publication.request, original_request);
    store.submit(publication).await?;
    // Lost reply and acknowledgement: replay the same original publication,
    // then reopen the Store and retry the public operation identity.
    store
        .publish(
            &before,
            1,
            &record(0),
            Some(ticket.record()),
            result,
            key("invoked/publish")?,
        )
        .await?;
    // The same publication releases the original waiter; only its exact state
    // record remains to be read. No input follow or current state view occurs.
    let committed =
        acyclic_stream::assert_requests!(provider, waiting.await, data = 1, control = 0)?;
    assert_eq!(
        acyclic_stream::assert_requests!(
            provider,
            store.wait_invocation(ticket.clone()).await,
            data = 2,
            control = 0
        )?,
        committed
    );
    assert!(committed.response().is_empty());
    assert_eq!(committed.receipt().terminal().value, *committed.response());
    assert_eq!(
        committed.receipt().call().outcome,
        crate::call::Outcome::Ok(committed.response().clone())
    );
    assert_eq!(
        committed.receipt().call().log,
        *invocation.call(before.path())?.log()
    );
    for name in ["acyclic.call.v2", "acyclic.call.v2.fence"] {
        assert!(matches!(
            invocation.call(before.path())?.settle_with_attributes(
                None,
                &crate::call::Outcome::Ok(Bytes::new().into()),
                [acyclic_stream::Attribute {
                    name: name.into(),
                    value: Bytes::from_static(b"forged").into(),
                }],
            ),
            Err(crate::call::Error::Malformed(_))
        ));
    }
    assert_eq!(committed.receipt().revision(), before.tail + 1);
    assert_eq!(
        committed.receipt().state().commit_id,
        committed.receipt().terminal().commit_id
    );
    let reopened = Store::new(provider, state_root()?);
    assert_eq!(reopened.invoke(&invocation).await?, committed);
    let changed = Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        address.clone(),
        "inbox".into(),
        invocation.operation().clone(),
        Bytes::from_static(b"changed"),
    )?;
    assert!(matches!(
        reopened.enqueue_invoke(&changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ));
    assert_eq!(
        reopened
            .load(&address)
            .await?
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    invocation_completion_rejects_changed_original_publication().await?;
    Ok(())
}

// Mutate actual prepared publications, never fabricated provider responses.
// A reply alone cannot attest a consumed cursor or borrow another state commit.
async fn invocation_completion_rejects_changed_original_publication() -> TestResult {
    for fault in 0..3 {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(provider.clone(), state_root()?);
        let address = actor("changed-publication")?;
        place(&store, &address).await?;
        let invocation = crate::invocation::Invocation::new(
            acyclic_stream::StreamPath::new("invoke-control/caller")?,
            address.clone(),
            "inbox".into(),
            Bytes::from_static(b"changed-publication"),
            Bytes::from_static(b"original-request"),
        )?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let before = store.load(&address).await?;
        let mut transition = transition(1, AlarmChange::Keep);
        transition.response = Some(Bytes::from_static(b"original-response"));
        let mut request = store
            .prepare_publish(
                &before,
                1,
                &record(0),
                Some(ticket.record()),
                transition,
                key("changed-publication/publish")?,
            )?
            .into_request()?;
        match fault {
            0 => {
                for mutation in &mut request.effects[0].mutations {
                    if let CommitMutation::Append { path, records } = mutation {
                        if path == before.path() {
                            records[0] = before.state().ok_or("missing state")?.encode().into();
                        }
                    }
                }
            }
            1 => request.effects[0].mutations.retain(|mutation| {
                !matches!(mutation, CommitMutation::Append { path, .. } if path == before.path())
            }),
            2 => {
                let mut state_only = request.clone();
                state_only.effects[0].mutations.retain(|mutation| {
                    matches!(mutation, CommitMutation::Append { path, .. } if path == before.path())
                });
                assert!(matches!(provider.commit(state_only).await?, CommitOutcome::Committed(_)));
                request.idempotency_key = key("changed-publication/separate-reply")?;
                request.effects[0].mutations.retain(|mutation| {
                    !matches!(mutation, CommitMutation::Append { path, .. } if path == before.path())
                });
                request.effects[0].conditions.retain(|condition| {
                    matches!(condition, CommitCondition::Absent { .. })
                });
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            provider.commit(request).await?,
            CommitOutcome::Committed(_)
        ));
        let result = store.wait_invocation(ticket).await;
        if fault == 1 {
            assert!(matches!(
                result,
                Err(Error::Invalid("invocation state receipt is missing"))
            ));
        } else {
            assert!(matches!(
                result,
                Err(Error::Invalid(
                    "invocation reply is not co-committed with consumed input"
                ))
            ));
        }
    }
    Ok(())
}

/// Reopening the actual durable provider must recover the same publication,
/// while issuing its read floor from the provider's current incarnation.
#[tokio::test]
async fn invocation_reconciles_the_original_records_after_durable_provider_reopen() -> TestResult {
    let directory = tempfile::tempdir()?;
    let address = actor("durable-invoked")?;
    let invocation = crate::invocation::Invocation::new(
        acyclic_stream::StreamPath::new("invoke-control/caller")?,
        address.clone(),
        "inbox".into(),
        Bytes::from_static(b"durable-op"),
        Bytes::from_static(b"durable-request"),
    )?;
    let committed = {
        let store = Store::new(
            Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
            state_root()?,
        );
        place(&store, &address).await?;
        let ticket = store.enqueue_invoke(&invocation).await?;
        let snapshot = store.load(&address).await?;
        let mut result = transition(9, AlarmChange::Keep);
        result.response = Some(Bytes::from_static(b"durable-response"));
        store
            .publish(
                &snapshot,
                1,
                &record(0),
                Some(ticket.record()),
                result,
                key("durable-invoke/publish")?,
            )
            .await?;
        store.wait_invocation(ticket).await?
    };
    let store = Store::new(
        Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
        state_root()?,
    );
    store
        .claim(
            &store.load(&address).await?,
            2,
            key("durable-invoke/new-owner")?,
        )
        .await?;
    let reopened = store.invoke(&invocation).await?;
    assert_eq!(reopened, committed);
    assert_eq!(
        reopened.response().as_ptr(),
        reopened.receipt().terminal().value.as_ptr()
    );
    let receipt = reopened.receipt();
    let floor = acyclic_stream::RevisionCut::of(receipt.ticket().storage_revision().clone());
    let applied = store.provider().wait_applied(&floor).await?;
    assert!(floor.proven_by(Some(&applied)));
    assert_eq!(
        store
            .provider()
            .tail(
                inbox(&address)?
                    .remove("inbox")
                    .ok_or("missing input")?
                    .path
            )
            .await?,
        1
    );
    Ok(())
}

/// Observation follows only committed actor state, preserving the original
/// input anchor and refusing a different subscription/path at the same cursor.
#[tokio::test]
async fn wait_applied_retains_its_cut_across_pause_and_never_consumes_input() -> TestResult {
    use acyclic_stream::AppendRequest;
    let provider = Arc::new(MemoryStream::default());
    let store = Store::new(Arc::clone(&provider), state_root()?);
    let address = actor("applied")?;
    place(&store, &address).await?;
    let path = inbox(&address)?.remove("inbox").ok_or("inbox")?.path;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"input").into()],
            if_tail: Some(0),
            idempotency_key: key("applied/input")?,
        })
        .await?;
    let original = provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("original input")?;
    let zero = store
        .wait_applied(&address, AppliedCut::empty("inbox".into(), path.clone())?)
        .await?;
    assert_eq!(
        zero.snapshot()
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(0)
    );
    assert!(matches!(
        store
            .wait_applied(&address, AppliedCut::empty("missing".into(), path.clone())?)
            .await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store
            .wait_applied(
                &address,
                AppliedCut::empty("inbox".into(), StreamPath::new("wrong/input")?)?
            )
            .await,
        Err(Error::Conflict)
    ));

    let cut = AppliedCut::through("inbox".into(), path.clone(), &original)?;
    let mut waiter = Box::pin(store.wait_applied(&address, cut.clone()));
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    let before = store.load(&address).await?;
    assert_eq!(before.tail, zero.revision(), "waiting published nothing");
    store
        .set_paused(&before, 1, true, key("applied/pause")?)
        .await?;
    assert!(
        futures::poll!(waiter.as_mut()).is_pending(),
        "pause is not input consumption"
    );
    let paused = store.load(&address).await?;
    store
        .set_paused(&paused, 1, false, key("applied/resume")?)
        .await?;
    assert!(futures::poll!(waiter.as_mut()).is_pending());
    let resumed = store.load(&address).await?;
    store
        .publish(
            &resumed,
            1,
            &record(0),
            Some(&original),
            transition(7, AlarmChange::Keep),
            key("applied/publish")?,
        )
        .await?;
    let applied = waiter.await?;
    assert_eq!(applied.cut(), &cut);
    assert_eq!(applied.revision(), resumed.tail + 1);
    assert_eq!(
        Some(applied.commit_id()),
        store.load(&address).await?.anchor
    );
    assert_eq!(
        applied
            .snapshot()
            .state()
            .and_then(|state| state.cursor("inbox")),
        Some(1)
    );

    // An already-consumed numerical cursor cannot authenticate a different
    // source history. Construct only the negative control from the real record.
    let mut different = original.clone();
    let mut anchor = *different.commit_id.as_bytes();
    anchor[0] ^= 1;
    different.commit_id = CommitId::from_bytes(anchor);
    assert!(matches!(
        store
            .wait_applied(
                &address,
                AppliedCut::through("inbox".into(), path, &different)?
            )
            .await,
        Err(Error::Stream(StreamError::PrefixNotRetained))
    ));
    assert_eq!(
        provider.tail(store.path(&address)?).await?,
        applied.revision()
    );
    Ok(())
}

fn actor(id: &str) -> Result<ActorReference, Box<dyn std::error::Error>> {
    Ok(ActorReference::new(b"account", b"agents", id.as_bytes())?)
}

fn key(value: &str) -> Result<IdempotencyKey, StreamError> {
    IdempotencyKey::new(value.as_bytes().to_vec())
}

fn transition(value: u8, alarm: AlarmChange) -> Transition {
    Transition {
        pause: false,
        response: None,
        participant: None,
        checkpoint: Checkpoint {
            codec: CodecIdentity {
                name: "counter".into(),
                version: "1".into(),
            },
            bytes: Bytes::copy_from_slice(&[value]),
        },
        alarm,
        effects: Vec::new(),
        subscriptions: Vec::new(),
    }
}

fn record(sequence: u64) -> Trigger {
    Trigger::Record {
        subscription: "inbox".into(),
        sequence,
    }
}

fn state_root() -> Result<StreamPath, StreamError> {
    StreamPath::new("harness/actors")
}

fn inbox(address: &ActorReference) -> Result<BTreeMap<String, Subscription>, StreamError> {
    Ok(BTreeMap::from([(
        "inbox".into(),
        Subscription {
            path: StreamPath::new(format!("inboxes/{}", address.id()))?,
            cursor: 0,
        },
    )]))
}

/// A foreign publication of only soft sends commits on the durable local
/// provider beside its empty hard effect, skips the send whose recipient is
/// absent, and a retry after a reopen replays the same receipt.
#[tokio::test]
async fn foreign_soft_only_publication_commits_and_replays_after_reopen() -> TestResult {
    use acyclic_stream::AppendRequest;
    let root = tempfile::tempdir()?;
    let inbox = StreamPath::new("peers/one/inbox")?;
    let absent = StreamPath::new("peers/two/inbox")?;
    let send = |to: &StreamPath| CommitEffect {
        conditions: vec![CommitCondition::Present { path: to.clone() }],
        mutations: vec![CommitMutation::Append {
            path: to.clone(),
            records: vec![Bytes::from_static(b"hello").into()],
        }],
        soft: true,
    };
    let mut prepared = PreparedPublish::foreign(key("soft-only")?);
    prepared.join(send(&inbox))?;
    prepared.join(send(&absent))?;
    let request = prepared.into_request()?;
    let provider = LocalStream::open(root.path(), Default::default()).await?;
    provider
        .append(AppendRequest {
            path: inbox.clone(),
            records: vec![Bytes::from_static(b"open").into()],
            if_tail: Some(0),
            idempotency_key: key("open")?,
        })
        .await?;
    let CommitOutcome::Committed(receipt) = provider.commit(request.clone()).await? else {
        return Err("soft-only publication conflicted".into());
    };
    // Effect 0 is the publication's empty hard effect.
    assert_eq!(receipt.skipped, [2]);
    drop(provider);
    let reopened = LocalStream::open(root.path(), Default::default()).await?;
    let CommitOutcome::Committed(replayed) = reopened.commit(request).await? else {
        return Err("replayed publication conflicted".into());
    };
    assert_eq!(
        (replayed.commit_id, &replayed.skipped),
        (receipt.commit_id, &receipt.skipped)
    );
    assert_eq!(reopened.tail(inbox).await?, 2);
    assert_eq!(reopened.tail(absent).await, Err(StreamError::NotFound));
    Ok(())
}

/// Create through the companion mutation in a standalone commit, then place.
async fn place<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
) -> Result<(), Error> {
    let path = store.path(address)?;
    let mut prepared = PreparedPublish::foreign(key(&format!("create/{path}"))?);
    for effect in [
        store.create(
            None,
            address,
            crate::local::Creation {
                subscriptions: inbox(address)?,
                checkpoint: None,
                escrow: None,
            },
        )?,
        store.activation(address)?,
    ] {
        prepared.join(effect)?;
    }
    let created = store.provider().commit(prepared.into_request()?).await?;
    PreparedPublish::settle(&created)?;
    store
        .claim(
            &store.load(address).await?,
            1,
            key(&format!("place/{path}"))?,
        )
        .await
        .map(|_| ())
}

async fn atomic_history<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    root: StreamPath,
) -> TestResult {
    let store = Store::new(Arc::clone(&provider), root);
    let address = actor("atomic")?;
    place(&store, &address).await?;
    let before = store.load(&address).await?;
    let output = StreamPath::new("outputs/atomic")?;
    let make_transition = || {
        let mut value = transition(
            7,
            AlarmChange::Set {
                deadline_unix_ms: 42,
            },
        );
        value.effects = vec![
            acyclic_stream::CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: output.clone(),
                }],
                vec![CommitMutation::Append {
                    path: output.clone(),
                    records: vec![Bytes::from_static(b"a").into()],
                }],
            ),
            acyclic_stream::CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: output.clone(),
                }],
                vec![CommitMutation::Append {
                    path: output.clone(),
                    records: vec![Bytes::from_static(b"b").into()],
                }],
            ),
        ];
        value
    };
    store
        .publish(
            &before,
            1,
            &record(0),
            None,
            make_transition(),
            key("commit")?,
        )
        .await?;
    // Lose the acknowledgement, then replay the exact request. No duplicate effects.
    store
        .publish(
            &before,
            1,
            &record(0),
            None,
            make_transition(),
            key("commit")?,
        )
        .await?;
    let after = store.load(&address).await?;
    let state = after.state().ok_or("missing state")?;
    assert_eq!(state.cursor("inbox"), Some(1), "atomic consumed cursor");
    assert_eq!(
        state.checkpoint().map(|v| v.bytes.as_ref()),
        Some([7].as_slice()),
        "atomic checkpoint"
    );
    assert_eq!(state.alarm().deadline_unix_ms(), Some(42), "atomic alarm");
    assert_eq!(state.alarm().generation(), 1);
    assert_eq!(
        provider.tail(output.clone()).await?,
        2,
        "exactly once committed effects"
    );
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: output,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await?;
    while let Some(effect) = records.try_next().await? {
        assert_eq!(
            Some(effect.commit_id),
            after.anchor,
            "one checkpoint/effect transaction"
        );
    }
    Ok(())
}

async fn fencing_history<P: StreamProvider + ?Sized>(provider: Arc<P>) -> TestResult {
    let store = Store::new(provider, state_root()?);
    let address = actor("fences")?;
    place(&store, &address).await?;
    let first = store.load(&address).await?;
    store
        .publish(
            &first,
            1,
            &record(0),
            None,
            transition(
                1,
                AlarmChange::Set {
                    deadline_unix_ms: 10,
                },
            ),
            key("set")?,
        )
        .await?;
    let old = store.load(&address).await?;
    store.claim(&old, 2, key("takeover")?).await?;
    let current = store.load(&address).await?;
    // A freshly loaded token still cannot be used by an old placement.
    assert!(
        matches!(
            store
                .publish(
                    &current,
                    1,
                    &record(1),
                    None,
                    transition(2, AlarmChange::Clear),
                    key("old-owner")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "epoch fencing"
    );
    assert!(
        matches!(
            store
                .publish(
                    &old,
                    1,
                    &record(1),
                    None,
                    transition(2, AlarmChange::Clear),
                    key("old-cut")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "exact cut fencing"
    );
    store
        .publish(
            &current,
            2,
            &record(1),
            None,
            transition(
                2,
                AlarmChange::Set {
                    deadline_unix_ms: 20,
                },
            ),
            key("replace")?,
        )
        .await?;
    let replaced = store.load(&address).await?;
    assert!(
        matches!(
            store
                .publish(
                    &replaced,
                    2,
                    &Trigger::Alarm {
                        generation: 1,
                        now_unix_ms: 30
                    },
                    None,
                    transition(3, AlarmChange::Keep),
                    key("old-alarm")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "alarm generation fencing"
    );
    pause_and_consumed_alarm_history(&store, &address, &replaced).await
}

async fn pause_and_consumed_alarm_history<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    address: &ActorReference,
    replaced: &Snapshot,
) -> TestResult {
    store.set_paused(replaced, 2, true, key("pause")?).await?;
    let paused = store.load(address).await?;
    assert!(
        matches!(
            store
                .publish(
                    &paused,
                    2,
                    &record(2),
                    None,
                    transition(3, AlarmChange::Clear),
                    key("paused")?
                )
                .await,
            Err(Error::Paused)
        ),
        "paused work remains unconsumed"
    );
    assert!(
        matches!(
            store
                .publish(
                    replaced,
                    2,
                    &record(2),
                    None,
                    transition(3, AlarmChange::Clear),
                    key("inflight")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "pause fences in-flight work"
    );
    store.set_paused(&paused, 2, false, key("resume")?).await?;
    let resumed = store.load(address).await?;
    store
        .publish(
            &resumed,
            2,
            &Trigger::Alarm {
                generation: 2,
                now_unix_ms: 20,
            },
            None,
            transition(3, AlarmChange::Keep),
            key("fire")?,
        )
        .await?;
    let fired = store.load(address).await?;
    let state = fired.state().ok_or("missing state")?;
    assert_eq!(state.cursor("inbox"), Some(2));
    assert_eq!(state.alarm().generation(), 2, "Keep preserves generation");
    assert_eq!(
        state.alarm().deadline_unix_ms(),
        Some(20),
        "Keep preserves deadline"
    );
    assert!(
        matches!(
            store
                .publish(
                    &fired,
                    2,
                    &Trigger::Alarm {
                        generation: 2,
                        now_unix_ms: 30
                    },
                    None,
                    transition(4, AlarmChange::Keep),
                    key("cleared")?
                )
                .await,
            Err(Error::Conflict)
        ),
        "a consumed alarm cannot publish twice even when Keep preserves its deadline"
    );
    Ok(())
}

#[tokio::test]
async fn memory_atomic_history() -> TestResult {
    atomic_history(Arc::new(MemoryStream::default()), state_root()?).await?;
    atomic_history(
        Arc::new(MemoryStream::default()),
        StreamPath::new("actors")?,
    )
    .await
}

#[tokio::test]
async fn memory_epoch_alarm_pause_fences() -> TestResult {
    fencing_history(Arc::new(MemoryStream::default())).await
}

#[tokio::test]
async fn local_atomic_history() -> TestResult {
    let root = tempfile::tempdir()?;
    atomic_history(
        Arc::new(LocalStream::open(root.path(), Default::default()).await?),
        state_root()?,
    )
    .await
}

#[tokio::test]
async fn local_epoch_alarm_pause_fences() -> TestResult {
    let root = tempfile::tempdir()?;
    fencing_history(Arc::new(
        LocalStream::open(root.path(), Default::default()).await?,
    ))
    .await
}

#[tokio::test]
async fn reopen_preserves_the_complete_committed_tuple() -> TestResult {
    let root = tempfile::tempdir()?;
    {
        let provider = Arc::new(LocalStream::open(root.path(), Default::default()).await?);
        atomic_history(provider, state_root()?).await?;
    }
    let store = Store::new(
        Arc::new(LocalStream::open(root.path(), Default::default()).await?),
        state_root()?,
    );
    let snapshot = store.load(&actor("atomic")?).await?;
    let state = snapshot.state().ok_or("missing recovered state")?;
    assert_eq!(state.epoch(), 1);
    assert_eq!(state.cursor("inbox"), Some(1));
    assert_eq!(state.alarm().generation(), 1);
    assert_eq!(state.alarm().deadline_unix_ms(), Some(42));
    assert_eq!(
        state.checkpoint().map(|value| value.bytes.as_ref()),
        Some([7].as_slice())
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn bounded_alarm_histories(actions in prop::collection::vec(any::<u8>(), 1..24)) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let store = Store::new(Arc::new(MemoryStream::default()), state_root()?);
            let address = actor("history")?;
            place(&store, &address).await?;
            let mut generation = 0;
            let mut deadline = None;
            for (index, action) in actions.into_iter().enumerate() {
                let alarm = match action % 3 { 0 => AlarmChange::Keep, 1 => AlarmChange::Clear, _ => AlarmChange::Set { deadline_unix_ms: u64::from(action) + 1 } };
                match alarm { AlarmChange::Keep => {}, AlarmChange::Clear => { generation += 1; deadline = None; }, AlarmChange::Set { deadline_unix_ms } => { generation += 1; deadline = Some(deadline_unix_ms); } }
                let snapshot = store.load(&address).await?;
                store.publish(&snapshot, 1, &record(u64::try_from(index)?), None, transition(action, alarm), key(&format!("history-{index}"))?).await?;
                let after = store.load(&address).await?;
                let state = after.state().ok_or("missing state")?;
                assert_eq!(state.alarm().generation(), generation);
                assert_eq!(state.alarm().deadline_unix_ms(), deadline);
                assert_eq!(state.cursor("inbox"), Some(u64::try_from(index)? + 1));
            }
            TestResult::Ok(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

/// Real durable provider: metadata, journal and alarm share one commit, survive
/// reopen, and cannot be rewritten by an old epoch or a failed transaction.
#[tokio::test]
async fn state_transaction_flush_is_one_durable_fenced_journal_commit() -> TestResult {
    let directory = tempfile::tempdir()?;
    let address = actor("state-transaction")?;
    {
        let provider = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
        let store = Store::new(Arc::clone(&provider), state_root()?);
        place(&store, &address).await?;
        let initial = store.load(&address).await?;
        let journal = StreamPath::new(format!("{}/journal", initial.path()))?;
        let initial_tail = initial.tail;
        let wakes_path = store.activation_record(&address)?.0;
        let wake_from = provider.tail(wakes_path.clone()).await?;
        let mut binding = store.bind_state(initial, 1)?;
        assert_eq!(binding.seed()?.journal_tail, "0");
        binding.transaction(|state| {
            state.put("retired".into(), Bytes::from_static(b"remove"));
            state.put("packet-only".into(), Bytes::from_static(b"remove"));
            state.put("metadata".into(), Bytes::from_static(b"replace"));
            Ok(())
        })?;
        binding.apply(crate::guest::ActorStateCommit {
            deletions: vec![
                "retired".into(),
                "packet-only".into(),
                "metadata".into(),
                "absent".into(),
            ],
            writes: vec![
                crate::guest::ActorMetadataWrite {
                    key: "metadata".into(),
                    value: Bytes::from_static(b"accepted"),
                },
                crate::guest::ActorMetadataWrite {
                    key: "retired".into(),
                    value: Bytes::from_static(b"durable"),
                },
            ],
            journal_from: "0".into(),
            journal: vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")],
            alarm: crate::guest::ActorStateAlarm::Set {
                deadline: "42".into(),
            },
        })?;
        binding.transaction(|state| {
            assert_eq!(
                state.get("metadata").map(Bytes::as_ref),
                Some(b"accepted".as_slice())
            );
            assert_eq!(state.entries().count(), 2);
            Ok(())
        })?;
        let refused: Result<(), Error> = binding.transaction(|state| {
            state.delete("metadata");
            assert!(state.get("metadata").is_none());
            state.put("metadata".into(), Bytes::from_static(b"rolled-back"));
            state.delete("metadata");
            state.append_journal(2, vec![Bytes::from_static(b"rolled-back")])?;
            state.alarm(None)?;
            Err(Error::Invalid("transaction refused"))
        });
        assert!(refused.is_err());
        assert_eq!(store.load(&address).await?.tail, initial_tail);
        assert!(matches!(
            provider.tail(journal.clone()).await,
            Err(StreamError::NotFound)
        ));
        binding.flush(key("state-transaction/first")?).await?;
        let confirmed = binding.finish()?;
        assert_eq!(confirmed.tail, initial_tail + 1);
        assert_eq!(confirmed.state().ok_or("missing state")?.journal_tail(), 2);
        assert_eq!(provider.tail(wakes_path.clone()).await?, wake_from + 1);
        let mut wakes = provider
            .read(ReadRequest {
                path: wakes_path,
                from: wake_from,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let wake = wakes.try_next().await?.ok_or("alarm omitted its wake")?;
        assert_eq!(wake.value.as_ref(), address.encode());
        assert_eq!(Some(wake.commit_id), confirmed.anchor);
        assert!(wakes.try_next().await?.is_none());
        let mut frames = provider
            .read(ReadRequest {
                path: journal,
                from: 0,
                limit: 2,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        for expected in [b"first".as_slice(), b"second".as_slice()] {
            let frame = frames.try_next().await?.ok_or("missing durable frame")?;
            assert_eq!(frame.value.as_ref(), expected);
            assert_eq!(Some(frame.commit_id), confirmed.anchor);
        }
        assert!(frames.try_next().await?.is_none());
        // The original delivery owner uses this updated cut, rather than an
        // obsolete pre-flush state, for its normal cursor/checkpoint commit.
        store
            .publish(
                &confirmed,
                1,
                &record(0),
                None,
                transition(7, AlarmChange::Keep),
                key("state-transaction/checkpoint")?,
            )
            .await?;
        // Deletion uses the same durable state publication and survives reopen.
        let mut removed = store.bind_state(store.load(&address).await?, 1)?;
        removed.transaction(|state| {
            state.delete("retired");
            Ok(())
        })?;
        removed.flush(key("state-transaction/delete")?).await?;
        removed.finish()?;
        // A changed native journal tail must refuse metadata and alarm too.
        let mut stale = store.bind_state(store.load(&address).await?, 1)?;
        let current = store.load(&address).await?;
        store
            .claim(&current, 2, key("state-transaction/takeover")?)
            .await?;
        stale.transaction(|state| {
            state.put("metadata".into(), Bytes::from_static(b"stale"));
            state.append_journal(2, vec![Bytes::from_static(b"stale")])?;
            state.alarm(None)
        })?;
        assert!(matches!(
            stale.flush(key("state-transaction/stale")?).await,
            Err(Error::Conflict)
        ));
        assert!(stale.transaction(|_| Ok(())).is_err());
        assert!(stale.finish().is_err());
    }
    let store = Store::new(
        Arc::new(LocalStream::open(directory.path(), Default::default()).await?),
        state_root()?,
    );
    let restored = store.load(&address).await?;
    let state = restored.state().ok_or("missing restored state")?;
    assert_eq!(state.epoch(), 2);
    assert_eq!(state.alarm().deadline_unix_ms(), Some(42));
    assert_eq!(
        state.metadata().collect::<Vec<_>>(),
        vec![("metadata", &Bytes::from_static(b"accepted"))]
    );
    let binding = store.bind_state(restored, 2)?;
    assert_eq!(
        binding
            .read_journal(0, 1)
            .await?
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>(),
        vec![b"first".as_slice()]
    );
    assert_eq!(
        binding
            .read_journal(1, 4)
            .await?
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&[u8]>>(),
        vec![b"second".as_slice()]
    );
    assert!(binding.read_journal(2, 1).await?.is_empty());
    assert!(binding.read_journal(3, 1).await.is_err());
    Ok(())
}

/// An actor's own subscription changes apply after its consumption, in order:
/// re-adding a bound id at the same path keeps the consumed cursor, a removed
/// id rebinds anywhere from its explicit start, and every other change is
/// refused. A refused change yields no next state, so nothing of its turn
/// (checkpoint or consumed cursor) can be published.
#[test]
fn actor_subscription_changes_keep_consumed_cursors_and_refuse_conflicts() -> TestResult {
    use crate::guest::SubscriptionChange;
    let own = StreamPath::new("harness/actors/agents/a")?;
    let first = StreamPath::new("inboxes/a")?;
    let second = StreamPath::new("inboxes/b")?;
    let state = State {
        epoch: 1,
        placement_branch: None,
        escrow: None,
        checkpoint: None,
        subscriptions: BTreeMap::from([(
            "inbox".into(),
            Subscription {
                path: first.clone(),
                cursor: 0,
            },
        )]),
        alarm: Alarm::default(),
        paused: false,
        metadata: BTreeMap::new(),
        journal_tail: 0,
        custody: false,
        consumed: None,
    };
    let add = |id: &str, path: &StreamPath, from| SubscriptionChange::Add {
        id: id.into(),
        path: path.clone(),
        from,
    };
    let turn = |changes: Vec<SubscriptionChange>| {
        let mut turn = transition(1, AlarmChange::Keep);
        turn.subscriptions = changes;
        next(state.clone(), &own, &Alarm::default(), &record(0), &turn)
    };
    let consumed = turn(Vec::new())?;
    assert_eq!(consumed.subscriptions["inbox"].cursor, 1);
    // Re-adding the bound id at its path, even from an older start, keeps
    // the cursor this same turn consumed.
    assert_eq!(turn(vec![add("inbox", &first, Some(0))])?, consumed);
    // Remove then add the same id binds the new path at its explicit start.
    let rebound = turn(vec![
        SubscriptionChange::Remove { id: "inbox".into() },
        add("inbox", &second, Some(7)),
    ])?;
    assert_eq!(
        rebound.subscriptions["inbox"],
        Subscription {
            path: second.clone(),
            cursor: 7
        }
    );
    // Removing an unbound id is a no-op.
    assert_eq!(
        turn(vec![SubscriptionChange::Remove { id: "other".into() }])?,
        consumed
    );
    for refused in [
        add("inbox", &second, Some(0)),
        add("mirror", &own, Some(0)),
        add("mirror", &second, None),
        add("", &second, Some(0)),
    ] {
        assert!(matches!(turn(vec![refused]), Err(Error::Invalid(_))));
    }
    Ok(())
}

/// The composed orphan-free oracle. Every scheduling entry (an activation or a
/// deadline-window record) and every participant custody intent names its
/// owner by itself, and that owner is a live actor; each intent's publication
/// is reachable by its own key. Resolving an owner never scans the state
/// root. Returns the orphaned entries.
pub(super) async fn orphans<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    custodians: &[ActorReference],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let provider = store.provider();
    let mut scheduled = Vec::new();
    for shard in 0..store.scheduling_shards() {
        let log = store.scheduling_path(shard)?;
        scheduled.push(log.clone());
        let mut after = None;
        loop {
            let page = match provider
                .children_page(acyclic_stream::ChildrenPageRequest {
                    tail_stream: None,
                    parent: Some(log.clone()),
                    after,
                    hierarchy_version: None,
                    limit: 64,
                })
                .await
            {
                Ok(page) => page,
                Err(error) if error.category() == StreamError::NotFound => break,
                Err(error) => return Err(error.into()),
            };
            scheduled.extend(page.children.into_iter().map(|child| child.path));
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
    let mut orphans = Vec::new();
    for path in scheduled {
        for (sequence, value) in values(&**provider, &path).await?.into_iter().enumerate() {
            let live = match ActorReference::decode(&value) {
                Ok(actor) => store.load(&actor).await?.state().is_some(),
                Err(_) => false,
            };
            if !live {
                orphans.push(format!("{path}@{sequence}"));
            }
        }
    }
    for actor in custodians {
        let path = custody_path(&store.path(actor)?)?;
        let live = store.load(actor).await?.state().is_some();
        for (sequence, value) in values(&**provider, &path).await?.into_iter().enumerate() {
            let applied = matches!(
                provider
                    .inspect_idempotency(record::Intent::decode(&value)?.key)
                    .await?
                    .map(|inspection| inspection.outcome),
                Some(acyclic_stream::IdempotencyOutcome::Commit(_))
            );
            if !(live && applied) {
                orphans.push(format!("{path}@{sequence}"));
            }
        }
    }
    Ok(orphans)
}

async fn values<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &StreamPath,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let tail = match provider.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    Ok(provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: u32::try_from(tail)?.max(1),
            at_least: Default::default(),
        })
        .await?
        .map_ok(|record| Bytes::copy_from_slice(record.value.as_ref()))
        .try_collect()
        .await?)
}
