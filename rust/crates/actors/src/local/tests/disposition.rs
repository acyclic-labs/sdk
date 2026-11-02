use super::*;
use acyclic_stream::{
    AppendRecord, AppendRequest, ChildrenPageRequest, RevisionCut, testing::Counted,
};

struct FailureWriter(CommitMutation);

impl crate::guest::Composition for FailureWriter {
    fn seal<'a>(
        &'a mut self,
        mut request: CommitRequest,
    ) -> crate::guest::GuestFuture<'a, CommitRequest> {
        request.mutations.push(self.0.clone());
        Box::pin(async move { Ok(request) })
    }

    fn settle<'a>(
        self: Box<Self>,
        _outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> crate::guest::GuestFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

async fn selected<P: StreamProvider>(
    provider: &P,
    path: &StreamPath,
    from: u64,
) -> Result<Record, Error> {
    provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from,
            limit: 1,
            at_least: RevisionCut::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or(Error::Invalid("missing selected failure record"))
}

async fn failed_input_contract<P: StreamProvider>(
    provider: P,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let provider = Arc::new(Counted::new(provider));
    let store = Store::new(provider.clone(), state_root()?);
    let owner = actor("failure-disposition")?;
    place(&store, &owner).await?;
    let source = inbox(&owner)?.remove("inbox").ok_or("missing input")?.path;
    let mut original = AppendRecord::latest(Bytes::new(), Bytes::from_static(b"key"), true)?;
    original.value = vec![b'p'; acyclic_stream::MAX_RECORD_BYTES - original.payload_bytes()].into();
    let mut oversized = original.clone();
    oversized.value = vec![b'p'; original.value.len() + 1].into();
    assert!(matches!(
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![oversized],
                if_tail: None,
                idempotency_key: key("failure/protective-frame")?,
            })
            .await,
        Err(StreamError::LimitExceeded)
    ));
    provider
        .append(AppendRequest {
            path: source.clone(),
            records: vec![original, Bytes::from_static(b"later").into()],
            if_tail: None,
            idempotency_key: key("failure/input")?,
        })
        .await?;
    let input = selected(provider.as_ref(), &source, 0).await?;
    let snapshot = store.load(&owner).await?;
    let epoch = snapshot.state().ok_or("missing state")?.epoch();
    let cause = GuestError::Decode("original selected failure".into());
    let before = provider.requests();
    assert!(matches!(
        store.prepare_failed_input(
            &snapshot,
            epoch,
            &record(1),
            &input,
            &cause,
            key("failure/wrong")?,
        ),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.prepare_failed_input(
            &snapshot,
            epoch + 1,
            &record(0),
            &input,
            &cause,
            key("failure/stale")?,
        ),
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store.prepare_failed_input(
            &snapshot,
            epoch,
            &record(0),
            &input,
            &GuestError::PreconditionChanged,
            key("failure/guard")?,
        ),
        Err(Error::PreconditionChanged)
    ));
    let prepared = store.prepare_failed_input(
        &snapshot,
        epoch,
        &record(0),
        &input,
        &cause,
        key("failure/skip")?,
    )?;
    let competing = store.prepare_failed_input(
        &snapshot,
        epoch,
        &record(0),
        &input,
        &cause,
        key("failure/competing-skip")?,
    )?;
    let original_request = prepared.request().clone();
    let work = provider.requests().since(before);
    assert_eq!(
        (work.data(), work.control()),
        (0, 0),
        "preparation must not reread storage"
    );
    assert_eq!(
        original_request.mutations.len(),
        3,
        "state, failure metadata and shared original input"
    );
    store.submit(prepared).await?;
    assert!(matches!(
        store.submit(competing).await,
        Err(Error::Conflict)
    ));
    // Lost acknowledgement: settle the SAME request before observing newer state.
    PreparedPublish::settle(&provider.commit(original_request).await?)?;
    let after = store.load(&owner).await?;
    assert_eq!(after.tail, snapshot.tail + 1);
    assert_eq!(
        after.state().and_then(|state| state.cursor("inbox")),
        Some(1)
    );
    assert_eq!(
        after.state().and_then(|state| state.checkpoint()),
        snapshot.state().and_then(|state| state.checkpoint())
    );

    let page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(store.failures_path(&owner)?),
            after: None,
            hierarchy_version: None,
            tail_stream: Some(StreamPath::new("status")?),
            limit: 1,
        })
        .await?;
    assert_eq!(page.children.len(), 1);
    assert!(page.next_after.is_none());
    let child = &page.children[0];
    let entry = child.tail_record.as_ref().ok_or("missing failure tail")?;
    assert!(
        entry.value.len() < input.value.len(),
        "failure metadata must not re-encode the input payload"
    );
    assert_eq!(entry.commit_id, after.anchor.ok_or("missing state cut")?);
    let FailureEntry::Failed(failed) = FailureEntry::decode(&child.path, entry)? else {
        return Err("failure resolved before admission".into());
    };
    let captured = selected(provider.as_ref(), &failed.input_path()?, 0).await?;
    assert_eq!(failed.input(&captured)?, input);
    // Ordinary read/follow producers elide their selected origin, unlike the
    // child-tail projection above. Both retain the same failure/body identity.
    let projected = acyclic_stream::wire_codec::record_wire(entry.clone(), Some(&entry.origin));
    assert!(projected.origin.is_empty());
    assert!(acyclic_stream::wire_codec::record(projected.clone(), None).is_err());
    assert_eq!(
        FailureEntry::decode(
            &child.path,
            &acyclic_stream::wire_codec::record(projected, Some(&entry.origin))?,
        )?,
        FailureEntry::Failed(failed.clone()),
    );
    let projected =
        acyclic_stream::wire_codec::record_wire(captured.clone(), Some(&failed.input_path()?));
    assert!(projected.origin.is_empty());
    assert_eq!(
        failed.input(&acyclic_stream::wire_codec::record(
            projected,
            Some(&failed.input_path()?),
        )?)?,
        input,
    );
    assert!(
        failed.input(&input).is_err(),
        "a live source record cannot replace retained custody"
    );
    assert_eq!(failed.source(), &source);
    assert_eq!(failed.subscription(), "inbox");
    assert_eq!(failed.cause(), &cause);
    // Every guest mutation route must preserve the original failure and its
    // terminal fence, including forks at either side of the reserved root.
    let failures = store.failures_path(&owner)?;
    let mut mutations = vec![
        CommitMutation::Append {
            path: entry.origin.clone(),
            records: vec![entry.value.clone().into()],
        },
        CommitMutation::SoftAppend {
            path: failed.input_path()?,
            records: vec![Bytes::from_static(b"corrupt").into()],
            closed: None,
        },
    ];
    for destination in [
        failures,
        child.path.clone(),
        snapshot.path.clone(),
        state_root()?,
    ] {
        mutations.push(CommitMutation::Fork {
            source: source.clone(),
            destination,
            at_tail: 1,
            records: Vec::new(),
            exclude: Vec::new(),
        });
    }
    let before = provider.requests();
    for mutation in mutations {
        let effect = Effect {
            conditions: vec![CommitCondition::Present {
                path: source.clone(),
            }],
            mutation: mutation.clone(),
        };
        let mut result = transition(2, AlarmChange::Keep);
        result.effects.push(effect.clone());
        assert!(matches!(
            store.prepare_publish(
                &snapshot,
                epoch,
                &record(0),
                Some(&input),
                result,
                key("failure/guest-write")?
            ),
            Err(Error::Invalid("effect changes the host's failures"))
        ));
        let mut publication = store.prepare_failed_input(
            &snapshot,
            epoch,
            &record(0),
            &input,
            &cause,
            key("failure/join-write")?,
        )?;
        let original = publication.request().clone();
        assert!(matches!(
            publication.join(effect),
            Err(Error::Invalid("effect changes the host's failures"))
        ));
        assert_eq!(
            publication.request(),
            &original,
            "refusal cannot add conditions"
        );
        publication.composition = Some(Box::new(FailureWriter(mutation)));
        assert!(matches!(
            store.submit(publication).await,
            Err(Error::Invalid("composition changed the actor publication"))
        ));
    }
    let mut publication = store.prepare_publish(
        &snapshot,
        epoch,
        &record(0),
        Some(&input),
        transition(2, AlarmChange::Keep),
        key("failure/sibling")?,
    )?;
    publication.join(Effect {
        conditions: Vec::new(),
        mutation: CommitMutation::Append {
            path: StreamPath::new(format!("{}/.failures-other/status", snapshot.path))?,
            records: vec![Bytes::from_static(b"allowed").into()],
        },
    })?;
    let work = provider.requests().since(before);
    assert_eq!(
        (work.data(), work.control()),
        (0, 0),
        "refuse before provider IO"
    );
    assert_eq!(selected(provider.as_ref(), &entry.origin, 0).await?, *entry);
    assert!(
        FailureEntry::decode(&source, entry).is_err(),
        "foreign selection cannot authorize disposition"
    );
    let before = provider.requests();
    let limits = RuntimeLimits::new(1, 1)?;
    let host = crate::hosting::Host::new(
        Store::new(provider.clone(), state_root()?),
        owner.clone(),
        Arc::new(crate::conformance::Package::new()?),
        Arc::new(SystemClock),
        limits,
    );
    let foreign_host = crate::hosting::Host::new(
        Store::new(provider.clone(), state_root()?),
        actor("another-failure-owner")?,
        Arc::new(crate::conformance::Package::new()?),
        Arc::new(SystemClock),
        limits,
    );
    assert!(matches!(
        foreign_host.prepare_disposition(
            &failed,
            FailureDisposition::Purge,
            key("failure/foreign-host")?,
        ),
        Err(Error::Invalid(_))
    ));
    let replay = host.prepare_disposition(
        &failed,
        FailureDisposition::Replay(&captured),
        key("failure/replay")?,
    )?;
    let purge = store.prepare_disposition(
        &owner,
        &failed,
        FailureDisposition::Purge,
        key("failure/purge-race")?,
    )?;
    let work = provider.requests().since(before);
    assert_eq!((work.data(), work.control()), (0, 0));
    assert_eq!(
        replay.request().mutations.len(),
        3,
        "terminal, exact input and original owner wake"
    );
    let replay_request = replay.request().clone();
    store.submit(replay).await?;
    assert!(matches!(store.submit(purge).await, Err(Error::Conflict)));
    let replayed = selected(provider.as_ref(), &source, 2).await?;
    assert_eq!(replayed.value, input.value);
    assert_eq!(replayed.headers, input.headers);
    assert_eq!(
        selected(provider.as_ref(), &source, 1)
            .await?
            .value
            .as_ref(),
        b"later"
    );
    let terminal = selected(provider.as_ref(), &entry.origin, 1).await?;
    assert_eq!(
        terminal.commit_id, replayed.commit_id,
        "replay and disposition are one decision"
    );
    assert_eq!(
        FailureEntry::decode(&child.path, &terminal)?,
        FailureEntry::Resolved(FailureResolution::Replay)
    );
    assert_eq!(
        store.load(&owner).await?.tail,
        after.tail,
        "resolution cannot rewrite actor state"
    );
    let foreign = Store::new(provider.clone(), StreamPath::new("another/actors")?);
    assert!(
        foreign
            .prepare_disposition(
                &owner,
                &failed,
                FailureDisposition::Purge,
                key("failure/foreign")?
            )
            .is_err()
    );
    // Another failed input can be purged without reading its retained body.
    let later = selected(provider.as_ref(), &source, 1).await?;
    store
        .submit(store.prepare_failed_input(
            &after,
            epoch,
            &record(1),
            &later,
            &cause,
            key("failure/second")?,
        )?)
        .await?;
    let second_page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(store.failures_path(&owner)?),
            after: Some(child.path.clone()),
            hierarchy_version: None,
            tail_stream: Some(StreamPath::new("status")?),
            limit: 1,
        })
        .await?;
    let second_child = second_page.children.first().ok_or("missing next failure")?;
    let second_path = second_child.path.clone();
    let second_entry = second_child
        .tail_record
        .as_ref()
        .ok_or("missing next failure tail")?;
    let FailureEntry::Failed(second) = FailureEntry::decode(&second_path, second_entry)? else {
        return Err("missing second failed input".into());
    };
    let before = provider.requests();
    let purge = store.prepare_disposition(
        &owner,
        &second,
        FailureDisposition::Purge,
        key("failure/purge")?,
    )?;
    assert_eq!(
        provider.requests().since(before).data(),
        0,
        "purge does not fetch input bytes"
    );
    assert_eq!(purge.request().mutations.len(), 1);
    store.submit(purge).await?;
    let purged = selected(provider.as_ref(), &second_entry.origin, 1).await?;
    assert_eq!(
        FailureEntry::decode(&second_path, &purged)?,
        FailureEntry::Resolved(FailureResolution::Purge)
    );
    // Reconcile the original replay only after a later input and disposition
    // have committed. Its retained identity must not dispatch the input twice.
    PreparedPublish::settle(&provider.commit(replay_request).await?)?;
    assert_eq!(provider.head(source.clone()).await?.tail, 3);
    assert_eq!(
        selected(provider.as_ref(), &entry.origin, 1).await?,
        terminal,
        "late reconciliation retains the original replay terminal"
    );
    let child_owner = actor("disposition-branch")?;
    let parent = store.load(&owner).await?;
    let mut fork = PreparedPublish::foreign(key("failure/branch")?);
    for effect in store.fork(&parent, &child_owner, inbox(&child_owner)?)? {
        fork.join(effect)?;
    }
    store.submit(fork).await?;
    let inherited = StreamPath::new(format!(
        "{}/{:020}/status",
        store.failures_path(&child_owner)?,
        after.tail
    ))?;
    assert!(
        matches!(
            provider
                .read(ReadRequest {
                    path: inherited,
                    key: None,
                    from: 0,
                    limit: 1,
                    at_least: RevisionCut::default(),
                })
                .await,
            Err(StreamError::NotFound)
        ),
        "fork must not inherit failed-input custody"
    );
    // The same version cannot silently accept a second field occurrence or a
    // future schema field, even when protobuf would otherwise skip it.
    for suffix in [&[0x10, 1][..], &[0x18, 1][..]] {
        let mut invalid = terminal.clone();
        let mut bytes = invalid.value.to_vec();
        bytes.extend_from_slice(suffix);
        invalid.value = bytes.into();
        assert!(FailureEntry::decode(&child.path, &invalid).is_err());
    }
    // A changed codec discriminator must refuse without interpreting old bytes.
    let mut invalid = terminal.clone();
    let mut bytes = invalid.value.to_vec();
    bytes[4] = 2;
    invalid.value = bytes.into();
    assert!(FailureEntry::decode(&child.path, &invalid).is_err());
    // The public failure boundary must retire addressed invocation callers too;
    // it cannot depend on a private runtime wrapper adding missing terminals.
    let invoked = actor("public-failure-invocation")?;
    place(&store, &invoked).await?;
    let invocation = crate::invocation::Invocation::new(
        StreamPath::new("failure-control/caller")?,
        invoked.clone(),
        "inbox".into(),
        Bytes::from_static(b"public-failure"),
        Bytes::from_static(b"original-invocation"),
    )?;
    let ticket = store.enqueue_invoke(&invocation).await?;
    let current = store.load(&invoked).await?;
    let before = provider.requests();
    let publication = store.prepare_failed_input(
        &current,
        current.state().ok_or("missing invocation state")?.epoch(),
        &record(ticket.record().sequence),
        ticket.record(),
        &cause,
        key("failure/public-invocation")?,
    )?;
    let work = provider.requests().since(before);
    assert_eq!((work.data(), work.control()), (0, 0));
    let call = ticket.call()?;
    // Finite negative: restoring the old public preparation fails here before
    // waiting for a terminal that the mutation would never publish.
    for path in [call.log(), call.settled()] {
        assert!(publication.request().mutations.iter().any(|mutation| {
            matches!(mutation, CommitMutation::Append { path: selected, .. } if selected == path)
        }), "missing original invocation terminal/settlement");
    }
    let published = store
        .submit_retained(publication)
        .await?
        .ok_or("missing failed invocation state")?;
    let Err(Error::InvocationFailed { source, receipt }) = store.wait_invocation(ticket).await
    else {
        return Err("public failure left invocation unsettled".into());
    };
    assert_eq!(source, cause);
    assert_eq!(
        receipt.state().commit_id,
        published.anchor.ok_or("missing failure cut")?
    );
    assert_eq!(receipt.terminal().commit_id, receipt.state().commit_id);
    assert_eq!(
        selected(provider.as_ref(), call.settled(), 0)
            .await?
            .commit_id,
        receipt.state().commit_id
    );
    Ok(vec![
        entry.clone(),
        captured,
        terminal,
        second_entry.clone(),
        purged,
        receipt.state().clone(),
        receipt.terminal().clone(),
        selected(provider.as_ref(), call.settled(), 0).await?,
    ])
}

#[tokio::test]
async fn failed_input_disposition_is_atomic_and_preserves_original_identity() -> TestResult {
    failed_input_contract(MemoryStream::default()).await?;
    let directory = tempfile::tempdir()?;
    let original =
        failed_input_contract(LocalStream::open(directory.path(), Default::default()).await?)
            .await?;
    // Drop every original Store/provider, then recover through the shipped
    // durable entry point. Reconstructing only Store would retain live memory.
    let reopened = LocalStream::open(directory.path(), Default::default()).await?;
    for expected in original {
        let retained = selected(&reopened, &expected.origin, expected.sequence).await?;
        assert_eq!(
            retained, expected,
            "durable recovery must retain every field"
        );
        // Use the ordinary read projection, including its selected-origin
        // elision, and the actual protobuf decoder rather than a DTO copy.
        use prost::Message as _;
        let bytes =
            acyclic_stream::wire_codec::record_wire(retained.clone(), Some(&retained.origin))
                .encode_to_vec();
        let wire = acyclic_stream::wire::Record::decode(bytes.as_slice())?;
        assert_eq!(
            acyclic_stream::wire_codec::record(wire.clone(), Some(&retained.origin))?,
            expected
        );
        assert!(acyclic_stream::wire_codec::record(wire, None).is_err());
    }
    Ok(())
}
