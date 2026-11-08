use crate::*;
#[cfg(feature = "local")]
use futures::StreamExt as _;

pub(crate) async fn large_retained_view<P: StreamProvider>(
    provider: &P,
) -> Result<HeadsRequest, StreamError> {
    let path = StreamPath::new("retained-size")?;
    let payload = OwnedBytes::from(vec![42; MAX_RECORD_BYTES / 2]);
    let count = MAX_COMMAND_BYTES / payload.len() + 1;
    let AppendOutcome::Committed(receipt) = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![payload.into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?
    else {
        return Err(StreamError::InvalidResponse);
    };
    Ok(HeadsRequest {
        retained: std::iter::repeat_n(
            RetainedRecord {
                commit_id: receipt.commit_id,
                path,
                sequence: 0,
            },
            count,
        )
        .collect(),
        durable_through: None,
        projection: HeadProjection::Tail,
        paths: Default::default(),
        children: None,
        limit: u32::try_from(count).map_err(|_| StreamError::LimitExceeded)?,
        at_least: Default::default(),
    })
}

async fn check_trusted_view<P: StreamProvider>(provider: &P) -> Result<(), StreamError> {
    let mut request = large_retained_view(provider).await?;
    let mut view = provider.heads(request.clone()).await?;
    view.validate_for(&request)?;
    let payload = vec![42; MAX_RECORD_BYTES / 2];
    assert!(view.retained.iter().all(|selected| {
        selected
            .record
            .as_ref()
            .is_some_and(|record| record.value.as_slice() == payload)
    }));
    assert_eq!(
        wire_codec::heads_to_wire(view.clone()),
        Err(StreamError::LimitExceeded)
    );
    request.retained.truncate(1);
    request.limit = 1;
    view.retained.truncate(1);
    wire_codec::heads_from_response(wire_codec::heads_to_wire(view)?)?.validate_for(&request)?;
    check_child_selection(provider).await?;
    Ok(())
}

async fn check_child_selection<P: StreamProvider>(provider: &P) -> Result<(), StreamError> {
    let selected = ChildrenPageRequest {
        parent: Some(StreamPath::new("membership")?),
        after: None,
        tail_stream: Some(StreamPath::new("events")?),
        hierarchy_version: None,
        limit: 2,
    };
    let empty_version = provider
        .children_page(selected.clone())
        .await?
        .hierarchy_version;
    for path in [
        "membership/a/events",
        "membership/b/events",
        "membership/c/events",
    ] {
        seed(provider, &StreamPath::new(path)?).await?;
    }
    provider
        .fork(ForkRequest {
            source: StreamPath::new("membership/a")?,
            destination: StreamPath::new("membership/d")?,
            exclude: Vec::new(),
            at_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let request = HeadsRequest {
        retained: Default::default(),
        durable_through: None,
        projection: HeadProjection::Watermarks,
        paths: Default::default(),
        children: Some(selected.clone()),
        limit: selected.limit,
        at_least: Default::default(),
    };
    let view = provider.heads(request.clone()).await?;
    view.validate_for(&request)?;
    let view = wire_codec::heads_from_response(wire_codec::heads_to_wire(view)?)?;
    view.validate_for(&request)?;
    let page = view.children.as_ref().ok_or(StreamError::InvalidResponse)?;
    assert_eq!(page.children.len(), 2);
    assert!(page.next_after.is_some());
    assert_eq!(provider.children_page(selected.clone()).await?, *page);
    let refuses = |candidate: &Heads, selection: &HeadsRequest| {
        assert_eq!(
            candidate.validate_for(selection),
            Err(StreamError::InvalidResponse),
            "child page escaped its original selection"
        );
    };
    for change in 0..4 {
        let mut changed = request.clone();
        let selection = changed
            .children
            .as_mut()
            .ok_or(StreamError::InvalidResponse)?;
        match change {
            0 => selection.parent = Some(page.children[0].path.clone()),
            1 => selection.after = Some(page.children[0].path.clone()),
            2 => selection.hierarchy_version = Some(empty_version),
            _ => selection.tail_stream = None,
        }
        refuses(&view, &changed);
    }
    let mut pinned = request.clone();
    pinned
        .children
        .as_mut()
        .ok_or(StreamError::InvalidResponse)?
        .hierarchy_version = Some(page.hierarchy_version);
    view.validate_for(&pinned)?;
    for change in 0..4 {
        let mut changed = view.clone();
        let changed_page = changed
            .children
            .as_mut()
            .ok_or(StreamError::InvalidResponse)?;
        match change {
            0 => changed_page.children.reverse(),
            1 => changed_page.children[1] = changed_page.children[0].clone(),
            2 => changed_page.next_after = Some(page.children[0].path.clone()),
            _ => changed_page.next_after = selected.parent.clone(),
        }
        refuses(&changed, &request);
    }
    // Authorization can hide every visible child while retaining the original
    // provider continuation. It still progresses beyond the caller's cursor.
    let mut hidden = view.clone();
    hidden
        .children
        .as_mut()
        .ok_or(StreamError::InvalidResponse)?
        .children
        .clear();
    hidden.validate_for(&request)?;
    let mut continued = request.clone();
    continued
        .children
        .as_mut()
        .ok_or(StreamError::InvalidResponse)?
        .after = page.next_after.clone();
    let next = provider.heads(continued.clone()).await?;
    next.validate_for(&continued)?;
    let mut stalled = next.clone();
    stalled
        .children
        .as_mut()
        .ok_or(StreamError::InvalidResponse)?
        .next_after = page.next_after.clone();
    refuses(&stalled, &continued);
    Ok(())
}

#[tokio::test]
async fn trusted_heads_use_the_result_budget_and_wire_heads_enforce_the_byte_bound()
-> Result<(), Box<dyn std::error::Error>> {
    check_trusted_view(&MemoryStream::default()).await?;
    #[cfg(feature = "local")]
    {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        check_trusted_view(&provider).await?;
    }
    Ok(())
}

fn request(
    path: &StreamPath,
    expected: u64,
    commit_id: CommitId,
    key: &'static [u8],
) -> Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Head {
            path: path.clone(),
            expected,
            commit_id,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(b"next")).into()],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(key))?,
    })
}

async fn seed<P: StreamProvider>(provider: &P, path: &StreamPath) -> Result<CommitId, StreamError> {
    let result = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(b"seed")).into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let AppendOutcome::Committed(receipt) = result else {
        return Err(StreamError::Unavailable);
    };
    Ok(receipt.commit_id)
}

#[tokio::test]
async fn head_condition_checks_identity_under_the_commit_lock_and_binds_retries()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let path = StreamPath::new("head")?;
    let original = seed(&provider, &path).await?;
    let wrong = CommitId::from_bytes([42; 32]);
    let rejected = request(&path, 1, wrong, b"rejected")?;
    assert_eq!(
        provider.commit(rejected.clone()).await?,
        CommitOutcome::Conflict(vec![CommitConflict::Head {
            path: path.clone(),
            expected: wrong,
            actual: original
        }])
    );
    assert_eq!(provider.tail(path.clone()).await?, 1);
    let mut changed = rejected;
    changed.conditions = vec![CommitCondition::Head {
        path: path.clone(),
        expected: 1,
        commit_id: original,
    }];
    let CommitOutcome::Committed(changed) = provider.commit(changed).await? else {
        return Err(StreamError::Unavailable);
    };
    let correct = request(&path, 2, changed.commit_id, b"correct")?;
    let result = provider.commit(correct.clone()).await?;
    assert!(matches!(result, CommitOutcome::Committed(_)));
    assert_eq!(provider.commit(correct).await?, result);
    assert_eq!(provider.tail(path).await?, 3);
    Ok(())
}

#[tokio::test]
async fn head_condition_on_a_fork_uses_the_inherited_record_identity() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = StreamPath::new("source")?;
    let original = seed(&provider, &source).await?;
    seed(&provider, &StreamPath::new("unrelated")?).await?;
    let path = StreamPath::new("fork")?;
    let fork = provider
        .fork(ForkRequest {
            exclude: Vec::new(),
            source,
            destination: path.clone(),
            at_tail: Some(1),
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    assert_ne!(fork.commit_id, original);
    assert!(matches!(
        provider
            .commit(request(&path, 1, fork.commit_id, b"wrong-fork-id")?)
            .await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(matches!(
        provider
            .commit(request(&path, 1, original, b"inherited")?)
            .await?,
        CommitOutcome::Committed(_)
    ));
    Ok(())
}

#[tokio::test]
async fn native_head_fence_cancellation_stops_before_publication()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::test_meter::Meter;
    use std::sync::Arc;
    let provider = MemoryStream::default();
    let path = StreamPath::new("head")?;
    let original = seed(&provider, &path).await?;
    let meter = Arc::new(Meter {
        cancelled: std::sync::atomic::AtomicBool::new(true),
        ..Meter::default()
    });
    assert!(
        provider
            .native_commit(
                request(&path, 1, original, b"denied")?,
                None,
                meter.original()
            )
            .await
            .is_err()
    );
    assert_eq!(provider.tail(path).await?, 1);
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn native_head_identity_hash_charge_is_independent_of_path_and_payload(
        path in "[a-z]{1,24}", payload in proptest::collection::vec(proptest::num::u8::ANY,0..256)
    ) {
        let result: Result<(),Box<dyn std::error::Error+Send+Sync>>=futures::executor::block_on(async {
            let mut hashed=Vec::new();
            for head in [false,true] {
                let provider=MemoryStream::default();
                let path=StreamPath::new(&path)?;
                let original=seed(&provider,&path).await?;
                let mut request=request(&path,1,original,b"native")?;
                request.mutations=vec![CommitMutation::Append {path:path.clone(),records:vec![payload.clone().into()]}];
                if !head {request.conditions=vec![CommitCondition::Tail {path,expected:1}];}
                let meter=std::sync::Arc::new(crate::test_meter::Meter::default());
                let result=provider.native_commit(request,None,meter.original()).await??;
                assert!(matches!(result.value(),CommitOutcome::Committed(_)));
                hashed.push(meter.hashed.load(std::sync::atomic::Ordering::SeqCst));
            }
            let [tail,head]:[u64;2]=hashed.try_into().map_err(|_|"missing native charges")?;
            assert_eq!(head,tail+32);
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(),"{result:?}");
    }
}

#[cfg(feature = "local")]
#[tokio::test]
async fn head_conditions_and_conflicts_survive_local_recovery_and_wire_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = StreamPath::new("head")?;
    let accepted;
    let rejected;
    let correct;
    let wrong;
    {
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let original = seed(&provider, &path).await?;
        wrong = request(&path, 1, CommitId::from_bytes([42; 32]), b"wrong")?;
        rejected = provider.commit(wrong.clone()).await?;
        correct = request(&path, 1, original, b"correct")?;
        let wire = wire_codec::commit_to_wire(&correct);
        assert_eq!(wire_codec::commit_from_wire(wire)?, correct);
        accepted = provider.commit(correct.clone()).await?;
    }
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    assert!(matches!(rejected, CommitOutcome::Conflict(_)));
    assert!(
        provider
            .inspect_idempotency(wrong.idempotency_key.clone())
            .await?
            .is_none()
    );
    assert!(matches!(
        provider.commit(wrong).await?,
        CommitOutcome::Conflict(_)
    ));
    assert_eq!(provider.commit(correct).await?, accepted);
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 2,
            at_least: Default::default(),
        })
        .await?;
    assert_eq!(
        records
            .next()
            .await
            .ok_or("missing seed")??
            .value
            .as_slice(),
        b"seed"
    );
    assert_eq!(
        records
            .next()
            .await
            .ok_or("missing append")??
            .value
            .as_slice(),
        b"next"
    );
    Ok(())
}

#[tokio::test]
async fn wire_tail_uses_one_head_view_for_offsets_receipt_and_revision() -> Result<(), StreamError>
{
    let provider = testing::Counted::new(MemoryStream::default());
    let path = StreamPath::new("coherent-tail")?;
    seed(&provider, &path).await?;
    let reply = crate::assert_requests!(
        provider,
        wire_codec::tail_response(
            &provider,
            wire::TailRequest {
                path: path.to_string(),
                accepted: true,
                durable_through: Some(1),
                at_least: Vec::new(),
            }
        )
        .await,
        control = 1,
        data = 0
    )?;
    assert_eq!((reply.tail, reply.durable), (1, Some(1)));
    assert_eq!(reply.applied.ok_or(StreamError::InvalidResponse)?.index, 1);
    // Retention is an explicit proof, never a crash-persistence watermark.
    assert_eq!(provider.watermarks(path.clone()).await?.durable, 0);
    let request = HeadsRequest {
        retained: Default::default(),
        durable_through: Some(1),
        projection: HeadProjection::Watermarks,
        paths: [path].into(),
        children: None,
        limit: 1,
        at_least: Default::default(),
    };
    let mut view = StreamProvider::heads(&provider, request.clone()).await?;
    view.validate_for(&request)?;
    let mut unsolicited = request.clone();
    unsolicited.durable_through = None;
    assert_eq!(
        view.validate_for(&unsolicited),
        Err(StreamError::InvalidResponse)
    );
    view.heads[0]
        .as_mut()
        .ok_or(StreamError::InvalidResponse)?
        .retained_through = Some(2);
    assert_eq!(
        wire_codec::heads_from_response(wire_codec::heads_to_wire(view)?),
        Err(StreamError::InvalidResponse)
    );
    Ok(())
}
