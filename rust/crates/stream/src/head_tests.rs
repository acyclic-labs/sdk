use crate::*;
use futures::StreamExt as _;

fn request(
    path: &StreamPath,
    expected: u64,
    commit_id: CommitId,
    key: &'static [u8],
) -> Result<CommitRequest, StreamError> {
    Ok(CommitRequest {
        conditions: vec![CommitCondition::Head {
            path: path.clone(),
            expected,
            commit_id,
        }],
        mutations: vec![CommitMutation::Append {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"next")],
        }],
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(key))?,
    })
}

async fn seed<P: StreamProvider>(provider: &P, path: &StreamPath) -> Result<CommitId, StreamError> {
    let result = provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: Some(0),
            idempotency_key: None,
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
    assert_eq!(
        provider.commit(changed).await,
        Err(StreamError::IdempotencyMismatch)
    );
    let correct = request(&path, 1, original, b"correct")?;
    let result = provider.commit(correct.clone()).await?;
    assert!(matches!(result, CommitOutcome::Committed(_)));
    assert_eq!(provider.commit(correct).await?, result);
    assert_eq!(provider.tail(path).await?, 2);
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
            source,
            destination: path.clone(),
            at_tail: Some(1),
            idempotency_key: None,
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
    assert_eq!(provider.commit(wrong).await?, rejected);
    assert_eq!(provider.commit(correct).await?, accepted);
    let mut records = provider
        .read(ReadRequest {
            path,
            from: 0,
            limit: 2,
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
