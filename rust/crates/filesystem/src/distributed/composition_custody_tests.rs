use super::*;
use acyclic_stream::{IdempotencyKey, MemoryStream, OwnedBytes, StreamError};

#[tokio::test]
async fn native_request_transfer_keeps_original_context_and_live_commit_backing()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let baseline = root.live_allocation_bytes();
    let (original, stop) = root.invocation_view()?;
    let mut work = AuthorityWork::native(&original, &cancellation);
    let meter = work.native_meter()?;
    let path = StreamPath::new("agent/custody")?;
    let mut conditions = work.vec(1)?;
    conditions.push(CommitCondition::Absent { path: path.clone() });
    let mut commit = FilesystemCommit::new(CommitRequest {
        conditions,
        mutations: Vec::new(),
        idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-custody"))?,
        deadline_unix_millis: None,
    });
    let first_bytes = work.request_allocation.as_ref().unwrap().bytes();
    assert!(first_bytes > 0);
    let live = root.live_allocation_bytes();
    let receipt = root.receipt();
    commit.retain_request_custody(&mut work)?;
    assert_eq!(commit.allocation.as_ref().unwrap().bytes(), first_bytes);
    assert_eq!(work.request_allocation.as_ref().unwrap().bytes(), 0);
    assert!(Arc::ptr_eq(&meter, &work.native_meter()?));
    assert_eq!(root.live_allocation_bytes(), live);
    assert_eq!(root.receipt(), receipt);

    let foreign =
        crate::NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
    let resource = original.native_volume_work(crate::performance::BOUNDARY_BUDGET)?;
    let (invocation, _) = root.invocation_view()?;
    // Same ledger alone is insufficient: resource and invocation are original
    // request authority too. Every refusal keeps both live allocation owners.
    for context in [&foreign, &resource, &invocation] {
        let mut other = AuthorityWork::native(context, &cancellation);
        let bytes = other.vec::<u8>(17)?;
        let request = commit.request().clone();
        let before = context.live_allocation_bytes();
        let spent = context.receipt();
        assert!(matches!(
            commit.retain_request_custody(&mut other),
            Err(AuthorityStoreError::NativeOriginal(
                crate::NativeTreeMaterializationError::InvalidLayout
            ))
        ));
        assert_eq!(commit.allocation.as_ref().unwrap().bytes(), first_bytes);
        assert_eq!(other.request_allocation.as_ref().unwrap().bytes(), 17);
        assert_eq!(commit.request(), &request);
        assert_eq!(context.live_allocation_bytes(), before);
        assert_eq!(context.receipt(), spent);
        drop(bytes);
    }
    drop(invocation);
    drop(resource);
    drop(foreign);

    let provider = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(provider.clone());
    let observed = store.authorized_observation(CommitCondition::Absent { path: path.clone() })?;
    store.prepare_records(
        &mut commit,
        [(
            observed,
            super::super::state_record::StateRecord::Companion {
                kind: super::super::state_record::CompanionKind::OperationOutcome,
                payload: OwnedBytes::from_static(b"original-outcome"),
            },
            crate::PublicationHold::none(),
        )],
        &[],
        Some(u64::MAX),
        &mut work,
    )?;
    assert!(commit.allocation.as_ref().unwrap().bytes() > first_bytes);
    assert_eq!(work.request_allocation.as_ref().unwrap().bytes(), 0);
    assert!(Arc::ptr_eq(&meter, &work.native_meter()?));
    // Further original construction backing accumulates; it cannot replace
    // the request's existing live capacity, even after cancellation.
    let bytes = work.vec::<u8>(23)?;
    let retained = commit.allocation.as_ref().unwrap().bytes();
    stop.cancel();
    let receipt = root.receipt();
    commit.retain_request_custody(&mut work)?;
    assert_eq!(commit.allocation.as_ref().unwrap().bytes(), retained + 23);
    assert_eq!(work.request_allocation.as_ref().unwrap().bytes(), 0);
    assert_eq!(root.receipt(), receipt);
    drop(bytes);
    drop(meter);
    drop(work);

    let request = commit.request().clone();
    let live = root.live_allocation_bytes();
    let mut stopped = AuthorityWork::native(&original, &cancellation);
    let FilesystemCommitOutcome::Unknown { commit, .. } =
        store.submit(commit, &mut stopped).await?
    else {
        return Err("cancelled submission lost its original commit".into());
    };
    assert_eq!(commit.request(), &request);
    assert_eq!(commit.allocation.as_ref().unwrap().bytes(), retained + 23);
    assert_eq!(root.live_allocation_bytes(), live);
    assert_eq!(provider.tail(path).await, Err(StreamError::NotFound));
    drop(stopped);
    drop(request);
    drop(commit);
    // Cancellation retires its funded parent registration. Compare the root
    // before invocation admission with the root after every invocation owner.
    drop(original);
    drop(stop);
    assert_eq!(root.live_allocation_bytes(), baseline);
    Ok(())
}
