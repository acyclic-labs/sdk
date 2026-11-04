use super::*;

#[tokio::test]
async fn stream_window_loads_one_head_and_stale_cas_never_changes_its_lease()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::testing::{Counted, RequestClass};
    let provider = Arc::new(Counted::new(acyclic_stream::MemoryStream::default()));
    let store = StreamOperationWindowStore::new(provider.clone());
    let workspace = WorkspaceId::from_bytes([151; 16]);
    let before = provider.requests();
    assert!(store.load(workspace).await?.is_none());
    let calls = provider.requests().since(before);
    assert_eq!(calls.class(RequestClass::Head), 1);
    assert_eq!(calls.data(), 0);
    let windows = OperationWindowCoordinator::new(store.clone());
    let lease = windows
        .begin_with_lease_id(
            workspace,
            GenerationId::new(crate::Digest::from_bytes([152; 32])),
            "tool",
            10,
            20,
            OperationLeaseId::from_bytes([153; 16]),
        )
        .await?;
    let before = provider.requests();
    let current = store
        .load(workspace)
        .await?
        .ok_or("missing active window")?;
    let calls = provider.requests().since(before);
    assert_eq!(calls.class(RequestClass::Head), 1);
    assert_eq!(calls.data(), 0);
    let (_, condition) = store.observe(workspace).await?;
    let acyclic_stream::CommitCondition::Head {
        path,
        expected,
        commit_id,
    } = condition
    else {
        return Err("active window did not carry its exact history identity".into());
    };
    let head = provider.inner().head(path).await?;
    assert_eq!(expected, current.revision);
    assert_eq!(commit_id, head.last.ok_or("missing window head")?.commit_id);
    let mut stale = current.clone();
    stale.revision += 1;
    stale.phase = OperationWindowPhase::Idle;
    let renewed = windows.renew(&lease, 11, 30).await?;
    let before = provider.requests();
    assert!(
        !store
            .compare_and_swap(workspace, current.revision, stale)
            .await?
    );
    let calls = provider.requests().since(before);
    assert_eq!(calls.class(RequestClass::Head), 1);
    assert_eq!(calls.data(), 0);
    let observed = store
        .load(workspace)
        .await?
        .ok_or("missing renewed window")?;
    assert!(
        matches!(observed.phase, OperationWindowPhase::Active { leases, .. }
        if leases.get(&renewed.lease_id).is_some_and(|lease| lease.expires_at_millis == 30))
    );
    Ok(())
}
