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
    use acyclic_stream::StreamProvider;
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

#[tokio::test]
async fn stream_window_corrupt_state_is_not_observed_as_absent()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::StreamProvider;
    for case in 0..4 {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let workspace = WorkspaceId::from_bytes([161; 16]);
        let snapshot = OperationWindowSnapshot {
            version: STATE_VERSION,
            revision: if case == 2 { 2 } else { 1 },
            workspace_id: if case == 1 {
                WorkspaceId::from_bytes([162; 16])
            } else {
                workspace
            },
            phase: OperationWindowPhase::Idle,
        };
        let payload = if case == 0 {
            b"invalid-json".to_vec()
        } else {
            serde_json::to_vec(&snapshot)?
        };
        let path = stream_window_path(workspace)?;
        // A real child append creates an empty implicit parent. Its existence
        // must refuse window activation just like malformed or foreign state.
        let path = if case == 3 {
            acyclic_stream::StreamPath::new(format!("{}/child", path.as_str()))?
        } else {
            path
        };
        provider
            .append(acyclic_stream::AppendRequest {
                path,
                records: vec![payload.into()],
                if_tail: Some(0),
                idempotency_key: acyclic_stream::IdempotencyKey::new(b"corrupt-window".to_vec())?,
            })
            .await?;
        let store = StreamOperationWindowStore::new(provider);
        assert!(matches!(
            store.load(workspace).await,
            Err(StreamOperationWindowStoreError::Corrupt(_))
        ));
    }
    Ok(())
}
