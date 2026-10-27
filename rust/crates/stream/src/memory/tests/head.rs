use super::super::*;

#[tokio::test]
async fn present_send_requires_the_precommit_path_and_preserves_retry_and_close_fences()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let inbox = StreamPath::new("mail/inbox")?;
    let closed = StreamPath::new("mail/inbox/closed")?;
    let request = CommitRequest {
        conditions: vec![
            CommitCondition::Present { path: inbox.clone() },
            CommitCondition::Absent { path: closed.clone() },
        ],
        mutations: vec![CommitMutation::Append {
            path: inbox.clone(),
            records: vec![Bytes::from_static(b"message").into()],
        }],
        idempotency_key: IdempotencyKey::new(b"send".as_slice())?,
    };
    assert_eq!(provider.commit(request.clone()).await?, CommitOutcome::Conflict(vec![
        CommitConflict::Missing { path: inbox.clone() },
    ]));
    assert!(provider.inspect_idempotency(request.idempotency_key.clone()).await?.is_none());
    assert_eq!(provider.tail(inbox.clone()).await, Err(StreamError::NotFound));

    append(&provider, "seed", "seed").await?;
    provider.fork(ForkRequest {
        source: StreamPath::new("seed")?,
        destination: inbox.clone(),
        at_tail: Some(0),
        exclude: Vec::new(),
        idempotency_key: Some(IdempotencyKey::new(b"open".as_slice())?),
    }).await?;
    assert_eq!(provider.tail(inbox.clone()).await?, 0);
    let sent = provider.commit(request.clone()).await?;
    assert!(matches!(sent, CommitOutcome::Committed(_)));
    append(&provider, closed.as_str(), "close").await?;
    assert_eq!(provider.commit(request.clone()).await?, sent);

    let next = CommitRequest { idempotency_key: IdempotencyKey::new(b"next".as_slice())?, ..request };
    assert_eq!(provider.commit(next).await?, CommitOutcome::Conflict(vec![
        CommitConflict::Exists { path: closed },
    ]));
    assert_eq!(provider.tail(inbox).await?, 1);
    Ok(())
}

async fn append(provider: &MemoryStream, path: &str, key: &str) -> Result<(), StreamError> {
    let outcome = provider.append(AppendRequest {
        path: StreamPath::new(path)?,
        records: vec![Bytes::from_static(b"record").into()],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new(key.as_bytes().to_vec())?),
    }).await?;
    assert!(matches!(outcome, AppendOutcome::Committed(_)));
    Ok(())
}

#[tokio::test]
async fn head_preserves_each_inherited_cut_through_nested_forks_and_reopen() -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    append(&provider, "tree", "root").await?;
    append(&provider, "tree/child", "child").await?;
    append(&provider, "tree/excluded/leaf", "excluded").await?;
    let first = provider.fork(ForkRequest {
        source: StreamPath::new("tree")?,
        destination: StreamPath::new("first")?,
        at_tail: Some(0),
        exclude: vec![StreamPath::new("excluded")?],
        idempotency_key: Some(IdempotencyKey::new(b"first".to_vec())?),
    }).await?;
    append(&provider, "tree/child", "source-later").await?;
    append(&provider, "first/child", "private-later").await?;
    append(&provider, "first/new", "new").await?;
    append(&provider, "first/excluded/leaf", "recreated").await?;
    let root = provider.head(StreamPath::new("first")?).await?;
    assert_eq!(root.tail, 0);
    assert_eq!(root.last, None);
    assert_eq!(root.origin, Some(crate::ForkOrigin {
        source: StreamPath::new("tree")?, forked_at: 0, commit_id: first.commit_id,
    }));
    let child = provider.head(StreamPath::new("first/child")?).await?;
    assert_eq!(child.tail, 2);
    assert_eq!(child.last.as_ref().map(|record| record.sequence), Some(1));
    assert_eq!(child.origin, Some(crate::ForkOrigin {
        source: StreamPath::new("tree/child")?, forked_at: 1, commit_id: first.commit_id,
    }));
    for path in ["first/new", "first/excluded/leaf"] {
        assert_eq!(provider.head(StreamPath::new(path)?).await?.origin, None);
    }
    let nested = provider.fork(ForkRequest {
        source: StreamPath::new("tree/child")?,
        destination: StreamPath::new("first/nested")?,
        at_tail: None,
        exclude: Vec::new(),
        idempotency_key: Some(IdempotencyKey::new(b"nested".to_vec())?),
    }).await?;
    assert_eq!(provider.head(StreamPath::new("first/nested")?).await?.origin, Some(crate::ForkOrigin {
        source: StreamPath::new("tree/child")?, forked_at: 2, commit_id: nested.commit_id,
    }));
    let second = provider.fork(ForkRequest {
        source: StreamPath::new("first")?,
        destination: StreamPath::new("second")?,
        at_tail: Some(0),
        exclude: Vec::new(),
        idempotency_key: Some(IdempotencyKey::new(b"second".to_vec())?),
    }).await?;
    for (path, source, cut) in [
        ("second/child", "first/child", 2),
        ("second/nested", "first/nested", 2),
        ("second/new", "first/new", 1),
        ("second/excluded/leaf", "first/excluded/leaf", 1),
    ] {
        assert_eq!(provider.head(StreamPath::new(path)?).await?.origin, Some(crate::ForkOrigin {
            source: StreamPath::new(source)?, forked_at: cut, commit_id: second.commit_id,
        }));
    }
    let page = provider.children_page(crate::ChildrenPageRequest {
        parent: Some(StreamPath::new("second")?),
        tail_stream: None,
        after: None,
        hierarchy_version: None,
        limit: 32,
    }).await?;
    for child in page.children {
        assert_eq!(child.origin, provider.head(child.path).await?.origin);
    }
    let reopened = MemoryStream::default();
    reopened.install_state(&provider.encode_state().await).await?;
    for path in ["first", "first/child", "first/new", "first/excluded/leaf", "first/nested", "second/child", "second/nested"] {
        assert_eq!(reopened.head(StreamPath::new(path)?).await?, provider.head(StreamPath::new(path)?).await?);
    }
    Ok(())
}
