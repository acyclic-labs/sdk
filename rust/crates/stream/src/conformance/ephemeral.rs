use super::*;

/// A leased subtree is written and observed like any other while held, is
/// never listed, inherited or named by a durable write, and is gone once its
/// lease is released. A provider that cannot hold one reports `Unsupported`.
pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), Failure> {
    let parent = path("ephemeral")?;
    let source = path("ephemeral/source")?;
    let root = path("ephemeral/lease")?;
    let forked = path("ephemeral/lease/fork")?;
    match provider.lease_ephemeral(root.clone()).await {
        Err(StreamError::Unsupported) => return Ok(()),
        // A lease never creates its durable ancestors.
        Err(StreamError::NotFound) => {}
        _ => return Err(violated!()),
    }
    for (target, retry) in [
        (&parent, b"ephemeral/parent".as_slice()),
        (&source, b"ephemeral/source".as_slice()),
    ] {
        provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"seed").into()],
                if_tail: Some(0),
                idempotency_key: key(retry)?,
            })
            .await?;
    }
    let lease = provider.lease_ephemeral(root.clone()).await?;
    if lease.root() != &root
        || provider.lease_ephemeral(root.clone()).await.err() != Some(StreamError::AlreadyExists)
    {
        return Err(violated!());
    }
    let mut follow = publish(provider, &source, &forked).await?;
    refusals(provider, &source, &forked).await?;
    isolation(provider, &parent, &source).await?;
    // Release removes the whole subtree; its follower observes the loss.
    lease.settle().await?;
    if !matches!(follow.next().await, Some(Err(StreamError::NotFound)))
        || provider.tail(forked).await != Err(StreamError::NotFound)
    {
        return Err(violated!());
    }
    provider.lease_ephemeral(root).await?.settle().await?;
    Ok(())
}

/// The consumer's shape: one conditioned, idempotent commit forks a durable
/// source into the lease, and later appends land beneath it. A follower sees
/// the inherited and new records in order; the returned one is still live.
async fn publish<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    forked: &StreamPath,
) -> Result<crate::RecordStream, Failure> {
    let fork = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                CommitCondition::Absent {
                    path: forked.clone(),
                },
            ],
            vec![CommitMutation::Fork {
                exclude: Vec::new(),
                source: source.clone(),
                destination: forked.clone(),
                at_tail: 1,
                records: vec![Bytes::from_static(b"forked").into()],
            }],
        )],
        idempotency_key: key(b"ephemeral/fork")?,
    };
    let CommitOutcome::Committed(first) = provider.commit(fork.clone()).await? else {
        return Err(violated!());
    };
    let CommitOutcome::Committed(retried) = provider.commit(fork).await? else {
        return Err(violated!());
    };
    if retried.commit_id != first.commit_id {
        return Err(violated!());
    }
    let mut follow = crate::records_only(
        provider
            .follow(crate::FollowRequest::new(forked.clone(), 0))
            .await?,
    );
    let AppendOutcome::Committed(appended) = provider
        .append(AppendRequest {
            path: forked.clone(),
            records: vec![Bytes::from_static(b"appended").into()],
            if_tail: Some(2),
            idempotency_key: key(b"ephemeral/append")?,
        })
        .await?
    else {
        return Err(violated!());
    };
    if appended.tail != 3 {
        return Err(violated!());
    }
    for expected in [b"seed".as_slice(), b"forked", b"appended"] {
        let Some(Ok(record)) = follow.next().await else {
            return Err(violated!());
        };
        if record.value.as_ref() != expected {
            return Err(violated!());
        }
    }
    Ok(follow)
}

/// A durable write naming the subtree anywhere, or mixing scopes, is refused.
async fn refusals<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    forked: &StreamPath,
) -> Result<(), Failure> {
    let append = |target: &StreamPath| CommitMutation::Append {
        path: target.clone(),
        records: vec![Bytes::from_static(b"refused").into()],
    };
    let conditioned = vec![CommitCondition::Tail {
        path: forked.clone(),
        expected: 3,
    }];
    for (conditions, mutations, retry) in [
        (
            conditioned,
            vec![append(source)],
            b"ephemeral/conditioned".as_slice(),
        ),
        (
            Vec::new(),
            vec![append(source), append(forked)],
            b"ephemeral/mixed".as_slice(),
        ),
    ] {
        let refused = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![CommitEffect::hard(conditions, mutations)],
                idempotency_key: key(retry)?,
            })
            .await;
        if refused.err() != Some(StreamError::InvalidArgument) {
            return Err(violated!());
        }
    }
    let escaped = provider
        .fork(ForkRequest {
            exclude: Vec::new(),
            source: forked.clone(),
            destination: path("ephemeral/escaped")?,
            at_tail: None,
            idempotency_key: key(b"ephemeral/escaped")?,
        })
        .await;
    if escaped.err() != Some(StreamError::InvalidArgument) {
        return Err(violated!());
    }
    Ok(())
}

/// Neither listings nor durable forks of an ancestor reach the subtree.
async fn isolation<P: StreamProvider + ?Sized>(
    provider: &P,
    parent: &StreamPath,
    source: &StreamPath,
) -> Result<(), Failure> {
    let page = provider
        .children_page(ChildrenPageRequest {
            tail_stream: None,
            parent: Some(parent.clone()),
            after: None,
            hierarchy_version: None,
            limit: 16,
        })
        .await?;
    if page.children.iter().map(|child| &child.path).ne([source]) {
        return Err(violated!());
    }
    provider
        .fork(ForkRequest {
            exclude: Vec::new(),
            source: parent.clone(),
            destination: path("ephemeral-copy")?,
            at_tail: None,
            idempotency_key: key(b"ephemeral/copy")?,
        })
        .await?;
    if provider.tail(path("ephemeral-copy/lease/fork")?).await != Err(StreamError::NotFound)
        || provider.tail(path("ephemeral-copy/source")?).await != Ok(1)
    {
        return Err(violated!());
    }
    Ok(())
}
