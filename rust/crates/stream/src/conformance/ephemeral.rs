use super::*;

/// A leased subtree, and every path a commit into it creates and so adopts,
/// is written and observed like any other while held, is never listed,
/// inherited or named by a durable write, and is gone once its lease is
/// released. A provider that cannot hold one reports `Unsupported`.
pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), Failure> {
    let parent = path("ephemeral")?;
    let source = path("ephemeral/source")?;
    let root = path("ephemeral/lease")?;
    let forked = path("ephemeral/lease/fork")?;
    let adopted = path("ephemeral/changes")?;
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
    let (mut follows, mut receipts) = publish(provider, &source, &forked, &adopted).await?;
    let fork_key = key(b"ephemeral/plain-fork")?;
    let fork = provider
        .fork(ForkRequest {
            source: source.clone(),
            destination: path("ephemeral/lease/plain-fork")?,
            exclude: Vec::new(),
            at_tail: Some(1),
            idempotency_key: fork_key.clone(),
        })
        .await?;
    receipts.push((fork_key, fork.commit_id));
    for (key, id) in &receipts {
        receipt(provider, key.clone(), *id).await?;
    }
    // Retry identities are account-scoped, not silently scoped to a lease.
    for (target, retry) in [
        (&source, receipts[0].0.clone()),
        (&forked, key(b"ephemeral/source")?),
    ] {
        if provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"key-collision").into()],
                if_tail: None,
                idempotency_key: retry,
            })
            .await
            .err()
            != Some(StreamError::IdempotencyMismatch)
        {
            return Err(violated!());
        }
    }
    refusals(provider, &source, &forked, &adopted).await?;
    isolation(provider, &parent, &source).await?;
    // Release removes the subtree and every adopted path; followers of both
    // observe the loss.
    lease.settle().await?;
    for (key, id) in receipts {
        if provider.inspect_idempotency(key).await?.is_some()
            || provider.read_commit(id).await.err() != Some(StreamError::NotFound)
        {
            return Err(violated!());
        }
    }
    // Negative control: ending a lease never retires durable recovery facts.
    if provider
        .inspect_idempotency(key(b"ephemeral/source")?)
        .await?
        .is_none()
    {
        return Err(violated!());
    }
    for (follow, gone) in follows.iter_mut().zip([&forked, &adopted]) {
        if !matches!(follow.next().await, Some(Err(StreamError::NotFound)))
            || provider.tail(gone.clone()).await != Err(StreamError::NotFound)
        {
            return Err(violated!());
        }
    }
    provider.lease_ephemeral(root).await?.settle().await?;
    Ok(())
}

/// The consumer's shape: one conditioned, idempotent commit forks a durable
/// source into the lease and creates `adopted` outside it, and later appends
/// land in both. Followers see the inherited and new records in order; the
/// returned ones, of `forked` then `adopted`, are still live.
async fn publish<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    forked: &StreamPath,
    adopted: &StreamPath,
) -> Result<
    (
        [crate::RecordStream; 2],
        Vec<(IdempotencyKey, crate::CommitId)>,
    ),
    Failure,
> {
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
            vec![
                CommitMutation::Fork {
                    exclude: Vec::new(),
                    source: source.clone(),
                    destination: forked.clone(),
                    at_tail: 1,
                    records: vec![Bytes::from_static(b"forked").into()],
                },
                CommitMutation::Append {
                    path: adopted.clone(),
                    records: vec![Bytes::from_static(b"adopted").into()],
                },
            ],
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
    let mut receipts = vec![(key(b"ephemeral/fork")?, first.commit_id)];
    let mut follows = [
        crate::records_only(
            provider
                .follow(crate::FollowRequest::new(forked.clone(), 0))
                .await?,
        ),
        crate::records_only(
            provider
                .follow(crate::FollowRequest::new(adopted.clone(), 0))
                .await?,
        ),
    ];
    for (target, tail, retry) in [
        (forked, 2, b"ephemeral/append".as_slice()),
        (adopted, 1, b"ephemeral/append-adopted".as_slice()),
    ] {
        let AppendOutcome::Committed(appended) = provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"appended").into()],
                if_tail: Some(tail),
                idempotency_key: key(retry)?,
            })
            .await?
        else {
            return Err(violated!());
        };
        if appended.tail != tail + 1 {
            return Err(violated!());
        }
        receipts.push((key(retry)?, appended.commit_id));
    }
    for (follow, records) in follows.iter_mut().zip([
        [b"seed".as_slice(), b"forked", b"appended"].as_slice(),
        [b"adopted".as_slice(), b"appended"].as_slice(),
    ]) {
        for expected in records {
            let Some(Ok(record)) = follow.next().await else {
                return Err(violated!());
            };
            if record.value.as_ref() != *expected {
                return Err(violated!());
            }
        }
    }
    Ok((follows, receipts))
}

async fn receipt<P: StreamProvider + ?Sized>(
    provider: &P,
    key: IdempotencyKey,
    id: crate::CommitId,
) -> Result<(), Failure> {
    let Some(observed) = provider.inspect_idempotency(key.clone()).await? else {
        return Err(violated!());
    };
    let actual = match &observed.outcome {
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) => receipt.commit_id,
        IdempotencyOutcome::Fork(receipt) => receipt.commit_id,
        IdempotencyOutcome::Commit(envelope) => envelope.commit_id,
        _ => return Err(violated!()),
    };
    let envelope = provider.read_commit(id).await?;
    if observed.idempotency_key != key
        || actual != id
        || envelope.commit_id != id
        || envelope.mutations.is_empty()
    {
        return Err(violated!());
    }
    if let IdempotencyOutcome::Commit(original) = observed.outcome
        && original != envelope
    {
        return Err(violated!());
    }
    Ok(())
}

/// A durable write naming an owned path anywhere, or one mixing scopes or
/// leases, is refused; a fresh path needs a parent.
async fn refusals<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    forked: &StreamPath,
    adopted: &StreamPath,
) -> Result<(), Failure> {
    let other = provider.lease_ephemeral(path("ephemeral/other")?).await?;
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
        (
            Vec::new(),
            vec![append(source), append(adopted)],
            b"ephemeral/adopted-durable".as_slice(),
        ),
        (
            Vec::new(),
            vec![append(&path("ephemeral/other/x")?), append(adopted)],
            b"ephemeral/two-leases".as_slice(),
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
    let orphan = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![append(forked), append(&path("ephemeral/orphan/x")?)],
            )],
            idempotency_key: key(b"ephemeral/orphan")?,
        })
        .await;
    if orphan.err() != Some(StreamError::NotFound) {
        return Err(violated!());
    }
    other.settle().await?;
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
        || provider.tail(path("ephemeral-copy/changes")?).await != Err(StreamError::NotFound)
        || provider.tail(path("ephemeral-copy/source")?).await != Ok(1)
    {
        return Err(violated!());
    }
    Ok(())
}
