//! Own-child projection shares page membership, snapshot and fork provenance.
use super::*;

pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), Failure> {
    let parent = path("conformance/own-page-tails")?;
    let rows = [
        path("conformance/own-page-tails/a")?,
        path("conformance/own-page-tails/b")?,
    ];
    for target in &rows {
        provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![Bytes::from_static(b"old").into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(target.as_str().as_bytes().to_vec())?,
            })
            .await?;
    }
    let request = ChildrenPageRequest {
        parent: Some(parent.clone()),
        after: None,
        hierarchy_version: None,
        limit: 1,
        tail_stream: Some(crate::ChildTail::Child),
    };
    let first = provider.children_page(request.clone()).await?;
    first.validate_for(&request)?;
    if first.children.len() != 1 || first.next_after != Some(rows[0].clone()) {
        return Err(violated!());
    }
    let cut = provider
        .commit(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key(b"own-page-tails-next")?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                rows.iter()
                    .map(|path| CommitMutation::Append {
                        path: path.clone(),
                        records: vec![Bytes::from_static(b"new").into()],
                    })
                    .collect(),
            )],
        })
        .await?;
    let CommitOutcome::Committed(cut) = cut else {
        return Err(violated!());
    };
    let original = first.children[0].tail_record.as_ref().ok_or(violated!())?;
    if first.children[0].tail != Some(1)
        || original.value.as_ref() != b"old"
        || original.origin != rows[0]
    {
        return Err(violated!());
    }
    let next = provider
        .children_page(ChildrenPageRequest {
            after: first.next_after,
            hierarchy_version: Some(first.hierarchy_version),
            ..request.clone()
        })
        .await?;
    if next.children.len() != 1 || next.children[0].path != rows[1] || next.next_after.is_some() {
        return Err(violated!());
    }
    verify_view(provider, &rows, request, cut.commit_id).await?;
    verify_fork(provider, parent, rows, cut.commit_id).await
}

async fn verify_view<P: StreamProvider + ?Sized>(
    provider: &P,
    rows: &[StreamPath; 2],
    request: ChildrenPageRequest,
    commit: crate::CommitId,
) -> Result<(), Failure> {
    let page = provider
        .heads(crate::HeadsRequest {
            paths: [rows[0].clone()].into_iter().collect(),
            retained: Default::default(),
            durable_through: None,
            projection: crate::HeadProjection::History,
            children: Some(ChildrenPageRequest {
                limit: 2,
                ..request.clone()
            }),
            limit: 3,
            at_least: Default::default(),
        })
        .await?;
    if page.heads[0]
        .as_ref()
        .and_then(|head| head.last.as_ref())
        .map(|record| record.commit_id)
        != Some(commit)
    {
        return Err(violated!());
    }
    let page = page.children.ok_or(violated!())?;
    if page.children.len() != 2
        || page.children.iter().any(|child| {
            child.tail != Some(2)
                || child.tail_record.as_ref().is_none_or(|record| {
                    record.commit_id != commit
                        || record.value.as_ref() != b"new"
                        || record.origin != child.path
                })
        })
    {
        return Err(violated!());
    }
    // Without an explicit selector, the same child page discloses no tail.
    let unprojected = provider
        .children_page(ChildrenPageRequest {
            tail_stream: None,
            limit: 2,
            ..request.clone()
        })
        .await?;
    if unprojected
        .children
        .iter()
        .any(|child| child.tail.is_some() || child.tail_record.is_some())
    {
        return Err(violated!());
    }
    Ok(())
}

async fn verify_fork<P: StreamProvider + ?Sized>(
    provider: &P,
    parent: StreamPath,
    rows: [StreamPath; 2],
    commit: crate::CommitId,
) -> Result<(), Failure> {
    let fork = path("conformance/own-page-tail-fork")?;
    provider
        .fork(ForkRequest {
            source: parent,
            destination: fork.clone(),
            at_tail: None,
            exclude: Vec::new(),
            idempotency_key: key(b"own-page-tail-fork")?,
        })
        .await?;
    let page = provider
        .children_page(ChildrenPageRequest {
            parent: Some(fork),
            after: None,
            hierarchy_version: None,
            limit: 2,
            tail_stream: Some(crate::ChildTail::Child),
        })
        .await?;
    if page.children.len() != 2
        || page.children.iter().zip(rows).any(|(child, original)| {
            child
                .tail_record
                .as_ref()
                .is_none_or(|record| record.origin != original || record.commit_id != commit)
        })
    {
        return Err(violated!());
    }
    Ok(())
}
