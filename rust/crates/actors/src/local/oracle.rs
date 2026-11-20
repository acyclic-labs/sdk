//! The composed orphan-free oracle, shared by the local and runtime test suites.

use super::*;
use acyclic_stream::StreamCategory;
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProvider};
use bytes::Bytes;
use futures::TryStreamExt as _;

/// The composed orphan-free oracle. Every scheduling entry (an activation or a
/// deadline-window record) and every participant custody intent names its
/// owner by itself, and that owner is a live actor; each intent's publication
/// is reachable by its own key. Resolving an owner never scans the state
/// root. Returns the orphaned entries.
pub(super) async fn orphans<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    custodians: &[ActorReference],
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let provider = store.provider();
    let mut scheduled = Vec::new();
    for shard in 0..store.scheduling_shards() {
        let log = store.scheduling_path(shard)?;
        scheduled.push(log.clone());
        let mut after = None;
        loop {
            let page = match provider
                .children_page(acyclic_stream::ChildrenPageRequest {
                    offset: 0,
                    tail_stream: None,
                    parent: Some(log.clone()),
                    after,
                    hierarchy_version: None,
                    limit: 64,
                })
                .await
            {
                Ok(page) => page,
                Err(error) if error.category() == StreamCategory::NotFound => break,
                Err(error) => return Err(error.into()),
            };
            scheduled.extend(page.children.into_iter().map(|child| child.path));
            after = page.next_after;
            if after.is_none() {
                break;
            }
        }
    }
    let mut orphans = Vec::new();
    for path in scheduled {
        for (sequence, value) in values(&**provider, &path).await?.into_iter().enumerate() {
            let live = match ActorReference::decode(&value) {
                Ok(actor) => store.load(&actor).await?.state().is_some(),
                Err(_) => false,
            };
            if !live {
                orphans.push(format!("{path}@{sequence}"));
            }
        }
    }
    for actor in custodians {
        let path = custody_path(&store.path(actor)?)?;
        let live = store.load(actor).await?.state().is_some();
        for (sequence, value) in values(&**provider, &path).await?.into_iter().enumerate() {
            let applied = matches!(
                provider
                    .inspect_idempotency(record::Intent::decode(&value)?.key)
                    .await?
                    .map(|inspection| inspection.outcome),
                Some(acyclic_stream::IdempotencyOutcome::Commit(_))
            );
            if !(live && applied) {
                orphans.push(format!("{path}@{sequence}"));
            }
        }
    }
    Ok(orphans)
}

async fn values<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &StreamPath,
) -> Result<Vec<Bytes>, Box<dyn std::error::Error>> {
    let tail = match provider.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    Ok(provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: u32::try_from(tail)?.max(1),
            at_least: Default::default(),
        })
        .await?
        .map_ok(|record| Bytes::copy_from_slice(record.value.as_ref()))
        .try_collect()
        .await?)
}
