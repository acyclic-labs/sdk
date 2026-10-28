use super::*;
use crate::StreamProviderExt as _;

pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let paths = [path("coherent/a/state")?, path("coherent/b/state")?];
    let mut previous = None;
    for generation in 0..2 {
        let outcome = provider
            .commit(CommitRequest {
                conditions: Vec::new(),
                mutations: paths
                    .iter()
                    .map(|path| CommitMutation::Append {
                        path: path.clone(),
                        records: vec![vec![generation].into()],
                    })
                    .collect(),
                idempotency_key: IdempotencyKey::new(Bytes::from(vec![b'v', generation]))?,
                deadline_unix_millis: None,
            })
            .await?;
        let CommitOutcome::Committed(receipt) = outcome else {
            return Err(StreamError::Unavailable);
        };
        let request = crate::HeadsRequest {
            projection: Default::default(),
            paths: vec![paths[0].clone(), paths[1].clone(), path("coherent/absent")?],
            children: Some(ChildrenPageRequest {
                parent: Some(path("coherent")?),
                tail_stream: Some(path("state")?),
                after: None,
                hierarchy_version: None,
                limit: 8,
            }),
            limit: 5,
            at_least: crate::RevisionCut::of(receipt.revision.clone()),
        };
        let view = provider.heads(request.clone()).await?;
        view.validate_for(&request)?;
        if view.snapshot != receipt.revision || view.heads[2].is_some() {
            return Err(StreamError::Unavailable);
        }
        for head in &view.heads[..2] {
            let record = head
                .as_ref()
                .and_then(|head| head.last.as_ref())
                .ok_or(StreamError::Unavailable)?;
            if record.value.as_ref() != [generation] || record.commit_id != receipt.commit_id {
                return Err(StreamError::Unavailable);
            }
        }
        let mut offsets = request.clone();
        offsets.projection = crate::HeadProjection::Watermarks;
        let projected = provider.heads(offsets.clone()).await?;
        projected.validate_for(&offsets)?;
        if projected.snapshot != view.snapshot
            || projected
                .heads
                .iter()
                .zip(&view.heads)
                .any(|(offsets, full)| match (offsets, full) {
                    (None, None) => false,
                    (Some(offsets), Some(full)) => {
                        offsets.tail != full.tail
                            || offsets.durable < full.durable
                            || offsets.last.is_some()
                            || offsets.origin.is_some()
                    }
                    _ => true,
                })
        {
            return Err(StreamError::Unavailable);
        }
        // A response for another projection cannot stand in for this request.
        if projected.validate_for(&request) != Err(StreamError::InvalidResponse) {
            return Err(StreamError::Unavailable);
        }
        let children = view.children.ok_or(StreamError::Unavailable)?;
        if children.children.len() != 2
            || children.children.iter().any(|child| {
                child
                    .tail_record
                    .as_ref()
                    .is_none_or(|record| record.commit_id != receipt.commit_id)
            })
        {
            return Err(StreamError::Unavailable);
        }
        if generation == 0 {
            // Negative control: the next commit will advance both roots after
            // this observation. Keeping it must expose a mixed generation.
            previous = provider.head(paths[0].clone()).await?.last;
            continue;
        }
        let old = previous.take().ok_or(StreamError::Unavailable)?;
        let current = provider
            .head(paths[1].clone())
            .await?
            .last
            .ok_or(StreamError::Unavailable)?;
        if old.value == current.value || old.commit_id == current.commit_id {
            return Err(StreamError::Unavailable);
        }
    }
    Ok(())
}
