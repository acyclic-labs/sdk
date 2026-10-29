use super::*;
use crate::StreamProviderExt as _;

pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let paths = [path("coherent/a/state")?, path("coherent/b/state")?];
    let mut previous = None;
    let mut retained_previous = None;
    for generation in 0..2 {
        let outcome = provider
            .commit(CommitRequest {
                conditions: Vec::new(),
                mutations: paths
                    .iter()
                    .enumerate()
                    .map(|(index, path)| CommitMutation::Append {
                        path: path.clone(),
                        records: vec![vec![generation, index as u8].into()],
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
            retained: [
                crate::RetainedRecord {
                    commit_id: receipt.commit_id,
                    path: paths[0].clone(),
                    sequence: generation as u64,
                },
                crate::RetainedRecord {
                    commit_id: receipt.commit_id,
                    path: paths[1].clone(),
                    sequence: generation as u64,
                },
                crate::RetainedRecord {
                    commit_id: receipt.commit_id,
                    path: paths[0].clone(),
                    sequence: u64::MAX,
                },
                crate::RetainedRecord {
                    commit_id: receipt.commit_id,
                    path: path("coherent/absent")?,
                    sequence: 0,
                },
            ]
            .into_iter()
            .collect(),
            durable_through: None,
            projection: Default::default(),
            paths: [paths[0].clone(), paths[1].clone(), path("coherent/absent")?]
                .into_iter()
                .collect(),
            children: Some(ChildrenPageRequest {
                parent: Some(path("coherent")?),
                tail_stream: Some(path("state")?),
                after: None,
                hierarchy_version: None,
                limit: 8,
            }),
            limit: 9,
            at_least: crate::RevisionCut::of(receipt.revision.clone()),
        };
        let view = provider.heads(request.clone()).await?;
        view.validate_for(&request)?;
        if view.snapshot != receipt.revision || view.heads[2].is_some() {
            return Err(StreamError::Unavailable);
        }
        if view.retained[0].record != view.heads[0].as_ref().and_then(|head| head.last.clone())
            || view.retained[1].record != view.heads[1].as_ref().and_then(|head| head.last.clone())
            || view.retained[2].record.is_some()
            || view.retained[3].record.is_some()
        {
            return Err(StreamError::Unavailable);
        }
        // Original co-commit bytes and time remain exact after both roots advance.
        if let Some((old_request, old_records)) = retained_previous.take() {
            let original = provider.heads(old_request).await?;
            if original.snapshot != view.snapshot || original.retained != old_records {
                return Err(StreamError::Unavailable);
            }
        } else {
            retained_previous = Some((request.clone(), view.retained.clone()));
        }
        let mut too_small = request.clone();
        too_small.limit = 6;
        if provider.heads(too_small).await != Err(StreamError::InvalidArgument) {
            return Err(StreamError::Unavailable);
        }
        let mut wrong_position = view.clone();
        // Both records have identical publication/sequence and different bytes.
        // Swapping their observations must fail solely on destination binding.
        wrong_position.retained.swap(0, 1);
        if wrong_position.validate_for(&request) != Err(StreamError::InvalidResponse) {
            return Err(StreamError::Unavailable);
        }
        // Missing results retain the same destination binding.
        let mut wrong_absence = view.clone();
        wrong_absence.retained.swap(2, 3);
        if wrong_absence.validate_for(&request) != Err(StreamError::InvalidResponse) {
            return Err(StreamError::Unavailable);
        }
        for (index, head) in view.heads[..2].iter().enumerate() {
            let record = head
                .as_ref()
                .and_then(|head| head.last.as_ref())
                .ok_or(StreamError::Unavailable)?;
            if record.value.as_ref() != [generation, index as u8]
                || record.commit_id != receipt.commit_id
                || record.committed_at_micros != receipt.committed_at_micros
            {
                return Err(StreamError::Unavailable);
            }
        }
        for (projection, through) in [
            (crate::HeadProjection::Watermarks, None),
            (crate::HeadProjection::Tail, None),
            (
                crate::HeadProjection::Watermarks,
                Some(u64::from(generation) + 1),
            ),
        ] {
            let mut offsets = request.clone();
            offsets.projection = projection;
            offsets.durable_through = through;
            let projected = provider.heads(offsets.clone()).await?;
            projected.validate_for(&offsets)?;
            for observed in [&view, &projected] {
                if crate::wire_codec::requests::heads_len(observed)
                    != prost::Message::encoded_len(&crate::wire_codec::heads_to_wire(
                        observed.clone(),
                    )?)
                {
                    return Err(StreamError::Unavailable);
                }
            }
            if projected.snapshot != view.snapshot
                || projected
                    .heads
                    .iter()
                    .zip(&view.heads)
                    .any(|(offsets, full)| match (offsets, full) {
                        (None, None) => false,
                        (Some(offsets), Some(full)) => {
                            offsets.retained_through != through
                                || offsets.tail != full.tail
                                || (projection == crate::HeadProjection::Watermarks
                                    && offsets.durable < full.durable)
                                || (projection == crate::HeadProjection::Tail
                                    && offsets.durable != 0)
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
