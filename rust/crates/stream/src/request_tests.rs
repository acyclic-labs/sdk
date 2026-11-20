pub use acyclic_stream_contract::request::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OwnedBytes as Bytes;
    use crate::*;
    use crate::{IdempotencyKey, IdempotencyObservation, MemoryStream, StreamPath, StreamProvider};

    #[test]
    fn cross_effect_forks_validate_segment_ranges_among_empty_guards() -> Result<(), StreamError> {
        let append = |name: &str| -> Result<CommitMutation, StreamError> {
            Ok(CommitMutation::Append {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"record").into()],
            })
        };
        let parent = StreamPath::new("a")?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"fork-segment-range"))?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: parent.clone(),
                }],
                vec![CommitMutation::Fork {
                    source: StreamPath::new("source")?,
                    destination: parent,
                    at_tail: 0,
                    records: Vec::new(),
                    exclude: vec![crate::RelativePath::new("child")?],
                }],
            )],
        };
        // Guard-only effects retain request order without contributing paths.
        for _ in 0..257 {
            request.effects.push(CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: StreamPath::new("guard")?,
                }],
                Vec::new(),
            ));
        }
        request.effects.push(CommitEffect::hard(
            Vec::new(),
            vec![append("a.")?, append("a/child")?],
        ));
        validate_commit(&mut request)?;
        request
            .effects
            .last_mut()
            .ok_or(StreamError::InvalidArgument)?
            .mutations
            .push(append("a/child/deeper")?);
        assert_eq!(
            validate_commit(&mut request).map_err(|error| error.category()),
            Err(crate::StreamCategory::InvalidArgument)
        );
        request
            .effects
            .last_mut()
            .ok_or(StreamError::InvalidArgument)?
            .mutations
            .pop();
        request.effects.push(CommitEffect {
            soft: true,
            conditions: Vec::new(),
            mutations: vec![append("a/child")?],
        });
        assert_eq!(
            validate_commit(&mut request).map_err(|error| error.category()),
            Err(crate::StreamCategory::InvalidArgument)
        );
        Ok(())
    }

    #[tokio::test]
    async fn recovery_binding_refuses_changed_facts_and_unfunded_hash_work()
    -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let path = StreamPath::new("binding/events")?;
        let mut request = CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"body-binding"))?,
            effects: vec![CommitEffect::hard(
                vec![CommitCondition::Absent { path: path.clone() }],
                vec![CommitMutation::Append {
                    path,
                    records: vec![Bytes::from_static(b"original").into()],
                }],
            )],
        };
        let binding = commit_binding_with(&mut request, |_| Ok::<(), StreamError>(()))??;
        let crate::CommitOutcome::Committed(receipt) = provider.commit(request).await? else {
            return Err(StreamError::Unavailable);
        };
        let mut facts = provider.read_commit(receipt.commit_id).await?;
        let mut charged = 0;
        assert!(binding.matches_with(&facts, |bytes| {
            charged = bytes;
            Ok::<(), StreamError>(())
        })??);
        assert!(charged > 0);
        assert_eq!(
            binding
                .matches_with(&facts, |_| Err::<(), _>(StreamError::LimitExceeded))
                .map_err(|error| error.category())
                .map(|result| result.map_err(|error| error.category())),
            Err(crate::StreamCategory::LimitExceeded)
        );
        let crate::CommittedMutation::Append(append) = &mut facts.mutations[0] else {
            return Err(StreamError::InvalidArgument);
        };
        // Keep the selected range, identity, time and sequence valid. The actual
        // immutable body must still be joined to the original request digest.
        append.records[0].value = Bytes::from_static(b"corrupted");
        assert!(!binding.matches_with(&facts, |_| Ok::<(), StreamError>(()))??);
        Ok(())
    }

    #[tokio::test]
    async fn provider_digest_matches_retained_observation() -> Result<(), StreamError> {
        let key = IdempotencyKey::new(Bytes::from_static(b"provider-digest"))?;
        let request = AppendRequest {
            path: StreamPath::new("digest/events")?,
            records: vec![Bytes::from_static(b"record").into()],
            if_tail: Some(0),
            idempotency_key: key.clone(),
        };
        let digest = append_digest(&request)?;
        let provider = MemoryStream::default();
        provider.append(request).await?;
        let Some(IdempotencyObservation { request_digest, .. }) =
            provider.inspect_idempotency(key).await?
        else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(digest, request_digest);
        Ok(())
    }

    #[test]
    fn effect_boundaries_and_soft_flags_are_part_of_the_digest() -> Result<(), StreamError> {
        let append = |name: &str| -> Result<CommitMutation, StreamError> {
            Ok(CommitMutation::Append {
                path: StreamPath::new(name)?,
                records: vec![Bytes::from_static(b"x").into()],
            })
        };
        let present = CommitCondition::Present {
            path: StreamPath::new("to")?,
        };
        let request = |effects| -> Result<CommitRequest, StreamError> {
            Ok(CommitRequest {
                deadline_unix_millis: None,
                effects,
                idempotency_key: IdempotencyKey::new(Bytes::from_static(b"grouping"))?,
            })
        };
        let together = request(vec![CommitEffect::hard(
            vec![present.clone()],
            vec![append("a")?, append("b")?],
        )])?;
        let split = request(vec![
            CommitEffect::hard(vec![present], vec![append("a")?]),
            CommitEffect::hard(Vec::new(), vec![append("b")?]),
        ])?;
        let mut soft = together.clone();
        soft.effects[0].soft = true;
        let digests = [together, split, soft]
            .map(|mut request| commit_digest(&mut request))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert!(digests[0] != digests[1] && digests[0] != digests[2] && digests[1] != digests[2]);
        Ok(())
    }
}
