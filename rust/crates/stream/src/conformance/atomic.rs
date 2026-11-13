//! Provider-independent atomic publication and exact replay.
use crate::StreamProviderExt as _;
use crate::{StreamError, StreamPath, StreamProvider};

pub(crate) fn request() -> Result<crate::CommitRequest, StreamError> {
    let paths = (0..1_025)
        .map(|index| StreamPath::new(format!("conformance/atomic/member-{index:04}")))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(crate::CommitRequest {
        effects: vec![crate::CommitEffect::hard(
            paths
                .iter()
                .cloned()
                .map(|path| crate::CommitCondition::Absent { path })
                .collect(),
            paths
                .into_iter()
                .map(|path| crate::CommitMutation::Append {
                    path,
                    records: vec![crate::OwnedBytes::from_static(b"member").into()],
                })
                .collect(),
        )],
        idempotency_key: crate::IdempotencyKey::new(b"atomic-membership".as_slice())?,
        deadline_unix_millis: None,
    })
}

async fn heads(
    provider: &dyn StreamProvider,
    paths: &[StreamPath],
    tail: Option<u64>,
) -> Result<(), super::Failure> {
    // Observations are paged; the publication itself remains one operation.
    for page in paths.chunks(crate::MAX_ITEMS) {
        let observed = provider
            .heads(crate::HeadsRequest {
                retained: Default::default(),
                durable_through: None,
                projection: Default::default(),
                paths: page.iter().cloned().collect(),
                children: None,
                limit: u32::try_from(page.len()).map_err(|_| StreamError::Capacity)?,
                at_least: Default::default(),
            })
            .await?;
        if observed.heads.len() != page.len()
            || observed
                .heads
                .iter()
                .any(|head| head.as_ref().map(|head| head.tail) != tail)
        {
            return Err(violated!());
        }
    }
    Ok(())
}

pub(crate) async fn verify(provider: &dyn StreamProvider) -> Result<(), super::Failure> {
    let atomic = request()?;
    let paths = atomic
        .mutations()
        .map(|mutation| mutation.path().clone())
        .collect::<Vec<_>>();
    for all in [false, true] {
        let mut refused = atomic.clone();
        for (index, condition) in refused.effects[0].conditions.iter_mut().enumerate() {
            if all || index + 1 == paths.len() {
                *condition = crate::CommitCondition::Present {
                    path: paths[index].clone(),
                };
            }
        }
        let crate::CommitOutcome::Conflict(conflicts) = provider.commit(refused).await? else {
            return Err(violated!());
        };
        if conflicts.len() != if all { paths.len() } else { 1 }
            || provider
                .inspect_idempotency(atomic.idempotency_key.clone())
                .await?
                .is_some()
        {
            return Err(violated!());
        }
        heads(provider, &paths, None).await?;
    }
    let committed = provider.commit(atomic.clone()).await?;
    let crate::CommitOutcome::Committed(receipt) = &committed else {
        return Err(violated!());
    };
    receipt.validate_request(&atomic)?;
    if receipt.appends.len() != paths.len() || provider.commit(atomic.clone()).await? != committed {
        return Err(violated!());
    }
    heads(provider, &paths, Some(1)).await?;
    let observed = provider
        .inspect_idempotency(atomic.idempotency_key)
        .await?
        .ok_or(violated!())?;
    let crate::IdempotencyOutcome::Commit(envelope) = observed.outcome else {
        return Err(violated!());
    };
    if envelope.commit_id != receipt.commit_id || envelope.mutations.len() != paths.len() {
        return Err(violated!());
    }
    for (mutation, path) in envelope.mutations.iter().zip(&paths) {
        let crate::CommittedMutation::Append(append) = mutation else {
            return Err(violated!());
        };
        if append.path != *path
            || append.start != 0
            || append.end != 1
            || append.records.len() != 1
            || append.records[0].sequence != 0
            || append.records[0].commit_id != receipt.commit_id
            || append.records[0].value.as_ref() != b"member"
        {
            return Err(violated!());
        }
    }
    let bytes = crate::persistence::encode_envelope(&envelope, crate::MAX_COMMAND_BYTES)?;
    if crate::persistence::decode_envelope(&bytes, bytes.len())? != envelope {
        return Err(violated!());
    }
    let append = crate::AppendRequest {
        path: paths[0].clone(),
        records: vec![crate::OwnedBytes::from_static(b"batch").into(); 1_025],
        if_tail: Some(1),
        idempotency_key: crate::IdempotencyKey::new(b"atomic-records".as_slice())?,
    };
    let result = provider.append(append.clone()).await?;
    if !matches!(&result, crate::AppendOutcome::Committed(receipt) if receipt.start == 1 && receipt.end == 1_026)
        || provider.append(append).await? != result
        || provider.tail(paths[0].clone()).await? != 1_026
    {
        return Err(violated!());
    }
    Ok(())
}
pub(super) async fn deadline(provider: &dyn StreamProvider) -> Result<(), super::Failure> {
    let deadline_path = StreamPath::new("conformance/deadline")?;
    let request = crate::CommitRequest {
        effects: vec![crate::CommitEffect::hard(
            vec![crate::CommitCondition::Absent {
                path: deadline_path.clone(),
            }],
            vec![crate::CommitMutation::Append {
                path: deadline_path.clone(),
                records: vec![crate::OwnedBytes::from_static(b"must-not-commit").into()],
            }],
        )],
        idempotency_key: crate::IdempotencyKey::new(crate::OwnedBytes::from_static(
            b"http-deadline",
        ))?,
        deadline_unix_millis: Some(1),
    };
    if provider.commit(request).await != Err(StreamError::DeadlineElapsed)
        || provider.tail(deadline_path).await != Err(StreamError::NotFound)
    {
        return Err(violated!());
    }
    Ok(())
}
