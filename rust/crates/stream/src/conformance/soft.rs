use super::*;

/// Commit effects: each effect's mutations apply all or none. A failed soft
/// effect is skipped and reported by index while the rest of the commit
/// publishes; a commit that skips every effect still commits and spends its
/// key, so a retry replays the identical receipt. A failed hard effect
/// refuses the whole commit and leaves its key unspent.
pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let open = path("effects/open")?;
    let pair = path("effects/open-pair")?;
    let closing = path("effects/closing")?;
    let marker = path("effects/closing-marker")?;
    let never = path("effects/never")?;
    let absent = path("effects/absent")?;
    let carrier = path("effects/carrier")?;
    let open_marker = path("effects/open-marker")?;
    for (stream, value) in [(&open, b"open"), (&closing, b"shut"), (&marker, b"mark")] {
        provider
            .append(AppendRequest {
                path: stream.clone(),
                records: vec![Bytes::from_static(value).into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(format!("effects/seed/{stream}"))?,
            })
            .await?;
    }
    let append = |path: &StreamPath| CommitMutation::Append {
        path: path.clone(),
        records: vec![Bytes::from_static(b"offer").into()],
    };
    let present = |path: &StreamPath| CommitCondition::Present { path: path.clone() };
    let absent_condition = |path: &StreamPath| CommitCondition::Absent { path: path.clone() };
    let soft = |conditions, mutations| CommitEffect {
        conditions,
        mutations,
        soft: true,
    };
    // A send to `to`: soft, while `to` exists and its marker does not.
    let send = |to: &StreamPath, marker: &StreamPath, mutations| {
        soft(vec![present(to), absent_condition(marker)], mutations)
    };
    let commit_key = key(b"effects-mixed")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![
            CommitEffect::hard(vec![absent_condition(&carrier)], vec![append(&carrier)]),
            // Applies both of its mutations.
            send(&open, &open_marker, vec![append(&pair), append(&open)]),
            // Skipped as a whole: neither mutation lands.
            send(&closing, &marker, vec![append(&closing), append(&never)]),
            // Skipped: its recipient does not exist.
            soft(vec![present(&absent)], vec![append(&absent)]),
        ],
        idempotency_key: commit_key.clone(),
    };
    let outcome = provider.commit(request.clone()).await?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        return Err(StreamError::Unavailable);
    };
    let mut canonical = request.clone();
    crate::request::validate_commit(&mut canonical)?;
    if receipt.skipped != [2, 3]
        || receipt.appends.len() != 3
        || provider.tail(open.clone()).await? != 2
        || provider.tail(pair.clone()).await? != 1
        || provider.tail(closing.clone()).await? != 1
        || provider.tail(carrier.clone()).await? != 1
        || provider.tail(never.clone()).await != Err(StreamError::NotFound)
        || provider.tail(absent.clone()).await != Err(StreamError::NotFound)
        || provider.commit(request.clone()).await? != outcome
    {
        return Err(StreamError::Unavailable);
    }
    receipt.validate_request(&canonical)?;
    let facts = provider.read_commit(receipt.commit_id).await?;
    let observation = provider
        .inspect_idempotency(commit_key.clone())
        .await?
        .ok_or(StreamError::Unavailable)?;
    let mut recovered = request.clone();
    let binding =
        crate::request::commit_binding_with(&mut recovered, |_| Ok::<(), StreamError>(()))??;
    if !receipt.matches_envelope(&facts)
        || observation.outcome != IdempotencyOutcome::Commit(facts.clone())
        || observation.request_digest != binding.digest()
        || !binding.matches_with(&facts, |_| Ok::<(), StreamError>(()))??
    {
        return Err(StreamError::Unavailable);
    }
    // The same mutations grouped differently are a different request.
    let mut regrouped = request.clone();
    let split = regrouped.effects[1].mutations.split_off(1);
    regrouped.effects.push(send(&open, &open_marker, split));
    if crate::request::commit_digest(&mut regrouped.clone())? == binding.digest()
        || provider.commit(regrouped).await != Err(StreamError::IdempotencyMismatch)
    {
        return Err(StreamError::Unavailable);
    }

    // Skipping every effect still commits and spends the key: a retry
    // replays the identical receipt and cannot redeliver.
    let skipped_key = key(b"effects-skipped")?;
    let skipped = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![soft(vec![present(&absent)], vec![append(&absent)])],
        idempotency_key: skipped_key.clone(),
    };
    let outcome = provider.commit(skipped.clone()).await?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        return Err(StreamError::Unavailable);
    };
    let facts = provider.read_commit(receipt.commit_id).await?;
    if receipt.skipped != [0]
        || !receipt.appends.is_empty()
        || !receipt.forks.is_empty()
        || !facts.mutations.is_empty()
        || provider.commit(skipped.clone()).await? != outcome
        || provider.tail(absent.clone()).await != Err(StreamError::NotFound)
        || provider
            .inspect_idempotency(skipped_key.clone())
            .await?
            .map(|observation| observation.outcome)
            != Some(IdempotencyOutcome::Commit(facts))
    {
        return Err(StreamError::Unavailable);
    }
    // Negative control: the spent key refuses a different body.
    let mut changed = skipped.clone();
    changed.effects[0].soft = false;
    if provider.commit(changed).await != Err(StreamError::IdempotencyMismatch) {
        return Err(StreamError::Unavailable);
    }

    // A failed hard effect conflicts, publishes nothing and leaves its key
    // unspent, even beside a soft effect that would apply.
    let conflict_key = key(b"effects-conflict")?;
    let conflicted = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![
            CommitEffect::hard(vec![present(&absent)], vec![append(&absent)]),
            soft(vec![present(&open)], vec![append(&open)]),
        ],
        idempotency_key: conflict_key.clone(),
    };
    if provider.commit(conflicted).await?
        != CommitOutcome::Conflict(vec![CommitConflict::Missing {
            path: absent.clone(),
        }])
        || provider
            .inspect_idempotency(conflict_key.clone())
            .await?
            .is_some()
        || provider.tail(open.clone()).await? != 2
    {
        return Err(StreamError::Unavailable);
    }
    let reused = CommitRequest {
        deadline_unix_millis: None,
        effects: vec![CommitEffect::hard(
            vec![present(&open)],
            vec![append(&open)],
        )],
        idempotency_key: conflict_key,
    };
    if !matches!(provider.commit(reused).await?, CommitOutcome::Committed(_))
        || provider.tail(open.clone()).await? != 3
    {
        return Err(StreamError::Unavailable);
    }

    // A hard effect without mutations guards the commit: an empty guard is a
    // no-op, a failed one refuses the soft send it guards, a held one lets it
    // publish.
    for (guard, name, committed, tail) in [
        (Vec::new(), b"effects-guard-empty".as_slice(), true, 4),
        (
            vec![absent_condition(&carrier)],
            b"effects-guard-failed",
            false,
            4,
        ),
        (vec![present(&carrier)], b"effects-guard-held", true, 5),
    ] {
        let outcome = provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![
                    CommitEffect::hard(guard, Vec::new()),
                    send(&open, &open_marker, vec![append(&open)]),
                ],
                idempotency_key: key(name)?,
            })
            .await?;
        let shaped = match &outcome {
            CommitOutcome::Committed(receipt) => committed && receipt.skipped.is_empty(),
            CommitOutcome::Conflict(_) => !committed,
        };
        if !shaped || provider.tail(open.clone()).await? != tail {
            return Err(StreamError::Unavailable);
        }
    }

    // Refusals: no effect, no mutation in the whole commit, a soft effect
    // without mutations, and a path mutated by two effects.
    for (request, error) in [
        (Vec::new(), StreamError::LimitExceeded),
        (
            vec![CommitEffect::hard(vec![present(&open)], Vec::new())],
            StreamError::LimitExceeded,
        ),
        (
            vec![
                CommitEffect::hard(Vec::new(), vec![append(&carrier)]),
                soft(vec![present(&open)], Vec::new()),
            ],
            StreamError::LimitExceeded,
        ),
        (
            vec![
                CommitEffect::hard(Vec::new(), vec![append(&carrier)]),
                soft(vec![present(&open)], vec![append(&carrier)]),
            ],
            StreamError::InvalidArgument,
        ),
    ] {
        let request = CommitRequest {
            deadline_unix_millis: None,
            effects: request,
            idempotency_key: key(b"effects-refused")?,
        };
        if provider.commit(request).await != Err(error) {
            return Err(StreamError::Unavailable);
        }
    }
    if provider.tail(open).await? != 5 || provider.tail(carrier).await? != 1 {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}
