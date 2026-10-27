//! Black-box fork histories. Concurrency, GC and authorization need separate proofs.
use super::{Failure, History, Invariant};
use crate::{
    AppendOutcome, AppendRequest, CommitCondition, CommitMutation, CommitOutcome, CommitRequest,
    ForkRequest, IdempotencyKey, IdempotencyOutcome, OwnedBytes, ReadRequest, StreamError,
    StreamPath, StreamProvider,
};
use futures::{StreamExt as _, TryStreamExt as _};
use sha2::{Digest as _, Sha256};

pub(super) fn path<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    suffix: &str,
) -> Result<StreamPath, Failure> {
    history.result(
        "derive_fork_path",
        StreamPath::new(format!("{}{suffix}", history.path.as_str())),
    )
}

pub(super) fn key<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    label: &str,
) -> Result<IdempotencyKey, Failure> {
    let digest = Sha256::digest(format!("{}:{label}", history.path.as_str()).as_bytes());
    history.result(
        "derive_fork_key",
        IdempotencyKey::new(OwnedBytes::from(digest.to_vec())),
    )
}

pub(super) async fn append<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
    label: &str,
    value: &'static [u8],
    tail: u64,
) -> Result<(), Failure> {
    let outcome = history.result(
        "append_fork_history",
        history
            .provider
            .append(AppendRequest {
                path: target.clone(),
                records: vec![OwnedBytes::from_static(value).into()],
                if_tail: Some(tail),
                idempotency_key: Some(key(history, label)?),
            })
            .await,
    )?;
    history.check("append_fork_history", Invariant::ForkIsolation,
        matches!(outcome, AppendOutcome::Committed(ref receipt) if receipt.start == tail && receipt.end == tail + 1 && receipt.tail == tail + 1))
}

pub(super) async fn absent<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
    invariant: Invariant,
) -> Result<(), Failure> {
    match history.provider.tail(target.clone()).await {
        Err(StreamError::NotFound) => Ok(()),
        Ok(_) => history.check("absent_fork_path", invariant, false),
        Err(source) => Err(super::provider_failure(
            history.case,
            "absent_fork_path",
            source,
        )),
    }
}

pub(super) async fn facts<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    receipt: &crate::CommitReceipt,
) -> Result<crate::CommittedEnvelope, Failure> {
    let envelope = history.result(
        "read_committed_facts",
        history.provider.read_commit(receipt.commit_id).await,
    )?;
    history.check(
        "committed_receipt",
        Invariant::CompositionReceipt,
        receipt.matches_envelope(&envelope),
    )?;
    Ok(envelope)
}

pub(super) async fn values<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
    expected: &[&[u8]],
    invariant: Invariant,
) -> Result<(), Failure> {
    let reader = match history
        .provider
        .read(ReadRequest {
            path: target.clone(),
            from: 0,
            limit: 8,
        })
        .await
    {
        Ok(reader) => reader,
        Err(StreamError::NotFound) => {
            return history.check("read_inherited_path", invariant, false);
        }
        Err(source) => {
            return Err(super::provider_failure(
                history.case,
                "read_inherited_path",
                source,
            ));
        }
    };
    let records = history.result(
        "read_fork_records",
        reader.take(9).try_collect::<Vec<_>>().await,
    )?;
    let actual_tail = history.result(
        "read_fork_tail",
        history.provider.tail(target.clone()).await,
    )?;
    history.check(
        "fork_values",
        invariant,
        records.len() == expected.len()
            && usize::try_from(actual_tail) == Ok(expected.len())
            && records
                .iter()
                .zip(expected)
                .enumerate()
                .all(|(index, (record, value))| {
                    usize::try_from(record.sequence) == Ok(index) && record.value.as_ref() == *value
                }),
    )
}

async fn prepare<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<StreamPath, Failure> {
    let destination = path(history, "-copy")?;
    absent(history, &destination, Invariant::FreshNamespace).await?;
    append(history, &history.path, "seed-root", b"root", 0).await?;
    append(
        history,
        &path(history, "/branch")?,
        "seed-branch",
        b"branch",
        0,
    )
    .await?;
    append(
        history,
        &path(history, "/branch/deep")?,
        "seed-deep",
        b"deep",
        0,
    )
    .await?;
    append(
        history,
        &path(history, "-sibling/leaf")?,
        "seed-sibling",
        b"sibling",
        0,
    )
    .await?;
    Ok(destination)
}

async fn check_inherited<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    destination: &StreamPath,
    invariant: Invariant,
) -> Result<(), Failure> {
    values(history, destination, &[b"root"], invariant).await?;
    values(
        history,
        &path(history, "-copy/branch")?,
        &[b"branch"],
        invariant,
    )
    .await?;
    values(
        history,
        &path(history, "-copy/branch/deep")?,
        &[b"deep"],
        invariant,
    )
    .await?;
    absent(
        history,
        &path(history, "-copy-sibling/leaf")?,
        Invariant::SubtreeMembership,
    )
    .await
}

async fn diverge<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    destination: &StreamPath,
) -> Result<(), Failure> {
    append(history, &history.path, "source-root", b"source-root", 1).await?;
    append(history, destination, "child-root", b"child-root", 1).await?;
    append(
        history,
        &path(history, "/branch")?,
        "source-branch",
        b"source-branch",
        1,
    )
    .await?;
    append(
        history,
        &path(history, "-copy/branch")?,
        "child-branch",
        b"child-branch",
        1,
    )
    .await?;
    append(
        history,
        &path(history, "/new")?,
        "new-source-path",
        b"new",
        0,
    )
    .await?;
    values(
        history,
        &history.path,
        &[b"root", b"source-root"],
        Invariant::ForkIsolation,
    )
    .await?;
    values(
        history,
        destination,
        &[b"root", b"child-root"],
        Invariant::ForkIsolation,
    )
    .await?;
    values(
        history,
        &path(history, "/branch")?,
        &[b"branch", b"source-branch"],
        Invariant::ForkIsolation,
    )
    .await?;
    values(
        history,
        &path(history, "-copy/branch")?,
        &[b"branch", b"child-branch"],
        Invariant::ForkIsolation,
    )
    .await?;
    values(
        history,
        &path(history, "-sibling/leaf")?,
        &[b"sibling"],
        Invariant::ForkIsolation,
    )
    .await?;
    absent(
        history,
        &path(history, "-copy/new")?,
        Invariant::ForkIsolation,
    )
    .await
}

pub(super) async fn subtree<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let destination = prepare(history).await?;
    let request = ForkRequest {
        exclude: Vec::new(),
        source: history.path.clone(),
        destination: destination.clone(),
        at_tail: Some(1),
        idempotency_key: Some(history.key.clone()),
    };
    let receipt = history.result("fork_subtree", history.provider.fork(request.clone()).await)?;
    history.check(
        "fork_receipt",
        Invariant::SubtreeMembership,
        receipt.source == history.path
            && receipt.destination == destination
            && receipt.forked_at == 1
            && receipt.tail == 1,
    )?;
    check_inherited(history, &destination, Invariant::SubtreeMembership).await?;
    diverge(history, &destination).await?;
    let replay = history.result("replay_fork", history.provider.fork(request.clone()).await)?;
    history.check("replay_fork", Invariant::ExactReplay, replay == receipt)?;
    history
        .require_recovery(IdempotencyOutcome::Fork(receipt))
        .await?;
    history.require_mismatch(
        "changed_fork",
        history
            .provider
            .fork(ForkRequest {
                at_tail: Some(0),
                ..request
            })
            .await,
    )
}

pub(super) async fn composed<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let destination = prepare(history).await?;
    let branch = path(history, "/branch")?;
    let request = CommitRequest {
        deadline_unix_millis: None,
        conditions: vec![
            CommitCondition::Tail {
                path: history.path.clone(),
                expected: 1,
            },
            CommitCondition::Tail {
                path: branch.clone(),
                expected: 1,
            },
            CommitCondition::Absent {
                path: destination.clone(),
            },
        ],
        mutations: vec![
            CommitMutation::Append {
                path: history.path.clone(),
                records: vec![OwnedBytes::from_static(b"root-after").into()],
            },
            CommitMutation::Append {
                path: branch.clone(),
                records: vec![OwnedBytes::from_static(b"branch-after").into()],
            },
            CommitMutation::Fork {
                exclude: Vec::new(),
                source: history.path.clone(),
                destination: destination.clone(),
                at_tail: 1,
                records: vec![],
            },
        ],
        idempotency_key: history.key.clone(),
    };
    let outcome = history.result(
        "composed_fork",
        history.provider.commit(request.clone()).await,
    )?;
    let CommitOutcome::Committed(receipt) = &outcome else {
        return history.check("composed_fork", Invariant::PreCommitCut, false);
    };
    let retained = facts(history, receipt).await?;
    history.check(
        "composed_envelope",
        Invariant::CompositionReceipt,
        retained.mutations.len() == 3,
    )?;
    values(
        history,
        &history.path,
        &[b"root", b"root-after"],
        Invariant::PreCommitCut,
    )
    .await?;
    values(
        history,
        &branch,
        &[b"branch", b"branch-after"],
        Invariant::PreCommitCut,
    )
    .await?;
    check_inherited(history, &destination, Invariant::PreCommitCut).await?;
    let replay = history.result(
        "replay_composed_fork",
        history.provider.commit(request).await,
    )?;
    history.check(
        "replay_composed_fork",
        Invariant::ExactReplay,
        replay == outcome,
    )?;
    history
        .require_recovery(IdempotencyOutcome::Commit(retained))
        .await
}
