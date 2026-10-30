//! Record origin across nested forks, read and followed from every fork cut.
use super::fork::{append, key, path};
use super::{Failure, History, Invariant};
use crate::StreamProviderExt as _;
use crate::{FollowRequest, ForkRequest, ReadRequest, Record, StreamPath, StreamProvider};
use futures::{StreamExt as _, TryStreamExt as _};

/// `root` appends two records, `a` forks it and appends one, `b` forks `a`
/// and appends none, and `c` forks `b` and appends one. Each record of `c`
/// names the segment that appended it, from every cut and through both reads
/// and follows.
pub(super) async fn nested<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
) -> Result<(), Failure> {
    let root = history.path.clone();
    append(history, &root, "origin-r0", b"r0", 0).await?;
    append(history, &root, "origin-r1", b"r1", 1).await?;
    let a = path(history, "-a")?;
    fork(history, &root, &a, 2, "origin-fork-a").await?;
    append(history, &a, "origin-a0", b"a0", 2).await?;
    let b = path(history, "-b")?;
    fork(history, &a, &b, 3, "origin-fork-b").await?;
    let c = path(history, "-c")?;
    fork(history, &b, &c, 3, "origin-fork-c").await?;
    append(history, &c, "origin-c0", b"c0", 3).await?;
    let expected: [(&[u8], &StreamPath); 4] =
        [(b"r0", &root), (b"r1", &root), (b"a0", &a), (b"c0", &c)];
    // Every cut, including each fork point and the empty fork's.
    for from in 0..4 {
        let want = &expected[from as usize..];
        let read = read(history, &c, from).await?;
        history.check(
            "read_origin",
            Invariant::RecordOrigin,
            matches(&read, from, want),
        )?;
        let followed = follow(history, &c, from, want.len()).await?;
        history.check(
            "follow_origin",
            Invariant::RecordOrigin,
            matches(&followed, from, want),
        )?;
    }
    let empty = read(history, &b, 0).await?;
    history.check(
        "empty_fork_origin",
        Invariant::RecordOrigin,
        matches(&empty, 0, &expected[..3]),
    )
}

fn matches(records: &[Record], from: u64, expected: &[(&[u8], &StreamPath)]) -> bool {
    records.len() == expected.len()
        && records
            .iter()
            .zip(expected)
            .zip(from..)
            .all(|((record, (value, origin)), sequence)| {
                record.sequence == sequence
                    && record.value.as_ref() == *value
                    && &record.origin == *origin
            })
}

async fn fork<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    source: &StreamPath,
    destination: &StreamPath,
    at_tail: u64,
    label: &str,
) -> Result<(), Failure> {
    let receipt = history.result(
        "fork_origin_history",
        history
            .provider
            .fork(ForkRequest {
                exclude: Vec::new(),
                source: source.clone(),
                destination: destination.clone(),
                at_tail: Some(at_tail),
                idempotency_key: key(history, label)?,
            })
            .await,
    )?;
    history.check(
        "fork_origin_history",
        Invariant::RecordOrigin,
        receipt.forked_at == at_tail,
    )
}

async fn read<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
    from: u64,
) -> Result<Vec<Record>, Failure> {
    let reader = history.result(
        "read_origin",
        history
            .provider
            .read(ReadRequest {
                key: None,
                path: target.clone(),
                from,
                limit: 8,
                at_least: Default::default(),
            })
            .await,
    )?;
    history.result("collect_origin", reader.take(9).try_collect().await)
}

async fn follow<P: StreamProvider + ?Sized>(
    history: &History<'_, P>,
    target: &StreamPath,
    from: u64,
    count: usize,
) -> Result<Vec<Record>, Failure> {
    let updates = history.result(
        "follow_origin",
        history
            .provider
            .follow(FollowRequest::new(target.clone(), from))
            .await,
    )?;
    // A follow is live, so it takes exactly the records the history holds.
    history.result(
        "collect_follow_origin",
        crate::records_only(updates).take(count).try_collect().await,
    )
}
