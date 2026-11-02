//! `read` selectors: latest-first keys, key ranges, a follow from beyond the
//! tail, and `children` paging, each with a negative control.
use super::*;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod model;

/// Every record of one indexed read, paged `page` at a time through the
/// shared continuation rule, [`crate::ReadKeySeek::advance`].
async fn selected<P: StreamProvider + ?Sized>(
    provider: &P,
    source: &StreamPath,
    mut seek: crate::ReadKeySeek,
    mut from: u64,
    page: u32,
) -> Result<Vec<u64>, StreamError> {
    let mut sequences = Vec::new();
    loop {
        let records: Vec<_> = provider
            .read(ReadRequest {
                key: Some(seek.clone()),
                path: source.clone(),
                from,
                limit: page,
                at_least: Default::default(),
            })
            .await?
            .try_collect()
            .await?;
        for record in &records {
            let previous = (seek.key.clone(), seek.before, from);
            seek.advance(&mut from, record.sequence, record.headers.keys())?;
            // A broken continuation must fail this control at its original
            // page, rather than repeatedly reading the same page forever.
            let progressed = match seek.order {
                crate::ReadOrder::LatestFirst => seek.before < previous.1,
                crate::ReadOrder::Ascending => {
                    seek.key > previous.0 || seek.key == previous.0 && from > previous.2
                }
            };
            if !progressed {
                return Err(StreamError::InvalidResponse);
            }
            sequences.push(record.sequence);
        }
        if records.len() < page as usize {
            return Ok(sequences);
        }
    }
}

pub(super) async fn verify<P: StreamProvider + ?Sized>(provider: &P) -> Result<(), StreamError> {
    let source = path("selectors/source")?;
    let record = |keys: &[&'static [u8]]| -> Result<crate::AppendRecord, StreamError> {
        Ok(crate::AppendRecord {
            value: Bytes::from_static(b"value"),
            headers: crate::RecordHeaders::new(
                keys.iter().map(|key| Bytes::from_static(key)).collect(),
                Vec::new(),
            )?,
        })
    };
    provider
        .append(AppendRequest {
            path: source.clone(),
            if_tail: Some(0),
            idempotency_key: key(b"selectors-source")?,
            records: vec![
                record(&[b"a"])?,
                record(&[b"b", b"c"])?,
                record(&[b"a", b"c"])?,
                record(&[b"b"])?,
                record(&[b"a"])?,
            ],
        })
        .await?;
    use crate::ReadKeySeek as Seek;

    // Latest first: one lookup finds the latest match; paging walks back.
    // The ascending order of the same key is the negative control.
    let latest = provider
        .latest(source.clone(), Bytes::from_static(b"a"), Default::default())
        .await?
        .ok_or(StreamError::Unavailable)?;
    if latest.sequence != 4
        || provider
            .latest(source.clone(), Bytes::from_static(b"z"), Default::default())
            .await?
            .is_some()
        || selected(provider, &source, Seek::latest(&b"a"[..], u64::MAX), 0, 1).await? != [4, 2, 0]
        || selected(provider, &source, Seek::latest(&b"a"[..], 4), 1, 10).await? != [2]
        || selected(provider, &source, Seek::exact(&b"a"[..], u64::MAX), 0, 1).await? != [0, 2, 4]
    {
        return Err(StreamError::Unavailable);
    }

    // Key ranges: ascending (key, sequence) entries; a record appears once
    // per key it carries in the range. Pages resume through the continuation.
    let ranges: [(Seek, u64, u32, &[u64]); 5] = [
        (
            Seek::range(&b"a"[..], &b"d"[..], u64::MAX),
            0,
            2,
            &[0, 2, 4, 1, 3, 1, 2],
        ),
        (Seek::range(&b"b"[..], &b"c"[..], 3), 0, 10, &[1]),
        (
            Seek::range(&b"a"[..], &b"c"[..], u64::MAX),
            3,
            10,
            &[4, 1, 3],
        ),
        (Seek::range(&b"c"[..], &b"c"[..], u64::MAX), 0, 10, &[]),
        (Seek::range(&b"d"[..], &b"z"[..], u64::MAX), 0, 10, &[]),
    ];
    for (seek, from, page, expected) in ranges {
        let actual = selected(provider, &source, seek, from, page).await?;
        if actual != expected {
            return Err(StreamError::Unavailable);
        }
    }
    // Ranges are ascending only, refused before any observation.
    let refused = provider
        .read(ReadRequest {
            key: Some(Seek {
                order: crate::ReadOrder::LatestFirst,
                ..Seek::range(&b"a"[..], &b"d"[..], u64::MAX)
            }),
            path: source.clone(),
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .err();
    if refused != Some(StreamError::InvalidArgument) {
        return Err(StreamError::Unavailable);
    }

    // `from` beyond the tail waits there: the k-th record is the first delivery.
    let mut follow = crate::records_only(
        provider
            .follow(crate::FollowRequest::new(source.clone(), 7))
            .await?,
    );
    for (index, sequence) in [5_u64, 6, 7].into_iter().enumerate() {
        let outcome = provider
            .append(AppendRequest {
                path: source.clone(),
                if_tail: Some(sequence),
                idempotency_key: key([
                    b"selectors-late-0",
                    b"selectors-late-1",
                    b"selectors-late-2",
                ][index])?,
                records: vec![record(&[b"a"])?],
            })
            .await?;
        if !matches!(outcome, AppendOutcome::Committed(_)) {
            return Err(StreamError::Unavailable);
        }
    }
    let first = follow
        .next()
        .await
        .transpose()?
        .ok_or(StreamError::Unavailable)?;
    if first.sequence != 7 {
        return Err(StreamError::Unavailable);
    }

    // `children` pages a parent's direct children; a leaf has none.
    let tree = path("selectors/tree")?;
    for (child, idempotency) in [
        ("selectors/tree/x", b"selectors-tree-x"),
        ("selectors/tree/y", b"selectors-tree-y"),
        ("selectors/tree/z", b"selectors-tree-z"),
    ] {
        provider
            .append(AppendRequest {
                path: path(child)?,
                if_tail: Some(0),
                idempotency_key: key(idempotency)?,
                records: vec![record(&[])?],
            })
            .await?;
    }
    let page = |parent: StreamPath, after: Option<StreamPath>| ChildrenPageRequest {
        tail_stream: None,
        parent: Some(parent),
        after,
        hierarchy_version: None,
        limit: 2,
    };
    let names = |page: &crate::ChildrenPage| {
        page.children
            .iter()
            .map(|child| child.path.to_string())
            .collect::<Vec<_>>()
    };
    let first = provider.children_page(page(tree.clone(), None)).await?;
    let rest = provider
        .children_page(page(tree, first.next_after.clone()))
        .await?;
    let leaf = provider
        .children_page(page(path("selectors/tree/x")?, None))
        .await?;
    if names(&first) != ["selectors/tree/x", "selectors/tree/y"]
        || first.next_after != Some(path("selectors/tree/y")?)
        || names(&rest) != ["selectors/tree/z"]
        || rest.next_after.is_some()
        || !leaf.children.is_empty()
    {
        return Err(StreamError::Unavailable);
    }
    Ok(())
}
