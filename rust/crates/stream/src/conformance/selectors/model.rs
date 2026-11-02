//! Independent sorted entries, checked through the shared conformance pager.
use super::*;
use crate::{FollowRequest, MemoryStream, OwnedBytes, ReadKeySeek, ReadOrder, RecordHeaders};
use futures::FutureExt as _;

fn expected(entries: &[(u8, u64)], seek: &ReadKeySeek, from: u64) -> Vec<u64> {
    let key = seek.key.as_ref()[0];
    let mut matches = entries
        .iter()
        .filter(|&&(k, sequence)| {
            sequence < seek.before
                && match &seek.end {
                    Some(end) => k >= key && k < end.as_ref()[0] && (k, sequence) >= (key, from),
                    None => k == key && sequence >= from,
                }
        })
        .map(|&(_, sequence)| sequence)
        .collect::<Vec<_>>();
    if seek.order == ReadOrder::LatestFirst {
        matches.reverse();
    }
    matches
}

fn agrees(actual: &[u64], expected: &[u64]) -> Result<(), StreamError> {
    if actual == expected {
        Ok(())
    } else {
        Err(StreamError::InvalidResponse)
    }
}

/// Input bytes choose overlapping key sets, cursor bounds, page sizes and
/// whether a live follow is first observed before or after its producer runs.
pub async fn check(bytes: &[u8]) -> Result<(), StreamError> {
    let provider = MemoryStream::default();
    let source = path("selector-model/source")?;
    let mut entries = Vec::new();
    let records = bytes
        .iter()
        .copied()
        .chain(std::iter::once(1))
        .enumerate()
        .map(|(sequence, mask)| {
            let keys = (0..8_u8)
                .filter(|bit| mask & (1 << bit) != 0)
                .map(|key| {
                    entries.push((key, sequence as u64));
                    OwnedBytes::from(vec![key])
                })
                .collect();
            Ok(crate::AppendRecord {
                value: OwnedBytes::from((sequence as u64).to_be_bytes().to_vec()),
                headers: RecordHeaders::new(keys, Vec::new())?,
            })
        })
        .collect::<Result<Vec<_>, StreamError>>()?;
    let tail = records.len() as u64;
    // Appends are chunked at the public command's record bound, never by a
    // fuzz-only input cutoff. The reference retains the original sequences.
    for chunk in records.chunks(crate::MAX_ITEMS) {
        let outcome = provider
            .append(AppendRequest {
                path: source.clone(),
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
                records: chunk.to_vec(),
            })
            .await?;
        if !matches!(outcome, AppendOutcome::Committed(_)) {
            return Err(StreamError::InvalidResponse);
        }
    }
    entries.sort_unstable();
    let byte = |index| bytes.get(index).copied().unwrap_or_default();
    let from = u64::from(byte(0)) % (tail + 2);
    let before = if byte(1) & 1 == 0 {
        u64::MAX
    } else {
        u64::from(byte(2)) % (tail + 2)
    };
    let key = byte(3) % 10;
    let end = byte(4) % 10;
    let page = 1 + u32::from(byte(5) % 8);
    for seek in [
        ReadKeySeek::exact(vec![key], before),
        ReadKeySeek::latest(vec![key], before),
        ReadKeySeek::range(vec![key], vec![end], before),
    ] {
        let model = expected(&entries, &seek, from);
        let actual = selected(&provider, &source, seek, from, page).await?;
        agrees(&actual, &model)?;
    }

    let start = tail + 1 + u64::from(byte(6) % 4);
    let mut follow = crate::records_only(
        provider
            .follow(FollowRequest::new(source.clone(), start))
            .await?,
    );
    if byte(7) & 1 != 0 && follow.next().now_or_never().is_some() {
        return Err(StreamError::InvalidResponse);
    }
    let outcome = provider
        .append(AppendRequest {
            path: source,
            if_tail: Some(tail),
            idempotency_key: IdempotencyKey::unique(),
            records: (tail..start + 2)
                .map(|sequence| OwnedBytes::from(sequence.to_be_bytes().to_vec()).into())
                .collect(),
        })
        .await?;
    if !matches!(outcome, AppendOutcome::Committed(_)) {
        return Err(StreamError::InvalidResponse);
    }
    for sequence in start..start + 2 {
        // The producer above has committed the exact requested records. A
        // pending result here is a progress defect, not a timeout to extend.
        let record = follow
            .next()
            .now_or_never()
            .ok_or(StreamError::Unavailable)?
            .ok_or(StreamError::Unavailable)??;
        if record.sequence != sequence || record.value.as_ref() != sequence.to_be_bytes() {
            return Err(StreamError::InvalidResponse);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_selectors_match_entries_and_detect_wrong_order_bounds_and_cursor()
    -> Result<(), StreamError> {
        for mask in 0..32 {
            for observed in [0, 1] {
                check(&[mask, 3, 7, 0, 8, 1, 3, observed]).await?;
            }
        }
        let entries = [(0, 0), (0, 1), (0, 2), (1, 1), (2, 0)];
        let latest = ReadKeySeek::latest(vec![0], 3);
        let correct = expected(&entries, &latest, 0);
        assert!(
            agrees(&[0, 1, 2], &correct).is_err(),
            "ascending is not latest-first"
        );
        assert!(
            agrees(
                &[2, 1, 0],
                &expected(&entries, &ReadKeySeek::latest(vec![0], 2), 0)
            )
            .is_err(),
            "before is exclusive"
        );
        assert!(
            agrees(
                &[1, 0, 1],
                &expected(&entries, &ReadKeySeek::range(vec![0], vec![2], 3), 1)
            )
            .is_err(),
            "range cursor applies to its first key only"
        );
        assert!(
            agrees(&[4, 6], &[4, 5]).is_err(),
            "a follow cannot skip its next sequence"
        );
        Ok(())
    }
}
