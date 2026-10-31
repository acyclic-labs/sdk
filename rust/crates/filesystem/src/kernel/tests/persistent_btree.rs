use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::tree_mutation::{TreeFormat, TreeMutation, TreeMutationError};
use crate::kernel::{FileKind, LogicalName, NameEncoding, TreeEntry};
use crate::storage::ObjectKind;

fn name(byte: u8) -> Result<LogicalName, crate::kernel::TreePageError> {
    LogicalName::new(NameEncoding::PosixBytes, vec![byte], 1)
}

fn entry(byte: u8) -> Result<TreeEntry, crate::kernel::TreePageError> {
    Ok(TreeEntry {
        name: name(byte)?,
        file_id: FileId::from_bytes([byte; 16]),
        kind: FileKind::Regular,
    })
}

fn child(byte: u8) -> Result<Child<LogicalName>, crate::kernel::TreePageError> {
    Ok(Child {
        first: name(byte)?,
        page: root(ObjectKind::TreePage, byte),
    })
}

fn root(kind: ObjectKind, byte: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([byte; 32]),
    }
}

fn remove(byte: u8) -> Result<TreeMutation, crate::kernel::TreePageError> {
    Ok(TreeMutation::Remove {
        name: name(byte)?,
        expected_file_id: None,
    })
}

#[test]
fn admission_rejects_every_invalid_request_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let limits = DecodeLimits::default();
    let mutation = remove(1)?;

    for (candidate_root, mutations, maximum, candidate_limits, expected) in [
        (
            root(ObjectKind::Blob, 1),
            vec![mutation.clone()],
            1,
            limits,
            "wrong-kind",
        ),
        (root(ObjectKind::TreePage, 1), vec![], 1, limits, "empty"),
        (
            root(ObjectKind::TreePage, 1),
            vec![mutation.clone()],
            0,
            limits,
            "too-many",
        ),
        (
            root(ObjectKind::TreePage, 1),
            vec![mutation.clone(), mutation.clone()],
            1,
            limits,
            "too-many",
        ),
        (
            root(ObjectKind::TreePage, 1),
            vec![mutation.clone()],
            1,
            DecodeLimits {
                maximum_page_items: 1,
                ..limits
            },
            "invalid-limits",
        ),
    ] {
        let failure = validate::<TreeFormat, TreeMutation>(
            Some(candidate_root),
            &mutations,
            maximum,
            candidate_limits,
        )
        .err()
        .ok_or("invalid persistent-tree request was admitted")?;
        match expected {
            "wrong-kind" => assert!(matches!(failure, Error::WrongRootKind)),
            "empty" => assert!(matches!(failure, Error::Empty)),
            "too-many" => assert!(matches!(failure, Error::TooManyMutations)),
            "invalid-limits" => assert!(matches!(failure, Error::InvalidLimits)),
            _ => unreachable!(),
        }
    }

    validate::<TreeFormat, TreeMutation>(
        Some(root(ObjectKind::TreePage, 1)),
        &[mutation],
        1,
        limits,
    )?;
    Ok(())
}

#[test]
fn pages_clone_without_changing_authenticated_frontiers() -> Result<(), Box<dyn std::error::Error>>
{
    let leaf_entries = vec![entry(1)?, entry(2)?];
    let leaf = Page::<TreeFormat>::Leaf(leaf_entries.clone());
    assert!(matches!(leaf.clone(), Page::Leaf(entries) if entries == leaf_entries));

    let internal_children = vec![child(1)?, child(2)?];
    let internal = Page::<TreeFormat>::Internal(internal_children.clone());
    assert!(matches!(internal.clone(), Page::Internal(children) if children == internal_children));
    Ok(())
}

/// Pages of items given as (encoded length, key hash), with whether each closed.
fn chunk(
    items: &[(usize, u64)],
    limits: DecodeLimits,
) -> Result<Vec<(usize, bool)>, Error<TreeMutationError>> {
    chunk_level(items, limits, 0)
}

fn chunk_level(
    items: &[(usize, u64)],
    limits: DecodeLimits,
    level: u16,
) -> Result<Vec<(usize, bool)>, Error<TreeMutationError>> {
    let mut chunks = PageChunks::new::<TreeMutationError>(
        items,
        limits,
        level,
        |item| Ok(item.0),
        |item| item.1,
    )?;
    let mut pages = Vec::new();
    while let Some((page, examined, closed)) = chunks.next::<TreeMutationError>()? {
        assert_eq!(examined, u64::try_from(page.len()).unwrap_or(u64::MAX));
        pages.push((page.len(), closed));
    }
    assert_eq!(pages.iter().map(|page| page.0).sum::<usize>(), items.len());
    Ok(pages)
}

fn page_sizes(
    lengths: &[usize],
    limits: DecodeLimits,
) -> Result<Vec<usize>, Error<TreeMutationError>> {
    let items = lengths
        .iter()
        .enumerate()
        .map(|(index, length)| (*length, stable_hash(&index.to_le_bytes())))
        .collect::<Vec<_>>();
    Ok(chunk(&items, limits)?
        .into_iter()
        .map(|page| page.0)
        .collect())
}

#[test]
fn chunking_is_bounded_by_items_bytes_and_encoder_contracts()
-> Result<(), Box<dyn std::error::Error>> {
    let mut limits = DecodeLimits {
        maximum_page_items: 2,
        maximum_page_bytes: 64,
        ..DecodeLimits::default()
    };
    assert_eq!(page_sizes(&[1; 3], limits)?, [2, 1]);
    // A small item limit makes boundaries frequent, yet no item is a certain
    // boundary: pages are not positional pairs that an insert would shift.
    limits.maximum_page_items = 4;
    let sizes = page_sizes(&[1; 64], limits)?;
    let (_, closed) = sizes.split_last().ok_or("no pages")?;
    assert!(
        closed.iter().all(|size| (2..=4).contains(size)),
        "{sizes:?}"
    );
    assert!(sizes.iter().any(|size| *size > 2), "{sizes:?}");

    // Past half its byte capacity a page closes, so the next item fits.
    limits.maximum_page_items = 3;
    limits.maximum_page_bytes = 17;
    assert_eq!(page_sizes(&[1; 3], limits)?, [2, 1]);

    // The byte bound splits work and never refuses it: a leaf item past half
    // the capacity closes the page it joins, never looking at the next item.
    limits.maximum_page_bytes = 15;
    assert_eq!(page_sizes(&[1; 3], limits)?, [1, 1, 1]);
    limits.maximum_page_items = 2;
    limits.maximum_page_bytes = 64;
    assert_eq!(page_sizes(&[1, 1, 40, 1, 1, 1], limits)?, [2, 1, 2, 1]);
    assert_eq!(page_sizes(&[1, 40, 1], limits)?, [2, 1]);
    // Internal pages always take two summaries, so every level shrinks.
    let large = [(40, 0), (40, 1), (40, 2)];
    assert_eq!(chunk_level(&large, limits, 1)?, [(2, true), (1, false)]);
    limits.maximum_page_items = 0;
    assert!(matches!(
        page_sizes(&[1; 3], limits),
        Err(Error::InvalidLimits)
    ));
    limits.maximum_page_items = 3;
    let mut failing = PageChunks::new::<TreeMutationError>(
        &[1_u8],
        limits,
        0,
        |_| Err(CanonicalDecodeError::Truncated),
        |_| 0,
    )?;
    assert!(matches!(
        failing.next(),
        Err(Error::<TreeMutationError>::Decode(
            CanonicalDecodeError::Truncated
        ))
    ));
    Ok(())
}

#[test]
fn chunking_resumed_at_any_page_start_reproduces_every_later_page()
-> Result<(), Box<dyn std::error::Error>> {
    let limits = DecodeLimits::default();
    let items = (0..4_096_usize)
        .map(|index| (24 + index % 40, stable_hash(&index.to_le_bytes())))
        .collect::<Vec<_>>();
    let whole = chunk(&items, limits)?;
    // Pages average the soft target: none is a forced single item, and the
    // count stays near the bytes over the target.
    let bytes = items.iter().map(|item| item.0).sum::<usize>();
    assert!(whole.len() > bytes / TARGET_PAGE_BYTES / 2);
    assert!(whole.len() < bytes / TARGET_PAGE_BYTES * 2);
    assert!(whole.iter().rev().skip(1).all(|page| page.1 && page.0 >= 2));
    let mut start = 0;
    for (index, page) in whole.iter().enumerate() {
        assert_eq!(chunk(&items[start..], limits)?, whole[index..]);
        start += page.0;
    }
    // Items at or above the soft target still pair up, so an internal level
    // always shrinks; only the hard limits force a single-item page.
    assert_eq!(page_sizes(&[TARGET_PAGE_BYTES * 2; 3], limits)?, [2, 1]);
    Ok(())
}

#[test]
fn bounds_cover_every_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let entries = [entry(2)?, entry(4)?, entry(6)?];
    let children = [child(2)?, child(4)?];

    assert!(leaf_within::<TreeFormat>(&entries, None, None));
    assert!(leaf_within::<TreeFormat>(
        &entries,
        Some(&name(2)?),
        Some(&name(7)?)
    ));
    assert!(!leaf_within::<TreeFormat>(&entries, Some(&name(3)?), None));
    assert!(!leaf_within::<TreeFormat>(&entries, None, Some(&name(6)?)));
    assert!(leaf_within::<TreeFormat>(&[], None, Some(&name(1)?)));

    assert!(children_within(&children, None, None));
    assert!(children_within(&children, Some(&name(2)?), Some(&name(5)?)));
    assert!(!children_within(&children, Some(&name(3)?), None));
    assert!(!children_within(&children, None, Some(&name(4)?)));
    assert!(children_within(&[], None, Some(&name(1)?)));
    Ok(())
}
