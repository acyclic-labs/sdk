use super::*;
use crate::foundation::{Digest, FileId};
use crate::kernel::tree_mutation::{TreeFormat, TreeMutation, TreeSemanticError};
use crate::kernel::{FileKind, LogicalName, NameEncoding, TreeEntry};
use crate::performance::WorkError;
use crate::storage::{ObjectKind, ObjectStoreError};

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
            candidate_root,
            &mutations,
            maximum,
            candidate_limits,
        )
        .err()
        .ok_or("invalid persistent-tree request was admitted")?;
        assert_eq!(*failure.work, WorkCounters::default());
        match expected {
            "wrong-kind" => assert!(matches!(failure.error, Error::WrongRootKind)),
            "empty" => assert!(matches!(failure.error, Error::Empty)),
            "too-many" => assert!(matches!(failure.error, Error::TooManyMutations)),
            "invalid-limits" => assert!(matches!(failure.error, Error::InvalidLimits)),
            _ => unreachable!(),
        }
    }

    validate::<TreeFormat, TreeMutation>(root(ObjectKind::TreePage, 1), &[mutation], 1, limits)?;
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

#[test]
fn chunking_is_bounded_by_items_bytes_and_encoder_contracts()
-> Result<(), Box<dyn std::error::Error>> {
    let items = [1_u8, 2, 3];
    let mut limits = DecodeLimits {
        maximum_page_items: 2,
        maximum_page_bytes: 64,
        ..DecodeLimits::default()
    };
    assert_eq!(
        page_chunk_end::<_, TreeSemanticError>(&items, 0, limits, |_| Ok(1))?,
        (2, 2)
    );
    assert_eq!(
        page_chunk_end::<_, TreeSemanticError>(&items, 2, limits, |_| Ok(1))?,
        (3, 1)
    );

    limits.maximum_page_items = 3;
    limits.maximum_page_bytes = 17;
    assert_eq!(
        page_chunk_end::<_, TreeSemanticError>(&items, 0, limits, |_| Ok(1))?,
        (2, 3)
    );

    limits.maximum_page_bytes = 15;
    assert!(matches!(
        page_chunk_end::<_, TreeSemanticError>(&items, 0, limits, |_| Ok(1)),
        Err(Error::PageItemTooLarge)
    ));
    limits.maximum_page_items = 0;
    limits.maximum_page_bytes = 64;
    assert!(matches!(
        page_chunk_end::<_, TreeSemanticError>(&items, 0, limits, |_| Ok(1)),
        Err(Error::PageItemTooLarge)
    ));
    limits.maximum_page_items = 3;
    assert!(matches!(
        page_chunk_end::<_, TreeSemanticError>(&items, 0, limits, |_| {
            Err(CanonicalDecodeError::Truncated)
        }),
        Err(Error::Decode(CanonicalDecodeError::Truncated))
    ));
    Ok(())
}

#[test]
fn search_unchanged_and_bounds_cover_every_frontier() -> Result<(), Box<dyn std::error::Error>> {
    let entries = [entry(2)?, entry(4)?, entry(6)?];
    assert_eq!(search::<TreeFormat>(&entries, &name(2)?).0, Ok(0));
    assert_eq!(search::<TreeFormat>(&entries, &name(4)?).0, Ok(1));
    assert_eq!(search::<TreeFormat>(&entries, &name(6)?).0, Ok(2));
    assert_eq!(search::<TreeFormat>(&entries, &name(1)?).0, Err(0));
    assert_eq!(search::<TreeFormat>(&entries, &name(3)?).0, Err(1));
    assert_eq!(search::<TreeFormat>(&entries, &name(7)?).0, Err(3));

    let children = [child(2)?, child(4)?];
    assert!(unchanged(&children, &children));
    assert!(!unchanged(&children[..1], &children));
    let mut changed = children.clone();
    changed[1].page = root(ObjectKind::TreePage, 9);
    assert!(!unchanged(&changed, &children));

    assert!(validate_leaf::<TreeFormat, TreeMutation>(&entries, None, None).is_ok());
    assert!(
        validate_leaf::<TreeFormat, TreeMutation>(&entries, Some(&name(2)?), Some(&name(7)?))
            .is_ok()
    );
    assert!(matches!(
        validate_leaf::<TreeFormat, TreeMutation>(&entries, Some(&name(3)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_leaf::<TreeFormat, TreeMutation>(&entries, None, Some(&name(6)?)),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(validate_leaf::<TreeFormat, TreeMutation>(&[], None, Some(&name(1)?)).is_ok());

    assert!(validate_children::<TreeFormat, TreeMutation>(&children, None, None).is_ok());
    assert!(
        validate_children::<TreeFormat, TreeMutation>(&children, Some(&name(2)?), Some(&name(5)?))
            .is_ok()
    );
    assert!(matches!(
        validate_children::<TreeFormat, TreeMutation>(&children, Some(&name(3)?), None),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(matches!(
        validate_children::<TreeFormat, TreeMutation>(&children, None, Some(&name(4)?)),
        Err(Error::ChildBoundsMismatch)
    ));
    assert!(validate_children::<TreeFormat, TreeMutation>(&[], None, Some(&name(1)?)).is_ok());
    Ok(())
}

#[test]
fn lower_layer_error_translation_is_total() {
    assert!(matches!(
        map_io::<TreeSemanticError>(persistent_io::Error::AllocationFailed),
        Error::AllocationFailed
    ));
    assert!(matches!(
        map_io::<TreeSemanticError>(persistent_io::Error::Allocation(
            AllocationError::CapacityExceeded
        )),
        Error::Allocation(AllocationError::CapacityExceeded)
    ));
    assert!(matches!(
        map_io::<TreeSemanticError>(persistent_io::Error::Storage(ObjectStoreError::Missing)),
        Error::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_io::<TreeSemanticError>(persistent_io::Error::Decode(
            CanonicalDecodeError::Truncated
        )),
        Error::Decode(CanonicalDecodeError::Truncated)
    ));
    assert!(matches!(
        map_io::<TreeSemanticError>(persistent_io::Error::Work(WorkError::Overflow)),
        Error::Work(WorkError::Overflow)
    ));
}
