use super::*;
use crate::kernel::{
    Dependency, DependencyState, FileRecord, InlineFileData, LogicalName, NameEncoding, TreeEntry,
};
use crate::kernel::{ExtentSeekTarget, FilePayload};
use crate::{Digest, FileId, ObjectId, ObjectKind};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
type Proof<T = ()> = Result<T, TestCaseError>;
fn failure(error: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(error.to_string())
}

fn id(value: u8) -> FileId {
    FileId::from_bytes([value; 16])
}
fn object(kind: ObjectKind, value: u8) -> ObjectId {
    ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes([value; 32]),
    }
}
fn name(value: u8) -> Proof<LogicalName> {
    LogicalName::new(NameEncoding::Utf8, vec![b'a' + value % 26], 255).map_err(failure)
}
fn file(value: u8, content: u8) -> Proof<FileRecord> {
    Ok(FileRecord {
        file_id: id(value),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, value),
        payload: FilePayload::InlineRegular(InlineFileData::new(&[content]).map_err(failure)?),
    })
}
fn directory(value: u8, tree: u8) -> FileRecord {
    FileRecord {
        file_id: id(value),
        kind: FileKind::Directory,
        link_count: 1,
        metadata: object(ObjectKind::Metadata, value),
        payload: FilePayload::Directory {
            entries: object(ObjectKind::TreePage, tree),
        },
    }
}
fn reads(regions: impl IntoIterator<Item = DependencyRegion>) -> Proof<CheckoutDependencies> {
    CheckoutDependencies::new(
        regions.into_iter().map(|region| Dependency {
            region,
            expected: DependencyState::Absent,
        }),
        [],
        100,
    )
    .map_err(failure)
}
fn empty() -> GenerationDiff {
    GenerationDiff {
        files: Vec::new(),
        bindings: Vec::new(),
        truncated: false,
    }
}
fn binding(parent: u8, component: u8) -> Proof<DependencyRegion> {
    Ok(DependencyRegion::DirectoryName {
        directory_id: id(parent),
        name: name(component)?,
    })
}
fn edge(
    parent: u8,
    component: u8,
    before: Option<u8>,
    after: Option<u8>,
) -> Proof<DirectoryBindingChange> {
    let entry = |file| -> Proof<TreeEntry> {
        Ok(TreeEntry {
            name: name(component)?,
            file_id: id(file),
            kind: FileKind::Directory,
        })
    };
    Ok(DirectoryBindingChange {
        directory_id: id(parent),
        name: name(component)?,
        before: before.map(entry).transpose()?,
        after: after.map(entry).transpose()?,
    })
}
fn disjoint(reads: &CheckoutDependencies, writes: &GenerationDiff) -> bool {
    CommitFootprint {
        reads: Some(reads),
        writes: &empty(),
    }
    .independent(
        CommitFootprint {
            reads: None,
            writes,
        },
        u64::MAX,
    )
    .independent
        == Ok(true)
}
fn independent(
    a: (&CheckoutDependencies, &GenerationDiff),
    b: (&CheckoutDependencies, &GenerationDiff),
) -> bool {
    CommitFootprint {
        reads: Some(a.0),
        writes: a.1,
    }
    .independent(
        CommitFootprint {
            reads: Some(b.0),
            writes: b.1,
        },
        u64::MAX,
    )
    .independent
        == Ok(true)
}

#[test]
fn ancestor_delete_and_cross_directory_rename_overlap_child_edits() -> Proof {
    let edit_reads = reads([binding(1, 0)?, binding(2, 1)?])?;
    let edit = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(3),
            before: Some(file(3, 0)?),
            after: Some(file(3, 1)?),
        }],
        ..empty()
    };
    for bindings in [
        vec![edge(1, 0, Some(2), None)?],
        vec![edge(1, 0, Some(2), None)?, edge(4, 2, None, Some(2))?],
    ] {
        let moved = GenerationDiff {
            bindings,
            ..empty()
        };
        assert!(!independent((&edit_reads, &edit), (&reads([])?, &moved)));
        assert!(!independent((&reads([])?, &moved), (&edit_reads, &edit)));
    }
    Ok(())
}

#[test]
fn same_new_directory_overlaps_but_existing_sibling_directories_do_not() -> Proof {
    let proposals = [5, 6].map(|child| -> Proof<_> {
        Ok(GenerationDiff {
            files: vec![FileRecordChange {
                file_id: id(child),
                before: None,
                after: Some(directory(child, child)),
            }],
            bindings: vec![edge(1, 0, None, Some(child))?],
            ..empty()
        })
    });
    let [a, b] = proposals;
    let (a, b) = (a?, b?);
    assert!(!independent(
        (&reads([binding(1, 0)?])?, &a),
        (&reads([binding(1, 0)?])?, &b)
    ));
    let sibling = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(2),
            before: Some(directory(2, 0)),
            after: Some(directory(2, 1)),
        }],
        bindings: vec![edge(2, 1, None, Some(8))?],
        ..empty()
    };
    let other = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(2),
            before: Some(directory(2, 0)),
            after: Some(directory(2, 2)),
        }],
        bindings: vec![edge(2, 2, None, Some(9))?],
        ..empty()
    };
    assert!(independent(
        (&reads([binding(2, 1)?])?, &sibling),
        (&reads([binding(2, 2)?])?, &other)
    ));
    Ok(())
}

#[test]
fn aliases_ranges_negative_lookups_and_incomplete_changes_fail_closed() -> Proof {
    let change = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(3),
            before: Some(file(3, 0)?),
            after: Some(file(3, 1)?),
        }],
        ..empty()
    };
    for region in [
        DependencyRegion::FileRecord(id(3)),
        DependencyRegion::ContentRange {
            file_id: id(3),
            offset: 0,
            length: 1,
        },
    ] {
        assert!(!disjoint(&reads([region])?, &change));
    }
    let added = GenerationDiff {
        bindings: vec![edge(2, 0, None, Some(3))?],
        ..empty()
    };
    assert!(!disjoint(&reads([binding(2, 0)?])?, &added));
    assert!(!disjoint(
        &reads([DependencyRegion::DirectoryRange {
            directory_id: id(2),
            after: None,
            maximum_entries: 1
        }])?,
        &added
    ));
    assert!(!disjoint(
        &reads([])?,
        &GenerationDiff {
            truncated: true,
            ..empty()
        }
    ));
    Ok(())
}

#[test]
fn indexed_query_visits_every_region_kind_among_unrelated_file_ids() -> Proof {
    for target in 1..8 {
        let changed = GenerationDiff {
            files: vec![FileRecordChange {
                file_id: id(target),
                before: None,
                after: Some(file(target, 1)?),
            }],
            ..empty()
        };
        for unrelated in 1..8 {
            if unrelated == target {
                continue;
            }
            for region in [
                DependencyRegion::FileRecord(id(target)),
                DependencyRegion::Metadata(id(target)),
                DependencyRegion::FileLength(id(target)),
                DependencyRegion::ContentRange {
                    file_id: id(target),
                    offset: 0,
                    length: 1,
                },
                DependencyRegion::SparseSeek {
                    file_id: id(target),
                    offset: 0,
                    target: ExtentSeekTarget::Data,
                },
                binding(target, 0)?,
                DependencyRegion::DirectoryRange {
                    directory_id: id(target),
                    after: None,
                    maximum_entries: 1,
                },
            ] {
                let ledger = reads([
                    DependencyRegion::FileRecord(id(unrelated)),
                    DependencyRegion::Metadata(id(unrelated)),
                    region,
                ])?;
                assert!(!disjoint(&ledger, &changed));
            }
        }
    }
    Ok(())
}

#[test]
fn footprint_query_charges_the_exact_bounded_prefix() -> Proof {
    let ledger = reads([
        DependencyRegion::Metadata(id(3)),
        DependencyRegion::FileLength(id(3)),
    ])?;
    let changes = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(3),
            before: Some(file(3, 0)?),
            after: Some(file(3, 1)?),
        }],
        ..empty()
    };
    let query = |maximum| {
        CommitFootprint {
            reads: Some(&ledger),
            writes: &empty(),
        }
        .independent(
            CommitFootprint {
                reads: None,
                writes: &changes,
            },
            maximum,
        )
    };
    let total = query(u64::MAX).items_examined;
    assert_eq!(total, 3);
    for maximum in 0..=total {
        let observed = query(maximum);
        assert_eq!(observed.items_examined, maximum);
        if maximum == total {
            assert_eq!(observed.independent, Ok(false));
        } else {
            assert_eq!(
                observed.independent,
                Err(WorkError::BudgetExceeded {
                    counter: "items_examined",
                    observed: maximum + 1,
                    maximum,
                })
            );
        }
    }
    Ok(())
}

#[test]
fn footprint_query_cost_is_independent_of_unrelated_reads() -> Proof {
    let changes = GenerationDiff {
        files: vec![FileRecordChange {
            file_id: id(3),
            before: Some(file(3, 0)?),
            after: Some(file(3, 1)?),
        }],
        ..empty()
    };
    let sparse = reads([DependencyRegion::FileLength(id(3))])?;
    let dense = reads((1..96).map(|value| {
        if value == 3 {
            DependencyRegion::FileLength(id(value))
        } else {
            DependencyRegion::Metadata(id(value))
        }
    }))?;
    for ledger in [&sparse, &dense] {
        let observed = CommitFootprint {
            reads: Some(ledger),
            writes: &empty(),
        }
        .independent(
            CommitFootprint {
                reads: None,
                writes: &changes,
            },
            u64::MAX,
        );
        assert_eq!(observed.independent, Ok(false));
        assert_eq!(observed.items_examined, 2);
    }
    Ok(())
}

#[test]
fn incomplete_or_unordered_footprints_cannot_certify_independence() -> Proof {
    for keys in [vec![3, 2], vec![3, 3]] {
        let files = keys
            .into_iter()
            .map(|key| -> Proof<_> {
                Ok(FileRecordChange {
                    file_id: id(key),
                    before: Some(file(key, 0)?),
                    after: Some(file(key, 1)?),
                })
            })
            .collect::<Proof<Vec<_>>>()?;
        let writes = GenerationDiff { files, ..empty() };
        assert!(!independent(
            (&reads([])?, &writes),
            (&reads([])?, &empty())
        ));
    }
    Ok(())
}

#[test]
fn delta_custody_includes_both_binding_values_and_the_index_key() -> Proof {
    let change = edge(1, 0, Some(2), Some(3))?;
    let names = change.name.retained_bytes()
        + change
            .before
            .as_ref()
            .map_or(0, |entry| entry.name.retained_bytes())
        + change
            .after
            .as_ref()
            .map_or(0, |entry| entry.name.retained_bytes());
    let changes = GenerationDiff {
        bindings: vec![change],
        ..empty()
    };
    let buffer = changes.bindings.capacity() * std::mem::size_of::<DirectoryBindingChange>();
    assert_eq!(
        changes.retained_bytes().map_err(failure)?,
        (buffer + names) as u64
    );
    // The former closure estimator retained only the key, losing both values.
    assert_ne!(
        (buffer
            + changes
                .bindings
                .first()
                .map_or(0, |entry| entry.name.retained_bytes())) as u64,
        changes.retained_bytes().map_err(failure)?
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn independent_edits_commute_and_have_the_serial_tree(a in 1u8..32, b in 1u8..32, x in any::<u8>(), y in any::<u8>()) {
        let delta = |key, content| -> Proof<_> { Ok(GenerationDiff { files: vec![FileRecordChange {
            file_id: id(key), before: Some(file(key, 0)?), after: Some(file(key, content)?),
        }], ..empty() }) };
        let left = delta(a, x)?;
        let right = delta(b, y)?;
        let left_reads = reads([DependencyRegion::FileRecord(id(a))])?;
        let right_reads = reads([DependencyRegion::FileRecord(id(b))])?;
        let admitted = independent((&left_reads, &left), (&right_reads, &right));
        prop_assert_eq!(admitted, a != b);
        if admitted {
            let mut forward = std::collections::BTreeMap::new();
            let mut reverse = std::collections::BTreeMap::new();
            for change in left.files.iter().chain(&right.files) { forward.insert(change.file_id, change.after); }
            for change in right.files.iter().chain(&left.files) { reverse.insert(change.file_id, change.after); }
            prop_assert_eq!(forward, reverse);
        }
    }
}
