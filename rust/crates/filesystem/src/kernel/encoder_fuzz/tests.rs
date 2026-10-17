use super::*;
use proptest::prelude::*;

fn object(kind: crate::ObjectKind, number: u8) -> crate::ObjectId {
    crate::ObjectId {
        kind,
        digest: crate::Digest::from_bytes([number; 32]),
    }
}
fn file(payload: &[u8]) -> FileRecord {
    FileRecord {
        file_id: crate::FileId::from_bytes([2; 16]),
        kind: FileKind::Regular,
        link_count: 1,
        metadata: object(crate::ObjectKind::Metadata, 3),
        payload: FilePayload::InlineRegular(InlineFileData::new(payload).unwrap()),
    }
}
fn fixture(kind: u8, payload: &[u8], number: u64) -> Vec<u8> {
    let mut observe = |_| Ok::<_, FsError>(());
    let fail = |_| FsError::from(CanonicalDecodeError::AllocationFailed);
    let name = || {
        LogicalName::new(
            NameEncoding::Utf8,
            format!("n{number:016x}").into_bytes(),
            256,
        )
        .unwrap()
    };
    let attribute = || {
        AttributeName::new(
            AttributeClass::PosixXattr,
            format!("a{number:016x}").into_bytes(),
            256,
        )
        .unwrap()
    };
    let internal = number & 1 != 0;
    match kind {
        0 => encode_file_metadata(
            FileMetadata {
                posix_mode: MetadataField::Value((number as u32) & 0o777),
                acl: MetadataField::Value(object(crate::ObjectKind::Blob, 3)),
                ..FileMetadata::default()
            },
            &mut observe,
            fail,
        ),
        1 => encode_generation_root(
            &GenerationRoot {
                volume_id: crate::VolumeId::from_bytes([1; 16]),
                root_file_id: crate::FileId::from_bytes([2; 16]),
                file_table: object(crate::ObjectKind::FileTablePage, 3),
                parents: (0..number % 3)
                    .map(|i| crate::GenerationId::new(crate::Digest::from_bytes([i as u8; 32])))
                    .collect(),
                required_features: 0,
            },
            &mut observe,
            fail,
        ),
        2 => {
            let page = if internal {
                FileTablePage::Internal(vec![Child {
                    first: crate::FileId::from_bytes([1; 16]),
                    page: object(crate::ObjectKind::FileTablePage, 2),
                }])
            } else {
                FileTablePage::Leaf(vec![file(payload)])
            };
            encode_file_table_page(&page, 32, &mut observe, fail)
        }
        3 => {
            let page = if internal {
                TreePage::Internal(vec![Child {
                    first: name(),
                    page: object(crate::ObjectKind::TreePage, 2),
                }])
            } else {
                TreePage::Leaf(vec![TreeEntry {
                    name: name(),
                    file_id: crate::FileId::from_bytes([1; 16]),
                    kind: FileKind::Regular,
                }])
            };
            encode_tree_page(&page, 32, &mut observe, fail)
        }
        4 => {
            let page = if internal {
                ExtentPage::Internal(vec![ExtentChild {
                    first_offset: 0,
                    end_offset: 1,
                    page: object(crate::ObjectKind::ExtentPage, 2),
                }])
            } else {
                ExtentPage::Leaf(vec![Extent {
                    offset: 0,
                    length: number % 65 + 1,
                    kind: ExtentKind::Hole,
                }])
            };
            encode_extent_page(&page, 32, &mut observe, fail)
        }
        5 => {
            let page = if internal {
                AttributePage::Internal(vec![Child {
                    first: attribute(),
                    page: object(crate::ObjectKind::AttributePage, 2),
                }])
            } else {
                AttributePage::Leaf(vec![AttributeEntry {
                    name: attribute(),
                    value_bytes: number,
                    value: object(crate::ObjectKind::Blob, 2),
                }])
            };
            encode_attribute_page(&page, 32, &mut observe, fail)
        }
        6 => {
            let node = if internal {
                BlobNode::Internal(vec![BlobChild {
                    first_offset: 0,
                    end_offset: 1,
                    page: object(crate::ObjectKind::Blob, 2),
                }])
            } else {
                BlobNode::Leaf(vec![BlobChunkRef {
                    first_offset: 0,
                    end_offset: 1,
                    chunk: object(crate::ObjectKind::BlobChunk, 2),
                }])
            };
            encode_blob_page(
                &BlobPage {
                    first_offset: 0,
                    end_offset: 1,
                    node,
                },
                32,
                &mut observe,
                fail,
            )
        }
        7 => super::super::volume::encode_volume_created(
            VolumeCreated {
                volume_id: crate::VolumeId::from_bytes([1; 16]),
                config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                initial_generation_root: object(crate::ObjectKind::GenerationRoot, 2),
            },
            &mut observe,
            fail,
        ),
        8 => super::super::retention::encode_retention_created(
            &RetentionCreated {
                volume_id: crate::VolumeId::from_bytes([1; 16]),
                kind: RetentionKind::Checkpoint,
                label: format!("r{number:016x}"),
                generation_root: object(crate::ObjectKind::GenerationRoot, 2),
                config: crate::VolumeConfig::portable(crate::Lifecycle::Durable),
            },
            &mut observe,
            fail,
        ),
        9 => super::super::retention::encode_workspace_deleted(
            crate::VolumeId::from_bytes([1; 16]),
            &mut observe,
            fail,
        ),
        _ => super::super::file_table::encode_file_record(file(payload), &mut observe, fail),
    }
    .unwrap()
}

fn input(kind: u8, mode: u8, wire: &[u8]) -> Vec<u8> {
    let mut input = vec![kind, 255, 255, 255, 255, 255, mode, 0, 0, 255];
    input.extend_from_slice(wire);
    input
}
fn modes() -> impl Iterator<Item = u8> {
    [
        0,
        #[cfg(not(target_arch = "wasm32"))]
        1,
        #[cfg(not(target_arch = "wasm32"))]
        2,
        #[cfg(not(target_arch = "wasm32"))]
        3,
        #[cfg(not(target_arch = "wasm32"))]
        4,
    ]
    .into_iter()
}

#[test]
fn every_writer_shape_round_trips_with_exact_completed_work() {
    for kind in 0..11 {
        for variant in 0..2 {
            let wire = fixture(kind, b"payload", variant);
            for mode in modes() {
                check(&input(kind, mode, &wire));
            }
            // The gate captures these canonical cases as checked-in fuzz seeds.
            eprintln!(
                "encoder_seed {kind}-{variant} {}",
                hex::encode(input(kind, 1, &wire))
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn generated_canonical_inputs_preserve_exact_budget_prefixes_and_refunds(
        kind in 0_u8..11, payload in prop::collection::vec(any::<u8>(), 0..65), number in any::<u64>(),
        encoded in 0_u16..512, copied in 0_u16..256, items in 0_u8..32, pages in 0_u8..3,
        fault in 0_u8..4, at in 0_u8..96,
    ) {
        let wire = fixture(kind, &payload, number);
        for mode in modes() {
            let mut data = input(kind, mode, &wire);
            check(&data);
            // Every relevant dimension succeeds at its exact complete receipt.
            let (_, receipt, _, _) = reference(kind, &wire, WorkBudget::UNBOUNDED, mode >= 3, 0, usize::MAX);
            for axis in [1,3,5,9] {
                let exact = match axis { 1 => receipt.bytes_encoded, 3 => receipt.bytes_copied, 5 => receipt.items_examined, _ => receipt.page_writes };
                if axis == 1 || axis == 3 { data[axis..axis+2].copy_from_slice(&(exact as u16).to_le_bytes()); }
                else { data[axis] = exact as u8; }
                check(&data);
                if exact != 0 {
                    if axis == 1 || axis == 3 { data[axis..axis+2].copy_from_slice(&((exact-1) as u16).to_le_bytes()); }
                    else { data[axis] = (exact-1) as u8; }
                    check(&data);
                }
                data = input(kind, mode, &wire);
            }
            data[1..3].copy_from_slice(&encoded.to_le_bytes());
            data[3..5].copy_from_slice(&copied.to_le_bytes()); data[5] = items; data[9] = pages;
            data[7] = fault; data[8] = at;
            check(&data);
        }
    }
    #[test]
    fn malformed_wire_lengths_and_truncation_remain_bounded(
        wire in prop::collection::vec(any::<u8>(), 0..512), controls in any::<[u8;10]>(),
    ) {
        let mut data = controls.to_vec(); data.extend_from_slice(&wire); check(&data);
    }
}

#[test]
fn missing_completion_is_detected_by_the_independent_receipt() {
    let wire = fixture(0, &[], 0);
    for mode in modes() {
        let panic = std::panic::catch_unwind(|| check_inner::<1, true>(&input(0, mode, &wire)))
            .expect_err("omitting completed work must fail the oracle");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("exact independent encoding receipt"),
            "{message}"
        );
    }
}

fn prefix_control<const MUTANT: u8>() {
    let wire = fixture(0, &[], 0);
    for mode in modes() {
        let mut data = input(0, mode, &wire);
        data[7] = 2;
        data[8] = 7; // Cancel at the real header completion.
        check(&data);
        let panic = std::panic::catch_unwind(|| check_inner::<MUTANT, true>(&data))
            .expect_err("wrong error position must fail the prefix oracle");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("independent encoding failure prefix"),
            "{message}"
        );
        check_inner::<MUTANT, false>(&data);
    }
}
#[test]
fn wrong_error_index_requires_the_prefix_assertion() {
    prefix_control::<2>();
}
#[test]
fn wrong_error_phase_requires_the_prefix_assertion() {
    prefix_control::<3>();
}
