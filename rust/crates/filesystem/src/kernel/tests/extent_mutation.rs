use super::*;
use crate::async_storage::AsyncObjectStore;
use crate::foundation::Digest;
use crate::kernel::extent::extent_page_encoded_length;
use crate::kernel::{ExtentChild, decode_extent_page};
use crate::memory::MemoryObjectStore;

use crate::storage::object_digest;

use crate::test_support::put_extent_page as put;

#[path = "../../../tests/support/sparse_extents.rs"]
mod sparse_extents;
use sparse_extents::{allocated, hole};

#[tokio::test]
async fn page_encoding_and_hashing_are_separately_admitted_before_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = CancellationToken::new();
    let page = ExtentPage::Leaf(Vec::new());
    let encoded_length = u64::try_from(extent_page_encoded_length(&page, 1)?)?;
    let mut encode_limited = KernelAccounting::standalone(
        WorkBudget {
            bytes_encoded: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let encode_initial_work = encode_limited.spent().unwrap();
    let encode_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits::default(),
        &mut encode_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero encoding budget unexpectedly succeeded")?;
    assert!(matches!(project_extent_error(encode_error),
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_encoded", observed, maximum: 0,
        }) if observed == encoded_length
    ));
    assert_eq!(encode_limited.spent().unwrap(), encode_initial_work);

    let mut hash_limited = KernelAccounting::standalone(
        WorkBudget {
            bytes_hashed: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let hash_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits::default(),
        &mut hash_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("zero hash budget unexpectedly succeeded")?;
    assert!(matches!(project_extent_error(hash_error),
        ExtentMutationError::Work(WorkError::BudgetExceeded {
            counter: "bytes_hashed", observed, maximum: 0,
        }) if observed == encoded_length
    ));
    assert_eq!(hash_limited.spent().unwrap().bytes_encoded, encoded_length);
    assert_eq!(hash_limited.spent().unwrap().bytes_hashed, 0);
    assert_eq!(hash_limited.spent().unwrap().backend_write_operations, 0);

    let mut byte_limited = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let byte_error = (put_extent_page_with_accounting(
        &store,
        &page,
        DecodeLimits {
            maximum_object_bytes: 1,
            ..DecodeLimits::default()
        },
        &mut byte_limited,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("one-byte page ceiling unexpectedly admitted an extent page")?;
    assert!(matches!(project_extent_error(byte_error),
        ExtentMutationError::Decode(CanonicalDecodeError::ObjectTooLarge {
            observed, maximum: 1,
        }) if observed == encoded_length
    ));
    assert_eq!(byte_limited.spent().unwrap().backend_write_operations, 0);

    Ok(())
}

#[test]
fn overlapping_ordered_edits_normalize_content_offsets_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    let blob = ObjectId {
        storage: None,
        kind: ObjectKind::Blob,
        digest: Digest::from_bytes([8; 32]),
    };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let plan = compile_patch_plan(
        100,
        &[
            ExtentMutation::Replace {
                offset: 10,
                length: 50,
                kind: ExtentKind::Content {
                    object: blob,
                    object_offset: 100,
                },
                extend: false,
            },
            ExtentMutation::Replace {
                offset: 20,
                length: 10,
                kind: ExtentKind::AllocatedZero,
                extend: false,
            },
        ],
        &mut accounting,
    )?;
    let patches = &plan.patches.value;
    assert_eq!(plan.final_size, 100);
    assert_eq!(patches.len(), 3);
    assert_eq!((patches[0].offset, patches[0].end), (10, 20));
    assert_eq!(
        patches[0].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 100,
        }
    );
    assert_eq!(patches[1].kind, ExtentKind::AllocatedZero);
    assert_eq!((patches[2].offset, patches[2].end), (30, 60));
    assert_eq!(
        patches[2].kind,
        ExtentKind::Content {
            object: blob,
            object_offset: 120,
        }
    );
    Ok(())
}

#[path = "extent_mutation/histories.rs"]
mod histories;
