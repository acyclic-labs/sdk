use super::*;
use crate::kernel::Extent;
use crate::memory::MemoryObjectStore;
use crate::test_support::OwnedReadObjectStore;
use std::sync::atomic::Ordering;

use crate::test_support::put_extent_page as put_page;

#[tokio::test]
async fn owned_extent_pages_preserve_sparse_results_and_exact_retention()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = put_page(
        &store.inner,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::AllocatedZero,
        }]),
        16,
    )
    .await?;
    let request = ExtentRangeRequest {
        root,
        file_size: 8,
        range: ByteRange {
            offset: 2,
            length: 4,
        },
        maximum_spans: Some(1),
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let plan = (plan_extent_range(&store, request, &CancellationToken::new())).await?;
    assert_eq!(plan.spans.len(), 1);
    assert_eq!(plan.spans[0].offset, 2);
    assert_eq!(plan.spans[0].length, 4);
    assert!(plan.work.bytes_copied > 0);
    Ok(())
}

#[tokio::test]
async fn owned_extent_page_releases_retention_when_decoded_cache_rejects_admission()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = put_page(
        &store.inner,
        &ExtentPage::Leaf(vec![Extent {
            offset: 0,
            length: 8,
            kind: ExtentKind::AllocatedZero,
        }]),
        16,
    )
    .await?;
    store
        .reject_decoded_admission
        .store(true, Ordering::Relaxed);
    let failure = (plan_extent_range(
        &store,
        ExtentRangeRequest {
            root,
            file_size: 8,
            range: ByteRange {
                offset: 0,
                length: 8,
            },
            maximum_spans: Some(1),
            limits: DecodeLimits::default(),
            budget: WorkBudget::UNBOUNDED,
        },
        &CancellationToken::new(),
    ))
    .await
    .err()
    .ok_or("decoded-cache rejection unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::Storage(ObjectStoreError::Corrupt)
    ));
    assert!(failure.work.as_deref().unwrap().peak_allocation_bytes > 0);
    Ok(())
}

#[test]
fn range_terminal_and_lower_layer_error_translation_is_total()
-> Result<(), Box<dyn std::error::Error>> {
    for error in [AllocationError::Overflow, AllocationError::ReleaseInvariant] {
        assert!(matches!(
            ExtentReadError::from(error),
            ExtentReadError::Work(WorkError::Overflow)
        ));
    }
    assert!(matches!(
        ExtentReadError::from(AllocationError::CapacityExceeded),
        ExtentReadError::AllocationFailed
    ));

    let root = ObjectId {
        storage: None,
        kind: ObjectKind::ExtentPage,
        digest: crate::foundation::Digest::from_bytes([91; 32]),
    };
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let request = ExtentRangeRequest {
        root,
        file_size: 4,
        range: ByteRange {
            offset: 0,
            length: 4,
        },
        maximum_spans: Some(1),
        limits: DecodeLimits::default(),
        budget: WorkBudget::UNBOUNDED,
    };
    let machine = RangeMachine::new(
        request,
        TraversalMode::Plan,
        std::convert::identity::<ExtentSlice>,
        &mut accounting,
    )?;
    let incomplete = machine
        .finish_plan(&mut accounting)
        .err()
        .ok_or("incomplete range machine produced a plan")?;
    assert!(matches!(
        range_error(incomplete),
        ExtentReadError::IncompleteCoverage
    ));

    let cancelled = range_error(crate::FsError::Cancelled(
        crate::cancellation::CancellationError,
    ));
    assert!(matches!(cancelled, ExtentReadError::Cancelled));

    let prospective = WorkCounters {
        object_probes: u64::MAX,
        ..WorkCounters::default()
    };
    if let KernelAccounting::Standalone { work, .. } = &mut accounting {
        *work = prospective;
    }
    let overflow = accounting
        .record_completed(WorkCounters {
            object_probes: 1,
            ..WorkCounters::default()
        })
        .err()
        .ok_or("overflowing backend prefix was accepted")?;
    assert!(matches!(
        range_error(overflow),
        ExtentReadError::Work(WorkError::Overflow)
    ));
    assert_eq!(accounting.spent().unwrap(), prospective);
    Ok(())
}

#[tokio::test]
async fn decoded_pages_and_output_spans_are_admitted_before_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let root = put_page(
        &store,
        &ExtentPage::Leaf(vec![
            Extent {
                offset: 0,
                length: 1,
                kind: ExtentKind::Hole,
            },
            Extent {
                offset: 1,
                length: 1,
                kind: ExtentKind::AllocatedZero,
            },
            Extent {
                offset: 2,
                length: 1,
                kind: ExtentKind::Hole,
            },
        ]),
        16,
    )
    .await?;
    let limits = DecodeLimits {
        maximum_page_height: 1,
        ..DecodeLimits::default()
    };
    let request = ExtentRangeRequest {
        root,
        file_size: 3,
        range: ByteRange {
            offset: 0,
            length: 3,
        },
        maximum_spans: Some(3),
        limits,
        budget: WorkBudget::UNBOUNDED,
    };
    let baseline = plan_extent_range(&store, request, &CancellationToken::new()).await?;
    assert_eq!(
        baseline.retained_allocation_bytes,
        u64::try_from(3 * size_of::<ExtentSlice>())?
    );
    let maximum = baseline.work.peak_allocation_bytes - 1;
    let failure = plan_extent_range(
        &store,
        ExtentRangeRequest {
            budget: WorkBudget {
                peak_allocation_bytes: maximum,
                ..WorkBudget::UNBOUNDED
            },
            ..request
        },
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("one-byte-short range allocation budget unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        ExtentReadError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            observed,
            maximum: observed_maximum,
        }) if observed == baseline.work.peak_allocation_bytes && observed_maximum == maximum
    ));
    assert_eq!(failure.work.as_deref().unwrap().backend_read_operations, 1);
    assert_eq!(failure.work.as_deref().unwrap().backend_write_operations, 0);
    Ok(())
}
