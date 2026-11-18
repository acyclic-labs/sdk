use super::*;

#[test]
fn batch_error_translation_is_total() {
    use persistent_batch::Error as BatchError;

    assert!(matches!(
        map_batch_error(BatchError::Cancelled),
        TreeReadError::Cancelled
    ));
    assert!(matches!(
        map_batch_error(BatchError::Empty),
        TreeReadError::EmptyBatch
    ));

    assert!(matches!(
        map_batch_error(BatchError::WrongRootKind),
        TreeReadError::WrongRootKind
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidLimits),
        TreeReadError::InvalidHeightLimit
    ));
    assert!(matches!(
        map_batch_error(BatchError::HeightExceeded),
        TreeReadError::HeightExceeded
    ));
    assert!(matches!(
        map_batch_error(BatchError::CycleOrAlias),
        TreeReadError::Cycle
    ));
    assert!(matches!(
        map_batch_error(BatchError::ChildBoundsMismatch),
        TreeReadError::ChildBoundsMismatch
    ));
    assert!(matches!(
        map_batch_error(BatchError::InvalidRouting),
        TreeReadError::InvalidRouting
    ));
    assert!(matches!(
        map_batch_error(BatchError::AllocationFailed),
        TreeReadError::AllocationFailed
    ));
    assert!(matches!(
        map_batch_error(BatchError::Storage(ObjectStoreError::Missing)),
        TreeReadError::Storage(ObjectStoreError::Missing)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Decode(CanonicalDecodeError::LengthOverflow)),
        TreeReadError::Decode(CanonicalDecodeError::LengthOverflow)
    ));
    assert!(matches!(
        map_batch_error(BatchError::Work(WorkError::Overflow)),
        TreeReadError::Work(WorkError::Overflow)
    ));
}
