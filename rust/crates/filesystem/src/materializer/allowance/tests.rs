use super::*;

#[test]
fn interrupted_admission_poisons_every_locked_resource_and_keeps_retirement_available() {
    let cancellation = crate::CancellationToken::new();
    let root = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let parent = root.native_volume_work(WorkBudget::UNBOUNDED).unwrap();
    let child = parent.native_volume_work(WorkBudget::UNBOUNDED).unwrap();
    let resource = child.resource.as_ref().unwrap();
    let before = root.receipt();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resource
            .admission(WorkCounters::UNCHARGED, 0, &mut || {
                panic!("interrupted original operation")
            })
            .unwrap();
    }));
    assert!(failure.is_err());
    for scope in [&parent, &child] {
        let resource = scope.resource.as_ref().unwrap();
        assert!(matches!(
            resource.remaining(),
            Err(DemandError::OriginalWorkPoisoned)
        ));
        assert!(matches!(
            resource.admit(WorkCounters::UNCHARGED, 0),
            Err(DemandError::OriginalWorkPoisoned)
        ));
        // Cleanup must still be possible without reviving the resource.
        resource.retire(0).unwrap();
    }
    assert_eq!(root.receipt(), before);
}
