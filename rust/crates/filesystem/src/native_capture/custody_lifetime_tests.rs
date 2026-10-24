use super::*;

#[test]
fn scope_shutdown_closes_retained_meters_without_waiting_for_their_destruction() {
    let root =
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap();
    let before = root.live_allocation_bytes();
    let mut scope = CaptureWorkScope::new(&root).unwrap();
    let stale = scope.native.clone();
    let owner = scope.native.claim(17, 1, WorkCounters::UNCHARGED).unwrap();
    // A transient allocation is drained by shutdown; the owned allocation and
    // closed backing remain funded until their final handle retires.
    scope
        .native
        .with_original_leaf(|work| work.claim_allocation(3, 1, WorkCounters::UNCHARGED))
        .unwrap();
    scope.release().unwrap();
    assert!(
        scope
            .native
            .with_original_leaf(|work| work.check())
            .is_err()
    );
    assert!(stale.with_original_leaf(|work| work.check()).is_err());
    assert_eq!(
        root.live_allocation_bytes(),
        before + 17 + crate::demand::CaptureTransientCustody::ALLOCATION_BYTES
    );
    scope.release().unwrap();
    drop(scope);
    drop(owner);
    assert_eq!(
        root.live_allocation_bytes(),
        before + crate::demand::CaptureTransientCustody::ALLOCATION_BYTES
    );
    drop(stale);
    assert_eq!(root.live_allocation_bytes(), before);
}
