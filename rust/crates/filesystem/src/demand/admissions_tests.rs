use super::*;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::performance::BOUNDARY_BUDGET;

#[tokio::test]
async fn read_then_bind_native_original_reconciles_without_another_payload_change()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace("bind-after-read", BOUNDARY_BUDGET, &cancellation)
        .await?
        .value;
    workspace
        .write("/entry", bytes::Bytes::from_static(b"before-bind"))
        .await?;
    let file = workspace
        .stat("/entry", BOUNDARY_BUDGET, &cancellation)
        .await?
        .value
        .file_id;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            BOUNDARY_BUDGET,
            &cancellation,
        )
        .await?
        .value;
    let read = checkout
        .read_file_range(
            file,
            crate::ByteRange {
                offset: 0,
                length: 11,
            },
            BOUNDARY_BUDGET,
            &cancellation,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"before-bind");
    let custody = checkout.private_control_custody();
    let retained = custody.retained_payload_bytes();
    assert!(retained > 0);
    assert!(!custody.native_accounting_pending());
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    assert!(custody.bind_native_original(&original.state));
    let mut observer = Observer {
        token: custody,
        paid: 0,
        retired: 0,
        native: true,
        non_native_bytes: 0,
    };
    let mut allocations = AllocationLedger::default();
    reconcile_native_observer(&mut observer, &mut allocations)?;
    assert_eq!(observer.non_native_bytes, retained);
    assert_eq!(observer_bytes(&observer), retained);
    assert_eq!(observer.retired, 0);
    assert!(!observer.token.native_accounting_pending());
    reconcile_native_observer(&mut observer, &mut allocations)?;
    assert_eq!(observer_bytes(&observer), retained);
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}

#[test]
fn native_observer_transition_requires_an_initial_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = Arc::new(ProofDataCustody::default());
    let mut header = original.claim_root_metadata(custody.logical_owned_bytes())?;
    original.register_native_checkout_control(&custody, &mut header)?;
    assert!(custody.native_accounting_pending());
    original.charge(WorkCounters::UNCHARGED)?;
    assert!(!custody.native_accounting_pending());
    assert_eq!(custody.native_control_events(), (0, 0));
    Ok(())
}
