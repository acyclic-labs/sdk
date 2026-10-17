use super::*;
use crate::model::{CheckoutMode, GenerationSelector};
use crate::performance::BOUNDARY_BUDGET;

proptest::proptest! {
    // Property: after every arbitrary owner/payload retirement and admitted
    // charge, live bytes equal the surviving allocations, while only the
    // requested item charge changes the completed-work receipt.
    #[test]
    fn arbitrary_retirement_traces_preserve_live_bytes_and_exact_charges(
        sizes in proptest::collection::vec(1_u16..257, 1..9),
        actions in proptest::collection::vec((proptest::bool::ANY, 0_usize..8), 0..65),
    ) {
        retirement_trace(&sizes, &actions)
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}

fn retirement_trace(sizes: &[u16], actions: &[(bool, usize)]) -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = original.live_allocation_bytes();
    let mut owners = Vec::with_capacity(sizes.len());
    let mut claims = Vec::with_capacity(sizes.len());
    for &bytes in sizes {
        let owner = native_custody(&original)?;
        let mut allocation = original.claim(u64::from(bytes), 1, WorkCounters::UNCHARGED)?;
        claims.push(Some(allocation.transfer_checkout_control(&owner, &owner)?));
        owners.push(Some(owner));
    }
    let mut expected_work = original.receipt();
    for &(payload, index) in actions {
        let index = index % sizes.len();
        if payload {
            drop(claims[index].take());
        } else {
            drop(owners[index].take());
        }
        original.charge(SCAN)?;
        expected_work.items_examined += 1;
        let headers = owners
            .iter()
            .zip(&claims)
            .filter(|(owner, claim)| owner.is_some() || claim.is_some())
            .count() as u64;
        let payload = claims
            .iter()
            .zip(sizes)
            .filter(|(claim, _)| claim.is_some())
            .map(|(_, &bytes)| u64::from(bytes))
            .sum::<u64>();
        let shared = if headers == 0 {
            0
        } else {
            NativeProofAccounting::ALLOCATION_BYTES
        };
        assert_eq!(
            original.live_allocation_bytes(),
            baseline + shared + headers * std::mem::size_of::<ProofDataCustody>() as u64 + payload
        );
        assert_eq!(original.receipt(), expected_work);
    }
    drop(claims);
    drop(owners);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), expected_work);
    Ok(())
}

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
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let shared = original.state.lock().proof_accounting()?;
    let baseline = original.live_allocation_bytes();
    assert!(custody.bind_native_original(&original.state, &shared));
    assert_eq!(shared.non_native.load(Ordering::Acquire), retained);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    let receipt = original.receipt();
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + retained);
    assert_eq!(original.receipt(), receipt);
    drop(read);
    drop(checkout);
    drop(custody);
    assert_eq!(shared.non_native.load(Ordering::Acquire), 0);
    drop(shared);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
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
    let shared = original
        .state
        .lock()
        .roots
        .proof_accounting
        .as_ref()
        .ok_or("missing proof accounting")?
        .clone();
    assert!(custody.is_registered_native_accounting(&shared));
    assert_eq!(shared.non_native.load(Ordering::Acquire), 0);
    original.charge(WorkCounters::UNCHARGED)?;
    assert!(!custody.has_native_control_events());
    Ok(())
}

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn native_custody(
    original: &crate::NativeMaterializationWork,
) -> Result<Arc<ProofDataCustody>, Box<dyn std::error::Error>> {
    let custody = Arc::new(ProofDataCustody::default());
    let mut header = original.claim_root_metadata(custody.logical_owned_bytes())?;
    original.register_native_checkout_control(&custody, &mut header)?;
    assert_eq!(header.bytes(), 0);
    Ok(custody)
}

#[test]
fn shared_accounting_charges_once_per_operation_for_any_owner_count() -> TestResult {
    for count in [1, 16, 128] {
        let cancellation = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut owners = Vec::with_capacity(count);
        let before = original.receipt();
        for _ in 0..count {
            owners.push(native_custody(&original)?);
        }
        let mut expected = before;
        expected.allocation_operations += count as u64 + 1;
        expected.peak_allocation_bytes = original.live_allocation_bytes();
        assert_eq!(original.receipt(), expected);
        let shared = original
            .state
            .lock()
            .roots
            .proof_accounting
            .as_ref()
            .ok_or("missing totals")?
            .clone();
        assert_eq!(Arc::strong_count(&shared), count + 2);
        let mut expected = original.receipt();
        for _ in 0..64 {
            original.charge(SCAN)?;
            expected.items_examined += 1;
            assert_eq!(original.receipt(), expected);
        }
        drop(shared);
        drop(owners);
        original.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        assert_eq!(original.receipt(), expected);
        assert!(original.state.lock().roots.proof_accounting.is_none());
    }
    Ok(())
}

#[test]
fn native_payload_outlives_checkout_and_retires_resource_exactly_once() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = native_custody(&original)?;
    let resource = original.native_volume_work(WorkBudget {
        peak_allocation_bytes: 16,
        ..BOUNDARY_BUDGET
    })?;
    let baseline = original.live_allocation_bytes();
    let mut allocation = resource.claim(16, 1, WorkCounters::UNCHARGED)?;
    let claim = allocation.transfer_checkout_control(&custody, &custody)?;
    let receipt = original.receipt();
    drop(custody);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + 16);
    assert_eq!(resource.remaining()?.peak_allocation_bytes, 0);
    assert!(resource.claim(1, 1, WorkCounters::UNCHARGED).is_err());
    drop(claim);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(resource.remaining()?.peak_allocation_bytes, 16);
    assert_eq!(
        original.live_allocation_bytes(),
        baseline
            - NativeProofAccounting::ALLOCATION_BYTES
            - std::mem::size_of::<ProofDataCustody>() as u64
    );
    assert_eq!(original.receipt(), receipt);
    drop(allocation);
    drop(resource);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[test]
fn final_native_payload_keeps_the_original_alive_until_retirement() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = native_custody(&original)?;
    let mut allocation = original.claim(16, 1, WorkCounters::UNCHARGED)?;
    let claim = allocation.transfer_checkout_data_bytes(16, &custody, &custody)?;
    let weak = Arc::downgrade(&original.state);
    drop(allocation);
    drop(custody);
    drop(original);
    assert!(weak.upgrade().is_some());
    drop(claim);
    assert_eq!(weak.strong_count(), 0);
    Ok(())
}

#[test]
fn prebound_ordinary_payload_keeps_custody_and_original_until_final_retirement() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = Arc::new(ProofDataCustody::default());
    let claim = crate::kernel::ProofDataClaim::new(Some(&custody), 16);
    let shared = original.state.lock().proof_accounting()?;
    assert!(custody.bind_native_original(&original.state, &shared));
    drop(shared);
    original.charge(WorkCounters::UNCHARGED)?;
    let weak = Arc::downgrade(&original.state);
    drop(custody);
    drop(original);
    let retained = weak.upgrade().ok_or("ordinary payload lost its original")?;
    assert_eq!(retained.lock().roots.paid_non_native, 16);
    drop(retained);
    drop(claim);
    assert_eq!(weak.strong_count(), 0);
    Ok(())
}

#[test]
fn wrong_original_and_cancelled_invocation_reject_registration_before_allocation() -> TestResult {
    let cancellation = CancellationToken::new();
    let first = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let second = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = native_custody(&first)?;
    let mut header = second.claim_root_metadata(custody.logical_owned_bytes())?;
    let receipt = second.receipt();
    let retained = second.live_allocation_bytes();
    assert!(
        second
            .register_native_checkout_control(&custody, &mut header)
            .is_err()
    );
    assert_eq!(second.receipt(), receipt);
    assert_eq!(second.live_allocation_bytes(), retained);
    assert_eq!(header.bytes(), custody.logical_owned_bytes());
    assert!(second.state.lock().roots.proof_accounting.is_none());
    let custody = Arc::new(ProofDataCustody::default());
    let (invocation, cancel) = second.invocation_view()?;
    cancel.cancel();
    let receipt = second.receipt();
    assert!(
        invocation
            .register_native_checkout_control(&custody, &mut header)
            .is_err()
    );
    assert_eq!(second.receipt(), receipt);
    assert_eq!(header.bytes(), custody.logical_owned_bytes());
    assert!(!custody.is_native_original(&second.state));
    assert!(second.state.lock().roots.proof_accounting.is_none());
    Ok(())
}

#[test]
fn non_native_birth_and_retirement_are_reconciled_as_live_bytes() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let custody = native_custody(&original)?;
    let baseline = original.live_allocation_bytes();
    let first = crate::kernel::ProofDataClaim::new(Some(&custody), 16);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + 16);
    let receipt = original.receipt();
    drop(first);
    let next = crate::kernel::ProofDataClaim::pending_control(&custody, 16);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + 16);
    assert_eq!(original.receipt(), receipt);
    drop(custody);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(original.live_allocation_bytes(), baseline + 16);
    drop(next);
    original.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(original.receipt(), receipt);
    Ok(())
}

#[test]
fn failed_shared_metadata_admission_keeps_header_and_owner_retryable() -> TestResult {
    let cancellation = CancellationToken::new();
    let denied = crate::NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 2,
            ..BOUNDARY_BUDGET
        },
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let custody = Arc::new(ProofDataCustody::default());
    let mut header = denied.claim_root_metadata(custody.logical_owned_bytes())?;
    let before = denied.receipt();
    let baseline = denied.live_allocation_bytes();
    assert!(
        denied
            .register_native_checkout_control(&custody, &mut header)
            .is_err()
    );
    assert_eq!(denied.receipt(), before);
    assert_eq!(denied.live_allocation_bytes(), baseline);
    assert_eq!(header.bytes(), custody.logical_owned_bytes());
    assert!(!custody.is_native_original(&denied.state));
    assert!(denied.state.lock().roots.proof_accounting.is_none());
    drop(header);
    let allowed = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut header = allowed.claim_root_metadata(custody.logical_owned_bytes())?;
    allowed.register_native_checkout_control(&custody, &mut header)?;
    assert_eq!(header.bytes(), 0);
    drop(header);
    drop(custody);
    allowed.charge(WorkCounters::UNCHARGED)?;
    assert_eq!(
        allowed.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}

#[test]
fn concurrent_final_owner_retirement_never_strands_shared_metadata() -> TestResult {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    for _ in 0..128 {
        let custody = native_custody(&original)?;
        let mut allocation = original.claim(16, 1, WorkCounters::UNCHARGED)?;
        let claim = allocation.transfer_checkout_control(&custody, &custody)?;
        drop(custody);
        drop(allocation);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let dropping = barrier.clone();
        let before = original.receipt();
        let thread = std::thread::spawn(move || {
            dropping.wait();
            drop(claim);
        });
        barrier.wait();
        original.charge(SCAN)?;
        thread.join().map_err(|_| "retirement panicked")?;
        original.charge(WorkCounters::UNCHARGED)?;
        assert_eq!(
            original.live_allocation_bytes(),
            crate::NativeMaterializationWork::accounting_state_bytes()
        );
        let mut expected = before;
        expected.items_examined += 1;
        assert_eq!(original.receipt(), expected);
    }
    Ok(())
}
