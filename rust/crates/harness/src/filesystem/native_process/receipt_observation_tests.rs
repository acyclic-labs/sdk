//! Controls exercise the actual closed receipt decoder and original ledger.

use super::receipt_observation::{decode, receipt_bytes};
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget, WorkCounters};

fn observed_record() -> Vec<u8> {
    let record = Receipt::Observed(Box::new(NativeObservation {
        effect: EffectObservation {
            provider: "native-process".into(),
            effect_id: crate::EffectId::from_bytes([1; 16]),
            attempt_id: crate::EffectAttemptId::from_bytes([2; 16]),
            request_digest: [3; 32], guarantee: EffectGuarantee::IdempotentRetry,
            status: EffectStatus::Indeterminate,
        },
        diagnostic: None, publication_failure: None,
        invocation_failure: Some("original physical failure".into()),
        work: WorkCounters { object_bytes_read: u64::MAX, backend_read_operations: u64::MAX,
            ..WorkCounters::default() },
    }));
    crate::contract::canonical_json_bytes(&record).expect("actual native receipt encoder")
}

#[test]
fn decoded_historical_work_is_data_not_current_query_work() {
    let bytes = observed_record();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = work.remaining().unwrap().peak_allocation_bytes;
    let mut custody = Some(work.retain_owned(0).unwrap());
    let decoded = decode(&bytes, Some(&work), &mut custody, OperationId::from_bytes([4; 16])).unwrap();
    assert_eq!(work.receipt().object_bytes_read, 0);
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert!(work.receipt().items_examined > 0);
    assert_eq!(custody.as_ref().unwrap().bytes(), receipt_bytes(&decoded).unwrap());
    drop(decoded);
    drop(custody);
    assert_eq!(work.remaining().unwrap().peak_allocation_bytes, baseline);
}

#[test]
fn receipt_decode_cancellation_preserves_original_token_and_prefix() {
    let bytes = observed_record();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut custody = Some(work.retain_owned(0).unwrap());
    let before = work.receipt();
    cancellation.cancel();
    assert!(matches!(decode(&bytes, Some(&work), &mut custody, OperationId::from_bytes([4; 16])),
        Err(NativeReceiptError::Native(acyclic_fs::NativeTreeMaterializationError::Cancelled))));
    assert_eq!(work.receipt(), before);
    assert!(work.cancellation().is_cancelled());
}

#[test]
fn lexical_receipt_failure_retains_its_original_diagnostic_admission() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut custody = Some(work.retain_owned(0).unwrap());
    let failure = decode(b"{\"kind\":\"observed\",", Some(&work), &mut custody, OperationId::from_bytes([4; 16]))
        .err().expect("malformed actual receipt");
    assert!(matches!(failure, NativeReceiptError::Codec { custody: Some(_), .. }));
    assert!(custody.is_none());
    assert!(work.receipt().items_examined > 0);
    drop(failure);
}

#[test]
fn effect_extraction_keeps_original_custody_and_retires_other_receipt_data() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    let effect = EffectObservation {
        provider: "native-process".into(),
        effect_id: crate::EffectId::from_bytes([1; 16]),
        attempt_id: crate::EffectAttemptId::from_bytes([2; 16]),
        request_digest: [3; 32],
        guarantee: EffectGuarantee::IdempotentRetry,
        status: EffectStatus::Failed { message: "actual retained failure".into() },
    };
    let fence = crate::scheduler::LeaseFence {
        reservation_id: "original-reservation".into(), placement: "original-worker".into(),
    };
    let invocation_failure = String::from("discarded receipt-only diagnostic");
    let retained = super::receipt_observation::effect_bytes(&effect).unwrap()
        + fence.reservation_id.capacity() as u64 + fence.placement.capacity() as u64
        + invocation_failure.capacity() as u64;
    let receipt = NativeProcessReceipt {
        task: crate::TaskId::from_bytes([4; 16]), command: OperationId::from_bytes([5; 16]),
        original_fence: fence, effect, observed: true, diagnostic: None,
        publication_failure: None, invocation_failure: Some(invocation_failure),
        work: WorkCounters::default(), _custody: Some(work.retain_owned(retained).unwrap()),
    };
    let allocations = work.receipt().allocation_operations;
    let (effect, custody) = receipt.into_effect_with_native_custody().unwrap();
    assert_eq!(custody.bytes(), super::receipt_observation::effect_bytes(&effect).unwrap());
    assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
    assert_eq!(work.receipt().allocation_operations, allocations);
    drop(effect);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
}
