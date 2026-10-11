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
        published: Vec::new(),
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
fn original_observation_without_final_snapshot_data_keeps_an_empty_proof() {
    let mut wire: serde_json::Value = crate::contract::json_from_slice(&observed_record()).unwrap();
    wire.as_object_mut().unwrap().remove("published");
    let bytes = crate::contract::canonical_json_bytes(&wire).unwrap();
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut custody = Some(work.retain_owned(0).unwrap());
    let decoded = decode(&bytes, Some(&work), &mut custody, OperationId::from_bytes([4; 16])).unwrap();
    let Receipt::Observed(observed) = &decoded else { panic!("actual original observed record") };
    assert!(observed.published.is_empty());
    assert_eq!(work.receipt().backend_read_operations, 0);
    assert_eq!(work.receipt().authority_records_appended, 0);
    drop(decoded);
    drop(custody);
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
fn receipt_codec_slot_denial_precedes_lexing_and_keeps_prefix() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget {
        allocation_operations: 1,
        peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes(),
        ..WorkBudget::UNCHARGED
    }, &cancellation).unwrap();
    let mut custody = Some(work.retain_owned(0).unwrap());
    let before = work.receipt();
    let error = decode(b"{", Some(&work), &mut custody, OperationId::from_bytes([4; 16]))
        .err().expect("the actual codec cause box requires admission");
    assert!(matches!(error, NativeReceiptError::Owner(Error::NativeWork { custody: None, .. })));
    assert_eq!(work.receipt(), before);
    assert!(custody.is_some());
}

#[test]
fn prepaid_codec_box_preserves_foreign_cause_custody_after_cancellation() {
    let box_cancellation = CancellationToken::new();
    let cause_cancellation = CancellationToken::new();
    let box_work = NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &box_cancellation,
    ).unwrap();
    let cause_work = NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED, &cause_cancellation,
    ).unwrap();
    let box_baseline = box_work.live_allocation_bytes();
    let cause_baseline = cause_work.live_allocation_bytes();
    let slot = NativeReceiptCodecSlot::new(&box_work).unwrap();
    let message = String::from("original owned codec failure");
    let original_pointer = message.as_ptr();
    let cause_custody = cause_work.retain_owned(message.capacity() as u64).unwrap();
    box_cancellation.cancel();
    cause_cancellation.cancel();
    let box_prefix = box_work.receipt();
    let cause_prefix = cause_work.receipt();
    let failure = slot.retain(Error::Storage(message), Some(cause_custody));
    let NativeReceiptError::Codec { code, source, custody: Some(custody) } = failure else {
        panic!("the prepaid slot must retain its actual codec cause");
    };
    assert_eq!(code, crate::wire::ErrorCode::Storage);
    let Error::Storage(message) = source.source().unwrap().downcast_ref::<Error>().unwrap() else {
        panic!("the original typed cause must remain reachable");
    };
    assert_eq!(message.as_ptr(), original_pointer);
    assert_eq!(box_work.receipt(), box_prefix);
    assert_eq!(cause_work.receipt(), cause_prefix);
    assert!(cause_work.live_allocation_bytes() > cause_baseline);
    drop(source);
    assert_eq!(cause_work.live_allocation_bytes(), cause_baseline);
    assert_eq!(box_work.live_allocation_bytes(), box_baseline + custody.bytes());
    drop(custody);
    assert_eq!(box_work.live_allocation_bytes(), box_baseline);
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
        published: Vec::new(),
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
