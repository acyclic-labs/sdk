use super::*;
use crate::store::history::{HistoryIo, HistoryWork};
use acyclic_fs::{CancellationToken, WorkBudget};

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 64,
        backend_read_operations: 64,
        authority_bytes_read: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        authority_records_appended: 64,
        authority_bytes_written: 16_777_216,
        backend_write_operations: 64,
        output_bytes: 16_777_216,
        items_examined: 1_000_000,
        items_returned: 64,
        allocation_operations: 10_000,
        peak_allocation_bytes: 16_777_216,
        ..WorkBudget::default()
    }
}

fn event() -> Event {
    Event {
        revision: 1, operation_id: OperationId::from_bytes([42; 16]), intent_digest: [43;32],
        scope: RecordedScope::from_wire("original".into(), Capabilities::new(["conversation:bind"]), "original-issuer".into(), Some(crate::AgentId::from_bytes([44;16]))),
        attestation: [45;32], causal_parent: None,
        payload: EventPayload::ConversationBound { agent: crate::AgentId::from_bytes([44;16]) },
    }
}
fn authority() -> Authority { Authority { kind: AggregateKind::Conversation, id: "original-authority".into() } }

#[test]
fn concrete_wire_guard_preserves_original_bytes_and_reencode_work() -> Result<()> {
    let authority = authority();
    let event = event();
    let bytes = encode_event(&authority, &event, &mut None)?;
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    let decoded = decode_event(&bytes, &mut Some(&mut work))?.0;
    assert_eq!(decoded, (authority, event));
    let receipt = work.finish(Ok(decoded)).map_err(|failure| failure.error)?;
    assert!(receipt.work.bytes_encoded >= bytes.len() as u64);
    assert!(receipt.work.peak_allocation_bytes > bytes.len() as u64);
    Ok(())
}

#[test]
fn typed_payload_quota_rejects_before_original_serde_error() -> Result<()> {
    let mut envelope = encode_event_envelope(&authority(), &event())?;
    // Valid JSON that would fail the original concrete enum deserializer.
    envelope.canonical_payload_json = br#"{"type":"not_an_event","untrusted":["owned","strings"]}"#.to_vec();
    let bytes = envelope.encode_to_vec();
    let carrier = admission::decode_quota(&bytes, &mut None)?;
    let mut allowance = budget();
    allowance.peak_allocation_bytes = carrier;
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(allowance, &cancellation);
    let failure = decode_event(&bytes, &mut Some(&mut work)).expect_err("typed admission cannot allocate past the carrier-only allowance");
    assert!(!failure.to_string().contains("unknown variant"));
    assert!(!failure.to_string().contains("not_an_event"));
    let receipt = work.finish::<()>(Err(failure)).expect_err("admission failure must preserve its measured prefix");
    assert_eq!(receipt.work.bytes_encoded, 0);
    assert!(receipt.work.items_examined > 0);
    assert_eq!(receipt.work.output_bytes, 0);
    assert_eq!(receipt.work.backend_read_operations, 0);
    Ok(())
}

#[test]
fn duplicate_carrier_and_unknown_field_are_not_canonical() -> Result<()> {
    let bytes = encode_event(&authority(), &event(), &mut None)?;
    let mut duplicate = bytes.clone();
    // EventEnvelope revision field 3, a duplicate canonical scalar value.
    duplicate.extend_from_slice(&[0x18, 0x01]);
    assert!(decode_event(&duplicate, &mut None).is_err());
    let mut unknown = bytes;
    unknown.extend_from_slice(&[0x78, 0x01]);
    assert!(decode_event(&unknown, &mut None).is_err());
    Ok(())
}

#[test]
fn malformed_frame_and_cancel_have_no_original_deserialization_work() -> Result<()> {
    let cancellation = CancellationToken::new();
    let mut work = HistoryWork::new(budget(), &cancellation);
    assert!(decode_event(&[0x4a, 0xff], &mut Some(&mut work)).is_err());
    let receipt = work.finish(Ok(())).map_err(|failure| failure.error)?;
    assert_eq!(receipt.work.bytes_encoded, 0);
    cancellation.cancel();
    let mut work = HistoryWork::new(budget(), &cancellation);
    let bytes = encode_event(&authority(), &event(), &mut None)?;
    let failure = decode_event(&bytes, &mut Some(&mut work)).expect_err("the original token must reject before framing");
    assert!(HistoryIo::check_cancellation(&work).is_err());
    let receipt = work.finish::<()>(Err(failure)).expect_err("original operation token is cancelled");
    assert_eq!(receipt.work.bytes_encoded, 0);
    assert_eq!(receipt.work.items_examined, 0);
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_malformed_prost_preserves_original_cause_and_preclaimed_custody() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    for bytes in [&[0x00][..], &[0x4a, 0xff], &[0x4a, 0x04, 0x00],
        &[0x7b, 0x74], &[0x7b; 101]]
    {
        let token = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &token).expect("original native ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original observer");
        let error = admission::decode_quota(bytes, &mut Some(&mut io))
            .expect_err("original prost framing must reject");
        let Error::NativeProstDecode { code, source, custody: Some(custody) } = error else {
            panic!("prost must keep its actual cause and original preclaimed guard");
        };
        assert_eq!(code, crate::wire::ErrorCode::Storage);
        assert!(custody.bytes() > 0);
        assert!(custody.work().shares_ledger(&native));
        assert!(native.receipt().items_examined > 0);
        assert_eq!(native.receipt().authority_bytes_read, 0);
        assert_eq!(native.receipt().backend_read_operations, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        let mut original = bytes;
        let expected = prost::encoding::decode_key(&mut original).and_then(|(tag, kind)|
            prost::encoding::skip_field(kind, tag, &mut original, prost::encoding::DecodeContext::default()))
            .expect_err("same original prost framing cause");
        assert_eq!(source, expected);
        // Actual diagnostic data precedes its original guard on destruction.
        drop(source);
        drop(custody);
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn native_diagnostic_quota_denies_before_even_malformed_key_decode() -> Result<()> {
    use acyclic_fs::NativeMaterializationWork;
    let token = CancellationToken::new();
    let mut allowance = budget();
    allowance.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
    let native = NativeMaterializationWork::new(allowance, &token).expect("accounting-only ledger");
    let mut io = crate::store::history::native_work_io(&native).expect("zero-byte original observer");
    let error = admission::decode_quota(&[0x00], &mut Some(&mut io))
        .expect_err("diagnostic admission must deny before allocating prost error");
    assert!(matches!(error, Error::NativeWork { .. }));
    assert_eq!(native.receipt().items_examined, 0);
    assert_eq!(native.receipt().backend_read_operations, 0);
    assert_eq!(native.receipt().authority_bytes_read, 0);
    Ok(())
}
