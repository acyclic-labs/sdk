use super::*;
use crate::store::HistoryWork;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use prost::Message as _;
fn budget() -> WorkBudget {
    WorkBudget {
        source_bytes_read: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        items_examined: 1_000_000,
        allocation_operations: 10_000,
        peak_allocation_bytes: 16_777_216,
        ..WorkBudget::default()
    }
}
#[test]
fn native_event_round_trip_releases_custody() -> Result<()> {
    let bytes = tests::fixture();
    let token = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = native.live_allocation_bytes();
    for suffix in [&[][..], &[0x18, 0x01], &[0x78, 0x01]] {
        let mut input = bytes.clone();
        input.extend_from_slice(suffix);
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let decoded = decode_event(&input, &mut Some(&mut io));
        if suffix.is_empty() {
            let ((authority, event), _) = decoded?;
            assert_eq!(encode_event(&authority, &event, &mut None)?, bytes);
        } else {
            assert!(decoded.is_err());
        }
        drop(io.into_custody()?);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
    token.cancel();
    assert!(decode_event(&bytes, &mut Some(&mut io)).is_err());
    Ok(())
}

#[test]
fn event_attachment_slots_follow_containers_and_cover_physical_decode_peak() -> Result<()> {
    use crate::conversation::{Attachment, ReferencedAttachments};
    let ((_, event), _) = decode_event(&tests::fixture(), &mut None)?;
    let EventPayload::ConversationMessageAppended { message } = event.payload else {
        panic!("conversation message fixture")
    };
    for count in [1, 4, 16] {
        let mut message = message.clone();
        message.attachments = ReferencedAttachments::Inline {
            items: (0..count)
                .map(|index| Attachment {
                    file: message.content.clone(),
                    label: Some(format!("attachment-{index}")),
                })
                .collect(),
        };
        let payload = EventPayload::ConversationMessageAppended { message };
        let bytes = canonical_json_bytes(&payload)?;
        let mut allowance = budget();
        allowance.peak_allocation_bytes = 8 * 1024 * 1024;
        let native = NativeMaterializationWork::new(allowance, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes();
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let mut decoded = None;
        let physical = acyclic_test_support::measure(|| {
            decoded = Some(decode_event_payload(
                payload.tag(),
                &bytes,
                &mut Some(&mut io),
            ));
        });
        let (actual, admitted) = decoded.expect("decoder ran")?;
        assert_eq!(actual, payload);
        assert!(
            physical.bytes_max <= admitted,
            "physical={physical:?}; admitted={admitted}"
        );
        drop(actual);
        drop(io.into_custody()?);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    Ok(())
}
#[test]
fn native_partial_payload_cause_keeps_original_guard_until_cause_drop() -> Result<()> {
    let mut envelope = wire::EventEnvelope::decode(tests::fixture().as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    envelope.event_type = "conversation_bound".into();
    envelope.canonical_payload_json =
        br#"{"kind":"conversation_bound","agent":"not-a-UUID"}"#.to_vec();
    let original =
        serde_json::from_slice::<EventPayload>(&envelope.canonical_payload_json).unwrap_err();
    let token = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = native.live_allocation_bytes();
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
    let Error::NativeJson {
        code,
        source,
        custody: Some(custody),
    } = decode_event(&envelope.encode_to_vec(), &mut Some(&mut io)).unwrap_err()
    else {
        panic!("original JSON cause required")
    };
    assert_eq!(code, Storage);
    assert_eq!(source.classify(), original.classify());
    assert!(native.shares_ledger(custody.work()));
    assert_eq!(io.into_custody()?.bytes(), 0);
    assert!(native.live_allocation_bytes() > baseline);
    drop(source);
    assert!(native.live_allocation_bytes() > baseline);
    drop(custody);
    assert_eq!(native.live_allocation_bytes(), baseline);
    Ok(())
}
#[test]
fn native_malformed_prost_preserves_original_cause_and_preclaimed_custody() -> Result<()> {
    for bytes in [
        &[0x00][..],
        &[0x4a, 0xff],
        &[0x4a, 0x04, 0x00],
        &[0x7b, 0x74],
        &[0x7b; 101],
    ] {
        let token = CancellationToken::new();
        let native = NativeMaterializationWork::new(budget(), &token)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = native.live_allocation_bytes();
        let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
        let Error::NativeProstDecode {
            source,
            custody: Some(custody),
            ..
        } = admission::decode_quota(bytes, &mut Some(&mut io)).unwrap_err()
        else {
            panic!("original Prost cause required")
        };
        let mut input = bytes;
        let expected = prost::encoding::decode_key(&mut input)
            .and_then(|(tag, kind)| {
                prost::encoding::skip_field(
                    kind,
                    tag,
                    &mut input,
                    prost::encoding::DecodeContext::default(),
                )
            })
            .unwrap_err();
        assert_eq!(source, expected);
        assert!(native.shares_ledger(custody.work()));
        assert_eq!(io.into_custody()?.bytes(), 0);
        drop(source);
        assert!(native.live_allocation_bytes() > baseline);
        drop(custody);
        assert_eq!(native.live_allocation_bytes(), baseline);
    }
    Ok(())
}
#[test]
fn native_quota_precedes_allocating_original_deserialize_failure() -> Result<()> {
    let mut envelope = wire::EventEnvelope::decode(tests::fixture().as_slice())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    envelope.canonical_payload_json = format!(
        r#"{{"type":"not_an_event","untrusted":["{}"]}}"#,
        "x".repeat(4096)
    )
    .into_bytes();
    let bytes = envelope.encode_to_vec();
    let carrier = admission::decode_quota(&bytes, &mut None)?.0;
    let mut allowance = budget();
    allowance.peak_allocation_bytes =
        NativeMaterializationWork::accounting_state_bytes() + carrier + 8192;
    let token = CancellationToken::new();
    let native = NativeMaterializationWork::new(allowance, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
    let error = decode_event(&bytes, &mut Some(&mut io)).unwrap_err();
    assert!(!matches!(
        error,
        Error::NativeJson { .. } | Error::NativeProstDecode { .. }
    ));
    drop(error);
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}
#[test]
fn native_diagnostic_quota_denies_before_even_malformed_key_decode() -> Result<()> {
    let mut allowance = budget();
    allowance.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
    let token = CancellationToken::new();
    let native = NativeMaterializationWork::new(allowance, &token)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut io = HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&native)?);
    assert!(matches!(
        admission::decode_quota(&[0x00], &mut Some(&mut io)),
        Err(Error::NativeWork { .. })
    ));
    Ok(())
}
