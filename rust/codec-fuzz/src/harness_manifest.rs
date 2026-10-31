//! The public attachment decoder receives an actual content-bound reference.
use acyclic_harness_core::{conversation as c, resources::ProviderRef};
use std::sync::OnceLock;

fn reference(bytes: &[u8]) -> acyclic_harness_core::Result<c::FileRef> {
    static VOLUME: OnceLock<c::VolumeRef> = OnceLock::new();
    let volume = VOLUME.get_or_init(|| {
        c::VolumeRef::new(
            ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)
                .expect("valid provider"),
            "attachments".into(),
            c::VolumeClass::Project,
            c::VolumeOwner::Project("project".into()),
            &mut None,
        )
        .expect("valid volume")
    });
    c::FileRef::new(
        volume.clone(),
        "attachments.json".into(),
        "one".into(),
        c::FileDescriptor::from_bytes(bytes, c::ATTACHMENT_MANIFEST_MEDIA_TYPE, &mut None)?,
        "attachments.json".into(),
        &mut None,
    )
}

pub(super) fn check(bytes: &[u8]) {
    // Derive the descriptor from this exact input so arbitrary mutations reach
    // JSON/semantic admission rather than stopping at the content hash.
    let Ok(original) = reference(bytes) else {
        return;
    };
    let ordinary = c::decode_attachment_manifest(&original, bytes, None, None);
    funded(&original, bytes, &ordinary);
    let Ok(items) = ordinary else {
        return;
    };
    assert_eq!(c::encode_attachment_manifest(&items).unwrap(), bytes);
    // Mutate the actual writer's leading version, retaining a correctly bound
    // descriptor for each refused header so content integrity is not the cause.
    for version in [b'0', b'2', u8::MAX] {
        let mut header = bytes[..5].to_vec();
        header[2] = version;
        let original = reference(&header).unwrap();
        assert_eq!(
            c::decode_attachment_manifest(&original, &header, None, None)
                .unwrap_err()
                .code(),
            acyclic_harness_core::wire::ErrorCode::Unsupported,
        );
    }
    let original_array = &bytes[5..bytes.len() - 1];
    let old = reference(original_array).unwrap();
    assert_eq!(
        c::decode_attachment_manifest(&old, original_array, None, None)
            .unwrap_err()
            .code(),
        acyclic_harness_core::wire::ErrorCode::Unsupported,
    );
    let current_header = &bytes[..5];
    let cut = reference(current_header).unwrap();
    assert_eq!(
        c::decode_attachment_manifest(&cut, current_header, None, None)
            .unwrap_err()
            .code(),
        acyclic_harness_core::wire::ErrorCode::Invalid,
    );
    if let Ok(count) = u32::try_from(items.len()) {
        assert_eq!(
            c::decode_attachment_manifest(&original, bytes, Some(count), None).unwrap(),
            items
        );
        let foreign_count = count ^ 1;
        assert!(
            c::decode_attachment_manifest(&original, bytes, Some(foreign_count), None).is_err()
        );
    }
    if let Some((_, truncated)) = bytes.split_last() {
        assert!(c::decode_attachment_manifest(&original, truncated, None, None).is_err());
    }
    let mut trailing = bytes.to_vec();
    trailing.push(b' ');
    if let Ok(altered) = reference(&trailing) {
        assert!(c::decode_attachment_manifest(&altered, &trailing, None, None).is_err());
    }
}

fn funded(
    original: &c::FileRef,
    bytes: &[u8],
    ordinary: &acyclic_harness_core::Result<Vec<c::Attachment>>,
) {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    use acyclic_harness_core::{filesystem::PreparationIo, wire::ErrorCode};

    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = work.live_allocation_bytes();
    {
        let io = PreparationIo::from_native_work(&work).unwrap();
        let measured = c::decode_attachment_manifest(original, bytes, None, Some(&io));
        match (ordinary, &measured) {
            (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
            (Err(expected), Err(actual)) => assert_eq!(actual.code(), expected.code()),
            _ => panic!("funded and ordinary attachment admission disagree"),
        }
    }
    assert_eq!(work.live_allocation_bytes(), baseline);

    // Cancellation uses the original ledger event; no body is awaited or retried.
    {
        let io = PreparationIo::from_native_work(&work).unwrap();
        cancellation.cancel();
        assert_eq!(
            c::decode_attachment_manifest(original, bytes, None, Some(&io))
                .unwrap_err()
                .code(),
            ErrorCode::Cancelled,
        );
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
    if ordinary.is_ok() {
        let cancellation = CancellationToken::new();
        let budget = WorkBudget {
            source_bytes_read: 0,
            ..WorkBudget::UNBOUNDED
        };
        let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let baseline = work.live_allocation_bytes();
        {
            let io = PreparationIo::from_native_work(&work).unwrap();
            assert!(c::decode_attachment_manifest(original, bytes, None, Some(&io)).is_err());
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
}
