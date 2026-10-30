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
    let Ok(items) = c::decode_attachment_manifest(&original, bytes, None, None) else {
        return;
    };
    assert_eq!(c::encode_attachment_manifest(&items).unwrap(), bytes);
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
