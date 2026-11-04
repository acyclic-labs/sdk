#![no_main]
use acyclic_stream::{FollowFilter, wire};
use libfuzzer_sys::fuzz_target;
use prost::Message;
fuzz_target!(|data: &[u8]| {
    if let Ok(value) = wire::SessionSubscription::decode(data) {
        let encoded = value.encode_to_vec();
        assert_eq!(
            wire::SessionSubscription::decode(encoded.as_slice()).unwrap(),
            value
        );
        if let Some(filter) = value.filter {
            let _ = FollowFilter::new(filter);
        }
    }
    // A change value has exactly one encoding: whatever decodes re-encodes
    // to the same bytes.
    if let Ok(value) = acyclic_stream::decode_changes(data) {
        let encoded = acyclic_stream::AppendRecord::changes(&value.context, &value.entries, vec![])
            .unwrap()
            .value;
        assert_eq!(encoded.as_ref(), data);
        // Reach the same lossless context and entry decoders as the serving
        // projector; accepting the outer envelope does not validate its facts.
        let _ = acyclic_fs::PublicationContext::from_bytes(&value.context);
        for profile in [
            acyclic_fs::model::FilesystemProfile::Portable,
            acyclic_fs::model::FilesystemProfile::Posix,
            acyclic_fs::model::FilesystemProfile::Windows,
        ] {
            for case_sensitivity in [
                acyclic_fs::model::CaseSensitivity::Sensitive,
                acyclic_fs::model::CaseSensitivity::ProfileFolded,
            ] {
                let config = acyclic_fs::VolumeConfig {
                    profile,
                    case_sensitivity,
                    ..acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral)
                };
                for entry in &value.entries {
                    let _ = acyclic_fs::PublicationEntry::from_parts(
                        &entry.key,
                        &entry.operation,
                        &entry.metadata,
                        config,
                    );
                }
            }
        }
    }
    if let Some((head, tail)) = data.split_at_checked(16) {
        let id: [u8; 16] = head.try_into().unwrap();
        if let Ok(path) = std::str::from_utf8(tail)
            && let Ok(resource) =
                acyclic_fs::session::resource(acyclic_fs::VolumeId::from_bytes(id), path)
        {
            assert!(acyclic_auth::valid_resource(&resource));
        }
    }
});
