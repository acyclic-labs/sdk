use crate::codec_association;
use crate::codec_properties;

mod properties {
    use super::codec_association::association;
    use super::codec_properties::{change, check_canonical};
    use acyclic_fs::kernel::{
        CanonicalDecodeError, DecodeLimits, decode_file_metadata, encode_file_metadata,
    };
    use acyclic_fs_format::{
        identity::Digest,
        metadata::{FileMetadata, MetadataField},
        object_reference::{ObjectId, ObjectKind},
    };
    use proptest::prelude::*;

    fn field_of<T: Clone + std::fmt::Debug>(
        value: impl Strategy<Value = T>,
    ) -> impl Strategy<Value = MetadataField<T>> {
        prop::option::of(value)
            .prop_map(|value| value.map_or(MetadataField::Unavailable, MetadataField::Value))
    }

    fn object_of(kind: ObjectKind) -> impl Strategy<Value = MetadataField<ObjectId>> {
        field_of(any::<u8>().prop_map(move |byte| ObjectId {
            storage: association(byte),
            kind,
            digest: Digest::from_bytes([byte; 32]),
        }))
    }

    fn metadata() -> impl Strategy<Value = FileMetadata> {
        let ids = (
            field_of(any::<u32>()),
            field_of(any::<u32>()),
            field_of(any::<u32>()),
        );
        let flags = (field_of(any::<u64>()), field_of(any::<u32>()));
        let times = prop::array::uniform4(field_of(any::<i64>()));
        let objects = (
            object_of(ObjectKind::AttributePage),
            object_of(ObjectKind::Blob),
            object_of(ObjectKind::Blob),
        );
        (ids, flags, times, objects).prop_map(
            |(
                (posix_mode, posix_uid, posix_gid),
                (posix_flags, windows_attributes),
                [created_ns, modified_ns, accessed_ns, changed_ns],
                (named_attributes, acl, security_descriptor),
            )| FileMetadata {
                posix_mode,
                posix_uid,
                posix_gid,
                posix_flags,
                windows_attributes,
                created_ns,
                modified_ns,
                accessed_ns,
                changed_ns,
                named_attributes,
                acl,
                security_descriptor,
            },
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn metadata_round_trips_canonically(metadata in metadata(), change in change()) {
            let decode = |bytes: &[u8]| decode_file_metadata(bytes, DecodeLimits::default(), &mut |_| Ok::<_, CanonicalDecodeError>(()));
            check_canonical(&metadata, |metadata| encode_file_metadata(*metadata, |_| Ok::<_, CanonicalDecodeError>(()), |_| CanonicalDecodeError::AllocationFailed), decode, &change)?;
        }
    }
}
