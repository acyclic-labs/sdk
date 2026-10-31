//! Scalar wire metadata joins the parser shared by native and hosted bindings.
use acyclic_fs::{WorkspaceMetadata, binding_contract::*, kernel::MetadataField};
use acyclic_fs_wire::wire::filesystem::v2 as wire;
use prost::Message;

pub(super) fn check(name: &str, bytes: &[u8]) {
    if name != "acyclic.filesystem.v2.Metadata" {
        return;
    }
    let Ok(original) = wire::Metadata::decode(bytes) else {
        return;
    };
    let Ok(value) = metadata_from_proto(Some(original.clone())) else {
        return;
    };
    let mut normalized = original.clone();
    macro_rules! scalar {
        ($field:ident, $type:ident, $module:ident) => {
            if normalized
                .$field
                .as_ref()
                .is_none_or(|field| field.value.is_none())
            {
                normalized.$field = Some(wire::$type {
                    value: Some(wire::$module::Value::Unavailable(true)),
                });
            }
            let mut refused = original.clone();
            refused.$field = Some(wire::$type {
                value: Some(wire::$module::Value::Unavailable(false)),
            });
            assert!(metadata_from_proto(Some(refused)).is_err());
        };
    }
    scalar!(posix_mode, OptionalU32, optional_u32);
    scalar!(posix_uid, OptionalU32, optional_u32);
    scalar!(posix_gid, OptionalU32, optional_u32);
    scalar!(posix_flags, OptionalU64, optional_u64);
    scalar!(windows_attributes, OptionalU32, optional_u32);
    scalar!(created_ns, OptionalI64, optional_i64);
    scalar!(modified_ns, OptionalI64, optional_i64);
    scalar!(accessed_ns, OptionalI64, optional_i64);
    scalar!(changed_ns, OptionalI64, optional_i64);
    assert_eq!(metadata_from_proto(Some(normalized)), Ok(value));
    for field in 0..3 {
        let mut refused = original.clone();
        match field {
            0 => refused.has_named_attributes = true,
            1 => refused.has_acl = true,
            _ => refused.has_security_descriptor = true,
        }
        assert!(metadata_from_proto(Some(refused)).is_err());
    }
}

pub(super) fn seeds() -> Vec<(&'static str, &'static str, Vec<u8>)> {
    let absent = WorkspaceMetadata {
        posix_mode: None,
        posix_uid: None,
        posix_gid: None,
        posix_flags: None,
        windows_attributes: None,
        created_ns: None,
        modified_ns: None,
        accessed_ns: None,
        changed_ns: None,
        has_named_attributes: false,
        has_acl: false,
        has_security_descriptor: false,
    };
    let zero = WorkspaceMetadata {
        posix_mode: Some(0),
        posix_uid: Some(0),
        posix_gid: Some(0),
        posix_flags: Some(0),
        windows_attributes: Some(0),
        created_ns: Some(0),
        modified_ns: Some(0),
        accessed_ns: Some(0),
        changed_ns: Some(0),
        ..absent
    };
    let extremes = WorkspaceMetadata {
        posix_mode: Some(u32::MAX),
        posix_uid: Some(1),
        posix_gid: Some(u32::MAX - 1),
        posix_flags: Some(u64::MAX),
        windows_attributes: Some(2),
        created_ns: Some(i64::MIN),
        modified_ns: Some(i64::MAX),
        accessed_ns: Some(-1),
        changed_ns: Some(1),
        ..absent
    };
    let mut seeds = Vec::new();
    for (name, input) in [
        ("metadata-unavailable", absent),
        ("metadata-zero", zero),
        ("metadata-extremes", extremes),
        (
            "metadata-mixed",
            WorkspaceMetadata {
                posix_uid: None,
                modified_ns: None,
                ..extremes
            },
        ),
    ] {
        let encoded = metadata_to_proto(input);
        let actual = metadata_from_proto(Some(encoded.clone())).unwrap();
        macro_rules! fact {
            ($($field:ident),+ $(,)?) => {$(
                assert_eq!(actual.$field, input.$field.map_or(MetadataField::Unavailable, MetadataField::Value));
            )+};
        }
        fact!(
            posix_mode,
            posix_uid,
            posix_gid,
            posix_flags,
            windows_attributes,
            created_ns,
            modified_ns,
            accessed_ns,
            changed_ns
        );
        let bytes = encoded.encode_to_vec();
        check("acyclic.filesystem.v2.Metadata", &bytes);
        seeds.push(("acyclic.filesystem.v2.Metadata", name, bytes));
    }
    for (name, input) in [
        (
            "metadata-opaque-attributes",
            WorkspaceMetadata {
                has_named_attributes: true,
                ..absent
            },
        ),
        (
            "metadata-opaque-acl",
            WorkspaceMetadata {
                has_acl: true,
                ..absent
            },
        ),
        (
            "metadata-opaque-security",
            WorkspaceMetadata {
                has_security_descriptor: true,
                ..absent
            },
        ),
    ] {
        let encoded = metadata_to_proto(input);
        assert!(metadata_from_proto(Some(encoded.clone())).is_err());
        seeds.push((
            "acyclic.filesystem.v2.Metadata",
            name,
            encoded.encode_to_vec(),
        ));
    }
    seeds
}
