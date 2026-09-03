use super::*;

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

#[test]
fn volume_creation_is_canonical_bounded_and_catalog_free() -> Result<(), Box<dyn std::error::Error>>
{
    let value = VolumeCreated {
        volume_id: VolumeId::from_bytes([1; 16]),
        config: config(),
        initial_generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([2; 32]),
        },
    };
    let encoded = encode_volume_created(value)?;
    assert_eq!(decode_volume_created(&encoded, u64::MAX)?, value);
    assert_eq!(
        volume_authority_id(value.volume_id).into_bytes(),
        value.volume_id.into_bytes()
    );
    let maximum = u64::try_from(encoded.len().saturating_sub(1)).unwrap_or(0);
    assert!(decode_volume_created(&encoded, maximum).is_err());
    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_volume_created(&trailing, u64::MAX).is_err());
    Ok(())
}

#[test]
fn invalid_config_and_object_kind_fail_closed() {
    let mut invalid = config();
    invalid.limits.maximum_path_bytes = 0;
    assert!(matches!(
        encode_volume_created(VolumeCreated {
            volume_id: VolumeId::from_bytes([3; 16]),
            config: invalid,
            initial_generation_root: ObjectId {
                kind: ObjectKind::GenerationRoot,
                digest: Digest::from_bytes([4; 32]),
            },
        }),
        Err(VolumeCreatedError::Config(VolumeConfigError::ZeroLimit))
    ));
    assert!(
        encode_volume_created(VolumeCreated {
            volume_id: VolumeId::from_bytes([5; 16]),
            config: config(),
            initial_generation_root: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::from_bytes([6; 32]),
            },
        })
        .is_err()
    );
}

#[test]
fn every_volume_semantic_variant_roundtrips_and_unknown_tags_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let value = VolumeCreated {
        volume_id: VolumeId::from_bytes([9; 16]),
        config: VolumeConfig {
            profile: FilesystemProfile::Browser,
            concurrency: ConcurrencyMode::ExclusiveWriter,
            lifecycle: Lifecycle::Durable,
            case_sensitivity: CaseSensitivity::ProfileFolded,
            unicode: UnicodePolicy::RequireNfc,
            symbolic_links: false,
            hard_links: false,
            sparse_files: false,
            limits: VolumeLimits::default(),
        },
        initial_generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([10; 32]),
        },
    };
    let encoded = encode_volume_created(value)?;
    assert_eq!(decode_volume_created(&encoded, u64::MAX)?, value);
    let config_prefix = [4_u8, 1, 2, 2, 2, 0, 0, 0];
    let config_offset = encoded
        .windows(config_prefix.len())
        .position(|window| window == config_prefix)
        .ok_or("canonical configuration prefix was not found")?;

    for relative in 0..=7 {
        let mut malformed = encoded.clone();
        malformed[config_offset + relative] = if relative < 5 { 0 } else { 2 };
        assert!(decode_volume_created(&malformed, u64::MAX).is_err());
    }

    let mut depth_overflow = encoded.clone();
    let depth_offset = config_offset + 8 + 4 + 4;
    depth_overflow[depth_offset..depth_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_volume_created(&depth_overflow, u64::MAX).is_err());

    let mut wrong_kind = encoded;
    let kind_offset = wrong_kind.len() - 33;
    wrong_kind[kind_offset] = ObjectKind::TreePage.canonical_tag();
    assert!(matches!(
        decode_volume_created(&wrong_kind, u64::MAX),
        Err(VolumeCreatedError::Decode(CanonicalDecodeError::Invariant(
            _
        )))
    ));
    Ok(())
}
