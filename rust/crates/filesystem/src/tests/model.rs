use super::*;

#[test]
fn checkout_modes_reject_contradictions() {
    let invalid = CheckoutMode {
        access: AccessMode::ReadOnly,
        consistency: ConsistencyMode::Pinned,
        mutations: MutationMode::PrivateOverlay,
    };
    assert_eq!(invalid.validate(), Err(CheckoutModeError::ReadOnlyMutation));

    for (mode, expected) in [
        (
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::None,
            },
            CheckoutModeError::WritableWithoutMutationMode,
        ),
        (
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::DirectLive,
            },
            CheckoutModeError::DirectRequiresLive,
        ),
    ] {
        assert_eq!(mode.validate(), Err(expected));
    }
}

#[test]
fn default_limits_are_valid() {
    let config = VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Durable,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    };
    assert_eq!(config.validate(), Ok(config));
    let mut invalid = config;
    invalid.limits.maximum_directory_page_entries = 1;
    assert_eq!(
        invalid.validate(),
        Err(VolumeConfigError::InsufficientPageFanout)
    );
    invalid = config;
    invalid.limits.maximum_component_bytes = invalid.limits.maximum_path_bytes + 1;
    assert_eq!(
        invalid.validate(),
        Err(VolumeConfigError::ComponentExceedsPath)
    );
}

#[test]
fn native_component_limit_matches_host_encoding() {
    let native = VolumeConfig::native(Lifecycle::Durable);
    assert_eq!(native.validate(), Ok(native));
    #[cfg(windows)]
    assert_eq!(native.limits.maximum_component_bytes, 510);
    #[cfg(not(windows))]
    assert_eq!(native.limits.maximum_component_bytes, 255);
}
