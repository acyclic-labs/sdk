use super::*;
use crate::kernel::NameEncoding;
use crate::speculation::{PromotionRejection, ResidencyReason, ResidencyRejection, StorageTier};

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

/// Pins the exact public strings (host bindings and TypeScript depend on
/// them byte-for-byte) and checks that parsing inverts `as_str`.
macro_rules! assert_names {
    ($($type:ident: [$($variant:ident),+] => $expected:literal;)+) => {$(
        let names = [$($type::$variant),+].map(|value| {
            assert_eq!($type::from_public_str(value.as_str()), Some(value));
            value.as_str()
        });
        assert_eq!(names.join(" "), $expected);
        assert_eq!($type::from_public_str(""), None);
    )+};
}

#[test]
fn public_names_are_pinned_and_round_trip() {
    assert_names! {
        FilesystemProfile: [Portable, Posix, Windows, Browser] => "portable posix windows browser";
        AccessMode: [ReadOnly, ReadWrite] => "read-only read-write";
        ConsistencyMode: [Pinned, TrackingSafe, Live, Manual] => "pinned tracking-safe live manual";
        ConcurrencyMode: [ExclusiveWriter, Optimistic, SerializedAuthority]
            => "exclusive-writer optimistic serialized-authority";
        Lifecycle: [Ephemeral, Durable] => "ephemeral durable";
        MutationMode: [None, PrivateOverlay, DirectLive] => "none private-cow direct-live";
        CaseSensitivity: [Sensitive, ProfileFolded] => "sensitive profile-folded";
        UnicodePolicy: [Preserve, RequireNfc] => "preserve require-nfc";
        NameEncoding: [Utf8, PosixBytes, WindowsUtf16Le] => "utf8 posix-bytes windows-utf16le";
        ResidencyReason: [DirectorySuccessor, SequentialRange, MetadataSuccessor, ConsumerHint]
            => "directory-successor sequential-range metadata-successor consumer-hint";
        ResidencyRejection: [
            WrongVolume, StaleGeneration, InvalidRequest, DuplicateObject, DuplicateOperation,
            OperationCapacity, ByteCapacity, CostBudget, LowUsefulness
        ] => "wrong-volume stale-generation invalid-request duplicate-object duplicate-operation operation-capacity byte-capacity cost-budget low-usefulness";
        StorageTier: [ProcessMemory, NodeLocal, SharedCache, DurableOrigin]
            => "process-memory node-local shared-cache durable-origin";
        PromotionRejection: [
            WrongVolume, StaleGeneration, InvalidRequest, InputCapacity, MissingSource,
            DuplicateObject, DuplicateOperation, ActiveCapacity, NoDestination, LowUsefulness
        ] => "wrong-volume stale-generation invalid-request input-capacity missing-source duplicate-object duplicate-operation active-capacity no-destination low-usefulness";
    }
}
