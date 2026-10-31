//! Funded continuation checks call the public production encoder and decoder.
use super::*;
use crate::{Digest, FilesystemProfile, GenerationId, ObjectId, ObjectKind, ObjectStorage};

const PROFILES: [FilesystemProfile; 4] = [
    FilesystemProfile::Portable,
    FilesystemProfile::Posix,
    FilesystemProfile::Windows,
    FilesystemProfile::Browser,
];

fn work(
    cancellation: &CancellationToken,
) -> Result<crate::NativeMaterializationWork, LazyWorkspaceError> {
    crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, cancellation)
        .map_err(|(error, _)| error.into())
}

fn check_profile(
    bytes: &[u8],
    profile: FilesystemProfile,
    work: &crate::NativeMaterializationWork,
) -> Result<(), LazyWorkspaceError> {
    let limits = crate::VolumeLimits::default();
    let Ok(value) = LazyDirectoryCursor::decode(bytes, profile, limits, work) else {
        return Ok(());
    };
    let encoded = value.encode(work)?;
    let repeated = LazyDirectoryCursor::decode(&encoded.0, profile, limits, work)?;
    assert_eq!(
        repeated.cut, value.cut,
        "continuation lost its original cut"
    );
    let canonical = repeated.encode(work)?;
    assert_eq!(canonical.0, encoded.0);
    let mut trailing = encoded.0.clone();
    trailing.push(0);
    assert!(LazyDirectoryCursor::decode(&trailing, profile, limits, work).is_err());
    for version in [0_u16, 2, u16::MAX] {
        let mut header = DOMAIN.to_vec();
        header.extend_from_slice(&version.to_le_bytes());
        assert!(matches!(
            LazyDirectoryCursor::decode(&header, profile, limits, work),
            Err(LazyWorkspaceError::Filesystem(crate::FsError::Decode(
                CanonicalDecodeError::UnsupportedVersion(found)
            ))) if found == version
        ));
    }
    let mut ctx = KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    };
    repeated.retire(&mut ctx)?;
    value.retire(&mut ctx)?;
    Ok(())
}

/// All native name profiles share the same parser and exact cut identity.
pub fn check(bytes: &[u8]) -> bool {
    let cancellation = CancellationToken::new();
    let Ok(work) = work(&cancellation) else {
        return false;
    };
    let baseline = work.live_allocation_bytes();
    let valid = PROFILES
        .into_iter()
        .all(|profile| check_profile(bytes, profile, &work).is_ok());
    assert_eq!(
        work.live_allocation_bytes(),
        baseline,
        "cursor retained decode custody"
    );
    valid
}

/// Capture every durable continuation phase with plain and sealed generations.
pub fn seeds() -> Result<Vec<(String, Vec<u8>)>, LazyWorkspaceError> {
    let cancellation = CancellationToken::new();
    let work = work(&cancellation)?;
    let baseline = work.live_allocation_bytes();
    let mut ctx = KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: work.cancellation(),
    };
    let limits = crate::VolumeLimits::default();
    let source = SourceReference {
        identity: [2; 16],
        epoch: 3,
    };
    let mut seeds = Vec::new();
    for sealed in [false, true] {
        let generation = GenerationId::from_root(ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([4; 32]),
            storage: sealed.then_some(ObjectStorage {
                locator: Digest::from_bytes([5; 32]),
                root_file_id: FileId::from_bytes([6; 16]),
                origin_generation: Digest::from_bytes([7; 32]),
            }),
        });
        let cut = LazySnapshotRef::new(
            WorkspaceId::from_bytes([1; 16]),
            generation,
            source,
            LazyOverlayId::from_bytes([8; 32]),
            LazyShadowId::from_bytes([9; 32]),
        );
        for (profile_tag, profile) in PROFILES.into_iter().enumerate() {
            for phase_tag in 0..4 {
                let (directory, mut allocation) = NamespacePath::parse_in_profile(
                    "/retained",
                    profile,
                    limits,
                    &mut ctx,
                    work.cancellation(),
                )?;
                let phase = match phase_tag {
                    0 => LazyDirectoryPhase::Source(None),
                    1 => LazyDirectoryPhase::Source(Some(SourceCursor { source, token: 11 })),
                    2 => LazyDirectoryPhase::Authored(None),
                    _ => {
                        let encoding = crate::kernel::NameEncoding::native_for_profile(profile);
                        let bytes = match encoding {
                            crate::kernel::NameEncoding::WindowsUtf16Le => {
                                "entry".encode_utf16().flat_map(u16::to_le_bytes).collect()
                            }
                            crate::kernel::NameEncoding::PosixBytes => b"entry-\xff".to_vec(),
                            crate::kernel::NameEncoding::Utf8 => b"entry".to_vec(),
                        };
                        let name =
                            LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                                .map_err(|error| {
                                    crate::FsError::from(CanonicalDecodeError::Invariant(
                                        CanonicalInvariant::Name(error),
                                    ))
                                })?;
                        let (name, mut backing) =
                            name.clone_with_accounting(&mut ctx, work.cancellation())?;
                        allocation.absorb(&mut backing)?;
                        LazyDirectoryPhase::Authored(Some(name))
                    }
                };
                let cursor = LazyDirectoryCursor {
                    cut,
                    directory,
                    phase,
                    allocation,
                };
                let encoded = cursor.encode(&work)?;
                let captured = encoded.0.clone();
                drop(encoded);
                seeds.push((
                    format!("directory-cursor-{sealed}-{profile_tag}-{phase_tag}"),
                    captured,
                ));
                cursor.retire(&mut ctx)?;
            }
        }
    }
    assert_eq!(
        work.live_allocation_bytes(),
        baseline,
        "cursor writer retained custody"
    );
    Ok(seeds)
}
