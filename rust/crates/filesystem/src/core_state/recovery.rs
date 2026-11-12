//! One durably owned native recovery binding per checkout.
use super::*;
use crate::native_host::{HostRoot, host_path};
use crate::{NativeMaterializationWork, NativeRootIdentity, NativeTreeMaterializationError};

const FAMILY: &str = "native-recovery-v1";

#[derive(Serialize, Deserialize)]
struct Claim {
    version: u32,
    root: [u8; 16],
    directory: Option<[u8; 16]>,
}

/// A checkout-specific recovery binding, shared by publication and source policy.
pub struct NativeRecoveryRoot;

impl NativeRecoveryRoot {
    /// Exact native binding name; no family of user names is reserved.
    #[must_use]
    pub fn name(identity: NativeRootIdentity) -> [u8; 43] {
        let mut name = [0; 43];
        name[..11].copy_from_slice(b".acyclic-r-");
        encode_hex(identity.to_bytes(), &mut name[11..]);
        name
    }

    /// Canonical exclusion used by both capture and lazy native discovery.
    pub fn prefix(
        identity: NativeRootIdentity,
        profile: crate::FilesystemProfile,
        limits: crate::VolumeLimits,
    ) -> Result<crate::kernel::NamespacePath, crate::CaptureError> {
        let mut path = [b'/'; 44];
        path[1..].copy_from_slice(&Self::name(identity));
        let path = std::str::from_utf8(&path).map_err(|_| crate::CaptureError::InvalidOptions)?;
        Ok(crate::kernel::NamespacePath::from_text_in_profile(
            path, profile, limits,
        )?)
    }

    /// Builds the operation path on the caller's original allocation ledger.
    pub fn operation_directory(
        root: &Path,
        identity: NativeRootIdentity,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(PathBuf, crate::NativeAllocation), NativeTreeMaterializationError> {
        let credit = crate::WorkCounters {
            bytes_encoded: 64,
            bytes_copied: 76,
            ..crate::WorkCounters::UNCHARGED
        };
        work.admit(credit)?;
        let mut name = [b'/'; 76];
        name[..43].copy_from_slice(&Self::name(identity));
        encode_hex(operation.into_bytes(), &mut name[44..]);
        work.spent(credit)?;
        let name = std::str::from_utf8(&name)
            .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
        work.path(root, Some(Path::new(name)))
    }
}

fn encode_hex(bytes: [u8; 16], output: &mut [u8]) {
    for (byte, pair) in bytes.into_iter().zip(output.chunks_exact_mut(2)) {
        pair[0] = b"0123456789abcdef"[usize::from(byte >> 4)];
        pair[1] = b"0123456789abcdef"[usize::from(byte & 15)];
    }
}

impl LocalCoreStateStore {
    /// Validates existing recovery ownership without creating native storage.
    pub async fn inspect_native_recovery_root(
        &self,
        root: &Path,
        identity: NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.native_recovery_root(root, identity, work, false).await
    }

    /// Claims recovery before its source is exposed. An unrelated existing
    /// directory is refused, never silently hidden by source exclusions.
    pub async fn bind_native_recovery_root(
        &self,
        root: &Path,
        identity: NativeRootIdentity,
        work: &NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.native_recovery_root(root, identity, work, true).await
    }

    async fn native_recovery_root(
        &self,
        root: &Path,
        identity: NativeRootIdentity,
        work: &NativeMaterializationWork,
        create: bool,
    ) -> Result<(), LocalCoreStateStoreError> {
        let (root, allocation) = work.path(root, None)?;
        self.materialization(work.clone()).transaction(move |namespace| {
            let work = allocation.work();
            let held = HostRoot::open(&root, &mut &*work)?;
            if held.identity() != identity {
                return Err(NativeTreeMaterializationError::AllocationChanged {
                    expected: identity, actual: held.identity(),
                }.into());
            }
            let name = NativeRecoveryRoot::name(identity);
            let name = std::str::from_utf8(&name).map_err(|_| LocalCoreStateStoreError::Integrity)?;
            let leaf = Path::new(name);
            let mut paths = namespace.record(FAMILY, &identity.to_bytes())?;
            with_lock(&mut paths, |paths| {
                let retained = read_recoverable::<Claim>(paths)?;
                let mut claim = match retained.as_ref() {
                    Some((claim, _)) => {
                        if claim.version != 1 || claim.root != identity.to_bytes() {
                            return Err(LocalCoreStateStoreError::Integrity);
                        }
                        Claim { version: 1, root: claim.root, directory: claim.directory }
                    }
                    None => {
                        match host_path(leaf, &mut &*work, |path, work| held.stat(path, work)) {
                            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                                if !create { return Ok(()); }
                            },
                            Ok(_) => return Err(NativeTreeMaterializationError::InvalidLayout.into()),
                            Err(error) => return Err(error.into()),
                        }
                        let claim = Claim { version: 1, root: identity.to_bytes(), directory: None };
                        write_journaled(paths, &claim)?;
                        claim
                    }
                };
                if !create && claim.directory.is_none() {
                    match host_path(leaf, &mut &*work, |path, work| held.stat(path, work)) {
                        Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                        Ok(_) => {},
                        Err(error) => return Err(error.into()),
                    }
                }
                if claim.directory.is_none() {
                    match work.with_original_leaf(|work| host_path(leaf, work, |path, work| held.create_dir(path, work)))
                        .map_err(NativeTreeMaterializationError::from) {
                        Ok(()) => {},
                        Err(NativeTreeMaterializationError::Io(error)) if retained.is_some() && error.kind() == std::io::ErrorKind::AlreadyExists => {},
                        Err(error) => return Err(error.into()),
                    }
                    held.sync_directory(&mut &*work)?;
                }
                let child = work.with_original_leaf(|work| host_path(leaf, work, |path, work| held.open_child(path, work)))
                    .map_err(NativeTreeMaterializationError::from)?;
                match claim.directory {
                    Some(expected) if expected != child.identity().to_bytes() => return Err(NativeTreeMaterializationError::InvalidLayout.into()),
                    Some(_) => {},
                    None => {
                        let (mut reader, _cursor) = host_path(Path::new(""), &mut &*work, |path, work| child.read_dir_stats(path, work))?;
                        if reader.next(&mut &*work)?.is_some() {
                            return Err(NativeTreeMaterializationError::ExternalMutation.into());
                        }
                        claim.directory = Some(child.identity().to_bytes());
                        write_journaled(paths, &claim)?;
                    }
                }
                Ok(())
            })
        }).await
    }
}
