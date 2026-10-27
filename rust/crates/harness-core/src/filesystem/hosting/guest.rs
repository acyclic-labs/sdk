//! Guest sources derive from the same Bound as hosted turns and publication.

use super::*;
use crate::filesystem::{PreparationIo, native_access::owner_mount_options};
use acyclic_fs::native_mount::{
    GuestPosixPolicy, GuestSessionAdmission, LocalGuestSource, MountFilesystem,
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};

/// Trusted self-host enrollment loaded by the local owner, never a guest
/// request. Cache behavior is chosen by the transport admission itself.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalGuestEnrollment {
    /// Exact volume in Bound's canonical filesystem host.
    pub volume: crate::conversation::VolumeRef,
    /// Source-owned POSIX identities and permitted metadata operations.
    pub posix: GuestPosixPolicy,
    /// Permit authored effects, subject to persisted volume policy.
    pub writable: bool,
}

struct EnrolledSource {
    source: Arc<dyn MountFilesystem>,
    enrollment: LocalGuestEnrollment,
    _custody: NativeAllocation,
}

impl LocalGuestSource for EnrolledSource {
    fn filesystem(&self) -> &Arc<dyn MountFilesystem> {
        &self.source
    }
    fn posix(&self) -> &GuestPosixPolicy {
        &self.enrollment.posix
    }
    fn writable(&self) -> bool {
        self.enrollment.writable
    }
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits a local guest from this binding's original signing custody and
    /// exact canonical source. Wrong providers, hosted requests and empty
    /// identity maps are refused before any volume IO.
    ///
    /// Hosted callers remain unsupported: a completed INVAL cannot prove
    /// consumption or revoke a private guest mapping. No TTL/cache flag can
    /// widen the original parent admission into that transport contract.
    pub async fn guest_session(
        &self,
        enrollment: LocalGuestEnrollment,
        work: &NativeMaterializationWork,
    ) -> crate::Result<GuestSessionAdmission> {
        if !matches!(self.admission, TurnAdmission::LocalRoot) {
            return Err(crate::Error::Unsupported(
                "guest transport cannot fence private mappings of a hosted scope".into(),
            ));
        }
        self.volumes
            .validate_provider(enrollment.volume.provider())?;
        if enrollment.posix.identities.is_empty() {
            return Err(crate::Error::Unauthorized(
                "guest identity map is empty".into(),
            ));
        }
        let io = PreparationIo::from_native_work(work)?;
        enrollment
            .volume
            .validate(&mut Some(&mut io.history_io()))?;
        let bytes =
            (std::mem::size_of::<EnrolledSource>() + 2 * std::mem::size_of::<usize>()) as u64;
        let bytes = bytes
            .checked_add(enrollment.volume.logical_owned_bytes()?)
            .and_then(|bytes| {
                bytes.checked_add(
                    (enrollment.posix.identities.capacity() * std::mem::size_of::<(u32, u32)>())
                        as u64,
                )
            })
            .ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "guest enrollment ownership overflow",
            })?;
        let custody = work.claim(bytes, 1, WorkCounters::UNCHARGED)?;
        let name = enrollment
            .volume
            .storage_name(&mut Some(&mut io.history_io()))?;
        let reference = crate::resources::WorkspaceRef::from_resource(
            crate::resources::ResourceRef::new(
                crate::resources::ResourceKind::Workspace,
                enrollment.volume.provider().clone(),
                name.into_bytes(),
                None,
                &mut Some(&mut io.history_io()),
            )?,
            &mut Some(&mut io.history_io()),
        )?;
        let workspace = self.volumes.open(&reference, Some(&io)).await?;
        let options =
            owner_mount_options(&workspace, &self.issuer, None, enrollment.writable, work).await?;
        let source = workspace.native_source(&options).await?;
        GuestSessionAdmission::local_root(Arc::new(EnrolledSource {
            source,
            enrollment,
            _custody: custody,
        }))
        .map_err(Into::into)
    }
}
