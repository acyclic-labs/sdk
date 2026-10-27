//! Public packet and standard vhost-user-fs entries over one projection.

use super::packet::PacketRequest;
use super::packet_router::PacketRouter;
use super::packet_session::PacketSession;
use super::vhost_session::VhostSession;
use super::*;
use crate::native_mount::LocalGuestSource;
use std::num::{NonZeroU16, NonZeroU32};
use std::os::unix::net::UnixStream;

pub(super) fn require_metadata(
    policy: &crate::native_mount::GuestPosixPolicy,
    request: &PacketRequest<'_>,
) -> Result<(), NativeMountError> {
    // Decode before entering the projection or source. Use the same ABI
    // decoder as dispatch so malformed setattr cannot bypass this decision.
    if request.opcode == 4 {
        let (_, changes) =
            super::packet_dispatch::decode_setattr(super::packet::PacketInput::new(request.body))?;
        if (changes.mode.is_some()
            || changes.uid.is_some()
            || changes.gid.is_some()
            || changes.atime.is_some()
            || changes.mtime.is_some()
            || changes.changed.is_some())
            && !policy.metadata
        {
            return Err(libc::EACCES.into());
        }
        if changes
            .uid
            .is_some_and(|uid| !policy.identities.iter().any(|identity| identity.0 == uid))
            || changes
                .gid
                .is_some_and(|gid| !policy.identities.iter().any(|identity| identity.1 == gid))
        {
            return Err(libc::EACCES.into());
        }
    }
    if matches!(request.opcode, 21 | 24) && !policy.metadata {
        return Err(libc::EACCES.into());
    }
    // Device creation has no enrolled device authority; ordinary files still
    // use CREATE and directories use MKDIR through the source's path policy.
    if request.opcode == 8 {
        return Err(libc::EACCES.into());
    }
    Ok(())
}

/// One local-root source admission. Cache behavior is fixed by this transport:
/// direct I/O, zero TTLs and no writeback or private-mapping revocation claim.
/// Session consumers accept this admission, never a path, uid or cache flag.
pub struct GuestSessionAdmission {
    protocol: PacketSession,
    work: crate::NativeMaterializationWork,
}

impl GuestSessionAdmission {
    /// Admits an enrollment issued by the trusted local source composition.
    /// Rechecks its original source guard before the first volume operation.
    pub fn local_root(owner: Arc<dyn LocalGuestSource>) -> Result<Self, NativeMountError> {
        let source = owner.filesystem();
        let work = source
            .native_original_work()?
            .ok_or(NativeMountError::ControlPending)?;
        let (_original, guard) = super::super::shared::metering::install(source.as_ref(), &work)?;
        if guard.is_none() {
            return Err(MountSourceError::AccessDenied.into());
        }
        let identity = *owner.posix().identities.first().ok_or(libc::EACCES)?;
        let root = source
            .lookup_with_meter(&ROOT_PATH, &work)?
            .ok_or(libc::ENOENT)?;
        let stamp = source.view_stamp_with_meter(&work)?;
        let core = ProjectionCore::new(
            Arc::clone(source),
            root,
            stamp,
            ProjectionConfig {
                writable: owner.writable(),
                owner: identity,
                inode_base: 0,
                cache: ProjectionCache::Direct,
            },
            &work,
        )?;
        let mut protocol = PacketSession::new(
            PacketRouter::Single(FuseProjection { core }),
            64 * 1024,
            NonZeroU32::new(4096).ok_or(libc::EINVAL)?,
        )?;
        protocol.guest = Some(owner);
        Ok(Self { protocol, work })
    }

    /// Largest request/reply storage needed by the admitted packet entry.
    #[must_use]
    pub fn packet_capacity(&self) -> usize {
        self.protocol.packet_capacity()
    }

    /// Takes the packet entry's original terminal callback cause and receipt.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        self.protocol.single_core()?.take_handler_failure()
    }

    /// Executes a raw Linux FUSE packet. `publish` must complete the actual
    /// write or used-ring publication before returning success. `None` is a
    /// no-reply completion. Failed publication closes the session; never replay
    /// a mutation whose physical completion is uncertain.
    pub fn packet(
        &self,
        packet: &[u8],
        response: &mut [u8],
        publish: impl FnOnce(Option<&[u8]>) -> Result<(), NativeMountError>,
    ) -> Result<bool, NativeMountError> {
        let request = PacketRequest::parse(packet)?;
        self.protocol
            .publish(&request, response, publish)
            .map_err(|error| error.into_native_error())
    }

    /// Serves standard vhost-user-fs on an already accepted Unix connection.
    /// Returns only after every physical worker has positively joined.
    pub fn serve(
        self,
        connection: UnixStream,
        tag: &[u8],
    ) -> Result<GuestSessionRetired, NativeMountError> {
        VhostSession::new(
            self.protocol,
            connection,
            NonZeroU16::new(512).ok_or(libc::EINVAL)?,
            tag,
            &self.work,
        )
        .map(|session| GuestSessionRetired(session.run()))
    }
}

/// Joined transport with its original typed callback cause and actual counters.
pub struct GuestSessionRetired(super::vhost_session::VhostRetired);

impl GuestSessionRetired {
    /// Consumes the original callback cause once, without substituting errno.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        self.0.take_callback_failure()
    }
    /// Work completed by the original callback owner.
    #[must_use]
    pub fn completed_callback_work(&self) -> crate::WorkCounters {
        self.0.completed_callback_work()
    }
    /// Transport result after workers have joined.
    pub fn transport(self) -> Result<(), NativeMountError> {
        self.0.transport
    }
}
