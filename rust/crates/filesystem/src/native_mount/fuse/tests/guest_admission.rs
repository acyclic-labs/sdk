//! Refusals use captured kernel requests with explicit negative perturbations.

use super::*;
use crate::native_mount::{
    GuestPosixPolicy, GuestSessionAdmission, LocalGuestSource, NativeMountCallbackFuture,
    NativeMountCallbackGuard,
};

struct Enrolled {
    source: Arc<dyn MountFilesystem>,
    posix: GuestPosixPolicy,
}
impl LocalGuestSource for Enrolled {
    fn filesystem(&self) -> &Arc<dyn MountFilesystem> {
        &self.source
    }
    fn posix(&self) -> &GuestPosixPolicy {
        &self.posix
    }
    fn writable(&self) -> bool {
        true
    }
}

struct Revocable(AtomicBool);
impl NativeMountCallbackGuard for Revocable {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded(work, || async {
            if self.0.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            }
        })
    }
    fn check_write<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.check(work)
    }
}

fn enrollment(
    guarded: bool,
) -> Result<
    (
        Arc<Enrolled>,
        Arc<Revocable>,
        crate::NativeMaterializationWork,
    ),
    Box<dyn std::error::Error>,
> {
    let config = posix_config();
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(
        memory_checkout(
            config,
            crate::model::CheckoutMode::tracking_transaction(),
            1,
        )?
        .into_iter()
        .next()
        .ok_or("one checkout")?,
    ));
    let work = cache_work(FINITE_HANDLER_POLICY, &crate::CancellationToken::new());
    let guard = Arc::new(Revocable(AtomicBool::new(true)));
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new_at_meter(
            shared,
            config,
            crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            work.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            &work,
            Some(&work),
        )?,
        vec![],
        None,
    )?;
    let source = if guarded {
        source.with_native_callback_guard(guard.clone())
    } else {
        source
    };
    let source = source.into_arc_meter(&work)?;
    Ok((
        Arc::new(Enrolled {
            source,
            posix: GuestPosixPolicy {
                identities: vec![(0, 0), (1000, 1000)],
                metadata: false,
            },
        }),
        guard,
        work,
    ))
}

fn captured(opcode: u32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    #[derive(serde::Deserialize)]
    struct Capture {
        packets: Vec<Packet>,
    }
    #[derive(serde::Deserialize)]
    struct Packet {
        request: Vec<u8>,
    }
    let capture: Capture = serde_json::from_str(include_str!(
        "../../../../fixtures/fuse/linux-kernel-packets.json"
    ))?;
    capture
        .packets
        .into_iter()
        .map(|packet| packet.request)
        .find(|packet| {
            super::super::packet::PacketRequest::parse(packet)
                .is_ok_and(|request| request.opcode == opcode)
        })
        .ok_or_else(|| "captured kernel opcode missing".into())
}

#[test]
fn revoked_source_is_refused_before_volume_io() -> Result<(), Box<dyn std::error::Error>> {
    let (owner, guard, work) = enrollment(true)?;
    guard.0.store(false, Ordering::Release);
    let before = work.receipt();
    assert!(GuestSessionAdmission::local_root(owner).is_err());
    let after = work.receipt();
    assert_eq!(
        after.backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        after.backend_write_operations,
        before.backend_write_operations
    );
    Ok(())
}

#[test]
fn guest_identity_metadata_and_late_revocation_refuse_before_volume_io()
-> Result<(), Box<dyn std::error::Error>> {
    let (owner, guard, work) = enrollment(true)?;
    let admission = GuestSessionAdmission::local_root(owner)?;
    let mut response = vec![0; admission.packet_capacity()];
    let init = captured(26)?;
    admission.packet(&init, &mut response, |_| Ok(()))?;
    let mut lookup = captured(1)?;
    // Negative vector: change only the captured kernel credential.
    lookup
        .get_mut(24..28)
        .ok_or("kernel uid")?
        .copy_from_slice(&1_u32.to_ne_bytes());
    let before = work.receipt();
    let refusal = |bytes: Option<&[u8]>| {
        let bytes = bytes.ok_or(libc::EPROTO)?;
        let errno = i32::from_ne_bytes(
            bytes
                .get(4..8)
                .ok_or(libc::EPROTO)?
                .try_into()
                .map_err(|_| libc::EPROTO)?,
        );
        assert_eq!(errno, -libc::EACCES);
        Ok(())
    };
    admission.packet(&lookup, &mut response, refusal)?;
    // Negative vector: REMOVE_XATTR of the real captured GETXATTR name.
    let mut metadata = captured(22)?;
    metadata
        .get_mut(4..8)
        .ok_or("kernel opcode")?
        .copy_from_slice(&24_u32.to_ne_bytes());
    metadata.drain(40..48);
    let length = u32::try_from(metadata.len())?;
    metadata
        .get_mut(..4)
        .ok_or("kernel length")?
        .copy_from_slice(&length.to_ne_bytes());
    admission.packet(&metadata, &mut response, refusal)?;
    assert_eq!(
        work.receipt(),
        before,
        "refused guest claims must not enter a source callback"
    );
    guard.0.store(false, Ordering::Release);
    lookup
        .get_mut(24..28)
        .ok_or("kernel uid")?
        .copy_from_slice(&1000_u32.to_ne_bytes());
    assert!(
        admission
            .packet(&lookup, &mut response, |_| Ok(()))
            .is_err()
    );
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    assert!(admission.take_callback_failure().is_some());
    Ok(())
}

#[test]
fn unguarded_source_is_refused_before_volume_io() -> Result<(), Box<dyn std::error::Error>> {
    let (owner, _, work) = enrollment(false)?;
    let before = work.receipt();
    assert!(GuestSessionAdmission::local_root(owner).is_err());
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    Ok(())
}
