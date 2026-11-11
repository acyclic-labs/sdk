//! Drive the physical host entry with recorded kernel frames and real policy
//! storage. Refresh must be able to notify the projection before lookup replies.

use super::super::{FuseProjection, packet_router::PacketRouter};
use super::super::{
    host_session::HostSession, packet::PacketRequest, packet_buffers::PacketBuffers,
};
use super::*;
use crate::native_mount::{NativeAccessOwner, NativeMountCallbackFuture, ViewOrigin};
use std::fs::File;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixDatagram;

#[derive(Default)]
struct Refresh {
    notification: Mutex<Option<(Weak<ProjectionCore>, ViewStamp)>>,
    panic: bool,
}

#[test]
fn mountpoint_access_waits_for_published_init_with_early_access_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let work = fuse_fixture_work();
    let core = projection_core(
        Arc::new(source),
        crate::WorkBudget::UNBOUNDED,
        work.cancellation(),
    )?;
    let (device, kernel) = UnixDatagram::pair()?;
    kernel.set_nonblocking(true)?;
    let descriptor: OwnedFd = device.into();
    let io = Arc::new(super::super::host_io::HostIo::new(
        File::from(descriptor),
        &work,
    )?);
    let session = HostSession::new(PacketRouter::Single(FuseProjection { core }), io, &work)?;
    let capture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../fixtures/fuse/linux-kernel-packets.json"
    ))?;
    let init = capture["packets"]
        .as_array()
        .ok_or("missing packets")?
        .iter()
        .find_map(|packet| {
            let bytes: Vec<u8> = serde_json::from_value(packet["request"].clone()).ok()?;
            PacketRequest::parse(&bytes)
                .is_ok_and(|request| request.opcode == 26)
                .then_some(bytes)
        })
        .ok_or("missing INIT")?;
    // No timing assumption: the real transport has not received INIT yet.
    // A container's pre-INIT namespace probe can return ENOTCONN here.
    let access = || {
        let mut reply = [0; 256];
        match kernel.recv(&mut reply) {
            Ok(length) => {
                assert!(length >= 16);
                assert_eq!(&reply[4..8], &0i32.to_ne_bytes());
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(NativeMountError::host_operation(
                    "statx mountpoint",
                    Path::new("/synthetic/mount"),
                    std::io::Error::from_raw_os_error(libc::ENOTCONN),
                ))
            }
            Err(error) => Err(error.into()),
        }
    };
    assert!(
        matches!(access(), Err(NativeMountError::HostOperation { source, .. }) if source.raw_os_error() == Some(libc::ENOTCONN)),
        "early-access negative control escaped"
    );
    kernel.send(&init)?;
    // Exercise the same barrier used by both ordinary and shared mounts,
    // including the real INIT decode and physical reply publication.
    session.after_init(Path::new("/synthetic/mount"), access)?;
    Ok(())
}

impl NativeAccessOwner for Refresh {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new(work, || async move {
            let (core, stamp) = self.notification.lock().unwrap().take().unwrap();
            // A different thread reproduces the real lock cycle even in builds
            // whose rank checker detects same-thread recursive acquisition.
            std::thread::spawn(move || {
                core.upgrade()
                    .unwrap()
                    .view_changed(stamp, ViewOrigin::new());
            })
            .join()
            .unwrap();
            assert!(!self.panic, "lookup policy refresh unwind");
            Err(MountSourceError::AccessDenied.into())
        })
    }

    fn select<'a>(
        &'a self,
        _: crate::GenerationId,
        _: &'a crate::NativeMaterializationWork,
    ) -> Result<NativeMountCallbackFuture<'a, crate::MountOptions>, NativeMountError> {
        Err(MountSourceError::NativeUnsupported.into())
    }
}

#[test]
fn lookup_replies_when_policy_refresh_notifies_or_fails() -> Result<(), Box<dyn std::error::Error>>
{
    for panic in [false, true] {
        let (source, _) = shared_sources(posix_config())?;
        let refresh = Arc::new(Refresh {
            panic,
            ..Refresh::default()
        });
        let source = Arc::new(source.with_access_refresh(Some(refresh.clone())));
        let work = fuse_fixture_work();
        let core = projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            work.cancellation(),
        )?;
        *refresh.notification.lock().unwrap() =
            Some((Arc::downgrade(&core), source.view_stamp().unwrap()));
        let (device, client) = UnixDatagram::pair()?;
        client.set_read_timeout(Some(Duration::from_secs(2)))?;
        let descriptor: OwnedFd = device.into();
        let io = Arc::new(super::super::host_io::HostIo::new(
            File::from(descriptor),
            &work,
        )?);
        let session = HostSession::new(PacketRouter::Single(FuseProjection { core }), io, &work)?;
        let capture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../fixtures/fuse/linux-kernel-packets.json"
        ))?;
        let packet = |opcode| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
            for packet in capture["packets"].as_array().ok_or("missing packets")? {
                let bytes: Vec<u8> = serde_json::from_value(packet["request"].clone())?;
                if PacketRequest::parse(&bytes).is_ok_and(|request| request.opcode == opcode) {
                    return Ok(bytes);
                }
            }
            Err("missing recorded opcode".into())
        };
        client.send(&packet(26)?)?;
        session.initialize()?;
        let mut reply = [0; 256];
        client.recv(&mut reply)?;
        // Advance persisted policy while leaving the enrolled access snapshot
        // unchanged, forcing the public refresh callback during authorization.
        let mut policy: crate::access::Policy =
            serde_json::from_slice(crate::access::INITIAL_POLICY)?;
        policy.version += 1;
        crate::native_mount::adapter::tests::policy_refresh::publish(source.as_ref(), &policy)?;
        let mut lookup = packet(1)?;
        lookup[16..24].copy_from_slice(&ROOT_INODE.to_ne_bytes());
        lookup[24..28].copy_from_slice(&nix::unistd::geteuid().as_raw().to_ne_bytes());
        let request = PacketRequest::parse(&lookup).map_err(std::io::Error::from_raw_os_error)?;
        let unique = request.unique;
        let mut buffers = PacketBuffers::new(session_capacity(), &work)?;
        client.send(&lookup)?;
        // The callback runtime translates a handler unwind to a typed failure.
        assert!(session.serve(&mut buffers)?);
        assert_eq!(client.recv(&mut reply)?, 16);
        assert_eq!(
            i32::from_ne_bytes(reply[4..8].try_into()?),
            -if panic { libc::EIO } else { libc::EACCES }
        );
        assert_eq!(u64::from_ne_bytes(reply[8..16].try_into()?), unique);
        assert!(refresh.notification.lock().unwrap().is_none());
        if !panic {
            // Fail after receiving a request but before dispatch can construct
            // PacketReply. The host obligation must still publish an error.
            buffers.output = PacketBuffers::new(16, &work)?.output;
            lookup[8..16].copy_from_slice(&(unique + 1).to_ne_bytes());
            client.send(&lookup)?;
            assert!(session.serve(&mut buffers).is_err());
            assert_eq!(client.recv(&mut reply)?, 16);
            assert_eq!(i32::from_ne_bytes(reply[4..8].try_into()?), -libc::EIO);
            assert_eq!(u64::from_ne_bytes(reply[8..16].try_into()?), unique + 1);
        }
    }
    Ok(())
}

fn session_capacity() -> usize {
    super::super::packet_session::MAXIMUM_WRITE as usize + 4096
}
