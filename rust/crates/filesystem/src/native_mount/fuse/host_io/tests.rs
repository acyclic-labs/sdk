//! Exercise the real embedded host, kernel socket backpressure and original
//! payload credit. The peer supplies device packets, never a mock server.

use super::super::{fuse_vec, mount::Device, tests::fuse_fixture_work};
use super::*;
use acyclic_native_runtime::{PacketDevice, fuse_protocol};
use rustix::net::{
    AddressFamily, SendAncillaryBuffer, SendAncillaryMessage, SendFlags, SocketFlags, SocketType,
};
use std::mem::MaybeUninit;
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{Sender, channel};

enum Event {
    Pending,
    Completed(io::Result<()>),
}

struct Stop<'a>(&'a HostIo);
impl Drop for Stop<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

fn send_device(socket: &UnixStream, device: &UnixStream) -> io::Result<()> {
    let rights = [device.as_fd()];
    let mut storage = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
    let mut ancillary = SendAncillaryBuffer::new(&mut storage);
    assert!(ancillary.push(SendAncillaryMessage::ScmRights(&rights)));
    assert_eq!(
        rustix::net::sendmsg(
            socket,
            &[IoSlice::new(&[1])],
            &mut ancillary,
            SendFlags::empty()
        )?,
        1
    );
    Ok(())
}

fn kernel_initialize(mut kernel: UnixStream, page: u32) -> io::Result<UnixStream> {
    kernel.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut request = [0; 56];
    for (offset, value) in [
        (0, 56_u32),
        (4, 26),
        (40, 7),
        (44, 31),
        (48, page),
        (52, fuse_protocol::MAX_PAGES as u32),
    ] {
        request[offset..offset + 4].copy_from_slice(&value.to_ne_bytes());
    }
    request[8..16].copy_from_slice(&1_u64.to_ne_bytes());
    kernel.write_all(&request)?;
    let mut response = [0; 80];
    assert_eq!(kernel.read(&mut response)?, response.len());
    assert_eq!(&response[4..8], &[0; 4]);
    assert_eq!(&response[8..16], &1_u64.to_ne_bytes());
    assert_eq!(&response[16..20], &7_u32.to_ne_bytes());
    assert_eq!(&response[20..24], &31_u32.to_ne_bytes());
    assert_eq!(&response[36..40], &page.to_ne_bytes());
    Ok(kernel)
}

fn fill(mut device: &UnixStream, filler: &[u8]) -> io::Result<usize> {
    let mut count = 0;
    loop {
        match device.write(filler) {
            Ok(length) => {
                assert_eq!(length, filler.len());
                count += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(count),
            Err(error) => return Err(error),
        }
    }
}

fn publish(
    io: &HostIo,
    work: &crate::NativeMaterializationWork,
    frame: &[u8],
    early: bool,
    events: Sender<Event>,
) -> io::Result<()> {
    SOCKET_COMPLETION.set(early);
    let pending = events.clone();
    BEFORE_RECEIPT.with_borrow_mut(|probe| {
        *probe = Some(Box::new(move || {
            let _ = pending.send(Event::Pending);
        }))
    });
    let mut payload = fuse_vec(frame.len(), work).map_err(io::Error::other)?;
    payload.extend_from_slice(frame);
    let result = io.publish(&[IoSlice::new(&payload)]);
    drop(payload);
    events
        .send(Event::Completed(result))
        .map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn hosted_publication_keeps_original_payload_until_device_write_and_stop_joins_it()
-> Result<(), Box<dyn std::error::Error>> {
    if acyclic_native_runtime::private_file::with_isolated_user_runtime(
        "native_mount::fuse::host_io::tests::hosted_publication_keeps_original_payload_until_device_write_and_stop_joins_it",
    )? {
        return Ok(());
    }
    let work = fuse_fixture_work();
    // SAFETY: sysconf has no pointer arguments or ownership effects.
    let page = u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })?;
    let capacity = page.checked_add(4096).ok_or("packet capacity overflow")?;
    let (device, kernel) = rustix::net::socketpair(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::CLOEXEC,
        None,
    )?;
    let mut writer = Some(UnixStream::from(device));
    writer
        .as_ref()
        .ok_or("device absent")?
        .set_nonblocking(true)?;
    let (receive, send) = UnixStream::pair()?;
    let allocation = work.claim(
        PacketDevice::storage_bytes(capacity)?,
        4,
        crate::WorkCounters::UNCHARGED,
    )?;
    let device = PacketDevice::receive_device(receive, capacity)?;
    send_device(&send, writer.as_ref().ok_or("device absent")?)?;
    drop(send);
    device.wait_mounted()?;
    let io = Arc::new(HostIo::host(
        Device::Hosted {
            device,
            allocation,
            maximum_write: page,
        },
        &work,
    )?);
    let _startup_cleanup = Stop(&io);
    let kernel = UnixStream::from(kernel);
    let peer = std::thread::spawn(move || kernel_initialize(kernel, page));
    let initialized = io.start_fuse(
        fuse_protocol::Configuration {
            maximum_write: page,
            page_size: page,
            cache_capabilities: 0,
        },
        None,
    );
    let mut kernel = peer.join().map_err(|_| "kernel peer panicked")??;
    initialized?;

    std::thread::scope(|scope| -> Result<(), Box<dyn std::error::Error>> {
        // This guard is inside the scope: a failed assertion interrupts the
        // publisher before scoped-thread joining, rather than stranding it.
        let _cleanup = Stop(&io);
        for (early, stop) in [(false, false), (true, false), (false, true)] {
            let filler = [0xa5; 16];
            let queued = fill(writer.as_ref().ok_or("device absent")?, &filler)?;
            assert!(queued > 0, "backpressure must consume actual kernel credit");
            let mut frame = [0; 16];
            frame[..4].copy_from_slice(&16_u32.to_ne_bytes());
            frame[8..].copy_from_slice(&2_u64.to_ne_bytes());
            let baseline = work.live_allocation_bytes();
            let (events, observed) = channel();
            let publisher_io = Arc::clone(&io);
            let publisher_work = work.clone();
            let worker =
                scope.spawn(move || publish(&publisher_io, &publisher_work, &frame, early, events));
            let pending = match observed.recv()? {
                Event::Pending => true,
                Event::Completed(result) => {
                    result?;
                    false
                }
            };
            // The old socket-completion path must fail this exact guarantee.
            assert_eq!(
                std::panic::catch_unwind(|| assert!(pending)).is_ok(),
                !early
            );
            if pending {
                assert!(
                    work.live_allocation_bytes() >= baseline + frame.len() as u64,
                    "original payload credit must remain live before the physical write"
                );
            } else {
                assert_eq!(
                    work.live_allocation_bytes(),
                    baseline,
                    "negative control must expose the early allocation release"
                );
            }
            if stop {
                drop(writer.take());
                io.stop();
            }
            let mut bytes = [0; 16];
            for _ in 0..queued {
                assert_eq!(kernel.read(&mut bytes)?, bytes.len());
                assert_eq!(bytes, filler);
            }
            if stop {
                assert_eq!(
                    kernel.read(&mut bytes)?,
                    0,
                    "retirement must close the real device endpoint"
                );
            } else {
                assert_eq!(kernel.read(&mut bytes)?, bytes.len());
                assert_eq!(bytes, frame);
            }
            if pending {
                let Event::Completed(result) = observed.recv()? else {
                    return Err("duplicate pending event".into());
                };
                if stop {
                    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ENODEV));
                } else {
                    result?;
                    assert_eq!(work.live_allocation_bytes(), baseline);
                }
            } else {
                // Drain the receipt the deliberately broken client skipped.
                assert_eq!(
                    io.publication(false)?,
                    acyclic_native_runtime::PacketDevicePublication::Published
                );
            }
            worker.join().map_err(|_| "publisher panicked")??;
        }
        Ok(())
    })?;
    Ok(())
}
