//! The actual static host against real kernel sockets and wire frames.

use super::super::*;
use crate::fuse_protocol::{self, Configuration, INPUT_HEADER, OUTPUT_HEADER};
use std::os::fd::AsRawFd;

fn request(opcode: u32, unique: u64, uid: u32, body: &[u8]) -> Vec<u8> {
    let mut header = [0; INPUT_HEADER];
    let length = u32::try_from(INPUT_HEADER + body.len()).expect("proof frame length");
    header[..4].copy_from_slice(&length.to_ne_bytes());
    header[4..8].copy_from_slice(&opcode.to_ne_bytes());
    header[8..16].copy_from_slice(&unique.to_ne_bytes());
    header[16..24].copy_from_slice(&1_u64.to_ne_bytes());
    header[24..28].copy_from_slice(&uid.to_ne_bytes());
    let mut bytes = Vec::with_capacity(INPUT_HEADER + body.len());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(body);
    bytes
}

fn host() -> io::Result<(UnixStream, PacketDeviceRetirement, UnixStream)> {
    let (source, custody, kernel, writer) = host_with_writer()?;
    drop(writer);
    Ok((source, custody, kernel))
}

fn host_with_writer() -> io::Result<(UnixStream, PacketDeviceRetirement, UnixStream, File)> {
    let mut pair = [-1; 2];
    // SAFETY: pair owns writable descriptor slots; success transfers both.
    if unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            pair.as_mut_ptr(),
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful socketpair returned two fresh owned descriptors.
    let (device, kernel) =
        unsafe { (OwnedFd::from_raw_fd(pair[0]), OwnedFd::from_raw_fd(pair[1])) };
    let mut kernel = UnixStream::from(kernel);
    kernel.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    // SAFETY: sysconf has no pointer arguments.
    let page =
        u32::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).map_err(io::Error::other)?;
    let (receive, helper) = UnixStream::pair()?;
    let admitted = PacketDevice::receive_device(
        receive,
        page.checked_add(4096).ok_or(io::ErrorKind::InvalidInput)?,
    )?;
    runner::send_message(&helper, &[device.as_raw_fd()], &[1])?;
    admitted.wait_mounted()?;
    drop(helper);
    let (source, mut custody) = admitted.into_parts();
    let source = UnixStream::from(OwnedFd::from(source));
    source.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    let mut init = [0; 16];
    for (field, value) in init
        .as_chunks_mut::<4>()
        .0
        .iter_mut()
        .zip([7, 31, page, 1 << 22])
    {
        field.copy_from_slice(&value.to_ne_bytes());
    }
    kernel.write_all(&request(26, 1, 0, &init))?;
    custody.start_fuse(
        Configuration {
            maximum_write: page,
            page_size: page,
            cache_capabilities: 0,
        },
        None,
    )?;
    let mut initialized = [0; 80];
    kernel.read_exact(&mut initialized)?;
    assert_eq!(&initialized[4..8], &0_i32.to_ne_bytes());
    Ok((source, custody, kernel, File::from(device)))
}

#[test]
fn full_request_window_keeps_unadmitted_packets_in_the_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    if crate::private_file::with_isolated_user_runtime(
        "process_tree::packet_device::tests::wire::full_request_window_keeps_unadmitted_packets_in_the_kernel",
    )? {
        return Ok(());
    }
    let (mut source, custody, mut kernel, device) = host_with_writer()?;
    for unique in 2..66 {
        let original = request(1, unique, crate::private_file::effective_user(), b"child\0");
        kernel.write_all(&original)?;
        let mut delivered = vec![0; original.len()];
        source.read_exact(&mut delivered)?;
        assert_eq!(delivered, original);
    }
    let parked = request(1, 66, crate::private_file::effective_user(), b"parked\0");
    kernel.write_all(&parked)?;
    let mut notification = [0; 40];
    notification[..4].copy_from_slice(&40_u32.to_ne_bytes());
    notification[4..8].copy_from_slice(&2_i32.to_ne_bytes());
    notification[16..24].copy_from_slice(&1_u64.to_ne_bytes());
    source.write_all(&notification)?;
    let mut published = [0; 40];
    kernel.read_exact(&mut published)?;
    assert_eq!(published, notification);
    assert_eq!(
        custody.publication(true)?,
        PacketDevicePublication::Published
    );
    // Notification completion is an event barrier after the host's progress
    // pass. An uncredited packet must still belong to the physical endpoint,
    // where closing the device can refuse it even if the backend dies.
    let mut pending: libc::c_int = 0;
    // SAFETY: pending is live writable storage for FIONREAD's integer result.
    if unsafe { libc::ioctl(device.as_raw_fd(), libc::FIONREAD, &raw mut pending) } < 0 {
        return Err(io::Error::last_os_error().into());
    }
    assert_eq!(usize::try_from(pending)?, parked.len());
    let mut reply = [0; OUTPUT_HEADER];
    fuse_protocol::reply_header(&mut reply, OUTPUT_HEADER, 2, None)
        .map_err(io::Error::from_raw_os_error)?;
    source.write_all(&reply)?;
    let mut published_reply = [0; OUTPUT_HEADER];
    kernel.read_exact(&mut published_reply)?;
    assert_eq!(published_reply, reply);
    assert_eq!(
        custody.publication(false)?,
        PacketDevicePublication::Published
    );
    let mut delivered = vec![0; parked.len()];
    source.read_exact(&mut delivered)?;
    assert_eq!(delivered, parked);
    Ok(())
}

#[test]
fn notification_backpressure_does_not_stop_kernel_request_delivery()
-> Result<(), Box<dyn std::error::Error>> {
    if crate::private_file::with_isolated_user_runtime(
        "process_tree::packet_device::tests::wire::notification_backpressure_does_not_stop_kernel_request_delivery",
    )? {
        return Ok(());
    }
    let (mut source, mut custody, mut kernel, mut writer) = host_with_writer()?;
    struct Disconnect(UnixStream);
    impl Drop for Disconnect {
        fn drop(&mut self) {
            let _ = self.0.shutdown(std::net::Shutdown::Both);
        }
    }
    // Failed controls must wake the actual blocked physical writer before
    // the original custody's destructor joins device retirement.
    let _disconnect = Disconnect(kernel.try_clone()?);
    rustix::fs::fcntl_setfl(
        &writer,
        rustix::fs::fcntl_getfl(&writer)? | rustix::fs::OFlags::NONBLOCK,
    )?;
    let filler = [0xa5; OUTPUT_HEADER];
    let mut queued = 0;
    loop {
        match writer.write(&filler) {
            Ok(length) => {
                assert_eq!(length, filler.len());
                queued += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => return Err(error.into()),
        }
    }
    assert!(
        queued > 0,
        "the original physical endpoint must exhaust actual kernel credit"
    );
    drop(writer);
    let mut notification = [0; 40];
    notification[..4].copy_from_slice(&40_u32.to_ne_bytes());
    notification[4..8].copy_from_slice(&2_i32.to_ne_bytes());
    notification[16..24].copy_from_slice(&1_u64.to_ne_bytes());
    source.write_all(&notification)?;
    // The first progress pass may have read only the frame header. A second
    // original kernel request must still be delivered after its full body
    // makes publication wait for the physical endpoint's missing credit.
    for unique in [2, 3] {
        let original = request(1, unique, crate::private_file::effective_user(), b"child\0");
        kernel.write_all(&original)?;
        let mut delivered = vec![0; original.len()];
        source.read_exact(&mut delivered)?;
        assert_eq!(delivered, original);
    }
    // The second request is delivered only after notification admission. Stop
    // while the original physical endpoint still has no write credit. Its
    // borrowed descriptor and packet must survive until physical completion.
    custody.request_stop()?;
    for _ in 0..queued {
        let mut physical = [0; OUTPUT_HEADER];
        kernel.read_exact(&mut physical)?;
        assert_eq!(physical, filler);
    }
    let mut notified = false;
    loop {
        let mut physical = [0; 40];
        let length = kernel.read(&mut physical)?;
        match length {
            0 => break,
            40 => {
                assert!(!notified);
                assert_eq!(physical, notification);
                notified = true;
            }
            OUTPUT_HEADER => {
                assert_eq!(&physical[4..8], &(-libc::EIO).to_ne_bytes());
                assert!(matches!(
                    u64::from_ne_bytes(physical[8..16].try_into()?),
                    2 | 3
                ));
            }
            _ => return Err("physical packet was split during retirement".into()),
        }
    }
    assert!(
        notified,
        "retirement closed a device still borrowed by the notifier"
    );
    custody.retire()?;
    Ok(())
}

#[test]
fn reply_and_notification_receipts_keep_their_original_publication_class()
-> Result<(), Box<dyn std::error::Error>> {
    if crate::private_file::with_isolated_user_runtime(
        "process_tree::packet_device::tests::wire::reply_and_notification_receipts_keep_their_original_publication_class",
    )? {
        return Ok(());
    }
    let (mut source, custody, mut kernel) = host()?;
    let mut reply = [0; OUTPUT_HEADER];
    fuse_protocol::reply_header(&mut reply, OUTPUT_HEADER, 2, None)
        .map_err(io::Error::from_raw_os_error)?;
    let mut notification = [0; 40];
    notification[..4].copy_from_slice(&40_u32.to_ne_bytes());
    notification[4..8].copy_from_slice(&2_i32.to_ne_bytes());
    notification[16..24].copy_from_slice(&1_u64.to_ne_bytes());
    source.write_all(&reply)?;
    source.write_all(&notification)?;
    let mut physical_reply = [0; OUTPUT_HEADER];
    let mut physical_notification = [0; 40];
    kernel.read_exact(&mut physical_reply)?;
    kernel.read_exact(&mut physical_notification)?;
    assert_eq!(physical_reply, reply);
    assert_eq!(physical_notification, notification);
    // The notification waiter consumes a reply receipt first, retaining it
    // for its original producer rather than claiming socket acceptance.
    assert_eq!(
        custody.publication(true)?,
        PacketDevicePublication::Published
    );
    assert_eq!(
        custody.publication(false)?,
        PacketDevicePublication::Published
    );
    Ok(())
}

#[test]
fn foreign_flush_uses_handle_authority_while_foreign_lookup_is_refused()
-> Result<(), Box<dyn std::error::Error>> {
    if crate::private_file::with_isolated_user_runtime(
        "process_tree::packet_device::tests::wire::foreign_flush_uses_handle_authority_while_foreign_lookup_is_refused",
    )? {
        return Ok(());
    }
    let (mut source, custody, mut kernel) = host()?;
    // This is a real host/kernel-wire conformance case: no callback or policy
    // function is mocked. The negative lookup must never reach the backend.
    // SAFETY: geteuid has no arguments or ownership effects.
    let foreign = crate::private_file::effective_user() ^ 1;
    let flush = request(25, 2, foreign, &[0; 24]);
    kernel.write_all(&flush)?;
    let mut ready = [
        libc::pollfd {
            fd: source.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: kernel.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    // A deadline only fails a broken routing proof; readiness is an OS event.
    // SAFETY: ready owns both initialized pollfd slots for this call.
    assert!(unsafe { libc::poll(ready.as_mut_ptr(), 2, 5000) } > 0);
    assert_ne!(
        ready[0].revents & libc::POLLIN,
        0,
        "FLUSH was refused instead of delegated"
    );
    let mut delegated = vec![0; flush.len()];
    source.read_exact(&mut delegated)?;
    assert_eq!(delegated, flush);
    let mut reply = [0; OUTPUT_HEADER];
    fuse_protocol::reply_header(&mut reply, OUTPUT_HEADER, 2, None)
        .map_err(io::Error::from_raw_os_error)?;
    source.write_all(&reply)?;
    kernel.read_exact(&mut reply)?;
    assert_eq!(
        custody.publication(false)?,
        PacketDevicePublication::Published
    );
    kernel.write_all(&request(1, 3, foreign, b"untrusted\0"))?;
    kernel.read_exact(&mut reply)?;
    assert_eq!(&reply[4..8], &(-libc::EACCES).to_ne_bytes());
    assert_eq!(&reply[8..16], &3_u64.to_ne_bytes());
    Ok(())
}

#[test]
fn receipt_backpressure_retires_instead_of_losing_publication()
-> Result<(), Box<dyn std::error::Error>> {
    if crate::private_file::with_isolated_user_runtime(
        "process_tree::packet_device::tests::wire::receipt_backpressure_retires_instead_of_losing_publication",
    )? {
        return Ok(());
    }
    let (mut source, mut custody, mut kernel) = host()?;
    let mut budget: libc::c_int = 0;
    let mut length = libc::socklen_t::try_from(std::mem::size_of_val(&budget))?;
    // Both control endpoints use unchanged default Unix socket funding in this
    // private network namespace. Each queued receipt costs at least its bytes.
    // SAFETY: budget/length own writable storage of the supplied lengths.
    if unsafe {
        libc::getsockopt(
            custody.control.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            (&raw mut budget).cast(),
            &raw mut length,
        )
    } < 0
    {
        return Err(io::Error::last_os_error().into());
    }
    let maximum = usize::try_from(budget)? / Receipt::SIZE + 2;
    let mut reply = [0; OUTPUT_HEADER];
    fuse_protocol::reply_header(&mut reply, OUTPUT_HEADER, 2, None)
        .map_err(io::Error::from_raw_os_error)?;
    let mut closed = false;
    // Deliberately withhold receipt reads. Physical device reads are the event
    // barrier for each publication; no elapsed-time readiness probe is used.
    for _ in 0..maximum {
        if let Err(error) = source.write_all(&reply) {
            assert!(matches!(
                error.kind(),
                io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
            ));
            closed = true;
            break;
        }
        let mut physical = [0; OUTPUT_HEADER];
        let length = kernel.read(&mut physical)?;
        if length == 0 {
            closed = true;
            break;
        }
        assert_eq!(length, OUTPUT_HEADER);
        assert_eq!(physical, reply);
    }
    assert!(
        closed,
        "full receipt queue silently lost completion without retiring its device"
    );
    // A later retirement receipt may fit once we start draining, or closure
    // may be unacknowledged. Either must release this wait; neither leaves a
    // live device after the publication receipt could not be sent.
    match custody.retire() {
        Ok(()) => assert!(custody.retired.load(std::sync::atomic::Ordering::Acquire)),
        Err(error) => assert!(
            matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::NotConnected
            ),
            "unexpected retirement failure: {error}"
        ),
    }
    Ok(())
}
