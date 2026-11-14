//! Thin libc adapter for exercising the static host's actual descriptor decoder.

#[path = "../../runner/ancillary.rs"]
mod ancillary;

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

/// Receives a helper reply with the mount host's decoder and custody rules.
/// The socket oracle and fuzzer use real SCM_RIGHTS and original file offsets.
#[doc(hidden)]
pub fn receive_packet_device_reply_for_test(socket: &UnixStream) -> io::Result<OwnedFd> {
    let mut data = [0; 5];
    #[repr(C, align(8))]
    struct Space([u8; 64]);
    let mut control = Space([0; 64]);
    let mut vector = libc::iovec {
        iov_base: data.as_mut_ptr().cast(),
        iov_len: data.len(),
    };
    // SAFETY: a zero msghdr describes no borrowed storage until set below.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &raw mut vector;
    message.msg_iovlen = 1;
    message.msg_control = control.0.as_mut_ptr().cast();
    message.msg_controllen = control.0.len();
    let length = loop {
        // SAFETY: every pointer names live writable storage of the given size.
        let length =
            unsafe { libc::recvmsg(socket.as_raw_fd(), &raw mut message, libc::MSG_CMSG_CLOEXEC) };
        if length >= 0 {
            break length;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    let close = |descriptor| {
        // SAFETY: the decoder surrenders each received descriptor exactly once.
        drop(unsafe { OwnedFd::from_raw_fd(descriptor) });
    };
    let control_length = message.msg_controllen;
    let handles = ancillary::decode::<8>(
        control
            .0
            .get(..control_length.min(control.0.len()))
            .ok_or(io::ErrorKind::InvalidData)?,
        message.msg_flags,
        close,
    )
    .map_err(|()| io::ErrorKind::InvalidData)?;
    if length == 0 {
        handles.close(close);
        return Err(io::ErrorKind::InvalidData.into());
    }
    let descriptor = handles
        .one(close)
        .map_err(|()| io::ErrorKind::InvalidData)?;
    // SAFETY: the actual decoder transferred exactly one live descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}
