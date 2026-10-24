use rustix::net::{RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags, ReturnFlags};
use std::{
    io::{self, IoSliceMut},
    mem::MaybeUninit,
    os::{fd::OwnedFd, unix::net::UnixStream},
};

#[cfg(test)]
#[path = "reply/check.rs"]
pub(super) mod check;

pub(super) fn receive_device(socket: &UnixStream) -> io::Result<OwnedFd> {
    let mut data = [0; 5];
    let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(2))];
    let mut control = RecvAncillaryBuffer::new(&mut space);
    let message = loop {
        match rustix::net::recvmsg(
            socket,
            &mut [IoSliceMut::new(&mut data)],
            &mut control,
            RecvFlags::CMSG_CLOEXEC,
        ) {
            Err(rustix::io::Errno::INTR) => continue,
            result => break result?,
        }
    };
    let files = control
        .drain()
        .filter_map(|message| match message {
            RecvAncillaryMessage::ScmRights(files) => Some(files),
            _ => None,
        })
        .flatten();
    one_device(message.bytes, message.flags, files)
}

pub(super) fn one_device(
    bytes: usize,
    flags: ReturnFlags,
    mut files: impl Iterator<Item = OwnedFd>,
) -> io::Result<OwnedFd> {
    let first = files.next().ok_or(io::ErrorKind::InvalidData)?;
    let extra = files.next().is_some();
    #[cfg(test)]
    let extra = extra && !ACCEPT_EXTRA.get();
    if bytes == 0 || flags.contains(ReturnFlags::CTRUNC) || extra {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(first)
}

#[cfg(test)]
std::thread_local! {
    static ACCEPT_EXTRA: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn accepting_extra<T>(f: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            ACCEPT_EXTRA.set(self.0);
        }
    }
    let _restore = Restore(ACCEPT_EXTRA.replace(true));
    f()
}
