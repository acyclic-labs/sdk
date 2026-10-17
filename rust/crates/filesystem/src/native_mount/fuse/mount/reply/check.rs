//! Shared socket round-trip and descriptor-custody oracle for properties and fuzzing.
use rustix::{
    fs::{FlockOperation, flock},
    net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags},
};
use std::{
    fs::File,
    io::{IoSlice, Read},
    mem::MaybeUninit,
    os::{fd::AsFd, unix::net::UnixStream},
};

pub fn check(input: &[u8]) {
    let count = usize::from(input.first().copied().unwrap_or(0) % 5);
    let payload = &input.get(1..).unwrap_or_default()[..input.len().saturating_sub(1).min(32)];
    let path = tempfile::NamedTempFile::new().unwrap();
    let expected = b"the received descriptor retains the complete original bytes";
    std::fs::write(path.path(), expected).unwrap();
    let source = File::open(path.path()).unwrap();
    let probe = File::open(path.path()).unwrap();
    flock(&source, FlockOperation::NonBlockingLockExclusive).unwrap();
    let files = (0..count)
        .map(|_| source.try_clone().unwrap())
        .collect::<Vec<_>>();
    let rights = files.iter().map(AsFd::as_fd).collect::<Vec<_>>();
    let mut storage = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(4))];
    let mut control = SendAncillaryBuffer::new(&mut storage);
    if count != 0 {
        assert!(control.push(SendAncillaryMessage::ScmRights(&rights)));
    }
    let (receive, send) = UnixStream::pair().unwrap();
    assert_eq!(
        rustix::net::sendmsg(
            &send,
            &[IoSlice::new(payload)],
            &mut control,
            SendFlags::empty()
        )
        .unwrap(),
        payload.len()
    );
    drop(control);
    drop(rights);
    drop(files);
    drop(source);
    drop(send);
    let received = super::receive_device(&receive);
    // Production closes the reply socket immediately too. Unread stream data
    // can retain kernel references to its original ancillary message.
    drop(receive);
    assert_eq!(received.is_ok(), count == 1 && !payload.is_empty());
    if let Ok(fd) = received {
        assert!(
            rustix::io::fcntl_getfd(&fd)
                .unwrap()
                .contains(rustix::io::FdFlags::CLOEXEC)
        );
        assert_eq!(
            flock(&probe, FlockOperation::NonBlockingLockExclusive),
            Err(rustix::io::Errno::AGAIN)
        );
        let mut file = File::from(fd);
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, expected);
    }
    // Every rejected or consumed descriptor must be closed, including truncated
    // ancillary messages. Independent open-file descriptions make this immune to
    // descriptor-number reuse by other tests.
    flock(&probe, FlockOperation::NonBlockingLockExclusive).unwrap();
}
