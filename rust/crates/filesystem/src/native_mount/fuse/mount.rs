use super::*;

pub(super) struct FuseMount {
    pub(super) path: FuseOwned<Vec<u8>>,
    pub(super) device: Option<OwnedFd>,
    pub(super) helper: Option<std::process::Child>,
    pub(super) socket: Option<std::os::unix::net::UnixStream>,
    pub(super) mounted: bool,
}

impl FuseMount {
    pub(super) fn prepare(destination: &Path, work: &crate::NativeMaterializationWork)
        -> Result<Self, NativeMountError>
    {
        let bytes = destination.as_os_str().as_bytes();
        if bytes.contains(&0) { return Err(NativeMountError::InvalidDestination); }
        let count = bytes.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let mut path = fuse_vec(count, work).map_err(NativeMountError::from)?;
        fuse_copy_admit(bytes.len() as u64, work).map_err(NativeMountError::from)?;
        path.extend_from_slice(bytes);
        path.push(0);
        fuse_copy_record(bytes.len() as u64, work).map_err(NativeMountError::from)?;
        Ok(Self { path, device: None, helper: None, socket: None, mounted: false })
    }

    pub(super) fn start(&mut self, config: &Config, writable: bool, mode: libc::mode_t)
        -> std::io::Result<()>
    {
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        if config.mount_options.contains(&MountOption::AutoUnmount) {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
            .map_err(|_| std::io::ErrorKind::InvalidInput)?;
        self.device = Some(std::fs::OpenOptions::new().read(true).write(true)
            .open("/dev/fuse")?.into());
        let fd = self.device.as_ref().ok_or(std::io::ErrorKind::NotConnected)?.as_raw_fd();
        // SAFETY: getuid/getgid have no preconditions.
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        let options = std::ffi::CString::new(format!(
            "fd={fd},rootmode={mode:o},user_id={uid},group_id={gid},default_permissions",
        ))?;
        let flags = libc::MS_NODEV | libc::MS_NOSUID | libc::MS_NOATIME
            | if writable { 0 } else { libc::MS_RDONLY };
        // SAFETY: all strings are terminated and the owned device stays open.
        if unsafe { libc::mount(c"acyclic-fs".as_ptr(), path.as_ptr(), c"fuse".as_ptr(),
            flags, options.as_ptr().cast()) } == 0 {
            self.mounted = true;
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EPERM) { return Err(error); }
        self.device.take();
        let (child_socket, socket) = std::os::unix::net::UnixStream::pair()?;
        self.socket = Some(socket);
        let fd = child_socket.as_raw_fd();
        let mut command = Command::new("fusermount3");
        command.stdout(Stdio::null()).stderr(Stdio::null())
            .args(["-o", if writable { "fsname=acyclic-fs,default_permissions,exec,noatime" }
                else { "fsname=acyclic-fs,default_permissions,exec,noatime,ro" }, "--"])
            .arg(OsStr::from_bytes(path.to_bytes())).env("_FUSE_COMMFD", fd.to_string());
        // SAFETY: the child only clears CLOEXEC on its inherited socket before exec.
        unsafe { command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) == -1 { Err(std::io::Error::last_os_error()) }
            else { Ok(()) }
        }); }
        self.helper = Some(command.spawn()?);
        drop(child_socket);
        self.device = Some(receive_fuse_device(
            self.socket.as_ref().ok_or(std::io::ErrorKind::NotConnected)?,
        )?);
        self.mounted = true;
        self.socket.take();
        Ok(())
    }

    pub(super) fn try_unmount(&mut self) -> std::io::Result<()> {
        if !self.mounted {
            return if self.helper.is_some() { Err(std::io::Error::from_raw_os_error(libc::EBUSY)) }
                else { Ok(()) };
        }
        let mut poll = libc::pollfd {
            fd: self.device.as_ref().ok_or(std::io::ErrorKind::NotConnected)?.as_raw_fd(),
            events: 0, revents: 0,
        };
        loop {
            // SAFETY: poll references one initialized descriptor owned by this mount.
            if unsafe { libc::poll(&mut poll, 1, 0) } >= 0 { break; }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted { return Err(error); }
        }
        if poll.revents & libc::POLLERR == 0 {
            let path = std::ffi::CStr::from_bytes_with_nul(&self.path)
                .map_err(|_| std::io::ErrorKind::InvalidInput)?;
            // SAFETY: the owned pathname is terminated and the device remains open.
            if unsafe { libc::umount2(path.as_ptr(), libc::MNT_DETACH) } != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
        self.mounted = false;
        Ok(())
    }

    pub(super) fn drain_helper(&mut self) -> std::io::Result<()> {
        self.socket.take();
        if let Some(helper) = self.helper.as_mut() {
            match helper.wait() {
                Ok(_) => {}
                Err(error) if error.raw_os_error() == Some(libc::ECHILD) => {}
                Err(error) => return Err(error),
            }
            self.helper.take();
        }
        Ok(())
    }
}

pub(super) fn receive_fuse_device(socket: &std::os::unix::net::UnixStream) -> std::io::Result<OwnedFd> {
    let mut byte = 0u8;
    let mut iov = libc::iovec { iov_base: std::ptr::from_mut(&mut byte).cast(), iov_len: 1 };
    let mut control = [0usize; 4];
    // SAFETY: zero is valid for msghdr; its buffers remain alive through recvmsg.
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = std::mem::size_of_val(&control);
    let received = loop {
        // SAFETY: message contains valid writable data and aligned control buffers.
        let received = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, libc::MSG_CMSG_CLOEXEC) };
        if received >= 0 { break received; }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted { return Err(error); }
    };
    let mut device = None;
    // SAFETY: CMSG helpers traverse the kernel-filled control buffer; every received fd is owned.
    unsafe {
        let mut header = libc::CMSG_FIRSTHDR(&message);
        while !header.is_null() {
            if (*header).cmsg_level == libc::SOL_SOCKET && (*header).cmsg_type == libc::SCM_RIGHTS {
                let count = ((*header).cmsg_len - libc::CMSG_LEN(0) as usize)
                    / std::mem::size_of::<libc::c_int>();
                for index in 0..count {
                    let fd = libc::CMSG_DATA(header).cast::<libc::c_int>().add(index).read_unaligned();
                    let owned = OwnedFd::from_raw_fd(fd);
                    if device.is_none() { device = Some(owned); }
                }
            }
            header = libc::CMSG_NXTHDR(&message, header);
        }
    }
    if received == 0 || message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    device.ok_or_else(|| std::io::ErrorKind::InvalidData.into())
}
