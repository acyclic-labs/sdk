//! Exclusive physical packet-device custody outside every consuming owner.
//!
//! The host is shared by this OS user in the host's network namespace. Its
//! readiness queue watches a mount-free thread's pidfd, rather than waiting
//! for the entire owner process (which may contain a blocked device caller).

use super::runner;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::{Mutex, OnceLock};

/// A host-owned physical device and the consuming owner's stream endpoint.
/// Closing it joins the host's actual device closure before returning.
pub struct PacketDevice {
    stream: File,
    retirement: PacketDeviceRetirement,
}

impl PacketDevice {
    /// Transfers exclusive custody before any request can enter the device.
    /// Frames in both directions begin with their total native-endian u32
    /// byte length. `capacity` is the device's negotiated receive envelope.
    pub fn host(device: File, capacity: u32) -> io::Result<Self> {
        if capacity < 4 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let owner = owner_exit()?;
        let control = connect()?;
        let (stream, theirs) = UnixStream::pair()?;
        let mut message = [0; 8];
        message[..4].copy_from_slice(&capacity.to_ne_bytes());
        message[4..].copy_from_slice(&1_u32.to_ne_bytes());
        runner::send_message(
            &control,
            &[device.as_raw_fd(), theirs.as_raw_fd(), owner.as_raw_fd()],
            &message,
        )?;
        let mut accepted = [0];
        (&control).read_exact(&mut accepted)?;
        if accepted != [1] {
            return Err(io::ErrorKind::InvalidData.into());
        }
        // No consumer ever retains /dev/fuse after acknowledged transfer.
        drop(device);
        drop(theirs);
        Ok(Self {
            stream: File::from(OwnedFd::from(stream)),
            retirement: PacketDeviceRetirement {
                control,
                retired: false,
            },
        })
    }

    /// Split the endpoint from the original physical-retirement obligation.
    pub fn into_parts(self) -> (File, PacketDeviceRetirement) {
        (self.stream, self.retirement)
    }
}

/// Original host custody. Its EOF acknowledges actual device closure.
pub struct PacketDeviceRetirement {
    control: UnixStream,
    retired: bool,
}

impl PacketDeviceRetirement {
    /// Stop admission and wait for EOF, published only after device closure.
    pub fn retire(&mut self) -> io::Result<()> {
        if self.retired {
            return Ok(());
        }
        self.control.shutdown(std::net::Shutdown::Write)?;
        let mut byte = [0];
        loop {
            match self.control.read(&mut byte) {
                Ok(0) => {
                    self.retired = true;
                    return Ok(());
                }
                Ok(_) => return Err(io::ErrorKind::InvalidData.into()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl Drop for PacketDeviceRetirement {
    fn drop(&mut self) {
        if let Err(error) = self.retire() {
            tracing::error!(%error, "packet device retirement failed");
        }
    }
}

fn owner_exit() -> io::Result<&'static OwnedFd> {
    static OWNER: OnceLock<OwnedFd> = OnceLock::new();
    static MAKING: Mutex<()> = Mutex::new(());
    let _making = MAKING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(owner) = OWNER.get() {
        return Ok(owner);
    }
    let (started, ready) = std::sync::mpsc::sync_channel(1);
    let (keep, parked) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("acyclic-device-owner".into())
        .spawn(move || {
            // This thread never accesses a volume. SIGKILL therefore ends it even
            // while another thread is waiting uninterruptibly on its own mount.
            // SAFETY: gettid has no arguments; pidfd_open transfers a fresh fd.
            let tid = unsafe { libc::syscall(libc::SYS_gettid) };
            let file = unsafe { libc::syscall(libc::SYS_pidfd_open, tid, libc::PIDFD_THREAD) };
            let result = if file < 0 {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "thread pidfd required for packet-device owner death: {}",
                        io::Error::last_os_error()
                    ),
                ))
            } else {
                // SAFETY: pidfd_open returned this fresh descriptor.
                Ok(unsafe { OwnedFd::from_raw_fd(file as i32) })
            };
            if started.send(result).is_ok() {
                let _ = parked.recv();
            }
        })?;
    let descriptor = ready
        .recv()
        .map_err(|_| io::Error::other("device owner thread exited during admission"))??;
    // Process lifetime owns these two handles, once per owner, never per view.
    static LIFETIME: OnceLock<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)> =
        OnceLock::new();
    let _ = LIFETIME.set((keep, thread));
    Ok(OWNER.get_or_init(|| descriptor))
}

fn address() -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    // SAFETY: getuid has no arguments or failure case.
    let name = format!("acyclic-packet-device-v1-{}", unsafe { libc::geteuid() });
    // SAFETY: all-zero is valid address storage, including the abstract NUL.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as _;
    if name.len() + 1 > address.sun_path.len() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    for (target, byte) in address.sun_path[1..].iter_mut().zip(name.bytes()) {
        *target = byte as _;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len();
    Ok((address, length as _))
}

fn socket() -> io::Result<OwnedFd> {
    // SOCK_SEQPACKET makes each descriptor-admission envelope atomic.
    // SAFETY: socket transfers a fresh descriptor on success.
    let file = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if file < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket returned this fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(file) })
}

fn connect() -> io::Result<UnixStream> {
    let (address, length) = address()?;
    let listener = socket()?;
    // Binding is the single kernel election: only one host can own the name.
    // SAFETY: the initialized address remains live throughout bind.
    if unsafe { libc::bind(listener.as_raw_fd(), (&raw const address).cast(), length) } == 0 {
        // The backlog is kernel backpressure, not a limit on admitted owners.
        // SAFETY: listener is the fresh bound socket.
        if unsafe { libc::listen(listener.as_raw_fd(), libc::SOMAXCONN) } < 0 {
            return Err(io::Error::last_os_error());
        }
        runner::start_packet_device_host(listener)?;
    } else {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EADDRINUSE) {
            return Err(error);
        }
    }
    let stream = socket()?;
    // SAFETY: live socket and initialized abstract address.
    if unsafe { libc::connect(stream.as_raw_fd(), (&raw const address).cast(), length) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let control = UnixStream::from(stream);
    let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
    // SAFETY: SO_PEERCRED fills the initialized peer structure.
    if unsafe {
        libc::getsockopt(
            control.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut peer).cast(),
            &raw mut size,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // A listener created by this client retains its creator's credentials;
    // ownership is authenticated by UID, not the listener's recorded PID.
    if peer.uid != unsafe { libc::geteuid() } {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    Ok(control)
}
