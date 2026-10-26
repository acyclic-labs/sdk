//! Acknowledged admission of a suspended process into the existing custodian.
//!
//! A PID is only a rendezvous: the spawning caller retains its process handle
//! and does not resume it until the custodian holds and watches the same Job
//! member. This also serves thin terminal adapters that cannot use `std::Command`.

use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

pub(super) const ENV: &str = "ACYCLIC_PROCESS_CUSTODY_ADMISSION";
pub(super) const ADMIT: usize = usize::MAX - 2;

pub(super) struct Request {
    pub pid: u32,
    pub reply: mpsc::SyncSender<bool>,
}

pub(super) struct Server {
    listener: TcpListener,
    pub endpoint: String,
}

impl Server {
    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let endpoint = listener.local_addr()?.to_string();
        Ok(Self { listener, endpoint })
    }

    pub fn start(self, port: HANDLE) -> io::Result<Receiver<Request>> {
        let (send, receive) = mpsc::channel();
        let port = port.expose_provenance();
        std::thread::Builder::new()
            .name("custodian-admission".into())
            .spawn(move || {
                for stream in self.listener.incoming() {
                    let Ok(mut stream) = stream else { break };
                    let mut pid = [0; 4];
                    if stream.read_exact(&mut pid).is_err() {
                        continue;
                    }
                    let (reply, result) = mpsc::sync_channel(0);
                    if send
                        .send(Request {
                            pid: u32::from_le_bytes(pid),
                            reply,
                        })
                        .is_err()
                    {
                        break;
                    }
                    // SAFETY: the custodian owns this completion port until exit.
                    if unsafe {
                        PostQueuedCompletionStatus(
                            std::ptr::with_exposed_provenance_mut(port),
                            0,
                            ADMIT,
                            std::ptr::null(),
                        )
                    } == 0
                    {
                        break;
                    }
                    let admitted = result.recv().unwrap_or(false);
                    let _ = stream.write_all(&[u8::from(admitted)]);
                }
            })?;
        Ok(receive)
    }
}

/// The caller must hold this suspended process's handle through the reply.
pub(super) fn admit(endpoint: &str, pid: u32) -> io::Result<()> {
    let mut stream = TcpStream::connect(endpoint)?;
    stream.set_nodelay(true)?;
    stream.write_all(&pid.to_le_bytes())?;
    let mut reply = [0];
    stream.read_exact(&mut reply)?;
    if reply == [1] {
        Ok(())
    } else {
        Err(io::Error::other("custodian rejected process admission"))
    }
}
