//! Acknowledged admission of a suspended process into the owner's custodian.
//!
//! A PID is only a rendezvous: the spawning member retains its process handle
//! and does not resume it until the custodian holds and watches the same Job
//! member. This serves thin terminal adapters that start members themselves.
//! [`ENV`] holds `127.0.0.1:<port>/<tree>`; a request is the little-endian
//! PID followed by the little-endian 64-bit tree, and the reply is one byte,
//! 1 once the process is watched as a member of that tree.

use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, TcpListener};
use std::sync::mpsc::{self, Receiver};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

pub(super) const ENV: &str = "ACYCLIC_PROCESS_CUSTODY_ADMISSION";
pub(super) const ADMIT: usize = usize::MAX - 2;

pub(super) struct Request {
    pub pid: u32,
    pub tree: usize,
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
                    let mut request = [0; 12];
                    if stream.read_exact(&mut request).is_err() {
                        continue;
                    }
                    let (pid, tree) = request.split_at(4);
                    let (Ok(pid), Some(tree)) = (
                        <[u8; 4]>::try_from(pid),
                        <[u8; 8]>::try_from(tree)
                            .ok()
                            .and_then(|tree| usize::try_from(u64::from_le_bytes(tree)).ok()),
                    ) else {
                        continue;
                    };
                    let (reply, result) = mpsc::sync_channel(0);
                    if send
                        .send(Request {
                            pid: u32::from_le_bytes(pid),
                            tree,
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
