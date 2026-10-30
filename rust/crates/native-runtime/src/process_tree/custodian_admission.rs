//! Acknowledged admission of a suspended process into the owner's custodian.
//!
//! A PID is only a rendezvous: the spawning member retains its process handle
//! and does not resume it until the custodian holds and watches the same Job
//! member. This serves thin terminal adapters that start members themselves.
//! [`ENV`] holds `127.0.0.1:<port>/<tree>/<secret>`; a request is the
//! little-endian PID, the little-endian 64-bit tree and the tree's 16-byte
//! secret, and the reply is one byte, 1 once the process is watched as a
//! member of that tree.
//!
//! Any local process can reach a loopback port, so the port alone admits
//! nothing: the owner draws a random secret per tree and hands it only to
//! the custodian and, through [`ENV`], to the tree's members. A request
//! without it is refused, and even an admitted request only makes the
//! custodian watch a process that is already a member of the tree's Job.

use std::io::{self, Read as _, Write as _};
use std::net::{Ipv4Addr, TcpListener};
use std::sync::mpsc::{self, Receiver};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::System::IO::PostQueuedCompletionStatus;

pub(super) const ENV: &str = "ACYCLIC_PROCESS_CUSTODY_ADMISSION";
pub(super) const ADMIT: usize = usize::MAX - 2;

/// A tree's admission secret.
pub(super) type Secret = [u8; 16];

/// A fresh admission secret.
pub(super) fn secret() -> io::Result<Secret> {
    let mut secret = [0; 16];
    getrandom::fill(&mut secret).map_err(io::Error::other)?;
    Ok(secret)
}

pub(super) fn encode(secret: &Secret) -> String {
    secret.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn decode(text: &str) -> Option<Secret> {
    let mut secret = [0; 16];
    if text.len() != 32 {
        return None;
    }
    for (byte, pair) in secret.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(secret)
}

/// Compares secrets in time independent of where they first differ.
pub(super) fn matches(expected: &Secret, presented: &Secret) -> bool {
    expected
        .iter()
        .zip(presented)
        .fold(0, |difference, (a, b)| difference | (a ^ b))
        == 0
}

pub(super) struct Request {
    pub pid: u32,
    pub tree: usize,
    pub secret: Secret,
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
                    let mut request = [0; 28];
                    if stream.read_exact(&mut request).is_err() {
                        continue;
                    }
                    let (pid, rest) = request.split_at(4);
                    let (tree, secret) = rest.split_at(8);
                    let (Ok(pid), Some(tree), Ok(secret)) = (
                        <[u8; 4]>::try_from(pid),
                        <[u8; 8]>::try_from(tree)
                            .ok()
                            .and_then(|tree| usize::try_from(u64::from_le_bytes(tree)).ok()),
                        Secret::try_from(secret),
                    ) else {
                        continue;
                    };
                    let (reply, result) = mpsc::sync_channel(0);
                    if send
                        .send(Request {
                            pid: u32::from_le_bytes(pid),
                            tree,
                            secret,
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

#[cfg(test)]
#[path = "custodian_admission_tests.rs"]
mod tests;
