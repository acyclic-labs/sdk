//! Standalone fixture children borrow the parent's one in-memory custody store.
//! This transport exists only in test-support builds; no production store can select it.
use super::*;
use std::{
    io::{self, Read as _, Write as _},
    net::{SocketAddr, TcpListener, TcpStream},
    thread::JoinHandle,
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use zeroize::Zeroizing;

const ADDRESS: &str = "ACYCLIC_FIXTURE_CUSTODY_ADDRESS";
const TOKEN: &str = "ACYCLIC_FIXTURE_CUSTODY_TOKEN";

fn platform(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::PlatformFailure(error.into())
}

pub(super) struct Remote {
    address: io::Result<SocketAddr>,
    token: io::Result<[u8; 32]>,
}

impl std::fmt::Debug for Remote {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("FixtureSigningStore")
    }
}

pub(super) fn remote() -> Option<Remote> {
    let address = std::env::var_os(ADDRESS)?;
    Some(Remote {
        address: address.to_string_lossy().parse().map_err(io::Error::other),
        token: (|| {
            let text = std::env::var(TOKEN).map_err(io::Error::other)?;
            let mut token = [0; 32];
            if text.len() != 64 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            for (index, byte) in token.iter_mut().enumerate() {
                *byte = u8::from_str_radix(
                    text.get(index * 2..index * 2 + 2)
                        .ok_or(io::ErrorKind::InvalidData)?,
                    16,
                )
                .map_err(io::Error::other)?;
            }
            Ok(token)
        })(),
    })
}

impl Remote {
    pub(super) fn request(
        &self,
        operation: u8,
        name: &str,
        secret: &[u8],
    ) -> keyring::Result<Vec<u8>> {
        let address = self
            .address
            .as_ref()
            .map_err(|_| platform(io::Error::other("fixture custody address is invalid")))?;
        let token = self
            .token
            .as_ref()
            .map_err(|_| platform(io::Error::other("fixture custody token is invalid")))?;
        let mut stream = TcpStream::connect(address).map_err(platform)?;
        stream.write_all(token).map_err(platform)?;
        stream.write_all(&[operation]).map_err(platform)?;
        if name.len() != 64 || !name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(platform(io::Error::from(io::ErrorKind::InvalidInput)));
        }
        stream.write_all(name.as_bytes()).map_err(platform)?;
        if operation == 1 && secret.len() != 32 {
            return Err(platform(io::Error::from(io::ErrorKind::InvalidInput)));
        }
        if operation == 1 {
            stream.write_all(secret).map_err(platform)?;
        }
        let mut status = [0];
        stream.read_exact(&mut status).map_err(platform)?;
        match status[0] {
            0 => {
                let mut secret = if operation == 2 {
                    vec![0; 32]
                } else {
                    Vec::new()
                };
                stream.read_exact(&mut secret).map_err(platform)?;
                Ok(secret)
            }
            1 => Err(Error::NoEntry),
            _ => Err(platform(io::Error::other(
                "fixture custody request refused",
            ))),
        }
    }
}

/// Owns one isolated in-memory credential store across standalone fixture children.
/// Secrets never reach disk and are cleared with the parent-owned store.
pub struct FixtureCustody {
    address: SocketAddr,
    token: [u8; 32],
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl FixtureCustody {
    /// Create the parent-owned in-memory fixture store.
    ///
    /// # Errors
    /// Returns the original entropy, socket or thread admission failure.
    pub fn new() -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let address = listener.local_addr()?;
        let token = crate::owner_record::new_key()?;
        let store = MemorySigningStore::new()?;
        listener.set_nonblocking(true)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()?;
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("fixture-custody".into())
            .spawn(move || {
                runtime.block_on(async {
                    let Ok(listener) = tokio::net::TcpListener::from_std(listener) else {
                        return;
                    };
                    loop {
                        let connection = tokio::select! {
                            _ = &mut stopped => break,
                            connection = listener.accept() => connection,
                        };
                        let Ok((mut stream, _)) = connection else {
                            break;
                        };
                        tokio::select! {
                            _ = &mut stopped => break,
                            _ = serve(&mut stream, &token, &store) => {},
                        }
                    }
                });
            })?;
        Ok(Self {
            address,
            token,
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    /// Configure a test-support child to borrow this in-memory store.
    /// Production builds do not recognize these environment variables.
    pub fn configure(&self, mut environment: impl FnMut(&str, &std::ffi::OsStr)) {
        environment(ADDRESS, self.address.to_string().as_ref());
        environment(
            TOKEN,
            blake3::Hash::from_bytes(self.token)
                .to_hex()
                .as_str()
                .as_ref(),
        );
    }
}

async fn serve(
    stream: &mut tokio::net::TcpStream,
    token: &[u8; 32],
    store: &MemorySigningStore,
) -> io::Result<()> {
    use subtle::ConstantTimeEq as _;
    let mut supplied = [0; 32];
    stream.read_exact(&mut supplied).await?;
    if !bool::from(supplied.ct_eq(token)) {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    let mut operation = [0];
    stream.read_exact(&mut operation).await?;
    // Delegate to the existing fixture backend. The host adds isolation only;
    // it has no second secret map, publication policy or cleanup protocol.
    let mut name = [0; 64];
    stream.read_exact(&mut name).await?;
    if !name.iter().all(u8::is_ascii_hexdigit) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    let name = std::str::from_utf8(&name).map_err(io::Error::other)?;
    let entry = store.entry(name)?;
    let result = match operation[0] {
        1 => {
            let mut secret = Zeroizing::new([0; 32]);
            stream.read_exact(&mut secret[..]).await?;
            entry.set_secret(&secret[..]).map(|()| Vec::new())
        }
        2 => entry.get_secret(),
        3 => entry.delete_credential().map(|()| Vec::new()),
        _ => return Err(io::ErrorKind::InvalidInput.into()),
    };
    match result {
        Ok(secret) => {
            let secret = Zeroizing::new(secret);
            stream.write_all(&[0]).await?;
            stream.write_all(&secret).await?;
        }
        Err(Error::NoEntry) => stream.write_all(&[1]).await?,
        Err(_) => stream.write_all(&[2]).await?,
    }
    Ok(())
}

impl Drop for FixtureCustody {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
