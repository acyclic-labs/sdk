#![allow(unsafe_code)]

//! Rustls-backed C ABI stream for the generated .NET gRPC transport.
//!
//! The ABI exposes bytes only. .NET owns HTTP/2 framing and generated protobuf
//! types, while Rust owns TLS configuration and client key handling.

use base64::Engine as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use std::ffi::{CStr, CString, c_char, c_void};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ptr;
use std::time::Duration;

struct Handle {
    stream: std::sync::Mutex<StreamOwned<ClientConnection, TcpStream>>,
}
thread_local! { static LAST_ERROR: std::cell::RefCell<Option<CString>> = const { std::cell::RefCell::new(None) }; }
fn set_error(error: impl std::fmt::Display) {
    let message = error.to_string().replace('\0', " ");
    LAST_ERROR.with(|slot| *slot.borrow_mut() = CString::new(message).ok());
}
fn pem_blocks(input: &[u8], label: &str) -> Result<Vec<Vec<u8>>, String> {
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let text = std::str::from_utf8(input).map_err(|_| "PEM is not UTF-8".to_owned())?;
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(&begin) {
        let body = &rest[start + begin.len()..];
        let close = body
            .find(&end)
            .ok_or_else(|| "PEM block is truncated".to_owned())?;
        let encoded: String = body[..close]
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect();
        blocks.push(
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| "PEM block is not valid base64".to_owned())?,
        );
        rest = &body[close + end.len()..];
    }
    if blocks.is_empty() {
        return Err(format!("PEM contains no {label} block"));
    }
    Ok(blocks)
}
fn c_bytes(pointer: *const u8, length: usize) -> Result<&'static [u8], String> {
    if pointer.is_null() {
        return Err("null byte pointer".to_owned());
    }
    Ok(unsafe { std::slice::from_raw_parts(pointer, length) })
}
fn c_text(pointer: *const c_char) -> Result<&'static str, String> {
    if pointer.is_null() {
        return Err("null string pointer".to_owned());
    }
    unsafe { CStr::from_ptr(pointer) }
        .to_str()
        .map_err(|_| "string is not UTF-8".to_owned())
}
fn endpoint_parts(endpoint: &str) -> Result<(String, u16), String> {
    let value = endpoint
        .strip_prefix("https://")
        .ok_or_else(|| "native transport requires an https endpoint".to_owned())?;
    let authority = value.split('/').next().unwrap_or(value);
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| "endpoint must include a port".to_owned())?;
    if host.is_empty() || host.contains(['[', ']']) {
        return Err("IPv6 endpoints are not supported by this ABI yet".to_owned());
    }
    Ok((
        host.to_owned(),
        port.parse::<u16>()
            .map_err(|_| "invalid endpoint port".to_owned())?,
    ))
}
fn open_stream(
    endpoint: &str,
    ca: &[u8],
    certificate: &[u8],
    private_key: &[u8],
) -> Result<StreamOwned<ClientConnection, TcpStream>, String> {
    let (host, port) = endpoint_parts(endpoint)?;
    let mut roots = RootCertStore::empty();
    for der in pem_blocks(ca, "CERTIFICATE")? {
        roots
            .add(CertificateDer::from(der))
            .map_err(|_| "CA certificate is invalid".to_owned())?;
    }
    let certificates = pem_blocks(certificate, "CERTIFICATE")?
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();
    let key = pem_blocks(private_key, "PRIVATE KEY")?
        .into_iter()
        .next()
        .map(|der| PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der)))
        .ok_or_else(|| "client private key is missing".to_owned())?;
    let mut config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, key)
        .map_err(|_| "client certificate or private key is invalid".to_owned())?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    let server_name =
        ServerName::try_from(host.clone()).map_err(|_| "invalid TLS hostname".to_owned())?;
    let connection = ClientConnection::new(std::sync::Arc::new(config), server_name)
        .map_err(|_| "could not create TLS client".to_owned())?;
    let stream = TcpStream::connect((host.as_str(), port)).map_err(|error| error.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|error| error.to_string())?;
    let mut stream = StreamOwned::new(connection, stream);
    stream.flush().map_err(|error| error.to_string())?;
    Ok(stream)
}

/// Opens a Rustls-backed HTTPS stream. The handle owns all TLS state.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_dotnet_tls_open(
    endpoint: *const c_char,
    ca: *const u8,
    ca_len: usize,
    certificate: *const u8,
    certificate_len: usize,
    private_key: *const u8,
    private_key_len: usize,
) -> *mut c_void {
    let result = (|| {
        let endpoint = c_text(endpoint)?;
        let ca = c_bytes(ca, ca_len)?;
        let certificate = c_bytes(certificate, certificate_len)?;
        let private_key = c_bytes(private_key, private_key_len)?;
        Ok::<_, String>(Box::new(Handle {
            stream: std::sync::Mutex::new(open_stream(endpoint, ca, certificate, private_key)?),
        }))
    })();
    match result {
        Ok(handle) => Box::into_raw(handle).cast(),
        Err(error) => {
            set_error(error);
            ptr::null_mut()
        }
    }
}

/// Reads decrypted bytes. Negative results indicate an I/O error.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_dotnet_tls_read(
    handle: *mut c_void,
    buffer: *mut u8,
    length: usize,
) -> isize {
    if handle.is_null() || buffer.is_null() {
        set_error("null stream or buffer");
        return -1;
    }
    let result = loop {
        let attempt = unsafe {
            match (&*handle.cast::<Handle>()).stream.lock() {
                Ok(mut stream) => stream.read(std::slice::from_raw_parts_mut(buffer, length)),
                Err(_) => Err(std::io::Error::other("native TLS stream lock poisoned")),
            }
        };
        match attempt {
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                std::thread::yield_now();
            }
            other => break other,
        }
    };
    match result {
        Ok(bytes) => bytes as isize,
        Err(error) => {
            set_error(error);
            -1
        }
    }
}

/// Writes plaintext bytes. Negative results indicate an I/O error.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_dotnet_tls_write(
    handle: *mut c_void,
    buffer: *const u8,
    length: usize,
) -> isize {
    if handle.is_null() || buffer.is_null() {
        set_error("null stream or buffer");
        return -1;
    }
    let result = unsafe {
        match (&*handle.cast::<Handle>()).stream.lock() {
            Ok(mut stream) => match stream.write(std::slice::from_raw_parts(buffer, length)) {
                Ok(bytes) => stream.flush().map(|()| bytes),
                Err(error) => Err(error),
            },
            Err(_) => Err(std::io::Error::other("native TLS stream lock poisoned")),
        }
    };
    match result {
        Ok(bytes) => bytes as isize,
        Err(error) => {
            set_error(error);
            -1
        }
    }
}

/// Closes a native TLS stream.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_dotnet_tls_close(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle.cast::<Handle>()));
        }
    }
}

/// Returns the last error for the calling thread.
#[unsafe(no_mangle)]
pub extern "C" fn acyclic_dotnet_tls_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(ptr::null(), |value| value.as_ptr())
    })
}
