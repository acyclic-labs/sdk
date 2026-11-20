use super::{MAXIMUM_RESPONSE_BYTES, transport_error};
use crate::Result;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use reqwest::{Url, header::HeaderValue};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
pub(crate) mod socket;

pub(crate) const HTTP_BUFFER_BYTES: usize = 32_768;

/// Which caller owns an exchange. The transport is one implementation; the
/// kind selects only the policy that genuinely differs between search and fetch.
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Search,
    Fetch { require_public_address: bool },
}

impl Kind {
    fn invalid(self, work: acyclic_fs::NativeWork<'_>, reason: &'static str) -> crate::Error {
        match self {
            Self::Search => crate::error(work, reason),
            Self::Fetch { .. } => crate::rejection(work, reason),
        }
    }
}

/// One HTTP response, before the caller applies its own status policy.
pub(crate) enum Response {
    Redirect {
        location: String,
        held: acyclic_fs::NativeCustody,
    },
    Content {
        bytes: Vec<u8>,
        held: acyclic_fs::NativeCustody,
        status: u16,
        content_type: Option<String>,
    },
}

/// Funds the bounded HTTP parser buffer and request/header copies before connecting.
pub(crate) fn request_scratch(
    endpoint: &Url,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<acyclic_fs::NativeCustody> {
    let copies = endpoint
        .as_str()
        .len()
        .checked_mul(4)
        .ok_or_else(|| crate::error(work, "HTTP request ownership overflow"))?;
    let bytes = copies
        .checked_add(HTTP_BUFFER_BYTES + 1024)
        .ok_or_else(|| crate::error(work, "HTTP request ownership overflow"))?;
    let held = crate::tool::edit::allocation(work, bytes)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.charge(acyclic_fs_work::performance::WorkCounters {
            allocation_operations: 8,
            bytes_copied: copies as u64,
            items_examined: endpoint.as_str().len() as u64,
            ..Default::default()
        })?;
    }
    Ok(held)
}

pub(crate) async fn read_body(
    mut body: hyper::body::Incoming,
    work: acyclic_fs::NativeWork<'_>,
    mut held: acyclic_fs::NativeCustody,
    read_reason: &'static str,
    limit_reason: &'static str,
) -> Result<(Vec<u8>, acyclic_fs::NativeCustody)> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| transport_error(work, read_reason))?;
        let Ok(chunk) = frame.into_data() else {
            continue;
        };
        if chunk.len() > MAXIMUM_RESPONSE_BYTES - bytes.len() {
            return Err(crate::rejection(work, limit_reason));
        }
        let length = bytes.len() + chunk.len();
        crate::tool::edit::reserve_vec(&mut held, &mut bytes, length)?;
        crate::tool::edit::charge(&held, chunk.len() as u64, 1)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok((bytes, held))
}

pub(crate) fn tls_config() -> Result<Arc<rustls::ClientConfig>> {
    use rustls_platform_verifier::BuilderVerifierExt as _;
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .and_then(|builder| builder.with_platform_verifier())
    .map_err(|_| crate::Error::Invalid("cannot create TLS client".into()))?
    .with_no_client_auth();
    Ok(Arc::new(config))
}

pub(super) fn tls(endpoint: &Url) -> Result<Option<Arc<rustls::ClientConfig>>> {
    if endpoint.scheme() != "https" {
        return Ok(None);
    }
    tls_config().map(Some)
}

pub(crate) fn copy_text(text: &str, held: &mut acyclic_fs::NativeCustody) -> Result<String> {
    let mut output = String::new();
    crate::tool::edit::reserve_string(held, &mut output, text.len())?;
    crate::tool::edit::charge(held, text.len() as u64, 1)?;
    output.push_str(text);
    Ok(output)
}

pub(crate) fn public_address(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || a == 0
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 192 && b == 0 && c == 0)
                || (a == 198 && (b == 18 || b == 19)))
        }
        IpAddr::V6(ip) => {
            // Permit global unicast only. IPv4-mapped addresses inherit IPv4 policy.
            if let Some(ip) = ip.to_ipv4_mapped() {
                return public_address(IpAddr::V4(ip));
            }
            let segments = ip.segments();
            (segments[0] & 0xe000) == 0x2000
                && !(segments[0] == 0x2001 && (segments[1] <= 0x01ff || segments[1] == 0x0db8))
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && (segments[1] & 0xf000) == 0)
        }
    }
}

pub(crate) async fn send(
    kind: Kind,
    method: http::Method,
    endpoint: &Url,
    headers: &[(http::header::HeaderName, HeaderValue)],
    body: bytes::Bytes,
    tls: Option<&Arc<rustls::ClientConfig>>,
    work: acyclic_fs::NativeWork<'_>,
    failure: &mut Option<crate::Error>,
) -> Result<Response> {
    let host = endpoint.host_str().ok_or_else(|| {
        kind.invalid(
            work,
            match kind {
                Kind::Search => "invalid search host",
                Kind::Fetch { .. } => "invalid fetch host",
            },
        )
    })?;
    let port = endpoint.port_or_known_default().ok_or_else(|| {
        kind.invalid(
            work,
            match kind {
                Kind::Search => "invalid search port",
                Kind::Fetch { .. } => "invalid fetch port",
            },
        )
    })?;
    let _request_held = request_scratch(endpoint, work)?;
    let socket = match kind {
        Kind::Search => tokio::net::TcpStream::connect((host.trim_matches(['[', ']']), port))
            .await
            .map_err(|_| transport_error(work, "cannot connect search provider"))?,
        Kind::Fetch {
            require_public_address,
        } => {
            let addresses = tokio::net::lookup_host((host.trim_matches(['[', ']']), port))
                .await
                .map_err(|_| transport_error(work, "cannot resolve fetch source"))?;
            validated_connect(addresses, require_public_address, work).await?
        }
    };
    let socket = socket::Socket::new(socket, work, failure)?;
    let host_header = match endpoint.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    let mut target = endpoint.path().to_owned();
    if let Some(query) = endpoint.query() {
        target.push('?');
        target.push_str(query);
    }
    let mut builder = http::Request::builder()
        .method(method)
        .uri(target)
        .header(http::header::HOST, host_header)
        .header(http::header::CONNECTION, "close");
    for (name, value) in headers {
        builder = builder.header(name.clone(), value.clone());
    }
    let request = builder.body(Full::new(body)).map_err(|_| {
        kind.invalid(
            work,
            match kind {
                Kind::Search => "cannot construct search request",
                Kind::Fetch { .. } => "cannot construct fetch request",
            },
        )
    })?;
    if endpoint.scheme() == "https" {
        let tls = tls.ok_or_else(|| kind.invalid(work, "missing TLS configuration"))?;
        let name =
            rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
                .map_err(|_| {
                    kind.invalid(
                        work,
                        match kind {
                            Kind::Search => "invalid search TLS name",
                            Kind::Fetch { .. } => "invalid fetch TLS name",
                        },
                    )
                })?;
        let socket = tokio_rustls::TlsConnector::from(tls.clone())
            .connect(name, socket)
            .await
            .map_err(|_| {
                transport_error(
                    work,
                    match kind {
                        Kind::Search => "search TLS handshake failed",
                        Kind::Fetch { .. } => "fetch TLS handshake failed",
                    },
                )
            })?;
        exchange(socket, request, kind, work).await
    } else {
        exchange(socket, request, kind, work).await
    }
}

async fn validated_connect(
    addresses: impl Iterator<Item = std::net::SocketAddr>,
    require_public_address: bool,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<tokio::net::TcpStream> {
    let (validated, _held) = validated_addresses(addresses, require_public_address, work)?;
    // Retain ordinary address fallback without a second hostname resolution.
    tokio::net::TcpStream::connect(validated.as_slice())
        .await
        .map_err(|_| transport_error(work, "cannot connect fetch source"))
}

fn validated_addresses(
    addresses: impl Iterator<Item = std::net::SocketAddr>,
    require_public_address: bool,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(Vec<std::net::SocketAddr>, acyclic_fs::NativeCustody)> {
    let mut held =
        crate::tool::edit::allocation(work, std::mem::size_of::<Vec<std::net::SocketAddr>>())?;
    let mut validated = Vec::new();
    for address in addresses {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.charge(acyclic_fs_work::performance::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if require_public_address && !public_address(address.ip()) {
            return Err(crate::rejection(
                work,
                "fetch redirect targets a non-public address",
            ));
        }
        let length = validated.len() + 1;
        crate::tool::edit::reserve_vec(&mut held, &mut validated, length)?;
        crate::tool::edit::charge(&held, std::mem::size_of_val(&address) as u64, 0)?;
        validated.push(address);
    }
    if validated.is_empty() {
        return Err(transport_error(work, "cannot resolve fetch source"));
    }
    Ok((validated, held))
}

async fn exchange<I: AsyncRead + AsyncWrite + Unpin>(
    socket: I,
    request: http::Request<Full<bytes::Bytes>>,
    kind: Kind,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<Response> {
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(HTTP_BUFFER_BYTES)
        .handshake(TokioIo::new(socket))
        .await
        .map_err(|_| {
            transport_error(
                work,
                match kind {
                    Kind::Search => "cannot initialize search connection",
                    Kind::Fetch { .. } => "cannot initialize fetch connection",
                },
            )
        })?;
    let exchange = async {
        let response = sender.send_request(request).await.map_err(|_| {
            transport_error(
                work,
                match kind {
                    Kind::Search => "search HTTP request failed",
                    Kind::Fetch { .. } => "fetch HTTP request failed",
                },
            )
        })?;
        let status = response.status();
        match kind {
            Kind::Search => {
                if !status.is_success() {
                    return Err(crate::rejection(work, "search HTTP request rejected"));
                }
            }
            Kind::Fetch { .. } => {
                if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                    let location = response
                        .headers()
                        .get(http::header::LOCATION)
                        .and_then(|value| value.to_str().ok())
                        .filter(|location| {
                            !location.is_empty()
                                && location.len() <= crate::url_fetch::MAXIMUM_URL_BYTES
                        })
                        .ok_or_else(|| {
                            crate::rejection(work, "fetch redirect has no bounded location")
                        })?;
                    let mut held =
                        crate::tool::edit::allocation(work, std::mem::size_of::<String>())?;
                    let location = copy_text(location, &mut held)?;
                    return Ok(Response::Redirect { location, held });
                }
                for encoding in response.headers().get_all(http::header::CONTENT_ENCODING) {
                    if !encoding
                        .to_str()
                        .is_ok_and(|value| value.eq_ignore_ascii_case("identity"))
                    {
                        return Err(crate::rejection(
                            work,
                            "fetch response uses unsupported content encoding",
                        ));
                    }
                }
            }
        }
        if response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(crate::rejection(
                work,
                match kind {
                    Kind::Search => "search response exceeds byte limit",
                    Kind::Fetch { .. } => "fetch response exceeds byte limit",
                },
            ));
        }
        let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
        let content_type = match kind {
            Kind::Search => None,
            Kind::Fetch { .. } => response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .map(|value| {
                    let value = value
                        .to_str()
                        .map_err(|_| crate::rejection(work, "invalid fetch content type"))?;
                    copy_text(value, &mut held)
                })
                .transpose()?,
        };
        let (bytes, held) = read_body(
            response.into_body(),
            work,
            held,
            match kind {
                Kind::Search => "cannot read search response",
                Kind::Fetch { .. } => "cannot read fetch response",
            },
            match kind {
                Kind::Search => "search response exceeds byte limit",
                Kind::Fetch { .. } => "fetch response exceeds byte limit",
            },
        )
        .await?;
        Ok(Response::Content {
            bytes,
            held,
            status: status.as_u16(),
            content_type,
        })
    };
    tokio::pin!(exchange, connection);
    tokio::select! { biased;
        result = &mut exchange => result,
        result = &mut connection => {
            result.map_err(|_| {
                transport_error(
                    work,
                    match kind {
                        Kind::Search => "search connection failed",
                        Kind::Fetch { .. } => "fetch connection failed",
                    },
                )
            })?;
            exchange.await
        }
    }
}

#[cfg(test)]
mod tests;
