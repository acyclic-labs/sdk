use super::{MAXIMUM_RESPONSE_BYTES, copy_text, transport_error};
use crate::{NativeCustody, NativeWork, Result};
use http_body_util::{BodyExt as _, Empty};
use hyper_util::rt::TokioIo;
use reqwest::Url;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub(super) enum Response {
    Redirect(String, NativeCustody),
    Content {
        bytes: Vec<u8>,
        held: NativeCustody,
        status: u16,
        content_type: Option<String>,
    },
}

pub(super) async fn send(
    endpoint: &Url,
    tls: &Arc<rustls::ClientConfig>,
    require_public_address: bool,
    work: NativeWork<'_>,
    failure: &mut Option<crate::Error>,
) -> Result<Response> {
    let host = endpoint
        .host_str()
        .ok_or_else(|| super::rejection(work, "invalid fetch host"))?;
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| super::rejection(work, "invalid fetch port"))?;
    // Keep request/header/URI temporaries funded until the connection has dropped.
    let scratch = crate::http::transport::request_scratch(endpoint, work)?;
    let addresses = tokio::net::lookup_host((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|_| transport_error(work, "cannot resolve fetch source"))?;
    let socket = validated_connect(addresses, require_public_address, work).await?;
    let socket = crate::http::transport::socket::Socket::new(socket, work, failure)?;
    let host_header = match endpoint.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    let mut target = endpoint.path().to_owned();
    if let Some(query) = endpoint.query() {
        target.push('?');
        target.push_str(query);
    }
    let request = http::Request::builder()
        .method(http::Method::GET)
        .uri(target)
        .header(http::header::HOST, host_header)
        .header(http::header::CONNECTION, "close")
        .header(http::header::ACCEPT_ENCODING, "identity")
        .body(Empty::<bytes::Bytes>::new())
        .map_err(|_| super::rejection(work, "cannot construct fetch request"))?;
    let result = if endpoint.scheme() == "https" {
        let name =
            rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
                .map_err(|_| super::rejection(work, "invalid fetch TLS name"))?;
        let socket = tokio_rustls::TlsConnector::from(tls.clone())
            .connect(name, socket)
            .await
            .map_err(|_| transport_error(work, "fetch TLS handshake failed"))?;
        exchange(socket, request, work).await
    } else {
        exchange(socket, request, work).await
    };
    drop(scratch);
    result
}

async fn validated_connect(
    addresses: impl Iterator<Item = std::net::SocketAddr>,
    require_public_address: bool,
    work: NativeWork<'_>,
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
    work: NativeWork<'_>,
) -> Result<(Vec<std::net::SocketAddr>, NativeCustody)> {
    let mut held =
        crate::tool::edit::allocation(work, std::mem::size_of::<Vec<std::net::SocketAddr>>())?;
    let mut validated = Vec::new();
    for address in addresses {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if require_public_address && !super::public_address(address.ip()) {
            return Err(super::rejection(
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
    request: http::Request<Empty<bytes::Bytes>>,
    work: NativeWork<'_>,
) -> Result<Response> {
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(crate::http::transport::HTTP_BUFFER_BYTES)
        .handshake(TokioIo::new(socket))
        .await
        .map_err(|_| transport_error(work, "cannot initialize fetch connection"))?;
    let exchange = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| transport_error(work, "fetch HTTP request failed"))?;
        let status = response.status();
        if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            let location = response
                .headers()
                .get(http::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .filter(|location| {
                    !location.is_empty() && location.len() <= super::MAXIMUM_URL_BYTES
                })
                .ok_or_else(|| super::rejection(work, "fetch redirect has no bounded location"))?;
            let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<String>())?;
            let location = copy_text(location, &mut held)?;
            return Ok(Response::Redirect(location, held));
        }
        if !status.is_success() {
            return Err(super::rejection(work, "fetch HTTP request rejected"));
        }
        if response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(super::rejection(work, "fetch response exceeds byte limit"));
        }
        for encoding in response.headers().get_all(http::header::CONTENT_ENCODING) {
            if !encoding
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("identity"))
            {
                return Err(super::rejection(
                    work,
                    "fetch response uses unsupported content encoding",
                ));
            }
        }
        let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
        let content_type = response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .map(|value| {
                let value = value
                    .to_str()
                    .map_err(|_| super::rejection(work, "invalid fetch content type"))?;
                copy_text(value, &mut held)
            })
            .transpose()?;
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| transport_error(work, "cannot read fetch response"))?;
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            if chunk.len() > MAXIMUM_RESPONSE_BYTES - bytes.len() {
                return Err(super::rejection(work, "fetch response exceeds byte limit"));
            }
            let length = bytes.len() + chunk.len();
            crate::tool::edit::reserve_vec(&mut held, &mut bytes, length)?;
            crate::tool::edit::charge(&held, chunk.len() as u64, 1)?;
            bytes.extend_from_slice(&chunk);
        }
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
            result.map_err(|_| transport_error(work, "fetch connection failed"))?;
            exchange.await
        }
    }
}

#[cfg(test)]
mod tests;
