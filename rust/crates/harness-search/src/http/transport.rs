use super::{MAXIMUM_RESPONSE_BYTES, transport_error};
use crate::{NativeCustody, NativeWork, Result};
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use reqwest::{Url, header::HeaderValue};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
pub(crate) mod socket;

pub(crate) const HTTP_BUFFER_BYTES: usize = 32_768;

/// Funds the bounded HTTP parser buffer and request/header copies before connecting.
pub(crate) fn request_scratch(endpoint: &Url, work: NativeWork<'_>) -> Result<NativeCustody> {
    let copies = endpoint
        .as_str()
        .len()
        .checked_mul(4)
        .ok_or_else(|| super::super::error(work, "HTTP request ownership overflow"))?;
    let bytes = copies
        .checked_add(HTTP_BUFFER_BYTES + 1024)
        .ok_or_else(|| super::super::error(work, "HTTP request ownership overflow"))?;
    let held = crate::tool::edit::allocation(work, bytes)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 8,
            bytes_copied: copies as u64,
            items_examined: endpoint.as_str().len() as u64,
            ..Default::default()
        })?;
    }
    Ok(held)
}

pub(super) fn tls(endpoint: &Url) -> Result<Option<Arc<rustls::ClientConfig>>> {
    use rustls_platform_verifier::BuilderVerifierExt as _;
    if endpoint.scheme() != "https" {
        return Ok(None);
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .and_then(|builder| builder.with_platform_verifier())
    .map_err(|_| crate::Error::Invalid("cannot create search TLS client".into()))?
    .with_no_client_auth();
    Ok(Some(Arc::new(config)))
}

pub(super) async fn send(
    method: http::Method,
    endpoint: &Url,
    key: &HeaderValue,
    body: bytes::Bytes,
    tls: Option<&Arc<rustls::ClientConfig>>,
    work: NativeWork<'_>,
    failure: &mut Option<crate::Error>,
) -> Result<(Vec<u8>, NativeCustody)> {
    let host = endpoint
        .host_str()
        .ok_or_else(|| super::super::error(work, "invalid search host"))?;
    let port = endpoint
        .port_or_known_default()
        .ok_or_else(|| super::super::error(work, "invalid search port"))?;
    let _request_held = request_scratch(endpoint, work)?;
    let socket = tokio::net::TcpStream::connect((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|_| transport_error(work, "cannot connect search provider"))?;
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
    let request = http::Request::builder()
        .method(method)
        .uri(target)
        .header(http::header::HOST, host_header)
        .header(http::header::CONNECTION, "close")
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("x-api-key", key.clone())
        .body(Full::new(body))
        .map_err(|_| super::super::error(work, "cannot construct search request"))?;
    if let Some(tls) = tls {
        let name =
            rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
                .map_err(|_| super::super::error(work, "invalid search TLS name"))?;
        let socket = tokio_rustls::TlsConnector::from(tls.clone())
            .connect(name, socket)
            .await
            .map_err(|_| transport_error(work, "search TLS handshake failed"))?;
        exchange(socket, request, work).await
    } else {
        exchange(socket, request, work).await
    }
}

async fn exchange<I: AsyncRead + AsyncWrite + Unpin>(
    socket: I,
    request: http::Request<Full<bytes::Bytes>>,
    work: NativeWork<'_>,
) -> Result<(Vec<u8>, NativeCustody)> {
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_buf_size(HTTP_BUFFER_BYTES)
        .handshake(TokioIo::new(socket))
        .await
        .map_err(|_| transport_error(work, "cannot initialize search connection"))?;
    let exchange = async {
        let response = sender
            .send_request(request)
            .await
            .map_err(|_| transport_error(work, "search HTTP request failed"))?;
        if !response.status().is_success() {
            return Err(super::super::rejection(
                work,
                "search HTTP request rejected",
            ));
        }
        if response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAXIMUM_RESPONSE_BYTES as u64)
        {
            return Err(super::super::rejection(
                work,
                "search response exceeds byte limit",
            ));
        }
        let mut body = response.into_body();
        let mut held = crate::tool::edit::allocation(work, std::mem::size_of::<Vec<u8>>())?;
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| transport_error(work, "cannot read search response"))?;
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            // Every byte in this complete frame was already admitted and
            // recorded at the socket, including headers and TLS records.
            if chunk.len() > MAXIMUM_RESPONSE_BYTES - bytes.len() {
                return Err(super::super::rejection(
                    work,
                    "search response exceeds byte limit",
                ));
            }
            let length = bytes.len() + chunk.len();
            crate::tool::edit::reserve_vec(&mut held, &mut bytes, length)?;
            crate::tool::edit::charge(&held, chunk.len() as u64, 1)?;
            bytes.extend_from_slice(&chunk);
        }
        Ok((bytes, held))
    };
    tokio::pin!(exchange, connection);
    tokio::select! { biased;
        result = &mut exchange => result,
        result = &mut connection => {
            result.map_err(|_| transport_error(work, "search connection failed"))?;
            exchange.await
        }
    }
}
