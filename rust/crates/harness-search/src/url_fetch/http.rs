use super::{FetchProvider, FetchRequest, FetchResponse, MAXIMUM_URL_BYTES, validate_url_text};
use crate::http::await_io;
use crate::http::transport::{self, Kind, Response, copy_text, public_address};
use crate::rejection;
use crate::tool::edit::{allocation, charge};
use crate::{Error, Result};
use acyclic_stream::BoxProviderFuture;
use reqwest::Url;
use std::sync::Arc;

#[cfg(test)]
mod native_tests;
#[cfg(test)]
pub(crate) mod tests;

const MAXIMUM_REDIRECTS: usize = 5;

/// Direct GET adapter. No keys, cookies, proxy credentials, or search provider are used.
pub struct HttpFetch {
    tls: Arc<rustls::ClientConfig>,
}

impl HttpFetch {
    /// Creates a credential-free HTTP client using platform TLS verification.
    pub fn new() -> Result<Self> {
        Ok(Self {
            tls: crate::http::transport::tls_config()?,
        })
    }

    async fn run(
        &self,
        request: &FetchRequest,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(FetchResponse, acyclic_fs::NativeCustody)> {
        await_io(self.retrieve(request, work), work).await?
    }

    async fn retrieve(
        &self,
        request: &FetchRequest,
        work: acyclic_fs::NativeWork<'_>,
    ) -> Result<(FetchResponse, acyclic_fs::NativeCustody)> {
        let (mut endpoint, mut endpoint_held) = parse_url(&request.url, work)?;
        let original_origin = endpoint.origin();
        let explicitly_local = endpoint.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| !public_address(ip))
                || host.eq_ignore_ascii_case("localhost")
        });
        for redirects in 0..=MAXIMUM_REDIRECTS {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = work {
                work.admit(acyclic_fs_work::performance::WorkCounters {
                    source_bytes_read: 1,
                    ..Default::default()
                })?;
                work.charge(acyclic_fs_work::performance::WorkCounters {
                    backend_read_operations: 1,
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let headers = [
                (
                    http::header::ACCEPT_ENCODING,
                    http::header::HeaderValue::from_static("identity"),
                ),
                (
                    http::header::USER_AGENT,
                    http::header::HeaderValue::from_static(concat!(
                        "acyclic-harness/",
                        env!("CARGO_PKG_VERSION")
                    )),
                ),
            ];
            let mut failure = None;
            let response = transport::send(
                Kind::Fetch {
                    require_public_address: redirects > 0
                        && (!explicitly_local || endpoint.origin() != original_origin),
                },
                transport::Request {
                    method: http::Method::GET,
                    endpoint: &endpoint,
                    headers: &headers,
                    body: bytes::Bytes::new(),
                },
                Some(&self.tls),
                work,
                &mut failure,
            )
            .await;
            if let Some(error) = failure {
                return Err(error);
            }
            match response? {
                Response::Redirect {
                    location,
                    held: _location_held,
                } => {
                    if redirects == MAXIMUM_REDIRECTS {
                        return Err(rejection(work, "fetch redirect limit exceeded"));
                    }
                    let next_held = url_allocation(endpoint.as_str().len() + location.len(), work)?;
                    charge(
                        &next_held,
                        (endpoint.as_str().len() + location.len()) as u64,
                        location.len() as u64,
                    )?;
                    let next = endpoint
                        .join(&location)
                        .map_err(|_| rejection(work, "invalid fetch redirect URL"))?;
                    validate_endpoint(&next, work)?;
                    if next.origin() != original_origin {
                        validate_redirect_host(&next, work)?;
                    }
                    if endpoint.scheme() == "https" && next.scheme() != "https" {
                        return Err(rejection(work, "fetch redirect cannot downgrade HTTPS"));
                    }
                    endpoint = next;
                    endpoint_held = next_held;
                }
                Response::Content {
                    bytes,
                    mut held,
                    status,
                    content_type,
                } => {
                    charge(&held, 0, bytes.len() as u64)?;
                    // Vec -> String keeps the admitted body buffer; no replacement copy.
                    let content = String::from_utf8(bytes)
                        .map_err(|_| rejection(work, "fetch response is not UTF-8"))?;
                    let mut output_held = allocation(work, std::mem::size_of::<FetchResponse>())?;
                    let source_url = copy_text(&request.url, &mut output_held)?;
                    let final_url = copy_text(endpoint.as_str(), &mut output_held)?;
                    let response = FetchResponse {
                        source_url,
                        final_url,
                        status,
                        content_type,
                        content,
                    };
                    #[cfg(not(target_arch = "wasm32"))]
                    if let (Some(output), Some(input)) = (&mut output_held, &mut held) {
                        output.absorb(input)?;
                        output.resize(super::response_bytes(&response)?)?;
                    }
                    let _ = (&mut held, &endpoint_held);
                    return Ok((response, output_held));
                }
            }
        }
        Err(rejection(work, "fetch redirect limit exceeded"))
    }
}

fn url_allocation(
    length: usize,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<acyclic_fs::NativeCustody> {
    // Fund the URL serializer's old/replacement text and host representation while parsing.
    let held = allocation(work, length * 4 + std::mem::size_of::<Url>() + 128)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.charge(acyclic_fs_work::performance::WorkCounters {
            allocation_operations: u64::from(usize::BITS - length.leading_zeros()) + 2,
            ..Default::default()
        })?;
    }
    Ok(held)
}

fn parse_url(
    text: &str,
    work: acyclic_fs::NativeWork<'_>,
) -> Result<(Url, acyclic_fs::NativeCustody)> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        work.charge(acyclic_fs_work::performance::WorkCounters {
            items_examined: text.len() as u64,
            ..Default::default()
        })?;
    }
    validate_url_text(text, work)?;
    let held = url_allocation(text.len(), work)?;
    charge(&held, text.len() as u64, text.len() as u64)?;
    let endpoint = Url::parse(text).map_err(|_| rejection(work, "invalid fetch URL"))?;
    validate_endpoint(&endpoint, work)?;
    Ok((endpoint, held))
}

pub(super) fn validate_url(text: &str, work: acyclic_fs::NativeWork<'_>) -> Result<()> {
    parse_url(text, work).map(|_| ())
}

fn validate_endpoint(endpoint: &Url, work: acyclic_fs::NativeWork<'_>) -> Result<()> {
    validate_url_text(endpoint.as_str(), work)?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || endpoint.port_or_known_default().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || endpoint.as_str().len() > MAXIMUM_URL_BYTES
    {
        return Err(rejection(work, "invalid fetch URL"));
    }
    Ok(())
}

// A URL explicitly supplied by the caller may name a local service. Redirects
// within that origin retain the same authority; redirects must not introduce a
// different local service. Resolved addresses are checked again before connect.
fn validate_redirect_host(endpoint: &Url, work: acyclic_fs::NativeWork<'_>) -> Result<()> {
    let unsafe_host = match endpoint.host_str() {
        Some(host) => {
            if let Ok(ip) = host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
                !public_address(ip)
            } else {
                let host = host.trim_end_matches('.');
                host.eq_ignore_ascii_case("localhost")
                    || host.to_ascii_lowercase().ends_with(".localhost")
            }
        }
        None => true,
    };
    if unsafe_host {
        return Err(rejection(
            work,
            "fetch redirect targets a non-public address",
        ));
    }
    Ok(())
}

impl FetchProvider for HttpFetch {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
    ) -> BoxProviderFuture<'a, Result<FetchResponse>> {
        Box::pin(async move { self.run(request, None).await.map(|result| result.0) })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_fetch(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn fetch_with_native_work<'a>(
        &'a self,
        request: &'a FetchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_fs::NativeFutureBundle<
            'a,
            (FetchResponse, acyclic_fs::NativeAllocation),
            crate::Error,
        >,
    > {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            let (response, held) = self.run(request, Some(work)).await?;
            Ok((
                response,
                held.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native fetch response lacks custody",
                })?,
            ))
        })
    }
}
