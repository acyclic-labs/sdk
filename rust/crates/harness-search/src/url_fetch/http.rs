use super::{
    FetchProvider, FetchRequest, FetchResponse, MAXIMUM_URL_BYTES, rejection, validate_url_text,
};
use crate::tool::edit::{allocation, charge, reserve_string};
use crate::{Error, NativeCustody, NativeWork, Result};
use acyclic_stream::BoxProviderFuture;
use reqwest::Url;
use std::{sync::Arc, time::Duration};

#[cfg(all(test, feature = "filesystem"))]
mod native_tests;
#[cfg(test)]
pub(crate) mod tests;
mod transport;

const MAXIMUM_RESPONSE_BYTES: usize = 1_048_576;
const MAXIMUM_REDIRECTS: usize = 5;

/// Direct GET adapter. No keys, cookies, proxy credentials, or search provider are used.
pub struct HttpFetch {
    tls: Arc<rustls::ClientConfig>,
}

impl HttpFetch {
    /// Creates a credential-free HTTP client using platform TLS verification.
    pub fn new() -> Result<Self> {
        use rustls_platform_verifier::BuilderVerifierExt as _;
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .and_then(|builder| builder.with_platform_verifier())
        .map_err(|_| Error::Invalid("cannot create fetch TLS client".into()))?
        .with_no_client_auth();
        Ok(Self { tls: Arc::new(tls) })
    }

    async fn run(
        &self,
        request: &FetchRequest,
        work: NativeWork<'_>,
    ) -> Result<(FetchResponse, NativeCustody)> {
        // The deadline covers the entire redirect chain, not thirty seconds per hop.
        await_io(
            tokio::time::timeout(Duration::from_secs(30), self.retrieve(request, work)),
            work,
        )
        .await?
        .map_err(|_| transport_error(work, "fetch HTTP request timed out"))?
    }

    async fn retrieve(
        &self,
        request: &FetchRequest,
        work: NativeWork<'_>,
    ) -> Result<(FetchResponse, NativeCustody)> {
        let (mut endpoint, mut endpoint_held) = parse_url(&request.url, work)?;
        let original_origin = endpoint.origin();
        let explicitly_local = endpoint.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| !public_address(ip))
                || host.eq_ignore_ascii_case("localhost")
        });
        for redirects in 0..=MAXIMUM_REDIRECTS {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = work {
                work.admit(acyclic_fs::WorkCounters {
                    source_bytes_read: 1,
                    ..Default::default()
                })?;
                work.charge(acyclic_fs::WorkCounters {
                    backend_read_operations: 1,
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let mut failure = None;
            let response = transport::send(
                &endpoint,
                &self.tls,
                redirects > 0 && (!explicitly_local || endpoint.origin() != original_origin),
                work,
                &mut failure,
            )
            .await;
            if let Some(error) = failure {
                return Err(error);
            }
            match response? {
                transport::Response::Redirect(location, _location_held) => {
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
                transport::Response::Content {
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
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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

fn copy_text(text: &str, held: &mut NativeCustody) -> Result<String> {
    let mut output = String::new();
    reserve_string(held, &mut output, text.len())?;
    charge(held, text.len() as u64, 1)?;
    output.push_str(text);
    Ok(output)
}

fn url_allocation(length: usize, work: NativeWork<'_>) -> Result<NativeCustody> {
    // Fund the URL serializer's old/replacement text and host representation while parsing.
    let held = allocation(work, length * 4 + std::mem::size_of::<Url>() + 128)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: u64::from(usize::BITS - length.leading_zeros()) + 2,
            ..Default::default()
        })?;
    }
    Ok(held)
}

fn parse_url(text: &str, work: NativeWork<'_>) -> Result<(Url, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
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

pub(super) fn validate_url(text: &str, work: NativeWork<'_>) -> Result<()> {
    parse_url(text, work).map(|_| ())
}

fn validate_endpoint(endpoint: &Url, work: NativeWork<'_>) -> Result<()> {
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
fn validate_redirect_host(endpoint: &Url, work: NativeWork<'_>) -> Result<()> {
    let unsafe_host = match endpoint.host_str() {
        Some(host) => match host.trim_matches(['[', ']']).parse::<std::net::IpAddr>() {
            Ok(ip) => !public_address(ip),
            Err(_) => {
                let host = host.trim_end_matches('.');
                host.eq_ignore_ascii_case("localhost")
                    || host.to_ascii_lowercase().ends_with(".localhost")
            }
        },
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

fn public_address(ip: std::net::IpAddr) -> bool {
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

async fn await_io<F: Future>(future: F, work: NativeWork<'_>) -> Result<F::Output> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.remaining()?;
        return tokio::select! {
            biased;
            _ = work.cancellation().cancelled() => Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into()),
            result = future => Ok(result),
        };
    }
    let _ = work;
    Ok(future.await)
}

fn transport_error(work: NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason,
        };
    }
    let _ = work;
    Error::Storage(reason.into())
}

impl FetchProvider for HttpFetch {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
    ) -> BoxProviderFuture<'a, Result<FetchResponse>> {
        Box::pin(async move { self.run(request, None).await.map(|result| result.0) })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_fetch(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        work.remaining()?;
        Ok(true)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn fetch_with_native_work<'a>(
        &'a self,
        request: &'a FetchRequest,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FetchResponse, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
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
