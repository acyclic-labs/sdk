//! The server's TLS listener. Every connection completes its handshake,
//! including any required peer verification, before it reaches the session
//! service, so no session is dispatched for an unverified peer.
use super::{PeerIdentity, TlsIdentity};
use crate::{Error, Result};
use futures::{Stream, StreamExt as _, future, stream};
use rustls::{
    DistinguishedName, SignatureScheme,
    client::danger::HandshakeSignatureValid,
    pki_types::{CertificateDer, PrivateKeyDer, UnixTime, pem::PemObject},
    server::{
        WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

/// A protective bound on one handshake, so a stalled peer cannot hold its
/// connection open unverified; a live peer never comes near it.
const HANDSHAKE: Duration = Duration::from_secs(30);

fn invalid(what: &str) -> impl FnOnce(String) -> Error + '_ {
    move |error| Error::Invalid(format!("TLS {what}: {error}"))
}

fn certificates(pem: &[u8], what: &str) -> Result<Vec<CertificateDer<'static>>> {
    let certificates = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| invalid(what)(error.to_string()))?;
    if certificates.is_empty() {
        return Err(invalid(what)("no PEM certificate".into()));
    }
    Ok(certificates)
}

/// The rustls configuration serving `tls`: its identity, and with a peer,
/// TLS 1.3 only and a client certificate verified as [`PeerVerifier`].
pub(super) fn config(tls: &TlsIdentity) -> Result<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let chain = certificates(&tls.certificate, "certificate")?;
    let key = PrivateKeyDer::from_pem_slice(&tls.key)
        .map_err(|error| invalid("key")(error.to_string()))?;
    let versions: &[&rustls::SupportedProtocolVersion] = if tls.peer.is_some() {
        &[&rustls::version::TLS13]
    } else {
        rustls::DEFAULT_VERSIONS
    };
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(versions)
        .map_err(|error| invalid("versions")(error.to_string()))?;
    let builder = match &tls.peer {
        Some(peer) => {
            builder.with_client_cert_verifier(Arc::new(PeerVerifier::new(peer, provider)?))
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(|error| invalid("identity")(error.to_string()))?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

/// The connections `listener` accepts, each yielded once its handshake
/// under `config` completes. A failed handshake drops only its connection;
/// handshakes run concurrently, so a slow peer delays no other.
pub(super) fn incoming(
    listener: TcpListener,
    config: rustls::ServerConfig,
) -> impl Stream<Item = std::io::Result<TlsStream<TcpStream>>> {
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (sender, mut handshaken) = tokio::sync::mpsc::unbounded_channel();
    let accepted = stream::unfold(listener, |listener| async {
        let accepted = listener.accept().await;
        Some((accepted, listener))
    })
    .filter_map(move |accepted| {
        future::ready(match accepted {
            Ok((socket, _)) => {
                let (acceptor, sender) = (acceptor.clone(), sender.clone());
                tokio::spawn(async move {
                    if let Ok(Ok(tls)) =
                        tokio::time::timeout(HANDSHAKE, acceptor.accept(socket)).await
                    {
                        let _ = sender.send(tls);
                    }
                });
                None
            }
            Err(error) => Some(Err(error)),
        })
    });
    stream::select(
        accepted,
        stream::poll_fn(move |context| handshaken.poll_recv(context)).map(Ok),
    )
}

/// Admits a client whose chain the peer's certificate authorities verify
/// and whose end-entity certificate names the peer's exact URI SAN. It
/// authenticates the peer only; each session's credential is still verified
/// by the server's [`super::Verifier`].
#[derive(Debug)]
struct PeerVerifier {
    chain: Arc<dyn ClientCertVerifier>,
    uri_san: String,
}

impl PeerVerifier {
    fn new(peer: &PeerIdentity, provider: Arc<rustls::crypto::CryptoProvider>) -> Result<Self> {
        if !is_uri(&peer.uri_san) {
            return Err(Error::Invalid(format!(
                "TLS peer URI SAN {:?} is not a URI",
                peer.uri_san
            )));
        }
        let mut roots = rustls::RootCertStore::empty();
        for authority in certificates(
            &peer.certificate_authorities,
            "peer certificate authorities",
        )? {
            roots
                .add(authority)
                .map_err(|error| invalid("peer certificate authority")(error.to_string()))?;
        }
        let chain = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|error| invalid("peer verifier")(error.to_string()))?;
        Ok(Self {
            chain,
            uri_san: peer.uri_san.clone(),
        })
    }

    fn names_peer(&self, certificate: &CertificateDer<'_>) -> bool {
        use x509_parser::extensions::GeneralName;
        let Ok((_, parsed)) = x509_parser::parse_x509_certificate(certificate.as_ref()) else {
            return false;
        };
        let Ok(Some(names)) = parsed.subject_alternative_name() else {
            return false;
        };
        names
            .value
            .general_names
            .iter()
            .any(|name| matches!(name, GeneralName::URI(uri) if *uri == self.uri_san))
    }
}

/// An absolute URI: a scheme, then a non-empty remainder, with no
/// whitespace or control characters.
fn is_uri(value: &str) -> bool {
    let Some((scheme, rest)) = value.split_once(':') else {
        return false;
    };
    scheme.starts_with(|first: char| first.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|part| part.is_ascii_alphanumeric() || matches!(part, '+' | '-' | '.'))
        && !rest.is_empty()
        && !value
            .chars()
            .any(|part| part.is_whitespace() || part.is_control())
}

impl ClientCertVerifier for PeerVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.chain.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> std::result::Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .chain
            .verify_client_cert(end_entity, intermediates, now)?;
        if self.names_peer(end_entity) {
            Ok(verified)
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.chain
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.chain
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.chain.supported_verify_schemes()
    }
}

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tests;
