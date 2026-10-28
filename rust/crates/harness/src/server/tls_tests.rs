use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, SanType,
};
use rustls::SupportedProtocolVersion;

type Outcome<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const PEER: &str = "spiffe://edge.cloud.spiffe.acyclic.dev/host/edge";
const TLS13: &[&SupportedProtocolVersion] = &[&rustls::version::TLS13];
const TLS12: &[&SupportedProtocolVersion] = &[&rustls::version::TLS12];

struct Authority {
    pem: String,
    der: CertificateDer<'static>,
    issuer: Issuer<'static, KeyPair>,
}

fn authority() -> Outcome<Authority> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let certificate = params.self_signed(&key)?;
    Ok(Authority {
        pem: certificate.pem(),
        der: certificate.der().clone(),
        issuer: Issuer::new(params, key),
    })
}

/// A PEM certificate and key `authority` issues for `name`.
struct Leaf {
    certificate: String,
    key: String,
}

fn leaf(authority: &Authority, name: SanType, usage: ExtendedKeyUsagePurpose) -> Outcome<Leaf> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.subject_alt_names = vec![name];
    params.extended_key_usages = vec![usage];
    Ok(Leaf {
        certificate: params.signed_by(&key, &authority.issuer)?.pem(),
        key: key.serialize_pem(),
    })
}

fn client(authority: &Authority, uri: &str) -> Outcome<Leaf> {
    leaf(
        authority,
        SanType::URI(uri.try_into()?),
        ExtendedKeyUsagePurpose::ClientAuth,
    )
}

/// The server's own authority and identity, and the authorities clients
/// are issued by.
struct Fixture {
    server: Authority,
    identity: Leaf,
    trusted: Authority,
    untrusted: Authority,
}

impl Fixture {
    fn new() -> Outcome<Self> {
        let server = authority()?;
        let identity = leaf(
            &server,
            SanType::DnsName("localhost".try_into()?),
            ExtendedKeyUsagePurpose::ServerAuth,
        )?;
        Ok(Self {
            server,
            identity,
            trusted: authority()?,
            untrusted: authority()?,
        })
    }

    fn tls(&self, peer: Option<PeerIdentity>) -> TlsIdentity {
        TlsIdentity {
            url: "https://localhost".into(),
            certificate: self.identity.certificate.clone().into_bytes(),
            key: self.identity.key.clone().into_bytes(),
            peer,
        }
    }

    fn peer(&self) -> Option<PeerIdentity> {
        Some(PeerIdentity {
            certificate_authorities: self.trusted.pem.clone().into_bytes(),
            uri_san: PEER.into(),
        })
    }

    fn connector(
        &self,
        certificate: Option<&Leaf>,
        versions: &'static [&'static SupportedProtocolVersion],
    ) -> Outcome<tokio_rustls::TlsConnector> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(self.server.der.clone())?;
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(versions)?
        .with_root_certificates(roots);
        let mut config = match certificate {
            Some(leaf) => builder.with_client_auth_cert(
                certificates(leaf.certificate.as_bytes(), "client")?,
                PrivateKeyDer::from_pem_slice(leaf.key.as_bytes())?,
            )?,
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![b"h2".to_vec()];
        Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
    }

    /// Whether the server under `peer` completes a handshake with a client
    /// presenting `certificate` over `versions`.
    async fn admits(
        &self,
        peer: Option<PeerIdentity>,
        certificate: Option<&Leaf>,
        versions: &'static [&'static SupportedProtocolVersion],
    ) -> Outcome<bool> {
        let acceptor = TlsAcceptor::from(Arc::new(config(&self.tls(peer))?));
        let connector = self.connector(certificate, versions)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = async {
            let (socket, _) = listener.accept().await?;
            Ok::<_, std::io::Error>(acceptor.accept(socket).await.is_ok())
        };
        let client = async {
            let socket = TcpStream::connect(address).await?;
            // The client's own view is not the verdict: a TLS 1.3 client
            // finishes before the server has checked its certificate.
            let _ = connector.connect("localhost".try_into()?, socket).await;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };
        let (admitted, connected) = tokio::join!(server, client);
        connected?;
        Ok(admitted?)
    }
}

#[tokio::test]
async fn a_required_peer_is_admitted_only_as_its_exact_verified_identity() -> Outcome {
    let fixture = Fixture::new()?;
    let peer = client(&fixture.trusted, PEER)?;
    // The control every refusal below differs from in exactly one input.
    assert!(fixture.admits(fixture.peer(), Some(&peer), TLS13).await?);

    let other = client(
        &fixture.trusted,
        "spiffe://edge.cloud.spiffe.acyclic.dev/host/other",
    )?;
    assert!(
        !fixture.admits(fixture.peer(), Some(&other), TLS13).await?,
        "wrong SAN"
    );
    let prefixed = client(&fixture.trusted, &format!("{PEER}/child"))?;
    assert!(
        !fixture
            .admits(fixture.peer(), Some(&prefixed), TLS13)
            .await?,
        "SAN prefix"
    );

    let foreign = client(&fixture.untrusted, PEER)?;
    assert!(
        !fixture
            .admits(fixture.peer(), Some(&foreign), TLS13)
            .await?,
        "wrong CA"
    );

    assert!(
        !fixture.admits(fixture.peer(), None, TLS13).await?,
        "no certificate"
    );

    assert!(
        !fixture.admits(fixture.peer(), Some(&peer), TLS12).await?,
        "TLS 1.2"
    );
    Ok(())
}

#[tokio::test]
async fn without_a_peer_the_server_admits_any_client_as_before() -> Outcome {
    let fixture = Fixture::new()?;
    assert!(fixture.admits(None, None, TLS13).await?);
    assert!(fixture.admits(None, None, TLS12).await?);
    // A client certificate is neither requested nor checked.
    let foreign = client(&fixture.untrusted, "spiffe://elsewhere/any")?;
    assert!(fixture.admits(None, Some(&foreign), TLS13).await?);
    Ok(())
}

#[test]
fn an_unusable_peer_identity_is_refused_at_configuration() -> Outcome {
    let fixture = Fixture::new()?;
    let refused =
        |peer: PeerIdentity| matches!(config(&fixture.tls(Some(peer))), Err(Error::Invalid(_)));
    let valid = || PeerIdentity {
        certificate_authorities: fixture.trusted.pem.clone().into_bytes(),
        uri_san: PEER.into(),
    };
    assert!(config(&fixture.tls(Some(valid()))).is_ok(), "control");
    for certificate_authorities in [
        Vec::new(),
        b"not a certificate".to_vec(),
        fixture.identity.key.clone().into_bytes(),
    ] {
        assert!(refused(PeerIdentity {
            certificate_authorities,
            ..valid()
        }));
    }
    for uri_san in [
        "",
        "edge",
        "://edge",
        "spiffe:",
        "spiffe://edge/ host",
        "1spiffe://edge",
    ] {
        assert!(
            refused(PeerIdentity {
                uri_san: uri_san.into(),
                ..valid()
            }),
            "{uri_san:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn the_listener_yields_only_verified_connections_without_stalling() -> Outcome {
    let fixture = Fixture::new()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut incoming = Box::pin(incoming(listener, config(&fixture.tls(fixture.peer()))?));
    // The server polls its listener as it serves; its first connection is
    // the first that verified.
    let first = tokio::spawn(async move { incoming.next().await });
    // A peer that never handshakes, and one that fails, hold back no other.
    let _stalled = TcpStream::connect(address).await?;
    let foreign = client(&fixture.untrusted, PEER)?;
    let refused = fixture.connector(Some(&foreign), TLS13)?;
    let _ = refused
        .connect("localhost".try_into()?, TcpStream::connect(address).await?)
        .await;
    let peer = client(&fixture.trusted, PEER)?;
    let admitted = fixture.connector(Some(&peer), TLS13)?;
    let _client = admitted
        .connect("localhost".try_into()?, TcpStream::connect(address).await?)
        .await?;
    let accepted = first.await?.ok_or("the listener ended")??;
    let expected = certificates(peer.certificate.as_bytes(), "client")?;
    assert_eq!(
        accepted
            .get_ref()
            .1
            .peer_certificates()
            .and_then(<[_]>::first),
        expected.first()
    );
    assert_eq!(
        accepted.get_ref().1.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3)
    );
    Ok(())
}
