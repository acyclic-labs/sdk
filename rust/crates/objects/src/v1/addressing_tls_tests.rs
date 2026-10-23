//! TLS proof uses in-memory record flights: no sockets, scheduler or polling.
use crate::addressing::{CanonicalHost, ResourceIdentity};
use rustls::{
    ClientConfig, ClientConnection, Connection, RootCertStore, ServerConfig, ServerConnection,
};
use std::{io::Cursor, sync::Arc};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn transfer(from: &mut Connection, to: &mut Connection) -> TestResult {
    let mut records = Vec::new();
    from.write_tls(&mut records)?;
    assert!(!records.is_empty(), "expected a TLS 1.3 record flight");
    let mut input = Cursor::new(records);
    to.read_tls(&mut input)?;
    assert_eq!(input.position(), input.get_ref().len() as u64);
    to.process_new_packets()?;
    Ok(())
}

fn handshake(host: &CanonicalHost, certificate_host: &str) -> TestResult<String> {
    let certificate = rcgen::generate_simple_self_signed([certificate_host.to_owned()])?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server_config = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der())
                .into(),
        )?;
    let mut roots = RootCertStore::empty();
    roots.add(certificate.cert.der().clone())?;
    let client_config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut client = Connection::Client(ClientConnection::new(
        Arc::new(client_config),
        rustls::pki_types::ServerName::try_from(host.as_str().to_owned())?,
    )?);
    let mut server = Connection::Server(ServerConnection::new(Arc::new(server_config))?);
    // A fresh, non-resumed TLS 1.3 handshake has these three record flights.
    // A missing flight or unfinished handshake fails immediately.
    transfer(&mut client, &mut server)?;
    transfer(&mut server, &mut client)?;
    transfer(&mut client, &mut server)?;
    assert!(!client.is_handshaking());
    assert!(!server.is_handshaking());
    let Connection::Server(server) = server else {
        return Err("expected server connection".into());
    };
    Ok(server.server_name().ok_or("SNI absent")?.to_owned())
}

#[test]
fn addressing_tls_uses_the_exact_direct_account_host_and_rejects_wrong_names() -> TestResult {
    let host = ResourceIdentity::new(b"account", b"worker")?.hostname("example.com")?;
    assert_eq!(handshake(&host, host.as_str())?, host.as_str());
    // Negative control: trusting the issuing certificate is insufficient when
    // its name belongs to another account. Name verification must still fail.
    let error = handshake(&host, "worker.other-account.example.com")
        .err()
        .ok_or("wrong-account certificate was accepted")?;
    let tls_error = error
        .downcast_ref::<rustls::Error>()
        .ok_or("expected TLS error")?;
    assert!(matches!(tls_error, rustls::Error::InvalidCertificate(_)));
    Ok(())
}
