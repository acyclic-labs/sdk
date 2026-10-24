use super::*;
use futures::TryStreamExt as _;
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

type FixtureResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// A minimal HTTP/2 peer checks the negotiated protocol and framing. No provider
// key, external endpoint or system trust mutation is involved.
async fn exchange(http1_only: bool, native: bool) -> FixtureResult<bool> {
    let certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let root = reqwest::Certificate::from_der(certificate.cert.der())?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(
        vec![certificate.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.signing_key.serialize_der()).into(),
    )?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint = format!("https://localhost:{}/", listener.local_addr()?.port());
    let builder = ReqwestTransport::client_builder()
        .no_proxy()
        .add_root_certificate(root);
    let builder = if http1_only {
        builder.http1_only()
    } else {
        builder
    };
    let transport = ReqwestTransport(builder.build()?);
    let (settled, settlement) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_http2(listener, config, settlement));
    let result: FixtureResult<()> = async {
        let request = HttpRequest {
            url: endpoint,
            headers: BTreeMap::new(),
            body: Bytes::from_static(b"{}"),
        };
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| acyclic_harness_core::Error::NativeConstruction { cause, work })?;
        let response = if native {
            transport.send_with_native_work(request, &work)?.await
        } else {
            transport.send(request).await
        };
        if http1_only {
            assert!(response.is_err());
        } else {
            let response = response?;
            assert_eq!(response.status, 200);
            let chunks = response.body.try_collect::<Vec<_>>().await?;
            assert_eq!(chunks.concat(), b"ok");
        }
        Ok(())
    }
    .await;
    let _ = settled.send(());
    if let Err(error) = result {
        server.abort();
        let _ = server.await;
        return Err(error);
    }
    server.await?
}

async fn serve_http2(
    listener: tokio::net::TcpListener,
    config: rustls::ServerConfig,
    settlement: tokio::sync::oneshot::Receiver<()>,
) -> FixtureResult<bool> {
    let (socket, _) = listener.accept().await?;
    let mut socket = tokio_rustls::TlsAcceptor::from(Arc::new(config))
        .accept(socket)
        .await?;
    if socket.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(false);
    }
    let mut preface = [0; 24];
    socket.read_exact(&mut preface).await?;
    assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
    // SETTINGS, with no extensions or altered flow-control limits.
    socket.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
    let stream = loop {
        let mut header = [0; 9];
        socket.read_exact(&mut header).await?;
        let [high, middle, low, kind, flags, a, b, c, d] = header;
        let length = (usize::from(high) << 16) | (usize::from(middle) << 8) | usize::from(low);
        assert!(length <= 16_384);
        let mut payload = vec![0; length];
        socket.read_exact(&mut payload).await?;
        if kind == 4 && flags & 1 == 0 {
            socket.write_all(&[0, 0, 0, 4, 1, 0, 0, 0, 0]).await?;
        }
        if matches!(kind, 0 | 1) && flags & 1 != 0 {
            break [a, b, c, d];
        }
    };
    // HEADERS: END_HEADERS and HPACK's indexed ':status: 200'.
    let mut headers = vec![0, 0, 1, 1, 4];
    headers.extend_from_slice(&stream);
    headers.push(0x88);
    socket.write_all(&headers).await?;
    let mut data = vec![0, 0, 2, 0, 1];
    data.extend_from_slice(&stream);
    data.extend_from_slice(b"ok");
    socket.write_all(&data).await?;
    socket.flush().await?;
    settlement.await?;
    Ok(true)
}

#[tokio::test]
async fn pooled_transport_negotiates_http2_and_receives_framed_body() -> FixtureResult<()> {
    for native in [false, true] {
        assert!(exchange(false, native).await?);
        assert!(
            !exchange(true, native).await?,
            "HTTP/1-only negative control advertised HTTP/2"
        );
    }
    Ok(())
}
