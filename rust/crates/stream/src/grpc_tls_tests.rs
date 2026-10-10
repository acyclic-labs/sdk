use super::*;

// Native trust selection is process-wide and may be cached by the verifier.
// Each scenario uses a fresh executable, never mutating the test runner's roots.
#[tokio::test]
async fn platform_roots_and_exclusive_ca() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const FIXTURE: &str = "ACYCLIC_TLS_FIXTURE";
    const MODE: &str = "ACYCLIC_TLS_MODE";
    let Ok(directory) = std::env::var(FIXTURE) else {
        let directory = tempfile::tempdir()?;
        let trusted = directory.path().join("trusted");
        std::fs::create_dir(&trusted)?;
        let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        std::fs::write(trusted.join("root.pem"), certified.cert.pem())?;
        std::fs::write(directory.path().join("key.pem"), certified.signing_key.serialize_pem())?;
        let unrelated = rcgen::generate_simple_self_signed(["unrelated".to_owned()])?;
        std::fs::write(directory.path().join("unrelated.pem"), unrelated.cert.pem())?;
        std::fs::write(directory.path().join("empty.pem"), "")?;
        let module = module_path!();
        let test = format!("{}::platform_roots_and_exclusive_ca", module.split_once("::").map_or(module, |(_, path)| path));
        let modes = if cfg!(target_os = "linux") {
            &["unrelated", "file", "dir", "empty"][..]
        } else {
            &["unrelated"][..]
        };
        for mode in modes {
            let mut child = std::process::Command::new(std::env::current_exe()?);
            child.args([test.as_str(), "--exact", "--nocapture"])
                .env(FIXTURE, directory.path()).env(MODE, mode)
                .env_remove("SSL_CERT_FILE").env_remove("SSL_CERT_DIR");
            match *mode {
                "file" => { child.env("SSL_CERT_FILE", trusted.join("root.pem")); }
                "dir" => { child.env("SSL_CERT_DIR", &trusted); }
                "empty" => { child.env("SSL_CERT_FILE", directory.path().join("empty.pem")); }
                _ => { child.env("SSL_CERT_FILE", directory.path().join("unrelated.pem")); }
            }
            let output = child.output()?;
            assert!(output.status.success(), "{mode}: {}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        }
        return Ok(());
    };
    let directory = std::path::Path::new(&directory);
    let pem = std::fs::read_to_string(directory.join("trusted/root.pem"))?;
    let key = std::fs::read_to_string(directory.join("key.pem"))?;
    let unrelated = std::fs::read(directory.join("unrelated.pem"))?;
    let mode = std::env::var(MODE)?;
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("https://localhost:{}", incoming.local_addr()?.port());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let identity = tonic::transport::Identity::from_pem(&pem, key);
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
            .add_service(StreamServiceServer::new(Service::new(Arc::new(MemoryStream::default()))))
            .serve_with_incoming_shutdown(incoming, async { let _ = stopped.await; }),
    );
    let result = async {
        let platform = Client::connect_eager(&endpoint, "exact-token").await;
        if mode == "file" || mode == "dir" {
            let client = platform?;
            assert_eq!(client.tail(StreamPath::new("platform/roots")?).await, Err(StreamError::NotFound));
            let installed = crate::StreamClient::connect(&endpoint, "exact-token").await?;
            assert_eq!(installed.stream("platform/roots")?.tail().await, Err(StreamError::NotFound));
        } else {
            let error = platform.err().ok_or("private endpoint unexpectedly trusted")?;
            assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
            assert!(format!("{error:?}").to_lowercase().contains("certificate"), "TLS cause lost: {error:?}");
        }
        let client = Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", pem.as_bytes()).await?;
        assert_eq!(client.tail(StreamPath::new("explicit/roots")?).await, Err(StreamError::NotFound));
        let denied = Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", &unrelated).await;
        let error = denied.err().ok_or("unrelated supplied CA unexpectedly trusted")?;
        assert!(std::error::Error::source(&error).is_some(), "cause lost: {error:?}");
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }.await;
    let _ = shutdown.send(());
    server.await??;
    result
}
