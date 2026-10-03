use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use acyclic_stream::{MemoryStream, grpc::Service};
use rcgen::generate_simple_self_signed;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};

fn native_library_name() -> &'static str {
    if cfg!(target_os = "windows") {
        "acyclic_stream_native.dll"
    } else if cfg!(target_os = "macos") {
        "libacyclic_stream_native.dylib"
    } else {
        "libacyclic_stream_native.so"
    }
}

fn node_module_path() -> PathBuf {
    let test_binary = std::env::current_exe().expect("test executable path");
    test_binary
        .parent()
        .and_then(Path::parent)
        .expect("Cargo target directory")
        .join(native_library_name())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn built_native_module_runs_against_canonical_tls_grpc_fixture()
-> Result<(), Box<dyn std::error::Error>> {
    let certificate =
        generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    let certificate_pem = certificate.cert.pem();
    let private_key_pem = certificate.signing_key.serialize_pem();
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let identity = Identity::from_pem(certificate_pem.clone(), private_key_pem);
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let service = Service::new(Arc::new(MemoryStream::default()));
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(
                acyclic_stream::wire::stream_service_server::StreamServiceServer::new(service),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_receiver.await;
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let module_source = node_module_path();
    assert!(
        module_source.exists(),
        "native N-API module must be built before runtime qualification"
    );
    let fixture_module =
        std::env::temp_dir().join(format!("acyclic-stream-native-{}.node", std::process::id()));
    std::fs::copy(&module_source, &fixture_module)?;
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/runtime_client.mjs");
    let output = Command::new("node")
        .arg(script)
        .env("ACYCLIC_STREAM_NATIVE_MODULE", &fixture_module)
        .env("ACYCLIC_STREAM_FIXTURE_ENDPOINT", endpoint)
        .env(
            "ACYCLIC_STREAM_FIXTURE_CA",
            base64(certificate_pem.as_bytes()),
        )
        .output()?;
    let _ = shutdown_sender.send(());
    let _ = server.await?;
    let _ = std::fs::remove_file(&fixture_module);
    if !output.status.success() {
        return Err(format!(
            "native runtime fixture failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"recoveredTail\":\"2\""));
    Ok(())
}

fn base64(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    for chunk in value.chunks(3) {
        let first = chunk[0] as usize;
        output.push(TABLE[first >> 2] as char);
        let second = if chunk.len() > 1 {
            chunk[1] as usize
        } else {
            0
        };
        output.push(TABLE[((first & 3) << 4) | (second >> 4)] as char);
        if chunk.len() > 1 {
            let third = if chunk.len() > 2 {
                chunk[2] as usize
            } else {
                0
            };
            output.push(TABLE[((second & 15) << 2) | (third >> 6)] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(TABLE[chunk[2] as usize & 63] as char);
        } else {
            output.push('=');
        }
    }
    output
}
