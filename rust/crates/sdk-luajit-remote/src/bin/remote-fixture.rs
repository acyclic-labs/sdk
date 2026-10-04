use std::{env, fs, sync::Arc};
use acyclic_stream::{grpc::Service, MemoryStream};
use rcgen::generate_simple_self_signed;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ca_path = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--ca-path" { ca_path = args.next(); }
    }
    let ca_path = ca_path.ok_or("--ca-path is required")?;
    let certified = generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    fs::write(&ca_path, certified.cert.pem())?;
    let identity = Identity::from_pem(certified.cert.pem(), certified.signing_key.serialize_pem());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let address = listener.local_addr()?;
    println!("https://{}", address);
    eprintln!("lua remote fixture ready at https://{}", address);
    Server::builder()
        .tls_config(ServerTlsConfig::new().identity(identity))?
        .add_service(acyclic_stream::wire::stream_service_server::StreamServiceServer::new(
            Service::new(Arc::new(MemoryStream::default())),
        ))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}