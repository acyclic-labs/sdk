use acyclic_objects::v2::wire;
use acyclic_stream::{MemoryStream, grpc::Service};
use futures::stream;
use rcgen::generate_simple_self_signed;
use std::{env, fs, sync::Arc};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

fn authenticated<T>(request: Request<T>) -> Result<T, Status> {
    if request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some("Bearer lua-fixture-token")
    {
        return Err(Status::unauthenticated("credential required"));
    }
    Ok(request.into_inner())
}

struct UploadFixture;

#[tonic::async_trait]
impl wire::objects_service_server::ObjectsService for UploadFixture {
    async fn put_object(
        &self,
        request: Request<tonic::Streaming<wire::PutObjectRequest>>,
    ) -> Result<Response<wire::ObjectInfo>, Status> {
        let mut frames = authenticated(request)?;
        let mut header_seen = false;
        let mut complete = false;
        let mut size = 0_u64;
        while let Some(frame) = frames.message().await? {
            match frame.frame {
                Some(wire::put_object_request::Frame::Header(_)) if !header_seen => {
                    header_seen = true;
                }
                Some(wire::put_object_request::Frame::Body(body)) if header_seen && !complete => {
                    size = size
                        .checked_add(body.len() as u64)
                        .ok_or_else(|| Status::resource_exhausted("upload size overflow"))?;
                }
                Some(wire::put_object_request::Frame::Complete(true))
                    if header_seen && !complete =>
                {
                    complete = true;
                }
                _ => return Err(Status::invalid_argument("invalid upload frame")),
            }
        }
        if !header_seen || !complete {
            return Err(Status::invalid_argument("upload completion frame required"));
        }
        Ok(Response::new(wire::ObjectInfo {
            etag: "lua-fixture".into(),
            size,
            metadata: None,
            last_modified: None,
        }))
    }

    type GetObjectStream = stream::Empty<Result<wire::GetObjectResponse, Status>>;

    async fn get_object(
        &self,
        _request: Request<wire::GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        Err(Status::unimplemented(
            "Lua fixture only serves upload qualification",
        ))
    }

    async fn head_object(
        &self,
        _request: Request<wire::HeadObjectRequest>,
    ) -> Result<Response<wire::HeadObjectResponse>, Status> {
        Err(Status::unimplemented(
            "Lua fixture only serves upload qualification",
        ))
    }

    async fn delete_object(
        &self,
        _request: Request<wire::DeleteObjectRequest>,
    ) -> Result<Response<wire::DeleteObjectResponse>, Status> {
        Err(Status::unimplemented(
            "Lua fixture only serves upload qualification",
        ))
    }

    async fn list_objects(
        &self,
        _request: Request<wire::ListObjectsRequest>,
    ) -> Result<Response<wire::ListObjectsResponse>, Status> {
        Err(Status::unimplemented(
            "Lua fixture only serves upload qualification",
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ca_path = None;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--ca-path" {
            ca_path = args.next();
        }
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
        .add_service(
            acyclic_stream::wire::stream_service_server::StreamServiceServer::new(Service::new(
                Arc::new(MemoryStream::default()),
            )),
        )
        .add_service(
            acyclic_objects::v2::wire::objects_service_server::ObjectsServiceServer::new(
                UploadFixture,
            ),
        )
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
