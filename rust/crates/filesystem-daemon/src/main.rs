//! Optional local transport and native-mount owner over the canonical engine.

mod mount_journal;

use acyclic_fs::wire::filesystem::daemon::v2::{
    HealthRequest, HealthResponse, MountRef, MountRequest, MountResponse, ShutdownRequest,
    ShutdownResponse, SyncMountResponse, UnmountResponse,
    filesystem_daemon_service_server::{FilesystemDaemonService, FilesystemDaemonServiceServer},
};
use acyclic_fs::wire::filesystem::v2::filesystem_service_server::FilesystemServiceServer;
use acyclic_fs::{
    FilesystemWireLimits, FilesystemWireService, LocalFs, LocalOptions, Mount, MountId,
    MountOptions, MountPublication, OperationId,
};
use mount_journal::MountJournal;
use serde::Serialize;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{Mutex, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use tonic_web::GrpcWebLayer;
use tower_http::cors::CorsLayer;

type LocalMount = Mount<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>;

#[derive(Debug, Error)]
enum DaemonError {
    #[error("usage: fsd --root <path> [--listen <loopback-address>]")]
    Usage,
    #[error("fsd accepts loopback listen addresses only")]
    NonLoopback,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("filesystem startup failed: {0}")]
    Filesystem(String),
    #[error(transparent)]
    MountJournal(#[from] mount_journal::MountJournalError),
    #[error("transport failed: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("native mount cleanup failed: {0}")]
    MountCleanup(String),
}

#[derive(Clone, Debug)]
struct Options {
    root: PathBuf,
    listen: SocketAddr,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Startup<'a> {
    schema: &'static str,
    endpoint: String,
    bearer_token: &'a str,
    recovered_mount_intents: u32,
}

#[derive(Clone)]
struct Daemon {
    filesystem: LocalFs,
    mounts: Arc<Mutex<HashMap<MountId, Arc<LocalMount>>>>,
    journal: Arc<MountJournal>,
    recovered_mount_intents: u32,
    shutdown: watch::Sender<bool>,
}

impl Daemon {
    fn new(
        filesystem: LocalFs,
        journal: MountJournal,
        recovered_mount_intents: u32,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        Self {
            filesystem,
            mounts: Arc::new(Mutex::new(HashMap::new())),
            journal: Arc::new(journal),
            recovered_mount_intents,
            shutdown,
        }
    }

    async fn workspace(
        &self,
        reference: Option<acyclic_fs::wire::filesystem::v2::WorkspaceRef>,
    ) -> Result<
        acyclic_fs::Workspace<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
        Status,
    > {
        let reference =
            reference.ok_or_else(|| Status::invalid_argument("workspace is required"))?;
        if reference.name.is_empty() {
            return Err(Status::invalid_argument("workspace name is required"));
        }
        let workspace = self
            .filesystem
            .open_workspace(&reference.name)
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        if reference.workspace_id != workspace.id().into_bytes() {
            return Err(Status::failed_precondition(
                "workspace identity does not match its name",
            ));
        }
        Ok(workspace)
    }

    async fn stop_all(&self) -> Result<(), DaemonError> {
        let mounts: Vec<(MountId, Arc<LocalMount>)> = self
            .mounts
            .lock()
            .await
            .iter()
            .map(|(mount_id, mount)| (*mount_id, Arc::clone(mount)))
            .collect();
        for (mount_id, mount) in mounts {
            mount
                .unmount()
                .await
                .map_err(|error| DaemonError::MountCleanup(error.to_string()))?;
            self.journal.complete(mount_id)?;
            self.mounts.lock().await.remove(&mount_id);
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl FilesystemDaemonService for Daemon {
    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            status: "ready".to_owned(),
            active_mounts: u32::try_from(self.mounts.lock().await.len()).unwrap_or(u32::MAX),
            recovered_mount_intents: self.recovered_mount_intents,
        }))
    }

    async fn mount(
        &self,
        request: Request<MountRequest>,
    ) -> Result<Response<MountResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspace(request.workspace).await?;
        let destination = canonical_mount_destination(&request.destination)?;
        let publication =
            match acyclic_fs::wire::filesystem::daemon::v2::MountPublication::try_from(
                request.publication,
            )
            .map_err(|_| Status::invalid_argument("mount publication is invalid"))?
            {
                acyclic_fs::wire::filesystem::daemon::v2::MountPublication::CloseAndSync => {
                    MountPublication::CloseAndSync
                }
                acyclic_fs::wire::filesystem::daemon::v2::MountPublication::PerMutation => {
                    MountPublication::PerMutation
                }
                acyclic_fs::wire::filesystem::daemon::v2::MountPublication::Manual => {
                    MountPublication::Manual
                }
                acyclic_fs::wire::filesystem::daemon::v2::MountPublication::Unspecified => {
                    return Err(Status::invalid_argument(
                        "mount publication must be specified",
                    ));
                }
            };
        let options = if request.writable {
            MountOptions::read_write()
        } else {
            MountOptions::read_only()
        }
        .subdirectory(request.subdirectory)
        .publication(publication);
        let mount_id = MountId::new();
        self.journal
            .admit(mount_id, &destination)
            .map_err(|error| Status::internal(error.to_string()))?;
        let mount = match workspace.mount(destination.clone(), options).await {
            Ok(mount) => Arc::new(mount),
            Err(error) => {
                self.journal
                    .complete(mount_id)
                    .map_err(|cleanup| Status::internal(cleanup.to_string()))?;
                return Err(Status::failed_precondition(error.to_string()));
            }
        };
        self.mounts.lock().await.insert(mount_id, mount);
        Ok(Response::new(MountResponse {
            mount_id: mount_id.into_bytes().to_vec(),
            destination: destination.to_string_lossy().into_owned(),
        }))
    }

    async fn sync_mount(
        &self,
        request: Request<MountRef>,
    ) -> Result<Response<SyncMountResponse>, Status> {
        let mount_id = mount_id(&request.into_inner().mount_id)?;
        let mount = self
            .mounts
            .lock()
            .await
            .get(&mount_id)
            .cloned()
            .ok_or_else(|| Status::not_found("mount is absent or stopped"))?;
        mount
            .sync()
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(SyncMountResponse {}))
    }

    async fn unmount(
        &self,
        request: Request<MountRef>,
    ) -> Result<Response<UnmountResponse>, Status> {
        let mount_id = mount_id(&request.into_inner().mount_id)?;
        let mount = self
            .mounts
            .lock()
            .await
            .get(&mount_id)
            .cloned()
            .ok_or_else(|| Status::not_found("mount is absent or stopped"))?;
        mount
            .unmount()
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        self.journal
            .complete(mount_id)
            .map_err(|error| Status::internal(error.to_string()))?;
        self.mounts.lock().await.remove(&mount_id);
        Ok(Response::new(UnmountResponse {}))
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        self.shutdown.send_replace(true);
        Ok(Response::new(ShutdownResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), DaemonError> {
    if std::env::args_os().any(|argument| argument == "--version") {
        println!("fsd {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    let options = parse_options(std::env::args_os().skip(1))?;
    std::fs::create_dir_all(&options.root)?;
    let journal = MountJournal::open(&options.root)?;
    let recovered_mount_intents = journal.recover()?;
    let filesystem = LocalFs::local(LocalOptions::new(&options.root))
        .await
        .map_err(|error| DaemonError::Filesystem(error.to_string()))?;
    let filesystem_service =
        FilesystemWireService::new(filesystem.clone(), FilesystemWireLimits::default())
            .map_err(|error| DaemonError::Filesystem(error.to_string()))?;
    let (shutdown, mut shutdown_requested) = watch::channel(false);
    let daemon = Daemon::new(
        filesystem,
        journal,
        recovered_mount_intents,
        shutdown.clone(),
    );
    let token = hex::encode(OperationId::new().into_bytes());
    let listener = tokio::net::TcpListener::bind(options.listen).await?;
    let listen = listener.local_addr()?;
    let authenticated_filesystem = FilesystemServiceServer::with_interceptor(
        filesystem_service,
        bearer_interceptor(token.clone()),
    );
    let authenticated_daemon = FilesystemDaemonServiceServer::with_interceptor(
        daemon.clone(),
        bearer_interceptor(token.clone()),
    );

    println!(
        "{}",
        serde_json::to_string(&Startup {
            schema: "acyclic-fsd-startup-v2",
            endpoint: format!("http://{listen}"),
            bearer_token: &token,
            recovered_mount_intents,
        })
        .map_err(|error| DaemonError::Filesystem(error.to_string()))?
    );

    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let requested = async move {
        while !*shutdown_requested.borrow() {
            if shutdown_requested.changed().await.is_err() {
                break;
            }
        }
    };
    Server::builder()
        .accept_http1(true)
        .layer(CorsLayer::very_permissive())
        .layer(GrpcWebLayer::new())
        .add_service(authenticated_filesystem)
        .add_service(authenticated_daemon)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
            tokio::select! {
                () = ctrl_c => {}
                () = requested => {}
            }
        })
        .await?;
    shutdown.send_replace(true);
    daemon.stop_all().await
}

fn bearer_interceptor(
    token: String,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |request: Request<()>| {
        let expected = format!("Bearer {token}");
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some(expected.as_str())
        {
            Ok(request)
        } else {
            Err(Status::unauthenticated("valid bearer token is required"))
        }
    }
}

fn canonical_mount_destination(value: &str) -> Result<PathBuf, Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument("mount destination is required"));
    }
    let path = Path::new(value);
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(Status::invalid_argument(
            "mount destination must be an absolute canonical path",
        ));
    }
    path.canonicalize()
        .map_err(|error| Status::failed_precondition(error.to_string()))
}

fn mount_id(value: &[u8]) -> Result<MountId, Status> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| Status::invalid_argument("mount identity must contain 16 bytes"))?;
    Ok(MountId::from_bytes(bytes))
}

fn parse_options(
    arguments: impl IntoIterator<Item = std::ffi::OsString>,
) -> Result<Options, DaemonError> {
    let mut root = None;
    let mut listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--root") => root = arguments.next().map(PathBuf::from),
            Some("--listen") => {
                listen = arguments
                    .next()
                    .and_then(|value| value.into_string().ok())
                    .and_then(|value| value.parse().ok())
                    .ok_or(DaemonError::Usage)?;
            }
            _ => return Err(DaemonError::Usage),
        }
    }
    if !listen.ip().is_loopback() {
        return Err(DaemonError::NonLoopback);
    }
    Ok(Options {
        root: root.ok_or(DaemonError::Usage)?,
        listen,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_only_a_root_and_loopback_transport() -> Result<(), DaemonError> {
        let options = parse_options(
            ["--root", "state", "--listen", "127.0.0.1:4318"]
                .into_iter()
                .map(std::ffi::OsString::from),
        )?;
        assert_eq!(options.root, PathBuf::from("state"));
        assert_eq!(
            options.listen,
            "127.0.0.1:4318".parse().map_err(|_| DaemonError::Usage)?
        );
        assert!(
            parse_options(
                ["--root", "state", "--listen", "0.0.0.0:4318"]
                    .into_iter()
                    .map(std::ffi::OsString::from),
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn mount_identity_is_exact() {
        assert!(matches!(
            mount_id(&[7; 16]).map(|value| value.into_bytes()),
            Ok(value) if value == [7; 16]
        ));
        assert!(mount_id(&[7; 15]).is_err());
    }
}
