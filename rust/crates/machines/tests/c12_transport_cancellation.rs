//! Cancellation conformance over the authenticated owner-private Machines transport.

#![cfg(all(feature = "grpc", target_os = "linux"))]
#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "transport conformance assertions fail on unexpected fixture outcomes"
)]

use acyclic_machines::{
    ExecCommand, ExecOutput, MachineId, MachinesProvider, grpc::GrpcProvider, wire,
};
use futures::{Future, StreamExt, stream};
use std::{
    os::unix::fs::{DirBuilderExt, PermissionsExt},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};
use wire::machines_service_server::{MachinesService, MachinesServiceServer};

#[derive(Default)]
struct Observations {
    exec: Mutex<Vec<wire::ExecRequest>>,
    reads: Mutex<Vec<wire::ReadOutputRequest>>,
    exec_closed: Arc<Notify>,
    read_started: Notify,
    read_closed: Arc<Notify>,
}

struct OnCancel(Arc<Notify>);
impl Drop for OnCancel {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Clone)]
struct Service(Arc<Observations>);

type Output = futures::stream::BoxStream<'static, Result<wire::ExecOutput, Status>>;

macro_rules! service {
    ($(($method:ident, $input:ident, $output:ident)),* $(,)?) => {
        #[tonic::async_trait]
        impl MachinesService for Service {
            $(async fn $method(&self, _: Request<wire::$input>)
                -> Result<Response<wire::$output>, Status> {
                Err(Status::unimplemented(stringify!($method)))
            })*

            type WatchOperationStream = futures::stream::BoxStream<'static, Result<wire::OperationState, Status>>;
            async fn watch_operation(&self, _: Request<wire::OperationRequest>)
                -> Result<Response<Self::WatchOperationStream>, Status> {
                Err(Status::unimplemented("watch_operation"))
            }

            type ExecStream = Output;
            async fn exec(&self, request: Request<wire::ExecRequest>) -> Result<Response<Output>, Status> {
                let request = request.into_inner();
                let machine = request.machine.clone();
                let first = {
                    let mut commands = self.0.exec.lock().unwrap();
                    commands.push(request);
                    commands.len() == 1
                };
                let data = wire::ExecOutput {
                    machine: machine.clone(),
                    output: Some(wire::exec_output::Output::Data(b"output".to_vec())),
                };
                let output = if first {
                    let guard = OnCancel(self.0.exec_closed.clone());
                    stream::iter([Ok(data)]).chain(stream::once(async move {
                        let _guard = guard;
                        std::future::pending().await
                    })).boxed()
                } else {
                    stream::iter([Ok(data), Ok(wire::ExecOutput {
                        machine,
                        output: Some(wire::exec_output::Output::ExitCode(0)),
                    })]).boxed()
                };
                Ok(Response::new(output))
            }

            async fn read_output(&self, request: Request<wire::ReadOutputRequest>)
                -> Result<Response<wire::MachineOutput>, Status> {
                let request = request.into_inner();
                self.0.reads.lock().unwrap().push(request.clone());
                if request.offset == 0 {
                    let _guard = OnCancel(self.0.read_closed.clone());
                    self.0.read_started.notify_one();
                    std::future::pending::<()>().await;
                }
                Ok(Response::new(wire::MachineOutput {
                    machine: request.machine,
                    offset: request.offset,
                    data: b"next".to_vec(),
                }))
            }
        }
    };
}

service! {
    (qualify_image, QualifyImageRequest, ImageQualification),
    (create, CreateMachineRequest, MachineAdmission),
    (checkpoint, CheckpointMachineRequest, CheckpointAdmission),
    (fork, ForkCheckpointRequest, ForkAdmission),
    (fork_machine, ForkMachineRequest, ForkMachineAdmission),
    (suspend, MachineMutationRequest, MutationAdmission),
    (wake, MachineMutationRequest, MutationAdmission),
    (set_suspension_policy, SetSuspensionPolicyRequest, PolicyAdmission),
    (destroy_machine, MachineMutationRequest, MutationAdmission),
    (destroy_checkpoint, CheckpointMutationRequest, MutationAdmission),
    (recover, RecoverRequest, RecoveredAdmission),
    (inspect_machine, InspectMachineRequest, MachineState),
    (inspect_checkpoint, InspectCheckpointRequest, CheckpointState),
    (list_machines, ListMachinesRequest, MachinePage),
    (events, EventsRequest, EventPage),
    (usage, UsageRequest, UsageReceipt),
    (cancel, OperationRequest, OperationState),
    (inspect_operation, OperationRequest, OperationState),
}

// A runnable yield keeps paused time from jumping to a server deadline during OS I/O.
async fn drive<F: Future>(future: F) -> F::Output {
    tokio::pin!(future);
    let started = std::time::Instant::now();
    loop {
        tokio::select! {
            biased;
            output = &mut future => return output,
            _ = tokio::task::yield_now() => {
                assert!(started.elapsed() < Duration::from_secs(1), "transport did not settle within one second");
            }
        }
    }
}

struct Fixture {
    root: PathBuf,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    provider: GrpcProvider,
    observations: Arc<Observations>,
}

impl Fixture {
    async fn open() -> Self {
        let root =
            std::env::temp_dir().join(format!("c12-machines-cancel-{}", uuid::Uuid::new_v4()));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .unwrap();
        let socket = root.join("machines.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let observations = Arc::new(Observations::default());
        let service = Service(observations.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(MachinesServiceServer::with_interceptor(
                    service,
                    |request: Request<()>| {
                        if request
                            .metadata()
                            .get("authorization")
                            .is_none_or(|value| value != "Bearer fixture")
                        {
                            return Err(Status::unauthenticated("missing fixture bearer"));
                        }
                        Ok(request)
                    },
                ))
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await
        });
        let provider = drive(GrpcProvider::connect_local(&socket, Some("fixture")))
            .await
            .unwrap();
        Self {
            root,
            server,
            provider,
            observations,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_file(self.root.join("machines.sock"));
        let _ = std::fs::remove_dir(&self.root);
    }
}

fn identity(last: u8) -> MachineId {
    MachineId::parse(&format!("00000000-0000-4000-8000-{last:012}")).unwrap()
}

#[tokio::test(start_paused = true)]
async fn dropping_exec_drains_server_stream_and_preserves_the_next_machine() {
    let wall = std::time::Instant::now();
    let fixture = drive(Fixture::open()).await;
    let clock = tokio::time::Instant::now();
    let first = identity(1);
    let next = identity(2);
    let command = || ExecCommand::new(vec!["echo".into(), "hi".into()], 60_000).unwrap();
    let mut stream = drive(fixture.provider.exec(first, command()))
        .await
        .unwrap();
    assert_eq!(
        drive(stream.next()).await.unwrap().unwrap(),
        ExecOutput::Data(b"output".to_vec())
    );
    drop(stream);
    drive(fixture.observations.exec_closed.notified()).await;
    assert_eq!(
        tokio::time::Instant::now(),
        clock,
        "cancellation must not rely on expiry"
    );
    let mut stream = drive(fixture.provider.exec(next, command())).await.unwrap();
    assert_eq!(
        drive(stream.next()).await.unwrap().unwrap(),
        ExecOutput::Data(b"output".to_vec())
    );
    assert_eq!(
        drive(stream.next()).await.unwrap().unwrap(),
        ExecOutput::ExitCode(0)
    );
    assert!(drive(stream.next()).await.is_none());
    let commands = fixture.observations.exec.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(
        commands
            .first()
            .unwrap()
            .machine
            .as_ref()
            .unwrap()
            .value
            .as_slice(),
        first.as_bytes()
    );
    assert_eq!(
        commands
            .get(1)
            .unwrap()
            .machine
            .as_ref()
            .unwrap()
            .value
            .as_slice(),
        next.as_bytes()
    );
    assert!(wall.elapsed() < Duration::from_secs(2));
}

#[tokio::test(start_paused = true)]
async fn dropping_console_read_drains_server_work_and_preserves_the_next_machine() {
    let wall = std::time::Instant::now();
    let fixture = drive(Fixture::open()).await;
    let clock = tokio::time::Instant::now();
    let first = identity(3);
    let next = identity(4);
    let provider = fixture.provider.clone();
    let read = tokio::spawn(async move { provider.read_output(first, 0, 16).await });
    drive(fixture.observations.read_started.notified()).await;
    read.abort();
    assert!(drive(read).await.unwrap_err().is_cancelled());
    drive(fixture.observations.read_closed.notified()).await;
    assert_eq!(
        tokio::time::Instant::now(),
        clock,
        "cancellation must not rely on expiry"
    );
    let output = drive(fixture.provider.read_output(next, 1, 16))
        .await
        .unwrap();
    assert_eq!(output.machine, next);
    assert_eq!(output.offset, 1);
    assert_eq!(output.data, b"next");
    let reads = fixture.observations.reads.lock().unwrap();
    assert_eq!(reads.len(), 2);
    assert_eq!(
        reads
            .first()
            .unwrap()
            .machine
            .as_ref()
            .unwrap()
            .value
            .as_slice(),
        first.as_bytes()
    );
    assert_eq!(
        reads
            .get(1)
            .unwrap()
            .machine
            .as_ref()
            .unwrap()
            .value
            .as_slice(),
        next.as_bytes()
    );
    assert!(wall.elapsed() < Duration::from_secs(2));
}
