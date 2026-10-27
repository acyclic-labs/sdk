use super::*;
use futures::stream;
use tokio::net::TcpListener;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use wire::machines_service_server::{MachinesService, MachinesServiceServer};

#[derive(Clone)]
enum WatchReply {
    Reject(Code),
    Items(Vec<WatchItem>),
}

#[derive(Clone)]
enum JoinReply {
    Reject(Code),
    Settlement(wire::OperationSettlement),
    Stream(Vec<Result<wire::OperationSettlement, Code>>),
    Silent,
    Script(Arc<std::sync::Mutex<JoinScript>>),
    #[cfg(feature = "core")]
    Core(crate::core::OperationRegistry),
}

struct JoinScript {
    replies: std::collections::VecDeque<JoinReply>,
    calls: Vec<&'static str>,
}

#[derive(Clone)]
enum WatchItem {
    State(wire::OperationState),
    Error(Code),
}

#[derive(Clone)]
struct OperationService {
    guest: Option<Arc<guest_io::GuestIo>>,
    expected_key: IdempotencyKey,
    expected_operation: OperationId,
    recovered: wire::RecoveredAdmission,
    inspected: wire::OperationState,
    cancelled: wire::OperationState,
    watch: WatchReply,
    join: JoinReply,
}

macro_rules! operation_service {
    ($(($method:ident, $input:ident, $output:ident)),+; $($operations:tt)*) => {
        #[tonic::async_trait]
        impl MachinesService for OperationService {
            $(async fn $method(&self, _: Request<wire::$input>)
                -> Result<Response<wire::$output>, Status> {
                Err(Status::unimplemented(stringify!($method)))
            })+
            $($operations)*
        }
    };
}

operation_service! {
    (qualify_image, QualifyImageRequest, ImageQualification),
    (create, CreateMachineRequest, MachineAdmission),
    (checkpoint, CheckpointMachineRequest, CheckpointAdmission),
    (fork, ForkCheckpointRequest, ForkAdmission),
    (fork_machine, ForkMachineRequest, ForkMachineAdmission),
    (wake, MachineMutationRequest, MutationAdmission),
    (set_suspension_policy, SetSuspensionPolicyRequest, PolicyAdmission),
    (destroy_machine, MachineMutationRequest, MutationAdmission),
    (destroy_checkpoint, CheckpointMutationRequest, MutationAdmission),
    (inspect_machine, InspectMachineRequest, MachineState),
    (inspect_checkpoint, InspectCheckpointRequest, CheckpointState),
    (list_machines, ListMachinesRequest, MachinePage),
    (events, EventsRequest, EventPage),
    (usage, UsageRequest, UsageReceipt);
    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        self.record_call("suspend");
        let request = request.into_inner();
        if request
            .idempotency_key
            .as_ref()
            .map(|key| key.value.as_slice())
            != Some(self.expected_key.as_bytes().as_slice())
        {
            return Err(Status::invalid_argument("idempotency key was substituted"));
        }
        let Some(wire::recovered_admission::Result::Suspend(admission)) =
            &self.recovered.result
        else {
            return Err(Status::internal("missing suspend admission"));
        };
        if request.machine != admission.machine {
            return Err(Status::invalid_argument("machine was substituted"));
        }
        Ok(Response::new(admission.clone()))
    }
    async fn recover(
        &self,
        request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        self.record_call("recover");
        if request
            .into_inner()
            .idempotency_key
            .as_ref()
            .map(|value| value.value.as_slice())
            != Some(self.expected_key.as_bytes().as_slice())
        {
            return Err(Status::invalid_argument("idempotency key was substituted"));
        }
        Ok(Response::new(self.recovered.clone()))
    }

    type JoinStream = Pin<Box<dyn Stream<Item = Result<wire::OperationSettlement, Status>> + Send>>;

    async fn join(&self, request: Request<wire::JoinRequest>) -> Result<Response<Self::JoinStream>, Status> {
        let request = request.into_inner();
        if request.idempotency_key.as_ref().map(|key| key.value.as_slice()) != Some(self.expected_key.as_bytes().as_slice()) || request.operation != operation_request(self.expected_operation).operation {
            return Err(Status::invalid_argument("join identity was substituted"));
        }
        self.record_call("join");
        let reply = match &self.join {
            JoinReply::Script(script) => script.lock().expect("join script").replies.pop_front().ok_or_else(|| Status::internal("unexpected join"))?,
            value => value.clone(),
        };
        match reply {
            JoinReply::Reject(code) => Err(Status::new(code, "join interrupted")),
            JoinReply::Settlement(value) => Ok(Response::new(Box::pin(stream::once(async { Ok(value) })))),
            JoinReply::Stream(values) => Ok(Response::new(Box::pin(stream::iter(values.into_iter().map(|value| value.map_err(|code| Status::new(code, "join body interrupted"))))))),
            JoinReply::Silent => Ok(Response::new(Box::pin(stream::pending()))),
            JoinReply::Script(_) => Err(Status::internal("nested join script")),
            #[cfg(feature = "core")]
            JoinReply::Core(core) => {
                let key = self.expected_key;
                let operation = self.expected_operation;
                Ok(Response::new(Box::pin(stream::once(async move {
                    let value = core.join_settlement(key, operation).await.map_err(|_| Status::unavailable("canonical join interrupted"))?;
                    encode_settlement(&value).map_err(|_| Status::internal("invalid canonical result"))
                }))))
            },
        }
    }

    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        self.require_operation(&request.into_inner())?;
        self.record_call("cancel");
        Ok(Response::new(self.cancelled.clone()))
    }

    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        self.require_operation(&request.into_inner())?;
        self.record_call("inspect");
        Ok(Response::new(self.inspected.clone()))
    }

        type ExecStream = Pin<Box<dyn Stream<Item = Result<wire::ExecOutput, Status>> + Send>>;

        async fn exec(
            &self,
            request: Request<wire::ExecRequest>,
        ) -> Result<Response<Self::ExecStream>, Status> {
            if let Some(guest) = &self.guest {
                return Ok(guest.exec(request));
            }
            Err(Status::unimplemented("exec"))
        }
        async fn read_output(
            &self,
            request: Request<wire::ReadOutputRequest>,
        ) -> Result<Response<wire::MachineOutput>, Status> {
            if let Some(guest) = &self.guest {
                return Ok(guest.read_output(request));
            }
            Err(Status::unimplemented("read_output"))
        }
    type WatchOperationStream = Pin<
        Box<dyn futures::Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>,
    >;

    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        self.require_operation(&request.into_inner())?;
        self.record_call("watch");
        match self.watch.clone() {
            WatchReply::Reject(code) => Err(Status::new(code, "watch rejected")),
            WatchReply::Items(items) => {
                let items = items.into_iter().map(|item| match item {
                    WatchItem::State(value) => Ok(value),
                    WatchItem::Error(code) => Err(Status::new(code, "watch interrupted")),
                });
                Ok(Response::new(Box::pin(stream::iter(items))))
            }
        }
    }
}

impl OperationService {
    fn record_call(&self, name: &'static str) {
        if let JoinReply::Script(script) = &self.join {
            script.lock().expect("join script").calls.push(name);
        }
    }

    fn require_operation(&self, request: &wire::OperationRequest) -> Result<(), Status> {
        if request
            .operation
            .as_ref()
            .map(|value| value.value.as_slice())
            != Some(self.expected_operation.as_bytes().as_slice())
        {
            return Err(Status::invalid_argument("operation was substituted"));
        }
        Ok(())
    }
}
fn scripted_operation_service(
    replies: Vec<JoinReply>,
) -> (
    OperationService,
    Arc<std::sync::Mutex<JoinScript>>,
    MachineId,
) {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000041").expect("key");
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042").expect("operation");
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043").expect("machine");
    let script = Arc::new(std::sync::Mutex::new(JoinScript {
        replies: replies.into(),
        calls: Vec::new(),
    }));
    (
        OperationService {
            guest: None,
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: operation_state(operation, wire::OperationStatus::Pending),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Items(Vec::new()),
            join: JoinReply::Script(script.clone()),
        },
        script,
        machine,
    )
}

fn settled_suspend(operation: OperationId, machine: MachineId) -> wire::OperationSettlement {
    wire::OperationSettlement {
        operation: Some(operation_state(operation, wire::OperationStatus::Succeeded)),
        settlement_commit_id: vec![7; 32],
        outcome: Some(wire::MutationOutcome {
            result: Some(wire::mutation_outcome::Result::Suspended(encode_machine(
                machine,
            ))),
        }),
    }
}

async fn serve_operation_service(
    service: OperationService,
) -> Result<
    (
        GrpcProvider,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // Contract fixtures keep real HTTP/2 framing and generated RPCs, while
    // Tokio owns every wakeup. A paused clock cannot outrun kernel socket I/O.
    let (incoming, connections) = tokio::sync::mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(MachinesServiceServer::with_interceptor(
                service,
                |request: Request<()>| {
                    let values: Vec<_> =
                        request.metadata().get_all("authorization").iter().collect();
                    if values.len() != 1
                        || values
                            .first()
                            .is_none_or(|value| *value != "Bearer opaque.account+/=")
                    {
                        return Err(Status::unauthenticated("missing or substituted bearer"));
                    }
                    Ok(request)
                },
            ))
            .serve_with_incoming_shutdown(ReceiverStream::new(connections), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let channel = TonicEndpoint::from_static("http://contract.fixture")
        .timeout(RPC_TIMEOUT)
        .connect_with_connector(tower::service_fn(move |_| {
            let incoming = incoming.clone();
            async move {
                let (client, server) = tokio::io::duplex(65536);
                incoming
                    .send(Ok::<_, std::io::Error>(server))
                    .await
                    .map_err(|_| std::io::Error::other("fixture server closed"))?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }))
        .await?;
    Ok((
        GrpcProvider::grpc_with_auth(channel, BearerAuth::new("opaque.account+/=")?),
        shutdown_tx,
        server,
    ))
}

fn operation_state(operation: OperationId, status: wire::OperationStatus) -> wire::OperationState {
    wire::OperationState {
        operation: Some(wire::OperationId {
            value: operation.as_bytes().to_vec(),
        }),
        status: status as i32,
    }
}

fn recovered_suspend(
    outer: OperationId,
    nested: OperationId,
    machine: MachineId,
) -> wire::RecoveredAdmission {
    wire::RecoveredAdmission {
        operation: Some(wire::OperationId {
            value: outer.as_bytes().to_vec(),
        }),
        result: Some(wire::recovered_admission::Result::Suspend(
            wire::MutationAdmission {
                operation: Some(wire::OperationId {
                    value: nested.as_bytes().to_vec(),
                }),
                machine: Some(encode_machine(machine)),
                checkpoint: None,
            },
        )),
    }
}

mod codecs;
mod operations;
mod recovery;
mod transport;

mod guest_io;
