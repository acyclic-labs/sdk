use super::*;
use futures::stream;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use wire::machines_service_server::{MachinesService, MachinesServiceServer};

#[derive(Clone)]
enum WatchReply {
    Reject(Code),
    Items(Vec<WatchItem>),
    Silent,
    Script(std::sync::Arc<std::sync::Mutex<ObservationScript>>),
}

struct ObservationScript {
    watches: std::collections::VecDeque<WatchReply>,
    inspections: std::collections::VecDeque<Result<wire::OperationState, Code>>,
    calls: Vec<&'static str>,
}

#[derive(Clone)]
enum WatchItem {
    State(wire::OperationState),
    Error(Code),
}

#[derive(Clone)]
struct OperationService {
    expected_key: IdempotencyKey,
    expected_operation: OperationId,
    recovered: wire::RecoveredAdmission,
    inspected: wire::OperationState,
    cancelled: wire::OperationState,
    watch: WatchReply,
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
        let WatchReply::Script(_) = &self.watch else {
            return Err(Status::unimplemented("suspend"));
        };
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

    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        self.require_operation(&request.into_inner())?;
        if let WatchReply::Script(script) = &self.watch {
            script
                .lock()
                .expect("observation script")
                .calls
                .push("cancel");
        }
        Ok(Response::new(self.cancelled.clone()))
    }

    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        self.require_operation(&request.into_inner())?;
        if let WatchReply::Script(script) = &self.watch {
            let mut script = script.lock().expect("observation script");
            return script
                .inspections
                .pop_front()
                .ok_or_else(|| Status::internal("unexpected inspection"))?
                .map(Response::new)
                .map_err(|code| Status::new(code, "inspection interrupted"));
        }
        Ok(Response::new(self.inspected.clone()))
    }

    type WatchOperationStream = Pin<
        Box<dyn futures::Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>,
    >;

    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        self.require_operation(&request.into_inner())?;
        let reply = match &self.watch {
            WatchReply::Script(script) => {
                let mut script = script.lock().expect("observation script");
                script.calls.push("watch");
                script
                    .watches
                    .pop_front()
                    .ok_or_else(|| Status::internal("unexpected watch"))?
            }
            reply => reply.clone(),
        };
        match reply {
            WatchReply::Reject(code) => Err(Status::new(code, "watch rejected")),
            WatchReply::Items(items) => {
                let items = items.into_iter().map(|item| match item {
                    WatchItem::State(value) => Ok(value),
                    WatchItem::Error(code) => Err(Status::new(code, "watch interrupted")),
                });
                Ok(Response::new(Box::pin(stream::iter(items))))
            }
            WatchReply::Script(_) => Err(Status::internal("nested observation script")),
            WatchReply::Silent => Ok(Response::new(Box::pin(stream::pending()))),
        }
    }
}

impl OperationService {
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
    watches: Vec<WatchReply>,
    inspections: Vec<Result<wire::OperationState, Code>>,
) -> (
    OperationService,
    std::sync::Arc<std::sync::Mutex<ObservationScript>>,
    MachineId,
) {
    let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000041").expect("key");
    let operation = OperationId::parse("00000000-0000-0000-0000-000000000042").expect("operation");
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000043").expect("machine");
    let script = std::sync::Arc::new(std::sync::Mutex::new(ObservationScript {
        watches: watches.into(),
        inspections: inspections.into(),
        calls: Vec::new(),
    }));
    (
        OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: operation_state(operation, wire::OperationStatus::Pending),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Script(script.clone()),
        },
        script,
        machine,
    )
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
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
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
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let channel = TonicEndpoint::from_shared(format!("http://{address}"))?
        .connect()
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
