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
    Delayed(wire::OperationState, Duration),
    Silent,
    Script(std::sync::Arc<std::sync::Mutex<ObservationScript>>),
}

struct ObservationScript {
    watches: std::collections::VecDeque<WatchReply>,
    inspections: std::collections::VecDeque<Result<wire::OperationState, Code>>,
    calls: Vec<&'static str>,
    selected_volumes: Vec<Option<wire::selection::VolumeRef>>,
    current_contract: Option<wire::MachineContract>,
    started: Arc<tokio::sync::Notify>,
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
    (checkpoint, CheckpointMachineRequest, CheckpointAdmission),
    (fork, ForkCheckpointRequest, ForkAdmission),
    (fork_machine, ForkMachineRequest, ForkMachineAdmission),
    (wake, MachineMutationRequest, MutationAdmission),
    (set_suspension_policy, SetSuspensionPolicyRequest, PolicyAdmission),
    (destroy_machine, MachineMutationRequest, MutationAdmission),
    (destroy_checkpoint, CheckpointMutationRequest, MutationAdmission),
    (inspect_checkpoint, InspectCheckpointRequest, CheckpointState),
    (list_machines, ListMachinesRequest, MachinePage),
    (events, EventsRequest, EventPage),
    (usage, UsageRequest, UsageReceipt);
    async fn create(&self, request: Request<wire::CreateMachineRequest>)
        -> Result<Response<wire::MachineAdmission>, Status> {
        let request = request.into_inner();
        if request.idempotency_key != Some(encode_key(self.expected_key)) {
            return Err(Status::invalid_argument("create key was substituted"));
        }
        let Some(wire::recovered_admission::Result::Create(admission)) = &self.recovered.result else {
            return Err(Status::unimplemented("create"));
        };
        if let WatchReply::Script(script) = &self.watch {
            let mut script = script.lock().expect("observation script");
            script.selected_volumes.push(request.selected_volume_ref);
            script.calls.push(
                if request.image.is_none() { "create_default" } else { "create_explicit" }
            );
        }
        Ok(Response::new(admission.clone()))
    }
    async fn inspect_machine(&self, request: Request<wire::InspectMachineRequest>)
        -> Result<Response<wire::MachineState>, Status> {
        let Some(wire::recovered_admission::Result::Create(admission)
            | wire::recovered_admission::Result::AttachVolume(admission)) = &self.recovered.result else {
            return Err(Status::unimplemented("inspect_machine"));
        };
        if request.into_inner().machine != admission.machine {
            return Err(Status::invalid_argument("machine was substituted"));
        }
        let contract = if let WatchReply::Script(script) = &self.watch {
            let mut script = script.lock().expect("observation script");
            script.calls.push("inspect_machine");
            script.current_contract.clone().or_else(|| admission.contract.clone())
        } else { admission.contract.clone() };
        Ok(Response::new(wire::MachineState {
            machine: admission.machine.clone(),
            status: wire::MachineStatus::Running as i32,
            contract,
            endpoints: Vec::new(),
            last_checkpoint: None,
            created_at_unix_ms: 1,
            changed_at_unix_ms: 1,
        }))
    }
    async fn attach_volume(&self, request: Request<wire::AttachVolumeRequest>)
        -> Result<Response<wire::MachineAdmission>, Status> {
        let request = request.into_inner();
        if request.idempotency_key != Some(encode_key(self.expected_key)) {
            return Err(Status::invalid_argument("attach key was substituted"));
        }
        let Some(wire::recovered_admission::Result::AttachVolume(admission)) = &self.recovered.result else {
            return Err(Status::unimplemented("attach_volume"));
        };
        if let WatchReply::Script(script) = &self.watch {
            let mut script = script.lock().expect("observation script");
            script.selected_volumes.push(request.selected_volume_ref);
            script.calls.push("attach_volume");
        }
        Ok(Response::new(admission.clone()))
    }
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

        type ExecStream = Pin<Box<dyn Stream<Item = Result<wire::ExecOutput, Status>> + Send>>;

        async fn exec(
            &self,
            request: Request<tonic::Streaming<wire::ExecInput>>,
        ) -> Result<Response<Self::ExecStream>, Status> {
            if let Some(guest) = &self.guest {
                return guest.exec(request).await;
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
        type ReadFileStream = Pin<Box<dyn Stream<Item = Result<wire::ReadFileResponse, Status>> + Send>>;
        async fn read_file(&self, request: Request<tonic::Streaming<wire::ReadFileRequest>>)
            -> Result<Response<Self::ReadFileStream>, Status> {
            let guest = self.guest.clone().ok_or_else(|| Status::unimplemented("read_file"))?;
            Ok(Response::new(Box::pin(stream::try_unfold((request.into_inner(), guest),
                |(mut input, guest)| async move {
                    let Some(chunk) = input.message().await? else { return Ok(None); };
                    let response = guest.read_file(Request::new(chunk))?.into_inner();
                    Ok(Some((response, (input, guest))))
                }))))
        }
        type WriteFileStream = Pin<Box<dyn Stream<Item = Result<wire::WriteFileResponse, Status>> + Send>>;
        async fn write_file(&self, request: Request<tonic::Streaming<wire::WriteFileRequest>>)
            -> Result<Response<Self::WriteFileStream>, Status> {
            let guest = self.guest.clone().ok_or_else(|| Status::unimplemented("write_file"))?;
            Ok(Response::new(Box::pin(stream::try_unfold((request.into_inner(), guest),
                |(mut input, guest)| async move {
                    let Some(chunk) = input.message().await? else { return Ok(None); };
                    let response = guest.write_file(Request::new(chunk))?.into_inner();
                    Ok(Some((response, (input, guest))))
                }))))
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
                script.started.notify_one();
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
            WatchReply::Delayed(value, delay) => Ok(Response::new(Box::pin(stream::once(async move {
                tokio::time::sleep(delay).await;
                Ok(value)
            })))),
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
        selected_volumes: Vec::new(),
        current_contract: None,
        started: Arc::new(tokio::sync::Notify::new()),
    }));
    (
        OperationService {
            guest: None,
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
        outcome: None,
    }
}

fn settled_state(
    operation: OperationId,
    machine: MachineId,
    contract: wire::MachineContract,
    attached: bool,
) -> wire::OperationState {
    let machine = wire::MachineState {
        machine: Some(encode_machine(machine)),
        status: wire::MachineStatus::Running as i32,
        contract: Some(contract),
        endpoints: Vec::new(),
        last_checkpoint: None,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    };
    let mut state = operation_state(operation, wire::OperationStatus::Succeeded);
    state.outcome = Some(wire::MutationOutcome {
        result: Some(if attached {
            wire::mutation_outcome::Result::VolumeAttached(machine)
        } else {
            wire::mutation_outcome::Result::Created(machine)
        }),
    });
    state
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
