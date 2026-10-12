use tonic::codegen::*;
#[derive(Debug, Clone)]
pub struct MachinesServiceClient<T> {
    inner: tonic::client::Grpc<T>,
}
macro_rules! unary {
    ($(($method:ident, $input:ident, $output:ident, $name:literal)),+ $(,)?) => {
        $(pub async fn $method(
            &mut self,
            request: impl tonic::IntoRequest<super::$input>,
        ) -> Result<tonic::Response<super::$output>, tonic::Status> {
            self.inner.ready().await.map_err(|error| {
                tonic::Status::unknown(format!("Service was not ready: {}", error.into()))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                concat!("/acyclic.machines.v1.MachinesService/", $name));
            let mut request = request.into_request();
            request.extensions_mut().insert(
                GrpcMethod::new("acyclic.machines.v1.MachinesService", $name));
            self.inner.unary(request, path, codec).await
        })+
    };
}

impl<T> MachinesServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
{
    pub fn new(inner: T) -> Self {
        let inner = tonic::client::Grpc::new(inner);
        Self { inner }
    }
    pub fn with_interceptor<F>(
        inner: T,
        interceptor: F,
    ) -> MachinesServiceClient<InterceptedService<T, F>>
    where
        F: tonic::service::Interceptor,
        T::ResponseBody: Default,
        T: tonic::codegen::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                >,
            >,
        <T as tonic::codegen::Service<http::Request<tonic::body::Body>>>::Error:
            Into<StdError> + std::marker::Send + std::marker::Sync,
    {
        MachinesServiceClient::new(InterceptedService::new(inner, interceptor))
    }
    /// Limits the maximum size of a decoded message.
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_decoding_message_size(limit);
        self
    }
    /// Limits the maximum size of an encoded message.
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.inner = self.inner.max_encoding_message_size(limit);
        self
    }
    unary! {
        (qualify_image, QualifyImageRequest, ImageQualification, "QualifyImage"),
        (create, CreateMachineRequest, MachineAdmission, "Create"),
        (checkpoint, CheckpointMachineRequest, CheckpointAdmission, "Checkpoint"),
        (fork, ForkCheckpointRequest, ForkAdmission, "Fork"),
        (fork_machine, ForkMachineRequest, ForkMachineAdmission, "ForkMachine"),
        (suspend, MachineMutationRequest, MutationAdmission, "Suspend"),
        (wake, MachineMutationRequest, MutationAdmission, "Wake"),
        (set_suspension_policy, SetSuspensionPolicyRequest, PolicyAdmission, "SetSuspensionPolicy"),
        (destroy_machine, MachineMutationRequest, MutationAdmission, "DestroyMachine"),
        (destroy_checkpoint, CheckpointMutationRequest, MutationAdmission, "DestroyCheckpoint"),
        (recover, RecoverRequest, RecoveredAdmission, "Recover"),
        (inspect_machine, InspectMachineRequest, MachineState, "InspectMachine"),
        (inspect_checkpoint, InspectCheckpointRequest, CheckpointState, "InspectCheckpoint"),
        (list_machines, ListMachinesRequest, MachinePage, "ListMachines"),
        (events, EventsRequest, EventPage, "Events"),
        (usage, UsageRequest, UsageReceipt, "Usage"),
        (cancel, OperationRequest, OperationState, "Cancel"),
        (inspect_operation, OperationRequest, OperationState, "InspectOperation"),
    }

    pub async fn watch_operation(
        &mut self,
        request: impl tonic::IntoRequest<super::OperationRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codec::Streaming<super::OperationState>>,
        tonic::Status,
    > {
        self.inner
            .ready()
            .await
            .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {}", e.into())))?;
        let codec = tonic_prost::ProstCodec::default();
        let path = http::uri::PathAndQuery::from_static(
            "/acyclic.machines.v1.MachinesService/WatchOperation",
        );
        let mut req = request.into_request();
        req.extensions_mut().insert(GrpcMethod::new(
            "acyclic.machines.v1.MachinesService",
            "WatchOperation",
        ));
        self.inner.server_streaming(req, path, codec).await
    }
}
