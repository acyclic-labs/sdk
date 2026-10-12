use tonic::codegen::*;
/// Generated trait containing gRPC methods that should be implemented for use with MachinesServiceServer.
#[async_trait]
pub trait MachinesService: std::marker::Send + std::marker::Sync + 'static {
    async fn qualify_image(
        &self,
        request: tonic::Request<super::QualifyImageRequest>,
    ) -> std::result::Result<tonic::Response<super::ImageQualification>, tonic::Status>;
    async fn create(
        &self,
        request: tonic::Request<super::CreateMachineRequest>,
    ) -> std::result::Result<tonic::Response<super::MachineAdmission>, tonic::Status>;
    async fn checkpoint(
        &self,
        request: tonic::Request<super::CheckpointMachineRequest>,
    ) -> std::result::Result<tonic::Response<super::CheckpointAdmission>, tonic::Status>;
    async fn fork(
        &self,
        request: tonic::Request<super::ForkCheckpointRequest>,
    ) -> std::result::Result<tonic::Response<super::ForkAdmission>, tonic::Status>;
    async fn fork_machine(
        &self,
        request: tonic::Request<super::ForkMachineRequest>,
    ) -> std::result::Result<tonic::Response<super::ForkMachineAdmission>, tonic::Status>;
    async fn suspend(
        &self,
        request: tonic::Request<super::MachineMutationRequest>,
    ) -> std::result::Result<tonic::Response<super::MutationAdmission>, tonic::Status>;
    async fn wake(
        &self,
        request: tonic::Request<super::MachineMutationRequest>,
    ) -> std::result::Result<tonic::Response<super::MutationAdmission>, tonic::Status>;
    async fn set_suspension_policy(
        &self,
        request: tonic::Request<super::SetSuspensionPolicyRequest>,
    ) -> std::result::Result<tonic::Response<super::PolicyAdmission>, tonic::Status>;
    async fn destroy_machine(
        &self,
        request: tonic::Request<super::MachineMutationRequest>,
    ) -> std::result::Result<tonic::Response<super::MutationAdmission>, tonic::Status>;
    async fn destroy_checkpoint(
        &self,
        request: tonic::Request<super::CheckpointMutationRequest>,
    ) -> std::result::Result<tonic::Response<super::MutationAdmission>, tonic::Status>;
    async fn recover(
        &self,
        request: tonic::Request<super::RecoverRequest>,
    ) -> std::result::Result<tonic::Response<super::RecoveredAdmission>, tonic::Status>;
    async fn inspect_machine(
        &self,
        request: tonic::Request<super::InspectMachineRequest>,
    ) -> std::result::Result<tonic::Response<super::MachineState>, tonic::Status>;
    async fn inspect_checkpoint(
        &self,
        request: tonic::Request<super::InspectCheckpointRequest>,
    ) -> std::result::Result<tonic::Response<super::CheckpointState>, tonic::Status>;
    async fn list_machines(
        &self,
        request: tonic::Request<super::ListMachinesRequest>,
    ) -> std::result::Result<tonic::Response<super::MachinePage>, tonic::Status>;
    async fn events(
        &self,
        request: tonic::Request<super::EventsRequest>,
    ) -> std::result::Result<tonic::Response<super::EventPage>, tonic::Status>;
    async fn usage(
        &self,
        request: tonic::Request<super::UsageRequest>,
    ) -> std::result::Result<tonic::Response<super::UsageReceipt>, tonic::Status>;
    async fn cancel(
        &self,
        request: tonic::Request<super::OperationRequest>,
    ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
    async fn inspect_operation(
        &self,
        request: tonic::Request<super::OperationRequest>,
    ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
    /// Server streaming response type for the WatchOperation method.
    type WatchOperationStream: tonic::codegen::tokio_stream::Stream<
            Item = std::result::Result<super::OperationState, tonic::Status>,
        > + std::marker::Send
        + 'static;
    async fn watch_operation(
        &self,
        request: tonic::Request<super::OperationRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchOperationStream>, tonic::Status>;
}
#[derive(Debug)]
pub struct MachinesServiceServer<T> {
    inner: Arc<T>,
}
impl<T> MachinesServiceServer<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
    pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
    where
        F: tonic::service::Interceptor,
    {
        InterceptedService::new(Self::new(inner), interceptor)
    }
}
macro_rules! unary {
    ($server:expr, $request:expr, $method:ident, $input:ident, $output:ident) => {{
        struct Rpc<T: MachinesService>(Arc<T>);
        impl<T: MachinesService> tonic::server::UnaryService<super::$input> for Rpc<T> {
            type Response = super::$output;
            type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
            fn call(&mut self, request: tonic::Request<super::$input>) -> Self::Future {
                let inner = Arc::clone(&self.0);
                Box::pin(async move { inner.$method(request).await })
            }
        }
        let inner = Arc::clone(&$server.inner);
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::default();
            let mut grpc = tonic::server::Grpc::new(codec);
            Ok(grpc.unary(Rpc(inner), $request).await)
        })
    }};
}

impl<T, B> tonic::codegen::Service<http::Request<B>> for MachinesServiceServer<T>
where
    T: MachinesService,
    B: Body + std::marker::Send + 'static,
    B::Error: Into<StdError> + std::marker::Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/acyclic.machines.v1.MachinesService/QualifyImage" => unary!(
                self,
                req,
                qualify_image,
                QualifyImageRequest,
                ImageQualification
            ),
            "/acyclic.machines.v1.MachinesService/Create" => {
                unary!(self, req, create, CreateMachineRequest, MachineAdmission)
            }
            "/acyclic.machines.v1.MachinesService/Checkpoint" => unary!(
                self,
                req,
                checkpoint,
                CheckpointMachineRequest,
                CheckpointAdmission
            ),
            "/acyclic.machines.v1.MachinesService/Fork" => {
                unary!(self, req, fork, ForkCheckpointRequest, ForkAdmission)
            }
            "/acyclic.machines.v1.MachinesService/ForkMachine" => unary!(
                self,
                req,
                fork_machine,
                ForkMachineRequest,
                ForkMachineAdmission
            ),
            "/acyclic.machines.v1.MachinesService/Suspend" => unary!(
                self,
                req,
                suspend,
                MachineMutationRequest,
                MutationAdmission
            ),
            "/acyclic.machines.v1.MachinesService/Wake" => {
                unary!(self, req, wake, MachineMutationRequest, MutationAdmission)
            }
            "/acyclic.machines.v1.MachinesService/SetSuspensionPolicy" => unary!(
                self,
                req,
                set_suspension_policy,
                SetSuspensionPolicyRequest,
                PolicyAdmission
            ),
            "/acyclic.machines.v1.MachinesService/DestroyMachine" => unary!(
                self,
                req,
                destroy_machine,
                MachineMutationRequest,
                MutationAdmission
            ),
            "/acyclic.machines.v1.MachinesService/DestroyCheckpoint" => unary!(
                self,
                req,
                destroy_checkpoint,
                CheckpointMutationRequest,
                MutationAdmission
            ),
            "/acyclic.machines.v1.MachinesService/Recover" => {
                unary!(self, req, recover, RecoverRequest, RecoveredAdmission)
            }
            "/acyclic.machines.v1.MachinesService/InspectMachine" => unary!(
                self,
                req,
                inspect_machine,
                InspectMachineRequest,
                MachineState
            ),
            "/acyclic.machines.v1.MachinesService/InspectCheckpoint" => unary!(
                self,
                req,
                inspect_checkpoint,
                InspectCheckpointRequest,
                CheckpointState
            ),
            "/acyclic.machines.v1.MachinesService/ListMachines" => {
                unary!(self, req, list_machines, ListMachinesRequest, MachinePage)
            }
            "/acyclic.machines.v1.MachinesService/Events" => {
                unary!(self, req, events, EventsRequest, EventPage)
            }
            "/acyclic.machines.v1.MachinesService/Usage" => {
                unary!(self, req, usage, UsageRequest, UsageReceipt)
            }
            "/acyclic.machines.v1.MachinesService/Cancel" => {
                unary!(self, req, cancel, OperationRequest, OperationState)
            }
            "/acyclic.machines.v1.MachinesService/InspectOperation" => unary!(
                self,
                req,
                inspect_operation,
                OperationRequest,
                OperationState
            ),
            "/acyclic.machines.v1.MachinesService/WatchOperation" => {
                #[allow(non_camel_case_types)]
                struct WatchOperationSvc<T: MachinesService>(pub Arc<T>);
                impl<T: MachinesService>
                    tonic::server::ServerStreamingService<super::OperationRequest>
                    for WatchOperationSvc<T>
                {
                    type Response = super::OperationState;
                    type ResponseStream = T::WatchOperationStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<super::OperationRequest>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as MachinesService>::watch_operation(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let inner = self.inner.clone();
                let fut = async move {
                    let method = WatchOperationSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::Body::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}
impl<T> Clone for MachinesServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self { inner }
    }
}
impl<T> tonic::server::NamedService for MachinesServiceServer<T> {
    const NAME: &'static str = "acyclic.machines.v1.MachinesService";
}
