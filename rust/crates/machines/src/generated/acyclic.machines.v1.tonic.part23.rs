/// Generated server implementations.
pub mod machines_service_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MachinesServiceServer.
    #[async_trait]
    pub trait MachinesService: std::marker::Send + std::marker::Sync + 'static {
        /// Server streaming response type for the Exec method.
        type ExecStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::ExecOutput, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        ///
        async fn exec(
            &self,
            request: tonic::Request<super::ExecRequest>,
        ) -> std::result::Result<tonic::Response<Self::ExecStream>, tonic::Status>;
        ///
        async fn read_output(
            &self,
            request: tonic::Request<super::ReadOutputRequest>,
        ) -> std::result::Result<tonic::Response<super::MachineOutput>, tonic::Status>;
        ///
        async fn qualify_image(
            &self,
            request: tonic::Request<super::QualifyImageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ImageQualification>,
            tonic::Status,
        >;
        ///
        async fn create(
            &self,
            request: tonic::Request<super::CreateMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MachineAdmission>,
            tonic::Status,
        >;
        ///
        async fn checkpoint(
            &self,
            request: tonic::Request<super::CheckpointMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CheckpointAdmission>,
            tonic::Status,
        >;
        ///
        async fn fork(
            &self,
            request: tonic::Request<super::ForkCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::ForkAdmission>, tonic::Status>;
        ///
        async fn fork_machine(
            &self,
            request: tonic::Request<super::ForkMachineRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ForkMachineAdmission>,
            tonic::Status,
        >;
        ///
        async fn suspend(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn wake(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn set_suspension_policy(
            &self,
            request: tonic::Request<super::SetSuspensionPolicyRequest>,
        ) -> std::result::Result<tonic::Response<super::PolicyAdmission>, tonic::Status>;
        ///
        async fn destroy_machine(
            &self,
            request: tonic::Request<super::MachineMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn destroy_checkpoint(
            &self,
            request: tonic::Request<super::CheckpointMutationRequest>,
        ) -> std::result::Result<
            tonic::Response<super::MutationAdmission>,
            tonic::Status,
        >;
        ///
        async fn recover(
            &self,
            request: tonic::Request<super::RecoverRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RecoveredAdmission>,
            tonic::Status,
        >;
        ///
        async fn inspect_machine(
            &self,
            request: tonic::Request<super::InspectMachineRequest>,
        ) -> std::result::Result<tonic::Response<super::MachineState>, tonic::Status>;
        ///
        async fn inspect_checkpoint(
            &self,
            request: tonic::Request<super::InspectCheckpointRequest>,
        ) -> std::result::Result<tonic::Response<super::CheckpointState>, tonic::Status>;
        ///
        async fn list_machines(
            &self,
            request: tonic::Request<super::ListMachinesRequest>,
        ) -> std::result::Result<tonic::Response<super::MachinePage>, tonic::Status>;
        ///
        async fn events(
            &self,
            request: tonic::Request<super::EventsRequest>,
        ) -> std::result::Result<tonic::Response<super::EventPage>, tonic::Status>;
        ///
        async fn usage(
            &self,
            request: tonic::Request<super::UsageRequest>,
        ) -> std::result::Result<tonic::Response<super::UsageReceipt>, tonic::Status>;
        ///
        async fn cancel(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
        ///
        async fn inspect_operation(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<tonic::Response<super::OperationState>, tonic::Status>;
        /// Server streaming response type for the WatchOperation method.
        type WatchOperationStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::OperationState, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        ///
        async fn watch_operation(
            &self,
            request: tonic::Request<super::OperationRequest>,
        ) -> std::result::Result<
            tonic::Response<Self::WatchOperationStream>,
            tonic::Status,
        >;
    }
    ///
    #[derive(Debug)]
    pub struct MachinesServiceServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> MachinesServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
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
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/acyclic.machines.v1.MachinesService/Exec" => {
                    include!("acyclic.machines.v1.tonic.part0.rs")
                }
                "/acyclic.machines.v1.MachinesService/ReadOutput" => {
                    include!("acyclic.machines.v1.tonic.part1.rs")
                }
                "/acyclic.machines.v1.MachinesService/QualifyImage" => {
                    include!("acyclic.machines.v1.tonic.part2.rs")
                }
                "/acyclic.machines.v1.MachinesService/Create" => {
                    include!("acyclic.machines.v1.tonic.part3.rs")
                }
                "/acyclic.machines.v1.MachinesService/Checkpoint" => {
                    include!("acyclic.machines.v1.tonic.part4.rs")
                }
                "/acyclic.machines.v1.MachinesService/Fork" => {
                    include!("acyclic.machines.v1.tonic.part5.rs")
                }
                "/acyclic.machines.v1.MachinesService/ForkMachine" => {
                    include!("acyclic.machines.v1.tonic.part6.rs")
                }
                "/acyclic.machines.v1.MachinesService/Suspend" => {
                    include!("acyclic.machines.v1.tonic.part7.rs")
                }
                "/acyclic.machines.v1.MachinesService/Wake" => {
                    include!("acyclic.machines.v1.tonic.part8.rs")
                }
                "/acyclic.machines.v1.MachinesService/SetSuspensionPolicy" => {
                    include!("acyclic.machines.v1.tonic.part9.rs")
                }
                "/acyclic.machines.v1.MachinesService/DestroyMachine" => {
                    include!("acyclic.machines.v1.tonic.part10.rs")
                }
                "/acyclic.machines.v1.MachinesService/DestroyCheckpoint" => {
                    include!("acyclic.machines.v1.tonic.part11.rs")
                }
                "/acyclic.machines.v1.MachinesService/Recover" => {
                    include!("acyclic.machines.v1.tonic.part12.rs")
                }
                "/acyclic.machines.v1.MachinesService/InspectMachine" => {
                    include!("acyclic.machines.v1.tonic.part13.rs")
                }
                "/acyclic.machines.v1.MachinesService/InspectCheckpoint" => {
                    include!("acyclic.machines.v1.tonic.part14.rs")
                }
                "/acyclic.machines.v1.MachinesService/ListMachines" => {
                    include!("acyclic.machines.v1.tonic.part15.rs")
                }
                "/acyclic.machines.v1.MachinesService/Events" => {
                    include!("acyclic.machines.v1.tonic.part16.rs")
                }
                "/acyclic.machines.v1.MachinesService/Usage" => {
                    include!("acyclic.machines.v1.tonic.part17.rs")
                }
                "/acyclic.machines.v1.MachinesService/Cancel" => {
                    include!("acyclic.machines.v1.tonic.part18.rs")
                }
                "/acyclic.machines.v1.MachinesService/InspectOperation" => {
                    include!("acyclic.machines.v1.tonic.part19.rs")
                }
                "/acyclic.machines.v1.MachinesService/WatchOperation" => {
                    include!("acyclic.machines.v1.tonic.part20.rs")
                }
                _ => include!("acyclic.machines.v1.tonic.part21.rs"),
            }
        }
    }
    impl<T> Clone for MachinesServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "acyclic.machines.v1.MachinesService";
    impl<T> tonic::server::NamedService for MachinesServiceServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
