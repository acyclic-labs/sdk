// Generated from the Rust-owned descriptor. Do not edit.
impl Client {
    /// Execute the canonical `PublishVersion` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn publish_version(&self, request: &crate::wire::PublishVersionRequest) -> Result<crate::wire::PublishVersionResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().publish_version(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.publish_version(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `SelectDeployment` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn select_deployment(&self, request: &crate::wire::SelectDeploymentRequest) -> Result<crate::wire::SelectDeploymentResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().select_deployment(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.select_deployment(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `SubmitJob` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn submit_job(&self, request: &crate::wire::SubmitJobRequest) -> Result<crate::wire::SubmitJobResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().submit_job(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.submit_job(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `InspectJob` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn inspect_job(&self, request: &crate::wire::InspectJobRequest) -> Result<crate::wire::InspectJobResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().inspect_job(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.inspect_job(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `CancelJob` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn cancel_job(&self, request: &crate::wire::CancelJobRequest) -> Result<crate::wire::CancelJobResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().cancel_job(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.cancel_job(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `InvokeVersion` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn invoke_version(&self, request: &crate::wire::InvokeVersionRequest) -> Result<crate::wire::InvokeResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().invoke_version(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.invoke_version(request).await.map_err(Error::from_http),
        }
    }
    /// Execute the canonical `InvokeDeployment` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn invoke_deployment(&self, request: &crate::wire::InvokeDeploymentRequest) -> Result<crate::wire::InvokeResponse, Error> {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(client) => client.clone().invoke_deployment(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc),
            Backend::Http(client) => client.invoke_deployment(request).await.map_err(Error::from_http),
        }
    }
}
