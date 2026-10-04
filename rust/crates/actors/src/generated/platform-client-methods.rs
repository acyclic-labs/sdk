// Generated from the Rust-owned descriptor. Do not edit.
impl Client {
    /// Execute the canonical `CreateActor` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn create_actor(&self, request: &crate::wire::CreateActorRequest) -> Result<crate::wire::CreateActorResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().create_actor(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.create_actor(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `UpdateActor` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn update_actor(&self, request: &crate::wire::UpdateActorRequest) -> Result<crate::wire::UpdateActorResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().update_actor(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.update_actor(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `InspectActor` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn inspect_actor(&self, request: &crate::wire::InspectActorRequest) -> Result<crate::wire::InspectActorResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().inspect_actor(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.inspect_actor(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `AddSubscription` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn add_subscription(&self, request: &crate::wire::AddSubscriptionRequest) -> Result<crate::wire::AddSubscriptionResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().add_subscription(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.add_subscription(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `RemoveSubscription` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn remove_subscription(&self, request: &crate::wire::RemoveSubscriptionRequest) -> Result<crate::wire::RemoveSubscriptionResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().remove_subscription(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.remove_subscription(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `ResumeSubscription` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn resume_subscription(&self, request: &crate::wire::ResumeSubscriptionRequest) -> Result<crate::wire::ResumeSubscriptionResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().resume_subscription(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.resume_subscription(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `CheckpointActor` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn checkpoint_actor(&self, request: &crate::wire::CheckpointActorRequest) -> Result<crate::wire::CheckpointActorResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().checkpoint_actor(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.checkpoint_actor(request).await.map_err(Error::from_http)
        }
    }
    /// Execute the canonical `InvokeActor` operation using the platform default transport.
    ///
    /// # Errors
    /// Returns a transport or canonical service error.
    pub async fn invoke_actor(&self, request: &crate::wire::InvokeActorRequest) -> Result<crate::wire::InvokeActorResponse, Error> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.inner.clone().invoke_actor(request.clone()).await.map(tonic::Response::into_inner).map_err(Error::from_grpc)
        }
        #[cfg(target_arch = "wasm32")]
        {
            self.inner.invoke_actor(request).await.map_err(Error::from_http)
        }
    }
}
