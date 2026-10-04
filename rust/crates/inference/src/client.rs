//! Platform-neutral Inference client facade.
//!
//! Native callers automatically prefer authenticated gRPC and fall back to
//! the descriptor-derived HTTP/JSON transport when gRPC is unavailable.
//! WebAssembly callers use HTTP/JSON through the browser fetch transport. The
//! operation surface and wire types stay identical on every platform.

use crate::wire;

/// Client setup, transport, or canonical service failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid endpoint, credential, request, or descriptor route.
    #[error("invalid Inference client configuration or request")]
    InvalidArgument,
    /// Transport setup or network failure.
    #[error("Inference transport failure: {0}")]
    Transport(String),
    /// The service rejected the operation.
    #[error("Inference service failure: {0}")]
    Service(String),
    /// A configured transport bound was exceeded.
    #[error("Inference response exceeds configured bound")]
    ResponseTooLarge,
    /// The service response did not match the Rust-owned descriptor.
    #[error("malformed Inference response")]
    MalformedResponse,
}

impl From<crate::http::Error> for Error {
    fn from(error: crate::http::Error) -> Self {
        match error {
            crate::http::Error::InvalidArgument => Self::InvalidArgument,
            crate::http::Error::Transport(error) => Self::Transport(error.to_string()),
            crate::http::Error::ResponseTooLarge => Self::ResponseTooLarge,
            crate::http::Error::MalformedResponse => Self::MalformedResponse,
            crate::http::Error::Service { status, detail } => Self::Service(
                detail
                    .map(|detail| format!("HTTP {status}: {detail:?}"))
                    .unwrap_or_else(|| format!("HTTP {status}")),
            ),
        }
    }
}

#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
impl From<crate::grpc::Error> for Error {
    fn from(error: crate::grpc::Error) -> Self {
        match error {
            crate::grpc::Error::Invalid => Self::InvalidArgument,
            crate::grpc::Error::Transport(error) => Self::Transport(error.to_string()),
            crate::grpc::Error::Status(error) => Self::Service(error.to_string()),
        }
    }
}

enum Backend {
    #[cfg(all(feature = "host", not(target_arch = "wasm32")))]
    Grpc(crate::grpc::Client),
    Http(crate::http::Client),
}

/// The transport selected by the Rust-owned capability policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transport {
    /// Authenticated native HTTP/2 gRPC.
    Grpc,
    /// Descriptor-derived HTTP/JSON, including browser fetch.
    Http,
}

/// Uniform typed Inference client for native and browser consumers.
pub struct Client {
    backend: Backend,
}

impl Client {
    /// Connect using the best available transport for the current platform.
    ///
    /// Native callers try mTLS gRPC first and automatically use HTTP/JSON if
    /// the gRPC channel cannot be established. Browser callers use HTTP/JSON;
    /// browser trust configuration remains owned by the browser.
    pub async fn connect(endpoint: &str, token: &str, ca_pem: &[u8]) -> Result<Self, Error> {
        Self::connect_with_limit(endpoint, token, ca_pem, crate::MAXIMUM_HTTP_JSON_BYTES).await
    }

    /// Connect with an explicit bounded HTTP response size.
    pub async fn connect_with_limit(
        endpoint: &str,
        token: &str,
        ca_pem: &[u8],
        maximum_response_bytes: usize,
    ) -> Result<Self, Error> {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
        {
            if endpoint.starts_with("https://") {
                if let Ok(client) = crate::grpc::Client::connect(endpoint, token, ca_pem).await {
                    return Ok(Self {
                        backend: Backend::Grpc(client),
                    });
                }
            }
            let client = crate::http::Client::new_with_ca(
                endpoint,
                token,
                maximum_response_bytes,
                (!ca_pem.is_empty()).then_some(ca_pem),
            )?;
            return Ok(Self {
                backend: Backend::Http(client),
            });
        }

        #[cfg(target_arch = "wasm32")]
        {
            let _ = ca_pem;
            let client = crate::http::Client::new(endpoint, token, maximum_response_bytes)?;
            Ok(Self {
                backend: Backend::Http(client),
            })
        }

        #[cfg(all(not(feature = "host"), not(target_arch = "wasm32")))]
        {
            let client = crate::http::Client::new_with_ca(
                endpoint,
                token,
                maximum_response_bytes,
                (!ca_pem.is_empty()).then_some(ca_pem),
            )?;
            Ok(Self {
                backend: Backend::Http(client),
            })
        }
    }

    /// Return the transport selected during connection setup.
    #[must_use]
    pub fn transport(&self) -> Transport {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(_) => Transport::Grpc,
            Backend::Http(_) => Transport::Http,
        }
    }

    /// Models/List.
    pub async fn list(&self, request: &wire::ListModelsRequest) -> Result<wire::ListModelsResponse, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.list(request).await?),
            Backend::Http(client) => Ok(client.list(request).await?),
        }
    }

    /// Contexts/Create.
    pub async fn create_context(&self, request: &wire::CreateContextRequest) -> Result<wire::MutationReceipt, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.create_context(request).await?),
            Backend::Http(client) => Ok(client.create_context(request).await?),
        }
    }

    /// Contexts/Inspect.
    pub async fn inspect_context(&self, request: &wire::InspectContextRequest) -> Result<wire::ContextView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.inspect_context(request).await?),
            Backend::Http(client) => Ok(client.inspect_context(request).await?),
        }
    }

    /// Contexts/Mutate.
    pub async fn mutate_context(&self, request: &wire::MutateContextRequest) -> Result<wire::MutationReceipt, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.mutate_context(request).await?),
            Backend::Http(client) => Ok(client.mutate_context(request).await?),
        }
    }

    /// WarmContexts/Retain.
    pub async fn retain_warm(&self, request: &wire::RetainWarmRequest) -> Result<wire::WarmView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.retain_warm(request).await?),
            Backend::Http(client) => Ok(client.retain_warm(request).await?),
        }
    }

    /// WarmContexts/Inspect.
    pub async fn inspect_warm(&self, request: &wire::InspectWarmRequest) -> Result<wire::WarmView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.inspect_warm(request).await?),
            Backend::Http(client) => Ok(client.inspect_warm(request).await?),
        }
    }

    /// WarmContexts/Renew.
    pub async fn renew_warm(&self, request: &wire::RenewWarmRequest) -> Result<wire::WarmView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.renew_warm(request).await?),
            Backend::Http(client) => Ok(client.renew_warm(request).await?),
        }
    }

    /// WarmContexts/Release.
    pub async fn release_warm(&self, request: &wire::ReleaseWarmRequest) -> Result<wire::WarmView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.release_warm(request).await?),
            Backend::Http(client) => Ok(client.release_warm(request).await?),
        }
    }

    /// Runs/Generate.
    pub async fn generate_run(&self, request: &wire::GenerateRunRequest) -> Result<wire::GenerateRunResponse, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.generate_run(request).await?),
            Backend::Http(client) => Ok(client.generate_run(request).await?),
        }
    }

    /// Runs/Inspect.
    pub async fn inspect_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.inspect_run(request).await?),
            Backend::Http(client) => Ok(client.inspect_run(request).await?),
        }
    }

    /// Runs/Watch, preserving the ordered event sequence.
    pub async fn watch_run(&self, request: &wire::WatchRunRequest) -> Result<Vec<wire::RunEvent>, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.watch_run(request).await?),
            Backend::Http(client) => Ok(client.watch_run(request).await?),
        }
    }

    /// Runs/Cancel.
    pub async fn cancel_run(&self, request: &wire::InspectRunRequest) -> Result<wire::RunView, Error> {
        match &self.backend {
#[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.cancel_run(request).await?),
            Backend::Http(client) => Ok(client.cancel_run(request).await?),
        }
    }

    /// Evaluations/Create.
    pub async fn create_evaluation(&self, request: &wire::CreateEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        match &self.backend {
            #[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.create_evaluation(request).await?),
            Backend::Http(client) => Ok(client.create_evaluation(request).await?),
        }
    }

    /// Evaluations/Inspect.
    pub async fn inspect_evaluation(&self, request: &wire::InspectEvaluationRequest) -> Result<wire::EvaluationView, Error> {
        match &self.backend {
            #[cfg(all(feature = "host", not(target_arch = "wasm32")))]
            Backend::Grpc(client) => Ok(client.inspect_evaluation(request).await?),
            Backend::Http(client) => Ok(client.inspect_evaluation(request).await?),
        }
    }
}
