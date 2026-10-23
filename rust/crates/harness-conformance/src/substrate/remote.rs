//! Optional HTTP bridge for the Rust hosting contract, not a HarnessService.
use super::*;
use acyclic_harness::package::Snapshot;
use serde::{Deserialize, Serialize};

/// POST one request to the endpoint. Response is serialized `Result<Value,String>`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// Install the exact pinned package.
    Install {
        /// Requested package and resource identity.
        manifest: Manifest,
    },
    /// Admit the initial checkpoint.
    Place {
        /// Explicit owning epoch.
        placement: Placement,
        /// Initial machine input.
        input: Value,
    },
    /// Admit one inbox event.
    Deliver {
        /// Expected owning epoch.
        placement: Placement,
        /// Immutable admitted event.
        event: Event,
    },
    /// Reconcile committed work and observe its checkpoint.
    Recover {
        /// Expected owning epoch.
        placement: Placement,
    },
    /// Fence and drain actor effects.
    Cancel {
        /// Expected owning epoch.
        placement: Placement,
        /// Cancellation admission identity.
        event: Event,
    },
    /// Publish immutable fixture content.
    Put {
        /// Stable publication identity.
        operation: OperationId,
        /// JSON payload.
        value: Value,
    },
    /// Load a pinned fs publication.
    Get {
        /// Complete immutable reference.
        file: FileRef,
    },
    /// Reconcile a publication by identity.
    Lookup {
        /// Stable publication identity.
        operation: OperationId,
    },
}
impl Request {
    /// Host bridge: dispatches only public Actors/fs operations. Installation
    /// verifies the supplied manifest against the locally pinned package code.
    pub async fn apply(self, runtime: &dyn ActorRuntime, volume: &dyn Volume) -> Result<Value> {
        match self {
            Self::Install { manifest } => {
                let package = HarnessPackage::new(manifest.bindings.clone());
                if package.manifest() != &manifest {
                    return Err(Error::Conflict("scenario package digest differs".into()));
                }
                runtime.install(Arc::new(package)).await?;
                Ok(Value::Null)
            }
            Self::Place { placement, input } => {
                serde_json::to_value(runtime.place(placement, input).await?).map_err(codec)
            }
            Self::Deliver { placement, event } => {
                serde_json::to_value(runtime.deliver(placement, event).await?).map_err(codec)
            }
            Self::Recover { placement } => {
                serde_json::to_value(runtime.recover(placement).await?).map_err(codec)
            }
            Self::Cancel { placement, event } => {
                serde_json::to_value(runtime.cancel(placement, event).await?).map_err(codec)
            }
            Self::Put { operation, value } => {
                serde_json::to_value(volume.put(operation, value).await?).map_err(codec)
            }
            Self::Get { file } => volume.get(file).await,
            Self::Lookup { operation } => {
                serde_json::to_value(volume.lookup(operation).await?).map_err(codec)
            }
        }
    }
}

#[derive(Clone)]
/// Client implementing both public runtime and fs bindings over HTTP.
pub struct Http {
    endpoint: String,
    client: reqwest::Client,
}
impl Http {
    /// Accepts HTTP(S) endpoints without ambient credentials or redirects.
    pub fn new(endpoint: String) -> TestResult<Self> {
        let url = reqwest::Url::parse(&endpoint)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err("endpoint requires an HTTP(S) URL without embedded credentials".into());
        }
        Ok(Self {
            endpoint,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
    async fn call<T: serde::de::DeserializeOwned>(&self, request: Request) -> Result<T> {
        let response = self
            .client
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .error_for_status()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let value = response
            .json::<std::result::Result<Value, String>>()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .map_err(Error::Storage)?;
        serde_json::from_value(value).map_err(codec)
    }
}
impl ActorRuntime for Http {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(self.call(Request::Install {
            manifest: package.manifest().clone(),
        }))
    }
    fn place(&self, placement: Placement, input: Value) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.call(Request::Place { placement, input }))
    }
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.call(Request::Deliver { placement, event }))
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.call(Request::Recover { placement }))
    }
    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(self.call(Request::Cancel { placement, event }))
    }
}
impl Volume for Http {
    fn put(&self, operation: OperationId, value: Value) -> BoxProviderFuture<'_, Result<FileRef>> {
        Box::pin(self.call(Request::Put { operation, value }))
    }
    fn get(&self, file: FileRef) -> BoxProviderFuture<'_, Result<Value>> {
        Box::pin(self.call(Request::Get { file }))
    }
    fn lookup(&self, operation: OperationId) -> BoxProviderFuture<'_, Result<Option<FileRef>>> {
        Box::pin(self.call(Request::Lookup { operation }))
    }
}
