//! Native JavaScript bridge over the Rust-owned Machines provider.
//!
//! The native ABI is intentionally a thin JSON envelope. Every request and
//! response is validated and projected by `sdk-machines-public`, the same
//! boundary used by the browser/WASM adapter.

#![allow(missing_docs)]

use acyclic_machines::{Machines, ProviderError, Tls};
use napi::{Error, Result, Status};
use napi_derive::napi;
use sdk_machines_public as public;

/// Explicit mutual-TLS connection material for the native Machines service.
#[napi(object)]
pub struct NativeMachinesOptions {
    /// HTTPS endpoint of the Machines service.
    pub endpoint: String,
    /// PEM-encoded service CA certificate.
    #[napi(js_name = "caCertificate")]
    pub ca_certificate: String,
    /// PEM-encoded client certificate.
    pub certificate: String,
    /// PEM-encoded client private key.
    #[napi(js_name = "privateKey")]
    pub private_key: String,
}

fn bridge_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn provider_error(error: ProviderError) -> Error {
    bridge_error(error)
}

/// Native Machines client whose every operation is dispatched by the canonical Rust public boundary.
#[napi]
pub struct MachinesNativeClient {
    inner: Machines,
}

impl MachinesNativeClient {
    fn from_machines(inner: Machines) -> Self {
        Self { inner }
    }

    async fn public(&self, operation: &str, payload: String) -> Result<String> {
        public::dispatch_json(self.inner.provider().as_ref(), operation, &payload)
            .await
            .map_err(bridge_error)
    }
}

#[napi]
impl MachinesNativeClient {
    /// Connects to an HTTPS Machines endpoint using the supplied mutual-TLS identity.
    #[napi(factory)]
    pub async fn connect(options: NativeMachinesOptions) -> Result<Self> {
        let client = Machines::connect(
            &options.endpoint,
            Tls {
                ca: options.ca_certificate.as_bytes(),
                certificate: options.certificate.as_bytes(),
                private_key: options.private_key.as_bytes(),
            },
        )
        .await
        .map_err(provider_error)?;
        Ok(Self::from_machines(client))
    }

    /// Connects using the canonical ACYCLIC_MACHINES_* environment configuration.
    #[napi(factory, js_name = "connectFromEnv")]
    pub async fn connect_from_env() -> Result<Self> {
        Ok(Self::from_machines(
            Machines::from_env().await.map_err(provider_error)?,
        ))
    }

    #[napi(js_name = "qualifyImage")]
    pub async fn qualify_image(&self, request_json: String) -> Result<String> {
        self.public("qualifyImage", request_json).await
    }
    #[napi]
    pub async fn create(&self, request_json: String) -> Result<String> {
        self.public("create", request_json).await
    }
    #[napi(js_name = "inspectMachine")]
    pub async fn inspect_machine(&self, request_json: String) -> Result<String> {
        self.public("inspectMachine", request_json).await
    }
    #[napi(js_name = "listMachines")]
    pub async fn list_machines(&self, request_json: String) -> Result<String> {
        self.public("listMachines", request_json).await
    }
    #[napi]
    pub async fn events(&self, request_json: String) -> Result<String> {
        self.public("events", request_json).await
    }
    #[napi]
    pub async fn usage(&self, request_json: String) -> Result<String> {
        self.public("usage", request_json).await
    }
    #[napi]
    pub async fn checkpoint(&self, request_json: String) -> Result<String> {
        self.public("checkpoint", request_json).await
    }
    #[napi(js_name = "inspectCheckpoint")]
    pub async fn inspect_checkpoint(&self, request_json: String) -> Result<String> {
        self.public("inspectCheckpoint", request_json).await
    }
    #[napi]
    pub async fn fork(&self, request_json: String) -> Result<String> {
        self.public("fork", request_json).await
    }
    #[napi(js_name = "forkMachine")]
    pub async fn fork_machine(&self, request_json: String) -> Result<String> {
        self.public("forkMachine", request_json).await
    }
    #[napi]
    pub async fn suspend(&self, request_json: String) -> Result<String> {
        self.public("suspend", request_json).await
    }
    #[napi]
    pub async fn wake(&self, request_json: String) -> Result<String> {
        self.public("wake", request_json).await
    }
    #[napi(js_name = "setSuspensionPolicy")]
    pub async fn set_suspension_policy(&self, request_json: String) -> Result<String> {
        self.public("setSuspensionPolicy", request_json).await
    }
    #[napi(js_name = "destroyMachine")]
    pub async fn destroy_machine(&self, request_json: String) -> Result<String> {
        self.public("destroyMachine", request_json).await
    }
    #[napi(js_name = "destroyCheckpoint")]
    pub async fn destroy_checkpoint(&self, request_json: String) -> Result<String> {
        self.public("destroyCheckpoint", request_json).await
    }
    #[napi]
    pub async fn recover(&self, request_json: String) -> Result<String> {
        self.public("recover", request_json).await
    }
    #[napi(js_name = "recoverOperation")]
    pub async fn recover_operation(&self, request_json: String) -> Result<String> {
        self.public("recoverOperation", request_json).await
    }
    #[napi(js_name = "inspectOperation")]
    pub async fn inspect_operation(&self, request_json: String) -> Result<String> {
        self.public("inspectOperation", request_json).await
    }
    #[napi]
    pub async fn cancel(&self, request_json: String) -> Result<String> {
        self.public("cancel", request_json).await
    }
    #[napi(js_name = "watchOperation")]
    pub async fn watch_operation(&self, request_json: String) -> Result<String> {
        self.public("watchOperation", request_json).await
    }
    #[napi(js_name = "assurance")]
    pub fn assurance_json(&self) -> Result<String> {
        serde_json::to_string(&self.inner.assurance()).map_err(bridge_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::{Machines, SimulatedMachines};
    use std::sync::Arc;

    #[tokio::test]
    async fn bridge_uses_shared_public_boundary_for_qualification() {
        let client = MachinesNativeClient::from_machines(Machines::new(Arc::new(
            SimulatedMachines::default(),
        )));
        let json = client
            .qualify_image(r#"{"kind":"custom","digestHex":"0101010101010101010101010101010101010101010101010101010101010101"}"#.into())
            .await
            .expect("qualification");
        assert!(json.contains("custom"));
    }

    #[tokio::test]
    async fn connection_rejects_non_https_before_network_io() {
        let options = NativeMachinesOptions {
            endpoint: "http://127.0.0.1:1".into(),
            ca_certificate: "fixture".into(),
            certificate: "fixture".into(),
            private_key: "fixture".into(),
        };
        assert!(MachinesNativeClient::connect(options).await.is_err());
    }
}
