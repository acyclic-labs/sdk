//! Native JavaScript bridge over the Rust-owned Machines provider.
//!
//! The bridge deliberately uses JSON strings at its boundary. This keeps the generated
//! language facade independent of Rust's private handle types while leaving request validation,
//! idempotency, error classification, and streaming behavior in `acyclic-machines`.

use acyclic_machines::{
    CheckpointId, CreateMachine, IdempotencyKey, Image, MachineId, Machines, OperationId,
    ProviderError, SuspensionPolicy, Tls,
};
use futures::StreamExt as _;
use napi::{Error, Result, Status};
use napi_derive::napi;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::num::NonZeroU32;

/// Explicit mutual-TLS connection material for the native Machines service.
#[napi(object)]
pub struct NativeMachinesOptions {
    /// HTTPS endpoint of the Machines service.
    pub endpoint: String,
    /// PEM-encoded service CA certificate.
    pub ca_certificate: String,
    /// PEM-encoded client certificate.
    pub certificate: String,
    /// PEM-encoded client private key.
    pub private_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MachineKey {
    machine: MachineId,
    idempotency_key: IdempotencyKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CheckpointKey {
    checkpoint: CheckpointId,
    idempotency_key: IdempotencyKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForkRequest {
    checkpoint: CheckpointId,
    count: u32,
    idempotency_key: IdempotencyKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MachineForkRequest {
    machine: MachineId,
    count: u32,
    idempotency_key: IdempotencyKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PolicyRequest {
    machine: MachineId,
    policy: SuspensionPolicy,
    idempotency_key: IdempotencyKey,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListRequest {
    after: Option<MachineId>,
    limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListEventsRequest {
    machine: MachineId,
    after_sequence: Option<u64>,
    limit: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UsageRequest {
    machine: MachineId,
    start_unix_ms: u64,
    end_unix_ms: u64,
}

fn bridge_error(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn decode<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value).map_err(bridge_error)
}

fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(bridge_error)
}

fn provider_error(error: ProviderError) -> Error {
    bridge_error(error)
}

fn count(value: u32) -> Result<NonZeroU32> {
    NonZeroU32::new(value).ok_or_else(|| bridge_error("count must be greater than zero"))
}

/// Native Machines client whose every operation is dispatched by the canonical Rust provider.
#[napi]
pub struct MachinesNativeClient {
    inner: Machines,
}

impl MachinesNativeClient {
    fn from_machines(inner: Machines) -> Self {
        Self { inner }
    }

    async fn call<T, F>(&self, operation: F) -> Result<String>
    where
        T: Serialize,
        F: std::future::Future<Output = std::result::Result<T, ProviderError>>,
    {
        encode(&operation.await.map_err(provider_error)?)
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

    /// Qualifies one immutable image.
    #[napi(js_name = "qualifyImage")]
    pub async fn qualify_image(&self, request_json: String) -> Result<String> {
        let request: Image = decode(&request_json)?;
        self.call(self.inner.provider().qualify_image(request))
            .await
    }

    /// Creates one machine and returns the canonical mutation outcome.
    #[napi]
    pub async fn create(&self, request_json: String) -> Result<String> {
        let request: CreateMachine = decode(&request_json)?;
        self.call(self.inner.provider().create(request)).await
    }

    /// Inspects one machine.
    #[napi(js_name = "inspectMachine")]
    pub async fn inspect_machine(&self, machine_json: String) -> Result<String> {
        let machine: MachineId = decode(&machine_json)?;
        self.call(self.inner.provider().inspect_machine(machine))
            .await
    }

    /// Lists machines after an optional stable cursor.
    #[napi(js_name = "listMachines")]
    pub async fn list_machines(&self, request_json: String) -> Result<String> {
        let request: ListRequest = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .list_machines(request.after, request.limit),
        )
        .await
    }

    /// Reads one bounded event page for a machine.
    #[napi]
    pub async fn events(&self, request_json: String) -> Result<String> {
        let request: ListEventsRequest = decode(&request_json)?;
        self.call(self.inner.provider().events(
            request.machine,
            request.after_sequence,
            request.limit,
        ))
        .await
    }

    /// Reads one half-open usage interval for a machine.
    #[napi]
    pub async fn usage(&self, request_json: String) -> Result<String> {
        let request: UsageRequest = decode(&request_json)?;
        self.call(self.inner.provider().usage(
            request.machine,
            request.start_unix_ms,
            request.end_unix_ms,
        ))
        .await
    }

    /// Creates a checkpoint for one machine.
    #[napi]
    pub async fn checkpoint(&self, request_json: String) -> Result<String> {
        let request: MachineKey = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .checkpoint(request.machine, request.idempotency_key),
        )
        .await
    }

    /// Inspects one checkpoint.
    #[napi(js_name = "inspectCheckpoint")]
    pub async fn inspect_checkpoint(&self, checkpoint_json: String) -> Result<String> {
        let checkpoint: CheckpointId = decode(&checkpoint_json)?;
        self.call(self.inner.provider().inspect_checkpoint(checkpoint))
            .await
    }

    /// Forks one checkpoint into a bounded number of machines.
    #[napi]
    pub async fn fork(&self, request_json: String) -> Result<String> {
        let request: ForkRequest = decode(&request_json)?;
        self.call(self.inner.provider().fork(
            request.checkpoint,
            count(request.count)?,
            request.idempotency_key,
        ))
        .await
    }

    /// Forks one running machine without an intermediate checkpoint.
    #[napi(js_name = "forkMachine")]
    pub async fn fork_machine(&self, request_json: String) -> Result<String> {
        let request: MachineForkRequest = decode(&request_json)?;
        self.call(self.inner.provider().fork_machine(
            request.machine,
            count(request.count)?,
            request.idempotency_key,
        ))
        .await
    }

    /// Suspends one machine.
    #[napi]
    pub async fn suspend(&self, request_json: String) -> Result<String> {
        let request: MachineKey = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .suspend(request.machine, request.idempotency_key),
        )
        .await
    }

    /// Wakes one machine.
    #[napi]
    pub async fn wake(&self, request_json: String) -> Result<String> {
        let request: MachineKey = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .wake(request.machine, request.idempotency_key),
        )
        .await
    }

    /// Changes one machine's suspension policy.
    #[napi(js_name = "setSuspensionPolicy")]
    pub async fn set_suspension_policy(&self, request_json: String) -> Result<String> {
        let request: PolicyRequest = decode(&request_json)?;
        self.call(self.inner.provider().set_suspension_policy(
            request.machine,
            request.policy,
            request.idempotency_key,
        ))
        .await
    }

    /// Destroys one machine.
    #[napi(js_name = "destroyMachine")]
    pub async fn destroy_machine(&self, request_json: String) -> Result<String> {
        let request: MachineKey = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .destroy_machine(request.machine, request.idempotency_key),
        )
        .await
    }

    /// Destroys one checkpoint.
    #[napi(js_name = "destroyCheckpoint")]
    pub async fn destroy_checkpoint(&self, request_json: String) -> Result<String> {
        let request: CheckpointKey = decode(&request_json)?;
        self.call(
            self.inner
                .provider()
                .destroy_checkpoint(request.checkpoint, request.idempotency_key),
        )
        .await
    }

    /// Recovers the exact outcome admitted for one idempotency key.
    #[napi]
    pub async fn recover(&self, key_json: String) -> Result<String> {
        let key: IdempotencyKey = decode(&key_json)?;
        self.call(self.inner.provider().recover(key)).await
    }

    /// Inspects one machine.
    #[napi(js_name = "inspectOperation")]
    pub async fn inspect_operation(&self, operation_json: String) -> Result<String> {
        let operation: OperationId = decode(&operation_json)?;
        self.call(self.inner.provider().inspect_operation(operation))
            .await
    }

    /// Resolves the operation associated with one idempotency key.
    #[napi(js_name = "recoverOperation")]
    pub async fn recover_operation(&self, key_json: String) -> Result<String> {
        let key: IdempotencyKey = decode(&key_json)?;
        self.call(self.inner.provider().recover_operation(key))
            .await
    }

    /// Cancels one operation and returns its latest state.
    #[napi]
    pub async fn cancel(&self, operation_json: String) -> Result<String> {
        let operation: OperationId = decode(&operation_json)?;
        self.call(self.inner.provider().cancel(operation)).await
    }

    /// Watches one operation to completion and returns the ordered observations as JSON.
    ///
    /// The Rust provider remains responsible for stream lifetime, timeout, and error semantics;
    /// the native ABI collects the bounded operation stream for the JavaScript caller.
    #[napi(js_name = "watchOperation")]
    pub async fn watch_operation(&self, operation_json: String) -> Result<String> {
        let operation: OperationId = decode(&operation_json)?;
        let mut stream = self
            .inner
            .provider()
            .watch_operation(operation)
            .await
            .map_err(provider_error)?;
        let mut observations = Vec::new();
        while let Some(value) = stream.next().await {
            observations.push(value.map_err(provider_error)?);
        }
        encode(&observations)
    }

    /// Returns the provider assurance selected by the Rust backend.
    #[napi(js_name = "assurance")]
    pub fn assurance_json(&self) -> Result<String> {
        encode(&self.inner.assurance())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::{ImageQualification, SimulatedMachines};
    use std::sync::Arc;

    #[test]
    fn bridge_request_shapes_reject_zero_fork_counts() {
        assert!(count(0).is_err());
        let request: MachineForkRequest = decode(
            r#"{"machine":"00000000-0000-0000-0000-000000000001","count":1,"idempotencyKey":"00000000-0000-0000-0000-000000000002"}"#,
        )
        .expect("valid request shape");
        assert_eq!(request.count, 1);
    }

    #[tokio::test]
    async fn bridge_uses_canonical_provider_for_qualification() {
        let client = MachinesNativeClient::from_machines(Machines::new(Arc::new(
            SimulatedMachines::default(),
        )));
        let image = Image::custom([7; 32]).expect("nonzero image");
        let json = client
            .qualify_image(serde_json::to_string(&image).expect("image JSON"))
            .await
            .expect("qualification");
        let value: ImageQualification = serde_json::from_str(&json).expect("result JSON");
        assert_eq!(value.image, image);
        assert!(!value.capabilities.is_empty());
    }
}
