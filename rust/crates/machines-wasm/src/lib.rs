//! Thin WebAssembly boundary over the canonical Machines simulator.

#![cfg(target_arch = "wasm32")]

use acyclic_machines::{MAX_PAGE_SIZE, SimulatedMachines};
use sha2::{Digest as _, Sha256};
use tsify::Tsify;
use uuid::Uuid;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

mod public;
mod watch;
pub use watch::WasmOperationWatch;

/// Largest integer a JavaScript number represents exactly.
pub(crate) const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Builds a Machines error with a stable failure code.
pub(crate) fn js_error(code: &str, message: impl AsRef<str>) -> JsValue {
    let error = js_sys::Error::new(message.as_ref());
    error.set_name("MachinesError");
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

/// Normalize a caller identity to a retained UUID or a deterministic UUIDv5-like value.
/// Valid non-nil UUID strings are preserved byte-for-byte in canonical form.
#[wasm_bindgen]
#[allow(
    clippy::needless_pass_by_value,
    clippy::indexing_slicing,
    reason = "wasm-bindgen exports owned JavaScript strings and SHA-256 output is fixed-width"
)]
pub fn normalize_identity(kind: String, value: String) -> Result<String, JsValue> {
    if let Ok(parsed) = Uuid::parse_str(&value)
        && !parsed.is_nil()
    {
        return Ok(parsed.to_string());
    }
    if value.is_empty() {
        return Err(js_error("invalid", "identity value is required"));
    }
    let mut digest = Sha256::new();
    digest.update(b"acyclic-machines-identity-v1\0");
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(value.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    // SHA-256 always produces 32 bytes; the first half is the UUID material.
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(Uuid::from_bytes(bytes).to_string())
}

/// Validates the canonical Machines page-size policy before any provider is
/// called, including custom TypeScript providers.
#[wasm_bindgen(js_name = validatePageSize)]
pub fn validate_page_size(
    #[wasm_bindgen(unchecked_param_type = "number")] value: &JsValue,
) -> Result<(), JsValue> {
    let value = value
        .as_f64()
        .ok_or_else(|| js_error("invalid-page-size", "machine page limit must be 1..=256"))?;
    if !public::is_safe_integer(value) || value < 1.0 || value > f64::from(MAX_PAGE_SIZE) {
        return Err(js_error(
            "invalid-page-size",
            "machine page limit must be 1..=256",
        ));
    }
    Ok(())
}

/// Parses and normalizes an immutable OCI image reference using the canonical
/// Machines image constructor.
#[wasm_bindgen(js_name = managedOci, unchecked_return_type = "ImageOut")]
pub fn managed_oci(reference: String) -> Result<JsValue, JsValue> {
    public::managed_oci(reference)
}

impl Default for WasmSimulatedMachines {
    fn default() -> Self {
        Self::new()
    }
}

/// Stateful adapter around `acyclic_machines::SimulatedMachines`.
#[wasm_bindgen]
pub struct WasmSimulatedMachines {
    inner: SimulatedMachines,
}

#[wasm_bindgen]
#[allow(
    missing_docs,
    reason = "wasm-bindgen documents the generated JavaScript bindings"
)]
impl WasmSimulatedMachines {
    /// Creates an isolated process-local simulator.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            inner: SimulatedMachines::default(),
        }
    }

    /// Runs one public operation through its Rust-derived input and output DTO.
    ///
    /// The operation-specific methods below deliberately keep the route names
    /// out of TypeScript.  Each method's ABI is generated from its Tsify DTO,
    /// while the shared Rust route table remains the single behavior source.
    #[wasm_bindgen(js_name = qualifyImage)]
    pub async fn qualify_image(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ImageIn")] image: JsValue,
    ) -> Result<<public::QualificationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::QualificationOut>("qualifyImage", image)
            .await
    }

    #[wasm_bindgen]
    pub async fn create(
        &self,
        #[wasm_bindgen(unchecked_param_type = "CreateIn")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("create", request)
            .await
    }

    #[wasm_bindgen(js_name = inspectMachine)]
    pub async fn inspect_machine(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] machine_id: JsValue,
    ) -> Result<<public::ObservationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::ObservationOut>("inspectMachine", machine_id)
            .await
    }

    #[wasm_bindgen(js_name = listMachines)]
    pub async fn list_machines(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ListIn")] request: JsValue,
    ) -> Result<<public::PageOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::PageOut>("listMachines", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn checkpoint(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MachineKey")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("checkpoint", request)
            .await
    }

    #[wasm_bindgen(js_name = inspectCheckpoint)]
    pub async fn inspect_checkpoint(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] checkpoint_id: JsValue,
    ) -> Result<<public::CheckpointOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::CheckpointOut>("inspectCheckpoint", checkpoint_id)
            .await
    }

    #[wasm_bindgen]
    pub async fn fork(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ForkIn")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("fork", request)
            .await
    }

    #[wasm_bindgen(js_name = forkMachine)]
    pub async fn fork_machine(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MachineForkIn")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("forkMachine", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn suspend(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MachineKey")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("suspend", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn wake(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MachineKey")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("wake", request)
            .await
    }

    #[wasm_bindgen(js_name = setSuspensionPolicy)]
    pub async fn set_suspension_policy(
        &self,
        #[wasm_bindgen(unchecked_param_type = "PolicyIn")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("setSuspensionPolicy", request)
            .await
    }

    #[wasm_bindgen(js_name = destroyMachine)]
    pub async fn destroy_machine(
        &self,
        #[wasm_bindgen(unchecked_param_type = "MachineKey")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("destroyMachine", request)
            .await
    }

    #[wasm_bindgen(js_name = destroyCheckpoint)]
    pub async fn destroy_checkpoint(
        &self,
        #[wasm_bindgen(unchecked_param_type = "CheckpointKey")] request: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("destroyCheckpoint", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn events(
        &self,
        #[wasm_bindgen(unchecked_param_type = "EventsIn")] request: JsValue,
    ) -> Result<<public::EventsOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::EventsOut>("events", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn usage(
        &self,
        #[wasm_bindgen(unchecked_param_type = "UsageIn")] request: JsValue,
    ) -> Result<<public::UsageOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::UsageOut>("usage", request)
            .await
    }

    #[wasm_bindgen]
    pub async fn recover(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] idempotency_key: JsValue,
    ) -> Result<<public::MutationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::MutationOut>("recover", idempotency_key)
            .await
    }

    #[wasm_bindgen(js_name = recoverOperation)]
    pub async fn recover_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] idempotency_key: JsValue,
    ) -> Result<String, JsValue> {
        self.public_call_string("recoverOperation", idempotency_key)
            .await
    }

    #[wasm_bindgen(js_name = inspectOperation)]
    pub async fn inspect_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] operation_id: JsValue,
    ) -> Result<<public::OperationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::OperationOut>("inspectOperation", operation_id)
            .await
    }

    #[wasm_bindgen]
    pub async fn cancel(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] operation_id: JsValue,
    ) -> Result<<public::OperationOut as Tsify>::JsType, JsValue> {
        self.public_call_value::<public::OperationOut>("cancel", operation_id)
            .await
    }

    #[wasm_bindgen(js_name = watchOperation)]
    pub async fn watch_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] operation_id: JsValue,
    ) -> Result<WasmOperationWatch, JsValue> {
        watch::open(&self.inner, operation_id).await
    }

    async fn public_call_value<Output: Tsify>(
        &self,
        operation_name: &str,
        payload: JsValue,
    ) -> Result<Output::JsType, JsValue> {
        let value = public::dispatch(&self.inner, operation_name, payload).await?;
        Ok(value.unchecked_into())
    }

    async fn public_call_string(
        &self,
        operation_name: &str,
        payload: JsValue,
    ) -> Result<String, JsValue> {
        let value = public::dispatch(&self.inner, operation_name, payload).await?;
        value
            .as_string()
            .ok_or_else(|| js_error("invalid", "Machines operation returned a non-string result"))
    }
}
