//! Thin WebAssembly boundary over the canonical Machines simulator.
//!
//! Requests use the serde representation of `acyclic-machines`.  The only
//! convenience added here is identity normalization: callers may use labels,
//! which are deterministically mapped to non-nil UUIDs before entering Rust.

#![cfg(target_arch = "wasm32")]

use acyclic_machines::SimulatedMachines;
use sha2::{Digest as _, Sha256};
use tsify_next::Tsify;
use uuid::Uuid;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

mod http;
mod public;

// Keep the hosted transport route/output relationship in one Rust declaration.
// The macro emits the runtime route table and the TypeScript declarations from
// the same entries, so a route cannot silently drift between those boundaries.
macro_rules! define_http_routes {
    ($( $name:ident => $route:literal => $request:literal => $response:literal ),+ $(,)?) => {
        pub(crate) mod http_route {
            use super::JsValue;

            $(pub const $name: &str = $route;)+

            pub const ALL: &[&str] = &[$($name),+];

            pub fn canonical(value: &str) -> Result<&'static str, JsValue> {
                ALL.iter()
                    .copied()
                    .find(|candidate| *candidate == value)
                    .ok_or_else(|| JsValue::from_str("unknown Machines HTTP route"))
            }
        }

        /// Returns the hosted route values emitted from the Rust route contract.
        #[wasm_bindgen(
            js_name = httpRoutes,
            unchecked_return_type = "MachinesHttpRoutes"
        )]
        pub fn http_routes() -> JsValue {
            let routes = js_sys::Object::new();
            $(js_sys::Reflect::set(
                &routes,
                &JsValue::from_str(stringify!($name)),
                &JsValue::from_str($route),
            ).expect("static HTTP route names are valid object keys");)+
            routes.into()
        }

        #[wasm_bindgen(typescript_custom_section)]
        const HTTP_ROUTE_CONTRACT: &'static str = concat!(
            "export type MachinesHttpRoute =\n",
            $("    | ", stringify!($route), "\n",)+
            ";\n\n",
            "export interface MachinesHttpRoutes {\n",
            $("    readonly ", stringify!($name), ": ", stringify!($route), ";\n",)+
            "}\n\n",
            "export function httpRoutes(): MachinesHttpRoutes;\n",
            "export function httpRoute(route: MachinesHttpRoute): MachinesHttpRoute;\n\n",
            "export interface MachinesHttpRequestMap {\n",
            $("    ", stringify!($route), ": ", $request, ";\n",)+
            "}\n\n",
            "export type MachinesHttpRequest<Route extends MachinesHttpRoute> =\n",
            "    MachinesHttpRequestMap[Route];\n",
            "export type MachinesHttpRequestUnion =\n",
            "    MachinesHttpRequestMap[MachinesHttpRoute];\n\n",
            "export interface MachinesHttpResponseMap {\n",
            $("    ", stringify!($route), ": ", $response, ";\n",)+
            "}\n\n",
            "export type MachinesHttpResponse<Route extends MachinesHttpRoute> =\n",
            "    MachinesHttpResponseMap[Route];\n",
            "export type MachinesHttpResponseUnion =\n",
            "    MachinesHttpResponseMap[MachinesHttpRoute];\n",
        );
    };
}

define_http_routes!(
    IMAGES_QUALIFY => "images/qualify" => "{ readonly image: ImageIn }" => "QualificationOut",
    MACHINES_CREATE => "machines/create" => "CreateIn" => "MutationOut",
    MACHINES_INSPECT => "machines/inspect" => "{ readonly machineId: string }" => "ObservationOut",
    MACHINES_LIST => "machines/list" => "ListIn" => "PageOut",
    CHECKPOINTS_INSPECT => "checkpoints/inspect" => "{ readonly checkpointId: string }" => "CheckpointOut",
    MACHINES_CHECKPOINT => "machines/checkpoint" => "MachineKey" => "MutationOut",
    MACHINES_FORK => "machines/fork" => "{ readonly machineId: string; readonly count: number; readonly idempotencyKey: string }" => "MutationOut",
    CHECKPOINTS_FORK => "checkpoints/fork" => "ForkIn" => "MutationOut",
    MACHINES_SUSPEND => "machines/suspend" => "MachineKey" => "MutationOut",
    MACHINES_WAKE => "machines/wake" => "MachineKey" => "MutationOut",
    MACHINES_SUSPENSION_POLICY => "machines/suspension-policy" => "PolicyIn" => "MutationOut",
    MACHINES_DESTROY => "machines/destroy" => "MachineKey" => "MutationOut",
    CHECKPOINTS_DESTROY => "checkpoints/destroy" => "CheckpointKey" => "MutationOut",
    MACHINES_EVENTS => "machines/events" => "EventsIn" => "EventsOut",
    MACHINES_USAGE => "machines/usage" => "UsageIn" => "UsageOut",
    OPERATIONS_RECOVER => "operations/recover" => "{ readonly idempotencyKey: string }" => "MutationOut",
    OPERATIONS_RECOVER_ID => "operations/recover-id" => "{ readonly idempotencyKey: string }" => "string",
    OPERATIONS_INSPECT => "operations/inspect" => "{ readonly operationId: string }" => "OperationOut",
    OPERATIONS_CANCEL => "operations/cancel" => "{ readonly operationId: string }" => "OperationOut",
    OPERATIONS_WATCH => "operations/watch" => "{ readonly operationId: string }" => "readonly OperationOut[]",
);

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
        return Err(JsValue::from_str("identity value is required"));
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

/// Canonicalizes a hosted HTTP route through the Rust-owned route table before
/// a client uses it to construct a request URL.
#[wasm_bindgen(js_name = httpRoute, unchecked_return_type = "MachinesHttpRoute")]
pub fn http_route(
    #[wasm_bindgen(unchecked_param_type = "MachinesHttpRoute")] route: String,
) -> Result<String, JsValue> {
    Ok(http_route::canonical(&route)?.to_owned())
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
#[allow(missing_docs)]
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

    #[wasm_bindgen(js_name = watchOperation, unchecked_return_type = "readonly OperationOut[]")]
    pub async fn watch_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "string")] operation_id: JsValue,
    ) -> Result<JsValue, JsValue> {
        self.public_call_raw("watchOperation", operation_id).await
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
            .ok_or_else(|| JsValue::from_str("Machines operation returned a non-string result"))
    }

    async fn public_call_raw(
        &self,
        operation_name: &str,
        payload: JsValue,
    ) -> Result<JsValue, JsValue> {
        public::dispatch(&self.inner, operation_name, payload).await
    }

    /// Validates and projects a hosted HTTP response against its request context.
    #[wasm_bindgen]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "wasm-bindgen exports owned JavaScript strings"
    )]
    pub fn validate_http_response(
        #[wasm_bindgen(unchecked_param_type = "MachinesHttpRoute")] route: String,
        response_json: String,
        expected_json: String,
    ) -> Result<(), JsValue> {
        http::validate(&route, &response_json, &expected_json)
    }

    /// Decodes a hosted HTTP response using the Rust-owned scalar wrappers and
    /// the same public DTO shape as simulator methods.
    #[wasm_bindgen(
        js_name = decodeHttpResponse,
        unchecked_return_type = "MachinesHttpResponseUnion"
    )]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "wasm-bindgen exports owned JavaScript strings"
    )]
    pub fn decode_http_response(
        #[wasm_bindgen(unchecked_param_type = "MachinesHttpRoute")] route: String,
        response_json: String,
        expected_json: String,
    ) -> Result<JsValue, JsValue> {
        http::decode(&route, &response_json, &expected_json)
    }

    /// Encodes a natural hosted request using Rust-owned bigint and bytes
    /// wrappers before it crosses the HTTP boundary.
    #[wasm_bindgen(js_name = encodeHttpRequest)]
    pub fn encode_http_request(
        #[wasm_bindgen(unchecked_param_type = "MachinesHttpRequestUnion")] request: JsValue,
    ) -> Result<String, JsValue> {
        http::encode_request(&request)
    }
}
