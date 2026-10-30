//! Admission and ownership of frontend commands, using the public Rust codec.

#[cfg(test)]
mod tests;

use acyclic_harness_client::{
    AgentBinding, AgentControlFailure, AgentControls, AgentExchangeError, CommandError,
    MAX_AGENT_COMMAND_BYTES, MAX_AGENT_REPLY_BYTES, PreparedAgentInvocation, PreparedCommand,
    decode_agent_reply,
};
use js_sys::Uint8Array;
use std::rc::Rc;
use wasm_bindgen::prelude::*;

fn command_error(cause: CommandError) -> JsValue {
    let error = js_sys::Error::new(&cause.to_string());
    error.set_name(cause.code());
    error.into()
}

fn exchange_error(cause: AgentExchangeError) -> JsValue {
    match cause {
        AgentExchangeError::Session(cause) => {
            let error = js_sys::Error::new(&cause.to_string());
            error.set_name(cause.code());
            if let Err(cause) = js_sys::Reflect::set(
                error.as_ref(),
                &JsValue::from_str("source"),
                &JsValue::from_str("session"),
            ) {
                return cause;
            }
            error.into()
        }
        AgentExchangeError::Control(AgentControlFailure::Unknown(request)) => {
            super::agent_client::actor_error(crate::agent::Error::Unknown(request))
        }
        AgentExchangeError::ActorStatus(status) => {
            let error = js_sys::Error::new("actor invocation refused");
            error.set_name("actor_invocation_refused");
            if let Err(cause) = js_sys::Reflect::set(
                error.as_ref(),
                &JsValue::from_str("status"),
                &JsValue::from_f64(f64::from(status)),
            ) {
                return cause;
            }
            error.into()
        }
        cause => {
            let error = js_sys::Error::new(&cause.to_string());
            error.set_name(cause.code());
            error.into()
        }
    }
}

#[wasm_bindgen(typescript_custom_section)]
const SESSION_FAILURE: &str = r#"
/** No agent result was decoded. The prepared invocation remains caller-owned.
 * The name is the original canonical session error code, including admission
 * and authorization refusals; this does not assert mutation settlement. */
export interface WasmAgentSessionError extends Error {
    readonly source: 'session';
}
"#;

pub(super) fn binding(encoded: &Uint8Array) -> Result<AgentBinding, JsValue> {
    let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(encoded, |length| {
        if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
            Err(command_error(CommandError::Capacity))
        } else {
            Ok(())
        }
    })?;
    AgentBinding::new(&bytes).map_err(command_error)
}

/// Thin ownership of public Rust controls on the host's existing shared pool.
#[wasm_bindgen]
pub struct WasmAgentControls {
    controls: Rc<AgentControls>,
}

/// Discovered controls and the endpoint's canonical routing and tool resource.
#[wasm_bindgen]
pub struct WasmAgentDiscovery {
    controls: Rc<AgentControls>,
    tools: String,
}

#[wasm_bindgen]
impl WasmAgentDiscovery {
    #[wasm_bindgen(getter)]
    pub fn controls(&self) -> WasmAgentControls {
        WasmAgentControls {
            controls: self.controls.clone(),
        }
    }

    /// An owned copy suitable for the controls constructor or AgentClient.controls.
    #[wasm_bindgen(getter)]
    pub fn binding(&self) -> Vec<u8> {
        self.controls.binding_bytes()
    }

    /// The root's tool-output resource.
    #[wasm_bindgen(getter)]
    pub fn tools(&self) -> String {
        self.tools.clone()
    }
}

/// Discover controls on the stream's existing session and registered credential.
/// The endpoint verifies the credential; no account or binding is caller-supplied.
#[wasm_bindgen(js_name = discoverAgentControls)]
pub async fn discover_agent_controls(
    stream: &acyclic_stream::WasmStream,
    credential: u32,
) -> Result<WasmAgentDiscovery, JsValue> {
    let client = session_client(stream)?;
    let (controls, tools) = AgentControls::discover(client, credential)
        .await
        .map_err(exchange_error)?;
    Ok(WasmAgentDiscovery {
        controls: Rc::new(controls),
        tools,
    })
}

fn session_client(
    stream: &acyclic_stream::WasmStream,
) -> Result<acyclic_stream::session::Client, JsValue> {
    stream.session_client().ok_or_else(|| {
        let error = js_sys::Error::new("an authenticated session is required");
        error.set_name("client_no_session");
        JsValue::from(error)
    })
}

impl WasmAgentControls {
    pub(super) fn from_controls(controls: AgentControls) -> Self {
        Self {
            controls: Rc::new(controls),
        }
    }
}

#[wasm_bindgen]
impl WasmAgentControls {
    /// Host bootstrap supplies canonical routing and the registered credential;
    /// this constructor creates no connection or independent transport.
    #[wasm_bindgen(constructor)]
    pub fn new(
        stream: &acyclic_stream::WasmStream,
        encoded_binding: &Uint8Array,
        credential: u32,
    ) -> Result<Self, JsValue> {
        let binding = binding(encoded_binding)?;
        let client = session_client(stream)?;
        Ok(Self::from_controls(AgentControls::new(
            client, credential, binding,
        )))
    }

    /// Own the exact encoded command and invocation before any side effect.
    pub fn prepare(&self, command: &WasmAgentCommand) -> Result<WasmAgentInvocation, JsValue> {
        let invocation = self
            .controls
            .prepare(command.command.clone())
            .map_err(command_error)?;
        Ok(WasmAgentInvocation {
            controls: self.controls.clone(),
            invocation,
        })
    }

    /// Observe through the same pool and slot used for these controls.
    pub fn observe(
        &self,
        agent: js_sys::JsString,
        conversation_from: u64,
        status_from: u64,
    ) -> Result<super::agent_client::WasmAgentClient, JsValue> {
        let selection = acyclic_harness_client::AgentSelection {
            agent: super::agent_client::agent_path(agent)?,
            conversation_from,
            status_from,
        };
        let client = self
            .controls
            .observe(selection)
            .map_err(super::agent_client::client_error)?;
        Ok(super::agent_client::WasmAgentClient::from_client(client))
    }
}

/// Caller-owned invocation survives interrupted exchange and JS error handling.
#[wasm_bindgen]
pub struct WasmAgentInvocation {
    controls: Rc<AgentControls>,
    invocation: PreparedAgentInvocation<'static>,
}

#[wasm_bindgen]
impl WasmAgentInvocation {
    /// Original canonical Actors request, including the original command
    /// intent, copied once into a new JS array. A command staged in parts has
    /// no one request; only [`Self::execute`] sends it.
    pub fn bytes(&self) -> Result<Uint8Array, JsValue> {
        match self.invocation.invocations() {
            [invocation] => Ok(Uint8Array::from(&invocation[..])),
            _ => Err(command_error(CommandError::Capacity)),
        }
    }

    /// Execute once; no automatic intent replacement or invocation replay.
    #[wasm_bindgen(unchecked_return_type = "AgentReply")]
    pub async fn execute(&self) -> Result<JsValue, JsValue> {
        let reply = self
            .controls
            .execute(&self.invocation)
            .await
            .map_err(exchange_error)?;
        super::json::to_js_client_admitted(&reply)
    }
}

/// One immutable admitted command. The caller owns it until the exchange has a
/// definitive result; transport interruption never creates a replacement intent.
#[wasm_bindgen]
pub struct WasmAgentCommand {
    command: PreparedCommand,
}

#[wasm_bindgen]
impl WasmAgentCommand {
    /// Canonical pre-copy limits, shared with endpoint conformance drivers.
    #[wasm_bindgen(js_name = maximumCommandBytes)]
    pub fn maximum_command_bytes() -> u32 {
        MAX_AGENT_COMMAND_BYTES as u32
    }
    #[wasm_bindgen(js_name = maximumInvocationBytes)]
    pub fn maximum_invocation_bytes() -> u32 {
        acyclic_stream::MAX_COMMAND_BYTES as u32
    }
    #[wasm_bindgen(js_name = maximumReplyBytes)]
    pub fn maximum_reply_bytes() -> u32 {
        MAX_AGENT_REPLY_BYTES as u32
    }

    /// Admit an encoded command of any size: past one frame, the shared
    /// Rust codec stages it in parts.
    #[wasm_bindgen(constructor)]
    pub fn new(encoded: &Uint8Array) -> Result<WasmAgentCommand, JsValue> {
        let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(encoded, |_| Ok(()))?;
        let command = PreparedCommand::from_bytes(bytes).map_err(command_error)?;
        Ok(Self { command })
    }

    /// The whole command's encoding, copied once into a new JS array; callers
    /// cannot mutate the request retained in WASM.
    pub fn bytes(&self) -> Uint8Array {
        Uint8Array::from(self.command.command())
    }

    /// Decode through the canonical Rust schema with exact integer values.
    #[wasm_bindgen(unchecked_return_type = "AgentCommand")]
    pub fn value(&self) -> Result<JsValue, JsValue> {
        let value = self.command.decoded().map_err(command_error)?;
        super::json::to_js_client_admitted(&value)
    }

    /// Decode the host result against this exact original command. An uncertain
    /// result throws the shared typed error with its owned protobuf request.
    #[wasm_bindgen(unchecked_return_type = "AgentReply")]
    pub fn reply(&self, encoded: &Uint8Array) -> Result<JsValue, JsValue> {
        let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(encoded, |length| {
            if length > MAX_AGENT_REPLY_BYTES as u64 {
                Err(command_error(CommandError::Capacity))
            } else {
                Ok(())
            }
        })?;
        let command = self.command.decoded().map_err(command_error)?;
        match decode_agent_reply(&bytes, &command).map_err(command_error)? {
            Ok(reply) => super::json::to_js_client_admitted(&reply),
            Err(AgentControlFailure::Unknown(request)) => Err(super::agent_client::actor_error(
                crate::agent::Error::Unknown(request),
            )),
            Err(cause) => {
                let error = js_sys::Error::new(&cause.to_string());
                error.set_name(cause.code());
                Err(error.into())
            }
        }
    }
}
