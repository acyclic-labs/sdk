//! Typed agent controls over the same canonical Actors session as observation.
use crate::{
    AgentCommand, AgentControlFailure, AgentReply, CommandError, PreparedCommand,
    decode_agent_reply, encode_agent_reply,
};
use acyclic_actors::{domain, wire};
use acyclic_stream::{StreamError, session::Client};
use bytes::Bytes;
use prost::Message as _;
use std::borrow::Cow;

/// The actor every [`AgentBinding::discovery`] invocation names.
const DISCOVERY_ACTOR: &str = "agents";
/// The routing URL of a discovery invocation.
const DISCOVERY_URL: &str = "/agents/bind";

/// Pure protocol admission used by both the client and the owning host. The
/// authenticated session supplies authority separately; no field here grants it.
fn decode_invocation(encoded: &[u8]) -> Result<wire::InvokeActorRequest, CommandError> {
    if encoded.len() > acyclic_stream::MAX_COMMAND_BYTES {
        return Err(CommandError::Capacity);
    }
    let request = wire::InvokeActorRequest::decode(encoded).map_err(|_| CommandError::Invalid)?;
    if request.encode_to_vec().as_slice() != encoded {
        return Err(CommandError::Invalid);
    }
    let request =
        domain::InvokeActorRequest::try_from(request).map_err(|_| CommandError::Invalid)?;
    Ok(wire::InvokeActorRequest::from(request))
}

/// The owning host's pure decoder for the same public invocation sent by the
/// client. Construct this before provider I/O, then pass the actual session's
/// CallAuthorization through the host's existing admission and execution path.
/// This value asserts no identity, performs no dispatch and owns no retry loop.
pub struct AgentCall {
    invocation: wire::InvokeActorRequest,
    command: AgentCommand,
}

impl AgentCall {
    /// Bound and validate the complete canonical Actors envelope and its agent
    /// command before any host work. Unknown fields cannot disappear in transit.
    pub fn decode(encoded: &[u8]) -> Result<Self, CommandError> {
        let invocation = decode_invocation(encoded)?;
        let command = PreparedCommand::decode(&invocation.body)?;
        Ok(Self {
            invocation,
            command,
        })
    }

    /// Canonical routing and original command bytes, never authenticated scope.
    pub fn invocation(&self) -> &wire::InvokeActorRequest {
        &self.invocation
    }

    /// The admitted public command, shared by every backend's dispatcher.
    pub fn command(&self) -> &AgentCommand {
        &self.command
    }

    /// Encode a successful exchange, including typed refusals and Unknown, in
    /// the canonical Actors response. Borrowing leaves the exact uncertain
    /// request with its host owner even if validation or output capacity fails.
    pub fn reply(
        &self,
        result: &Result<AgentReply, acyclic_harness_core::agent::Error>,
    ) -> Result<Bytes, CommandError> {
        let response = wire::InvokeActorResponse {
            status: 200,
            headers: Vec::new(),
            body: encode_agent_reply(result, &self.command)?,
        };
        if response.encoded_len() > acyclic_stream::wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES
        {
            return Err(CommandError::Capacity);
        }
        let response =
            domain::InvokeActorResponse::try_from(response).map_err(|_| CommandError::Invalid)?;
        Ok(wire::InvokeActorResponse::from(response)
            .encode_to_vec()
            .into())
    }
}

/// A host-published canonical Actors address and dispatch metadata. It contains
/// no command body or reusable per-invocation ordering sequence. Authentication
/// remains the session credential, not this routing value.
#[derive(Clone)]
pub struct AgentBinding(wire::InvokeActorRequest);

impl AgentBinding {
    /// The routing every endpoint answers [`AgentCommand::Bind`] on: no
    /// account, namespace or tree, only the session's credential.
    #[must_use]
    pub fn discovery() -> Self {
        Self(wire::InvokeActorRequest {
            actor_id: DISCOVERY_ACTOR.into(),
            method: "POST".into(),
            url: DISCOVERY_URL.into(),
            ..Default::default()
        })
    }

    /// Whether `invocation` is routed as [`Self::discovery`].
    #[must_use]
    pub fn is_discovery(invocation: &wire::InvokeActorRequest) -> bool {
        invocation.actor_id == DISCOVERY_ACTOR
            && invocation.namespace.is_none()
            && invocation.account_id.is_empty()
    }

    /// The binding a [`crate::AgentReply::Bound`] reply carries, and the
    /// resource announcing the root's tool output.
    ///
    /// # Errors
    /// When the reply is not a binding, or its bytes are not one.
    pub fn from_reply(reply: &crate::AgentReply) -> Result<(Self, String), CommandError> {
        use base64::Engine as _;
        let crate::AgentReply::Bound { binding, tools } = reply else {
            return Err(CommandError::Invalid);
        };
        let binding = Self::new(
            &base64::engine::general_purpose::STANDARD
                .decode(binding)
                .map_err(|_| CommandError::Invalid)?,
        )?;
        Ok((binding, tools.clone()))
    }

    /// The [`crate::AgentReply::Bound`] reply naming `binding` and the root's
    /// tool-output resource `tools`.
    #[must_use]
    pub fn reply(binding: &wire::InvokeActorRequest, tools: String) -> crate::AgentReply {
        use base64::Engine as _;
        crate::AgentReply::Bound {
            binding: base64::engine::general_purpose::STANDARD.encode(binding.encode_to_vec()),
            tools,
        }
    }

    /// Admit bounded canonical host metadata before retaining it. The host uses
    /// the same binding for local, Actors and managed deployments.
    pub fn new(encoded: &[u8]) -> Result<Self, CommandError> {
        let request = decode_invocation(encoded)?;
        if !request.body.is_empty() || request.invocation_order.is_some() {
            return Err(CommandError::Invalid);
        }
        Ok(Self(request))
    }

    /// Attach exact admitted command bytes in Rust. No TS routing codec, fresh
    /// intent or provider-specific control path is needed.
    pub fn prepare<'a>(
        &self,
        command: &'a PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'a>, CommandError> {
        PreparedAgentInvocation::new(command, self.encode(command)?)
    }

    /// Own both the original command and invocation across browser awaits.
    pub fn prepare_owned(
        &self,
        command: PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'static>, CommandError> {
        let encoded = self.encode(&command)?;
        let prepared = PreparedAgentInvocation::new(&command, encoded.clone())?;
        drop(prepared);
        Ok(PreparedAgentInvocation {
            command: Cow::Owned(command),
            invocation: encoded,
        })
    }

    fn encode(&self, command: &PreparedCommand) -> Result<Bytes, CommandError> {
        // Body framing adds at most five bytes for this finite command budget.
        // Check before cloning the routing metadata or allocating its body.
        if self.0.encoded_len() + command.as_bytes().len() + 5 > acyclic_stream::MAX_COMMAND_BYTES {
            return Err(CommandError::Capacity);
        }
        let mut invocation = self.0.clone();
        invocation.body = command.as_bytes().to_vec();
        Ok(invocation.encode_to_vec().into())
    }
}

/// Controls use the very same pool and credential as the selected agent fold.
/// Cloning the pool handle creates no connection, task, queue or result index.
#[derive(Clone)]
pub struct AgentControls {
    client: Client,
    credential: u32,
    binding: AgentBinding,
}

impl AgentControls {
    /// Compose the host's existing session and published binding. Endpoint
    /// bootstrap belongs to the host; this constructor invents no routing.
    pub fn new(client: Client, credential: u32, binding: AgentBinding) -> Self {
        Self {
            client,
            credential,
            binding,
        }
    }

    /// Start bounded observation on this same authenticated pool and slot.
    pub fn observe(
        &self,
        selection: crate::AgentSelection,
    ) -> Result<crate::AgentClient, crate::AgentClientError> {
        crate::AgentClient::new(
            self.client.clone(),
            selection.agent,
            selection.conversation_from,
            selection.status_from,
            self.credential,
        )
    }

    /// Prepare and retain an exact invocation before starting any transport.
    pub fn prepare(
        &self,
        command: PreparedCommand,
    ) -> Result<PreparedAgentInvocation<'static>, CommandError> {
        self.binding.prepare_owned(command)
    }

    /// Execute once. The caller retains the prepared value on every outcome.
    pub async fn execute(
        &self,
        invocation: &PreparedAgentInvocation<'_>,
    ) -> Result<AgentReply, AgentExchangeError> {
        invocation.execute(&self.client, self.credential).await
    }
}

/// Session failures are distinct from decoded host control results. The caller's
/// borrowed invocation remains available with its exact original intent.
#[derive(Debug, thiserror::Error)]
pub enum AgentExchangeError {
    /// Original session admission, authorization, transport or framing failure.
    /// This is not a decoded agent refusal or proof of mutation settlement.
    #[error("agent session failed: {0}")]
    Session(StreamError),
    #[error(transparent)]
    Command(#[from] CommandError),
    #[error(transparent)]
    Control(#[from] AgentControlFailure),
    #[error("actor invocation refused with status {0}")]
    ActorStatus(u32),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AgentCommand;

    fn invocation(body: Vec<u8>) -> wire::InvokeActorRequest {
        wire::InvokeActorRequest {
            actor_id: "fixture".into(),
            method: "POST".into(),
            url: "https://fixture.invalid/command".into(),
            body,
            ..Default::default()
        }
    }

    #[test]
    fn host_and_client_share_exact_invocation_and_reply_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        let original = b"{ \"kind\" : \"root\" }";
        let encoded = invocation(original.to_vec()).encode_to_vec();
        let call = AgentCall::decode(&encoded)?;
        assert_eq!(call.invocation().body.as_slice(), original);
        assert!(matches!(call.command(), AgentCommand::Root {}));
        let agent = crate::AgentPath::try_from(
            "harness/agents/00000000000000000000000000000001".to_owned(),
        )?;
        let result = Ok(AgentReply::Root {
            agent: agent.clone(),
        });
        let encoded = call.reply(&result)?;
        let response = wire::InvokeActorResponse::decode(encoded)?;
        assert_eq!(response.status, 200);
        assert!(matches!(
            decode_agent_reply(&response.body, &AgentCommand::Root {})?,
            Ok(AgentReply::Root { agent: actual }) if actual == agent
        ));
        assert!(matches!(&result, Ok(AgentReply::Root { agent: actual }) if actual == &agent));
        Ok(())
    }

    #[test]
    fn host_call_refuses_hidden_envelope_fields_and_asserted_identity() {
        let mut encoded = invocation(b"{\"kind\":\"root\"}".to_vec()).encode_to_vec();
        encoded.extend_from_slice(&[0xf8, 0x07, 0x01]);
        assert!(matches!(
            AgentCall::decode(&encoded),
            Err(CommandError::Invalid)
        ));
        for kind in ["bind", "root"] {
            let body = format!(r#"{{"kind":"{kind}"}}"#);
            assert!(AgentCall::decode(&invocation(body.into_bytes()).encode_to_vec()).is_ok());
            let body = format!(r#"{{"kind":"{kind}","principal":"admin"}}"#);
            let encoded = invocation(body.into_bytes()).encode_to_vec();
            assert!(
                matches!(AgentCall::decode(&encoded), Err(CommandError::Invalid)),
                "{kind} must reject an asserted principal"
            );
        }
    }

    #[test]
    fn invocation_retains_exact_command_and_rejects_another_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = PreparedCommand::new(&AgentCommand::Root {})?;
        let bytes: Bytes = invocation(command.as_bytes().to_vec())
            .encode_to_vec()
            .into();
        let held = PreparedAgentInvocation::new(&command, bytes.clone())?;
        assert_eq!(held.as_bytes(), &bytes);
        let changed = invocation(b"{}".to_vec()).encode_to_vec().into();
        assert!(matches!(
            PreparedAgentInvocation::new(&command, changed),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn invocation_does_not_discard_unknown_wire_fields() -> Result<(), Box<dyn std::error::Error>> {
        let command = PreparedCommand::new(&AgentCommand::Root {})?;
        let mut encoded = invocation(command.as_bytes().to_vec()).encode_to_vec();
        encoded.extend_from_slice(&[0xf8, 0x07, 0x01]); // unknown varint field 127
        assert!(matches!(
            PreparedAgentInvocation::new(&command, encoded.into()),
            Err(CommandError::Invalid)
        ));
        Ok(())
    }

    #[test]
    fn binding_preserves_routing_and_original_owned_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let template = invocation(Vec::new());
        let binding = AgentBinding::new(&template.encode_to_vec())?;
        let command = PreparedCommand::from_bytes(b"{ \"kind\" : \"root\" }".to_vec())?;
        let original = command.as_bytes().to_vec();
        let prepared = binding.prepare_owned(command)?;
        let request = wire::InvokeActorRequest::decode(prepared.as_bytes().clone())?;
        assert_eq!(request.body, original);
        assert_eq!(request.actor_id, template.actor_id);
        assert_eq!(request.url, template.url);
        assert_eq!(request.method, template.method);
        Ok(())
    }

    #[test]
    fn binding_cannot_reuse_an_order_or_hide_a_command_body() {
        let body = invocation(b"{\"kind\":\"root\"}".to_vec());
        assert!(matches!(
            AgentBinding::new(&body.encode_to_vec()),
            Err(CommandError::Invalid)
        ));
        let mut ordered = invocation(Vec::new());
        ordered.invocation_order = Some(wire::InvocationOrder {
            session: "0".repeat(32),
            sequence: 1,
        });
        assert!(matches!(
            AgentBinding::new(&ordered.encode_to_vec()),
            Err(CommandError::Invalid)
        ));
    }

    #[test]
    fn exchange_preserves_every_session_failure_code_and_cause() {
        for cause in StreamError::ALL {
            let error = AgentExchangeError::Session(cause.clone());
            assert_eq!(error.code(), cause.code());
            assert!(matches!(error, AgentExchangeError::Session(original) if original == cause));
        }
    }
}

impl AgentExchangeError {
    /// Stable classification without flattening an owned uncertain request.
    pub fn code(&self) -> &str {
        match self {
            Self::Session(cause) => cause.code(),
            Self::Command(cause) => cause.code(),
            Self::Control(cause) => cause.code(),
            Self::ActorStatus(_) => "actor_invocation_refused",
        }
    }
}

/// One exact command bound to its canonical Actors invocation. Host composition
/// supplies the actor/method binding; neither the client nor transport invents a
/// backend-specific route. The command remains caller-owned across every await.
pub struct PreparedAgentInvocation<'a> {
    command: Cow<'a, PreparedCommand>,
    invocation: Bytes,
}

impl<'a> PreparedAgentInvocation<'a> {
    /// Validate the canonical Actors envelope before opening a session channel.
    /// Its body must be the exact admitted command bytes, including the intent.
    pub fn new(command: &'a PreparedCommand, encoded: Bytes) -> Result<Self, CommandError> {
        let invocation = decode_invocation(encoded.as_ref())?;
        if invocation.body.as_slice() != command.as_bytes() {
            return Err(CommandError::Invalid);
        }
        Ok(Self {
            command: Cow::Borrowed(command),
            invocation: encoded,
        })
    }

    /// The original encoded invocation, retained unchanged after interruption.
    pub fn as_bytes(&self) -> &Bytes {
        &self.invocation
    }

    /// Execute once on the caller's existing session. This method installs no
    /// queue, follow loop, retry, result index or backend-specific error mapping.
    pub async fn execute(
        &self,
        session: &Client,
        credential: u32,
    ) -> Result<AgentReply, AgentExchangeError> {
        let bytes = session
            .actor_call(
                &self.invocation,
                credential,
                acyclic_stream::wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES,
            )
            .await
            .map_err(AgentExchangeError::Session)?;
        let response =
            wire::InvokeActorResponse::decode(bytes.as_ref()).map_err(|_| CommandError::Invalid)?;
        if response.encode_to_vec().as_slice() != bytes.as_ref() {
            return Err(CommandError::Invalid.into());
        }
        let response =
            domain::InvokeActorResponse::try_from(response).map_err(|_| CommandError::Invalid)?;
        if response.status() != 200 {
            return Err(AgentExchangeError::ActorStatus(response.status()));
        }
        let command = PreparedCommand::decode(self.command.as_bytes())?;
        decode_agent_reply(response.body(), &command)?.map_err(AgentExchangeError::Control)
    }
}
