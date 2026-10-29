//! Discovery over real session admission and the exported WASM controls.
use super::*;
use acyclic_actors::wire;
use acyclic_auth::{authority::PolicyDecision, contract::*};
use acyclic_harness_client::{AgentCall, AgentCommand, AgentPath, AgentReply};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamError, WasmStream, session::*};
use bytes::Bytes;
use prost::Message as _;
use std::sync::{Arc, Mutex};
use wasm_bindgen_test::wasm_bindgen_test;

const RESOURCE: &str = "actors:/fixture";
const TOOLS: &str = "fs:/fixture/tools";

fn route() -> wire::InvokeActorRequest {
    wire::InvokeActorRequest {
        actor_id: "root".into(),
        namespace: Some("fixture".into()),
        account_id: b"account".into(),
        method: "POST".into(),
        url: "/agents/command".into(),
        ..Default::default()
    }
}

/// Stages parts and answers a head as the command they assemble to.
#[derive(Default)]
struct Endpoint {
    staged: Arc<Mutex<Vec<String>>>,
}

const AGENT: &str = "harness/agents/00000000000000000000000000000001";

impl CallProvider for Endpoint {
    fn resource(&self, invocation: &[u8]) -> Result<String, StreamError> {
        AgentCall::decode(invocation).map_err(|_| StreamError::InvalidArgument)?;
        Ok(RESOURCE.into())
    }

    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        let staged = self.staged.clone();
        Box::pin(async move {
            authorization.authorize(
                RESOURCE,
                &[],
                &RecordAttributes::try_new(vec![]).map_err(|_| StreamError::InvalidArgument)?,
                PolicyDecision::Allow,
            )?;
            let mut call =
                AgentCall::decode(&invocation).map_err(|_| StreamError::InvalidArgument)?;
            if let AgentCommand::Staged { digest, .. } = call.command() {
                // Each staged command's parts are taken once, by its head.
                let whole =
                    std::mem::take(&mut *staged.lock().map_err(|_| StreamError::Unavailable)?)
                        .concat();
                if blake3::hash(whole.as_bytes()).to_hex().as_str() != digest {
                    return Err(StreamError::InvalidArgument);
                }
                call = call
                    .assembled(whole.as_bytes())
                    .map_err(|_| StreamError::InvalidArgument)?;
            }
            let agent =
                || AgentPath::try_from(AGENT.to_owned()).map_err(|_| StreamError::InvalidArgument);
            let delivery = acyclic_harness_core::agent::Delivered::Committed { inbox: 3 };
            let reply = match call.command() {
                AgentCommand::Stage { part, text, .. } => {
                    let mut staged = staged.lock().map_err(|_| StreamError::Unavailable)?;
                    assert_eq!(*part as usize, staged.len());
                    staged.push(text.clone());
                    AgentReply::Staged {}
                }
                AgentCommand::Prompt { .. } => AgentReply::Delivered { delivery },
                AgentCommand::Spawn { parent, .. } => AgentReply::Spawned {
                    agent: parent
                        .child(&acyclic_harness_core::agent::AgentId::from_bytes([2; 16]))
                        .map_err(|_| StreamError::InvalidArgument)?,
                    delivery,
                },
                AgentCommand::Bind {} => {
                    assert!(AgentBinding::is_discovery(call.invocation()));
                    AgentBinding::reply(&route(), TOOLS.into())
                }
                AgentCommand::Root {} => {
                    let mut routing = call.invocation().clone();
                    routing.body.clear();
                    assert_eq!(routing, route());
                    AgentReply::Root { agent: agent()? }
                }
                _ => return Err(StreamError::InvalidArgument),
            };
            call.reply(&Ok(reply))
                .map_err(|_| StreamError::InvalidArgument)
        })
    }
}

/// A session on the fixture endpoint, with an invoking credential in slot 7.
fn session() -> Result<WasmStream, JsValue> {
    let provider = Arc::new(MemoryStream::default());
    let client = Client::in_process_with_products(
        provider.clone(),
        testing::authority(),
        None,
        Arc::new(Endpoint::default()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    let stream = WasmStream::from_session(provider, client);
    let credential = acyclic_auth::testing::root(|block| {
        let key = &mut block
            .certificates
            .as_mut()
            .unwrap()
            .keys
            .first_mut()
            .unwrap()
            .key;
        key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: RESOURCE.into(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    });
    stream.register_credential(7, &credential)?;
    Ok(stream)
}

/// A browser command past one frame executes as its parts then its head, and
/// decodes and answers as the whole command; its head is never sent alone.
#[wasm_bindgen_test]
async fn a_browser_command_past_one_frame_executes_and_answers_as_itself() -> Result<(), JsValue> {
    let stream = session()?;
    let controls = discover_agent_controls(&stream, 7).await?.controls();
    let text = "x".repeat(2 * WasmAgentCommand::maximum_command_bytes() as usize);
    let content = serde_json::json!(text);
    let delivery = acyclic_harness_core::agent::Delivered::Committed { inbox: 3 };
    let child = AgentPath::try_from(AGENT.to_owned())
        .map_err(|error| error.to_string())?
        .child(&acyclic_harness_core::agent::AgentId::from_bytes([2; 16]))
        .map_err(|error| error.to_string())?;
    for (command, kind, reply, answer) in [
        (
            serde_json::json!({"kind": "prompt", "agent": AGENT, "id": "large", "body": content}),
            "prompt",
            "delivered",
            AgentReply::Delivered { delivery },
        ),
        (
            serde_json::json!({"kind": "spawn", "parent": AGENT, "id": "large", "brief": content}),
            "spawn",
            "spawned",
            AgentReply::Spawned {
                agent: child,
                delivery,
            },
        ),
    ] {
        let encoded = serde_json::to_vec(&command).map_err(|error| error.to_string())?;
        let command = WasmAgentCommand::new(&Uint8Array::from(encoded.as_slice()))?;
        assert_eq!(owned(&command.bytes())?, encoded);
        let value = command.value()?;
        assert_eq!(js_sys::Reflect::get(&value, &"kind".into())?, kind);
        let prepared = controls.prepare(&command)?;
        // Negative control: a staged command has no one request to send.
        assert!(prepared.bytes().is_err());
        let executed = prepared.execute().await?;
        assert_eq!(js_sys::Reflect::get(&executed, &"kind".into())?, reply);
        let whole = acyclic_harness_client::PreparedCommand::decode_whole(&encoded)
            .map_err(|error| error.to_string())?;
        let body = acyclic_harness_client::encode_agent_reply(&Ok(answer), &whole)
            .map_err(|error| error.to_string())?;
        let answered = command.reply(&Uint8Array::from(body.as_slice()))?;
        assert_eq!(js_sys::Reflect::get(&answered, &"kind".into())?, reply);
    }
    Ok(())
}

/// A returned array's bytes, read through the one caller-array snapshot.
fn owned(array: &Uint8Array) -> Result<Vec<u8>, JsValue> {
    Ok(crate::wasm::bytes::snapshot(array, |_| Ok(()))?.0)
}

#[wasm_bindgen_test]
async fn discovers_controls_from_credential_and_refuses_wrong_authority() -> Result<(), JsValue> {
    let stream = session()?;
    let discovered = discover_agent_controls(&stream, 7).await?;
    assert_eq!(discovered.binding(), route().encode_to_vec());
    assert_eq!(discovered.tools(), TOOLS);
    let controls = discovered.controls();
    drop(discovered);
    let command = WasmAgentCommand::new(&Uint8Array::from(b"{ \"kind\" : \"root\" }".as_slice()))?;
    let prepared = controls.prepare(&command)?;
    let cancel = WasmAgentCommand::new(&Uint8Array::from(
        br#"{"kind":"cancel","agent":"harness/agents/00000000000000000000000000000001","id":"original-cancel"}"#.as_slice(),
    ))?;
    let cancel = controls.prepare(&cancel)?;
    let cancel_bytes = owned(&cancel.bytes()?)?;
    let original = owned(&prepared.bytes()?)?;
    // A second discovery cannot replace an already prepared command's bytes.
    discover_agent_controls(&stream, 7).await?;
    assert_eq!(owned(&prepared.bytes()?)?, original);
    assert_eq!(owned(&cancel.bytes()?)?, cancel_bytes);
    let reply = prepared.execute().await?;
    assert_eq!(js_sys::Reflect::get(&reply, &"kind".into())?, "root");
    stream.register_credential(8, &testing::credential())?;
    let error = match discover_agent_controls(&stream, 8).await {
        Ok(_) => panic!("follow-only credential received controls"),
        Err(error) => error,
    };
    assert_eq!(
        js_sys::Reflect::get(&error, &"name".into())?,
        "access_denied"
    );
    assert_eq!(js_sys::Reflect::get(&error, &"source".into())?, "session");
    Ok(())
}
