//! Discovery over real session admission and the exported WASM controls.
use super::*;
use acyclic_actors::wire;
use acyclic_auth::{authority::PolicyDecision, contract::*};
use acyclic_harness_client::{AgentCall, AgentCommand, AgentPath, AgentReply};
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamError, WasmStream, session::*};
use bytes::Bytes;
use prost::Message as _;
use std::sync::Arc;
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

struct Endpoint;
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
        Box::pin(async move {
            authorization.authorize(
                RESOURCE,
                &[],
                &RecordAttributes::try_new(vec![]).map_err(|_| StreamError::InvalidArgument)?,
                PolicyDecision::Allow,
            )?;
            let call = AgentCall::decode(&invocation).map_err(|_| StreamError::InvalidArgument)?;
            let reply = match call.command() {
                AgentCommand::Bind {} => {
                    assert!(AgentBinding::is_discovery(call.invocation()));
                    AgentBinding::reply(&route(), TOOLS.into())
                }
                AgentCommand::Root {} => {
                    let mut routing = call.invocation().clone();
                    routing.body.clear();
                    assert_eq!(routing, route());
                    AgentReply::Root {
                        agent: AgentPath::try_from(
                            "harness/agents/00000000000000000000000000000001".to_owned(),
                        )
                        .map_err(|_| StreamError::InvalidArgument)?,
                    }
                }
                _ => return Err(StreamError::InvalidArgument),
            };
            call.reply(&Ok(reply))
                .map_err(|_| StreamError::InvalidArgument)
        })
    }
}

#[wasm_bindgen_test]
async fn discovers_controls_from_credential_and_refuses_wrong_authority() -> Result<(), JsValue> {
    let provider = Arc::new(MemoryStream::default());
    let client = Client::in_process_with_products(
        provider.clone(),
        testing::authority(),
        None,
        Arc::new(Endpoint),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
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
    let cancel_bytes = cancel.bytes();
    let original = prepared.bytes();
    // A second discovery cannot replace an already prepared command's bytes.
    discover_agent_controls(&stream, 7).await?;
    assert_eq!(prepared.bytes(), original);
    assert_eq!(cancel.bytes(), cancel_bytes);
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
