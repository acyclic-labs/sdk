use super::*;
use crate::agent::{AgentId, AgentPath};
use crate::hosting_test_support::invocation;
use acyclic_actors::wire;
use acyclic_harness_client::decode_agent_reply;
use prost::Message as _;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[test]
fn serving_reply_codec_preserves_invalid_refusals_and_complete_large_results() -> Result {
    let command = AgentCommand::Root {};
    let call = acyclic_harness_client::AgentCall::decode(&invocation("agents", &command)?)?;
    let bind_command = AgentCommand::Bind { agent: None };
    let bind_call =
        acyclic_harness_client::AgentCall::decode(&invocation("agents", &bind_command)?)?;
    let mut binding = bind_call.invocation().clone();
    binding.body.clear();
    binding.invocation_order = None;
    let large = Ok(acyclic_harness_client::AgentBinding::reply(
        &binding,
        format!(
            "fs://tools/{}",
            "x".repeat(acyclic_harness_client::MAX_AGENT_COMMAND_BYTES + 1)
        ),
        None,
    ));
    assert!(matches!(
        super::reply(&call, &large),
        Err(StreamError::InvalidResponse)
    ));
    let encoded = super::reply(&bind_call, &large)?;
    let response = wire::InvokeActorResponse::decode(encoded)?;
    let Ok(decoded) = decode_agent_reply(&response.body, &bind_command)? else {
        return Err("valid large binding reply was refused".into());
    };
    let (decoded_binding, tools) = acyclic_harness_client::AgentBinding::from_reply(&decoded)?;
    let routed = decoded_binding.routed(&binding.url, &call.invocation().body)?;
    let mut routed = wire::InvokeActorRequest::decode(routed)?;
    routed.body.clear();
    routed.invocation_order = None;
    assert_eq!(routed, binding);
    let Ok(AgentReply::Bound {
        tools: expected, ..
    }) = &large
    else {
        unreachable!("the public binding writer returns Bound");
    };
    assert_eq!(&tools, expected);
    let root = AgentPath::root(&AgentId::from_bytes([13; 16]));
    let encoded = super::reply(
        &call,
        &Ok(AgentReply::Root {
            agent: root.clone(),
        }),
    )?;
    let response = wire::InvokeActorResponse::decode(encoded)?;
    assert!(matches!(
        decode_agent_reply(&response.body, &command)?,
        Ok(AgentReply::Root { agent }) if agent == root
    ));
    Ok(())
}

#[test]
fn a_children_cursor_carries_its_hierarchy_version_and_last_path() -> Result {
    let version = CommitId::from_bytes([0xa5; 32]);
    let after = AgentPath::root(&crate::agent::AgentId::from_bytes([0x5a; 16]))
        .child(&crate::agent::AgentId::from_bytes([0x3c; 16]))?;
    let cursor = children_cursor(&version, after.as_str());
    assert_eq!(
        page_json_bytes(&cursor)? - 2,
        children_cursor_bytes(&version, after.as_str()) as u64
    );
    assert_eq!(
        parse_children_cursor(&cursor)?,
        (version, StreamPath::new(after.as_str())?)
    );
    assert!(parse_children_cursor("agents/root/children/a").is_err());
    Ok(())
}
