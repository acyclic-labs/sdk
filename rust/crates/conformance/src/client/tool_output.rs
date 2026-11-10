//! Live output uses the public observer, alongside actual agent records.
use super::{Failure, Fixture, Report, failure};
use acyclic_harness_client::{AgentClient, AgentClientError, AgentEvent, WireSessionError};
use acyclic_stream::{StreamProvider, session::Client, wire, wire_codec};
use prost::Message as _;
use serde::{Deserialize, Serialize};

/// Provisioned through the owning AgentHost and filesystem publisher. The
/// selected subtree must contain exactly the supplied first matching record.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolOutputFixture {
    pub observation: Fixture,
    pub resource: String,
    pub subscription: Vec<u8>,
    /// Canonical encoded Streams Record carrying the filesystem publication.
    pub record: Vec<u8>,
}

/// Same endpoint and authenticated session as the control/observation suites.
/// No fixture, queue, follow or result is synthesized around the public client.
pub async fn verify<P: StreamProvider + ?Sized>(
    provider: &P,
    session: Client,
    credential: u32,
    fixture: &ToolOutputFixture,
) -> Result<Report, Failure> {
    super::validate_fixture(provider, &fixture.observation).await?;
    if fixture.subscription.len() > acyclic_stream::MAX_COMMAND_BYTES
        || fixture.record.len() > acyclic_stream::MAX_RECORD_BYTES + 128
    {
        return Err(failure("tool_fixture", "fixture byte capacity exceeded"));
    }
    wire_codec::session::validate_resource(&fixture.resource)
        .map_err(|cause| failure("tool_fixture", cause))?;
    let subscription = wire::SessionSubscription::decode(fixture.subscription.as_slice())
        .map_err(|cause| failure("tool_fixture", cause))?;
    let record = wire::Record::decode(fixture.record.as_slice())
        .map_err(|cause| failure("tool_fixture", cause))?;
    if subscription.encode_to_vec() != fixture.subscription
        || record.encode_to_vec() != fixture.record
        || subscription.credential != credential
        || !matches!(&subscription.kind, Some(wire::session_subscription::Kind::Follow(follow)) if follow.resource == fixture.resource)
    {
        return Err(failure(
            "tool_fixture",
            "noncanonical or mismatched fixture",
        ));
    }
    let observed = &fixture.observation;
    let mut client = AgentClient::new(
        session,
        observed.agent.clone(),
        observed.conversation_from,
        observed.status_from,
        credential,
    )
    .map_err(|cause| failure("tool_open", cause))?;
    let revision = client.session().revision();
    let mut foreign = subscription.clone();
    foreign.credential ^= 1;
    if !matches!(
        client.open_resource(foreign),
        Err(AgentClientError::Session(
            WireSessionError::InvalidSubscription
        ))
    ) || client.session().revision() != revision
        || client.session().followed_interests() != 2
    {
        return Err(failure(
            "tool_foreign_credential",
            "foreign slot changed observation",
        ));
    }
    client
        .open_resource(subscription)
        .map_err(|cause| failure("tool_open", cause))?;
    let (mut conversation, mut status, mut output) = (false, false, false);
    while !(conversation && status && output) {
        match client
            .next()
            .await
            .map_err(|cause| failure("tool_observe", cause))?
        {
            AgentEvent::Conversation {
                agent,
                sequence,
                entry,
            } => {
                if conversation
                    || agent != observed.agent
                    || sequence != observed.conversation_from
                    || entry != observed.entry
                {
                    return Err(failure(
                        "tool_conversation",
                        "unexpected conversation identity or record",
                    ));
                }
                conversation = true;
            }
            AgentEvent::Status {
                agent,
                status: actual,
            } => {
                if status || agent != observed.agent || actual != observed.status {
                    return Err(failure(
                        "tool_status",
                        "unexpected status identity or record",
                    ));
                }
                status = true;
            }
            AgentEvent::Resource { resource, .. } => {
                if resource != fixture.resource {
                    return Err(failure("tool_output", "unrequested resource delivery"));
                }
                let view = client
                    .resource_view(&resource)
                    .map_err(|cause| failure("tool_output", cause))?;
                if let Some(actual) = view.record(0) {
                    if view.records().len() != 1 || actual.encode_to_vec() != fixture.record {
                        return Err(failure(
                            "tool_output",
                            "publication differs from the canonical host record",
                        ));
                    }
                    output = true;
                }
            }
            AgentEvent::Progress => {}
        }
    }
    if !client
        .release_resource(&fixture.resource)
        .map_err(|cause| failure("tool_release", cause))?
        || !matches!(
            client.resource_view(&fixture.resource),
            Err(AgentClientError::Session(WireSessionError::NotRequested))
        )
        || client.session().followed_interests() != 2
    {
        return Err(failure(
            "tool_release",
            "released output retained custody or removed agent interests",
        ));
    }
    Ok(Report {
        passed: vec![
            "tool_foreign_credential",
            "tool_conversation",
            "tool_status",
            "tool_output",
            "tool_release",
        ],
    })
}
