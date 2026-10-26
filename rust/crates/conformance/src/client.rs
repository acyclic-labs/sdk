//! Endpoint-independent cases over the shipping AgentClient and Streams pool.
//!
//! The caller provisions a disposable, authorized fixture through its real host.
//! This suite neither replaces the endpoint with memory nor creates an agent by
//! writing records around AgentHost. It checks observation, not turn execution.

use acyclic_harness::{
    agent::{AgentPath, AgentRecord as _, ConversationEntry, StatusRecord},
    client::{
        AgentClient, AgentClientError, AgentEvent, AgentSelection, DemandError, WireSessionError,
    },
};
use acyclic_stream::{StreamProvider, session::Client};
use serde::{Deserialize, Serialize};

/// One host-created settled record and status, with exclusive starting cursors.
/// Each stream must end immediately after the supplied record at suite entry.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// Agent created through the endpoint's ordinary agent entry.
    pub agent: AgentPath,
    /// Conversation sequence of `entry`.
    pub conversation_from: u64,
    /// Status sequence of `status`.
    pub status_from: u64,
    /// Exact immutable conversation record supplied by the host.
    pub entry: ConversationEntry,
    /// Exact published status supplied by the host.
    pub status: StatusRecord,
}

/// A case refusal retains its failing step; it is never a zero-case pass.
#[derive(Debug, Serialize)]
pub struct Failure {
    /// Stable case identifier.
    pub case: &'static str,
    /// Original provider diagnostic or failed invariant.
    pub detail: String,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(output, "CLIENT_CONFORMANCE {}: {}", self.case, self.detail)
    }
}

impl std::error::Error for Failure {}

fn failure(case: &'static str, error: impl std::fmt::Display) -> Failure {
    Failure {
        case,
        detail: error.to_string(),
    }
}

/// Completed observation cases only; not a credit, scale, outbox or OS proof.
#[derive(Debug, Serialize)]
pub struct Report {
    /// Nonempty stable case identifiers, emitted only after their oracles pass.
    pub passed: Vec<&'static str>,
}

/// Run identical actor observation cases against any actual provider adapter.
/// `session` must belong to `provider` and already have `credential` registered.
/// The host owns fixture creation, credentials, runtime and cancellation.
pub async fn verify<P: StreamProvider + ?Sized>(
    provider: &P,
    session: Client,
    credential: u32,
    fixture: &Fixture,
) -> Result<Report, Failure> {
    fixture
        .entry
        .encode()
        .map_err(|error| failure("fixture", error))?;
    fixture
        .status
        .encode()
        .map_err(|error| failure("fixture", error))?;
    for (path, from) in [
        (fixture.agent.conversation(), fixture.conversation_from),
        (fixture.agent.status(), fixture.status_from),
    ] {
        let path = path.map_err(|error| failure("fixture", error))?;
        let end = from
            .checked_add(1)
            .ok_or_else(|| failure("fixture", "cursor overflow"))?;
        if provider
            .tail(path)
            .await
            .map_err(|error| failure("fixture_tail", error))?
            != end
        {
            return Err(failure(
                "fixture_tail",
                "fixture is not the supplied settled cut",
            ));
        }
    }
    let selection = || AgentSelection {
        agent: fixture.agent.clone(),
        conversation_from: fixture.conversation_from,
        status_from: fixture.status_from,
    };
    let mut client = AgentClient::new(
        session,
        fixture.agent.clone(),
        fixture.conversation_from,
        fixture.status_from,
        credential,
    )
    .map_err(|error| failure("open", error))?;
    let mut conversation = false;
    let mut status = false;
    while !conversation || !status {
        match client
            .next()
            .await
            .map_err(|error| failure("observe", error))?
        {
            AgentEvent::Conversation {
                agent,
                sequence,
                entry,
            } => {
                if conversation
                    || agent != fixture.agent
                    || sequence != fixture.conversation_from
                    || entry != fixture.entry
                {
                    return Err(failure(
                        "exact_conversation",
                        "wrong identity, duplicate, sequence or payload",
                    ));
                }
                conversation = true;
            }
            AgentEvent::Status {
                agent,
                status: observed,
            } => {
                if status || agent != fixture.agent || observed != fixture.status {
                    return Err(failure(
                        "exact_status",
                        "wrong identity, duplicate or payload",
                    ));
                }
                status = true;
            }
            AgentEvent::Progress => {}
        }
    }
    let cursor = client.conversation().next_sequence();
    let revision = client.conversation().revision();
    client
        .set_view(selection(), std::iter::empty())
        .map_err(|error| failure("retain_selection", error))?;
    if client.conversation().next_sequence() != cursor
        || client.conversation().revision() != revision
        || client.conversation().records().len() != 1
    {
        return Err(failure(
            "retain_selection",
            "unchanged demand reset the fold or cursor",
        ));
    }
    // Deliberately over-budget input is refused before any demand mutation.
    let composition = client.session().revision();
    match client.set_view(
        selection(),
        std::iter::repeat_with(selection).take(AgentClient::selected_capacity()),
    ) {
        Err(AgentClientError::Session(WireSessionError::Demand(DemandError::SelectedCapacity))) => {
        }
        result => {
            return Err(failure(
                "atomic_capacity_refusal",
                format!("unexpected {result:?}"),
            ));
        }
    }
    if client.conversation().next_sequence() != cursor
        || client.selected_agents().count() != 1
        || client.agent() != &fixture.agent
        || client.session().revision() != composition
    {
        return Err(failure(
            "atomic_capacity_refusal",
            "refusal changed admitted demand",
        ));
    }
    let unknown = match fixture.agent.parent() {
        Some(parent) => parent,
        None => fixture
            .agent
            .child(&acyclic_harness::agent::AgentId::from_bytes([0x7f; 16]))
            .map_err(|error| failure("unknown_authority", error))?,
    };
    match client.agent_status(&unknown) {
        Err(AgentClientError::Session(WireSessionError::NotRequested)) => {}
        result => {
            return Err(failure(
                "unknown_authority",
                format!("unexpected {result:?}"),
            ));
        }
    }
    // Demand is lazy: do not poll the unselected fixture's replacement. This
    // tests ownership release and explicit cursor admission, not absent follows.
    client
        .set_view(
            AgentSelection {
                agent: unknown,
                conversation_from: 0,
                status_from: 0,
            },
            std::iter::empty(),
        )
        .map_err(|error| failure("reselection_cursor", error))?;
    if client.agent_conversation(&fixture.agent).is_some() {
        return Err(failure(
            "reselection_cursor",
            "deselection retained its record window",
        ));
    }
    client
        .set_view(
            AgentSelection {
                agent: fixture.agent.clone(),
                conversation_from: cursor,
                status_from: fixture.status_from + 1,
            },
            std::iter::empty(),
        )
        .map_err(|error| failure("reselection_cursor", error))?;
    if client.conversation().next_sequence() != cursor || client.conversation().records().len() != 0
    {
        return Err(failure(
            "reselection_cursor",
            "reselection reused the evicted history or reset its cursor",
        ));
    }
    Ok(Report {
        passed: vec![
            "exact_conversation",
            "exact_status",
            "retain_selection",
            "atomic_capacity_refusal",
            "unknown_authority",
            "reselection_cursor",
        ],
    })
}
