//! Host-provisioned vectors executed only through the shipping public client.
use super::{Failure, Report, failure};
use acyclic_harness_client::{
    AgentBinding, AgentCommand, AgentControlFailure, AgentControls, AgentExchangeError, AgentReply,
    Delivered, PreparedCommand, decode_agent_reply,
};
use acyclic_stream::{session::Client, wire_codec};
use prost::Message as _;
use serde::{Deserialize, Serialize};

/// Exact shared fixture bytes: neither runner reconstructs an intent, staged
/// descriptor, approval binding, invocation or uncertain commit request.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlVector {
    pub case: String,
    pub command: Vec<u8>,
    pub invocation: Vec<u8>,
    pub expected: ControlExpected,
}

/// An expected definitive result. Transport interruption is always a suite
/// failure, never an expected pass or a reason to replay the invocation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlExpected {
    Reply { bytes: Vec<u8> },
    ActorStatus { status: u32 },
}

/// Shared host/native/TS vector bundle, produced by the actual AgentHost fixture.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlFixture {
    /// One canonical bodyless host binding, identical for every control case.
    pub binding: Vec<u8>,
    pub cases: Vec<ControlVector>,
}

const CASES: &[&str] = &[
    "root",
    "prompt",
    "cancel",
    "prompt_after_cancel",
    "spawn",
    "children_page",
    "approvals_page",
    "approval_decision",
    "typed_refusal",
    "unknown_custody",
];

fn same_control(expected: &AgentControlFailure, actual: &AgentControlFailure) -> bool {
    match (expected, actual) {
        (
            AgentControlFailure::Refused { code: left, .. },
            AgentControlFailure::Refused { code: right, .. },
        ) => left == right,
        (AgentControlFailure::Unknown(left), AgentControlFailure::Unknown(right)) => {
            wire_codec::commit_to_wire(left).encode_to_vec()
                == wire_codec::commit_to_wire(right).encode_to_vec()
        }
        _ => false,
    }
}

/// Execute the same finite command vectors against any canonical session
/// endpoint. `provider` is the same provider underlying `session`, also used by
/// the observation suite; host fixtures own provisioning and credentials.
pub async fn verify<P: acyclic_stream::StreamProvider + ?Sized>(
    _provider: &P,
    session: &Client,
    credential: u32,
    fixture: &ControlFixture,
) -> Result<Report, Failure> {
    if fixture.cases.len() != CASES.len() {
        return Err(failure(
            "control_fixture",
            "the complete finite control vector set is required",
        ));
    }
    let binding =
        AgentBinding::new(&fixture.binding).map_err(|cause| failure("control_fixture", cause))?;
    let controls = AgentControls::new(session.clone(), credential, binding);
    let mut used = std::collections::BTreeSet::new();
    let mut total = fixture.binding.len();
    // Validate every vector before dispatching any command with side effects.
    for (index, vector) in fixture.cases.iter().enumerate() {
        if CASES.get(index) != Some(&vector.case.as_str()) || !used.insert(vector.case.as_str()) {
            return Err(failure(
                "control_fixture",
                "unknown or duplicate control case",
            ));
        }
        if vector.case == "prompt_after_cancel" && !used.contains("cancel") {
            return Err(failure(
                "control_fixture",
                "prompt_after_cancel requires the preceding cancel vector",
            ));
        }
        if vector.command.len() > acyclic_harness_client::MAX_AGENT_COMMAND_BYTES
            || vector.invocation.len() > acyclic_stream::MAX_COMMAND_BYTES
        {
            return Err(failure(
                "control_fixture",
                "command/invocation byte capacity exceeded",
            ));
        }
        let command = PreparedCommand::from_bytes(vector.command.clone())
            .map_err(|cause| failure("control_fixture", cause))?;
        let invocation = controls
            .prepare(command)
            .map_err(|cause| failure("control_fixture", cause))?;
        if invocation.as_bytes().as_ref() != vector.invocation.as_slice() {
            return Err(failure(
                "control_fixture",
                "vector differs from the public host binding",
            ));
        }
        let expected_bytes = match &vector.expected {
            ControlExpected::Reply { bytes } => {
                let command = PreparedCommand::decode(&vector.command)
                    .map_err(|cause| failure("control_fixture", cause))?;
                let expected = decode_agent_reply(bytes, &command)
                    .map_err(|cause| failure("control_fixture", cause))?;
                let valid = match vector.case.as_str() {
                    "typed_refusal" => matches!(expected, Err(AgentControlFailure::Refused { .. })),
                    "unknown_custody" => matches!(expected, Err(AgentControlFailure::Unknown(_))),
                    "root" => matches!(
                        (&command, &expected),
                        (AgentCommand::Root {}, Ok(AgentReply::Root { .. }))
                    ),
                    "prompt" | "prompt_after_cancel" => matches!(
                        (&command, &expected),
                        (
                            AgentCommand::Prompt { .. },
                            Ok(AgentReply::Delivered {
                                delivery: Delivered::Committed { .. }
                            })
                        )
                    ),
                    "cancel" => matches!(
                        (&command, &expected),
                        (
                            AgentCommand::Cancel { .. },
                            Ok(AgentReply::Delivered {
                                delivery: Delivered::Committed { .. }
                            })
                        )
                    ),
                    "approval_decision" => matches!(
                        (&command, &expected),
                        (
                            AgentCommand::Decide { .. },
                            Ok(AgentReply::Delivered {
                                delivery: Delivered::Committed { .. }
                            })
                        )
                    ),
                    "spawn" => matches!(
                        (&command, &expected),
                        (AgentCommand::Spawn { .. }, Ok(AgentReply::Spawned { .. }))
                    ),
                    "children_page" => matches!(
                        (&command, &expected),
                        (
                            AgentCommand::Children { .. },
                            Ok(AgentReply::Children { .. })
                        )
                    ),
                    "approvals_page" => matches!(
                        (&command, &expected),
                        (
                            AgentCommand::Approvals { .. },
                            Ok(AgentReply::Approvals { .. })
                        )
                    ),
                    _ => false,
                };
                if !valid {
                    return Err(failure(
                        "control_fixture",
                        "case label does not match its command and result oracle",
                    ));
                }
                bytes.len()
            }
            ControlExpected::ActorStatus { status }
                if vector.case == "typed_refusal" && (400..=599).contains(status) =>
            {
                0
            }
            ControlExpected::ActorStatus { .. } => {
                return Err(failure(
                    "control_fixture",
                    "expected refusal status required",
                ));
            }
        };
        total = total
            .checked_add(vector.command.len())
            .and_then(|n| n.checked_add(vector.invocation.len()))
            .and_then(|n| n.checked_add(expected_bytes))
            .filter(|n| *n <= 8 * 1024 * 1024)
            .ok_or_else(|| failure("control_fixture", "fixture byte capacity exceeded"))?;
    }
    let mut passed = Vec::new();
    for vector in &fixture.cases {
        let case = CASES
            .iter()
            .copied()
            .find(|name| *name == vector.case)
            .ok_or_else(|| failure("control_fixture", "unvalidated case"))?;
        let command = PreparedCommand::from_bytes(vector.command.clone())
            .map_err(|cause| failure(case, cause))?;
        let invocation = controls
            .prepare(command)
            .map_err(|cause| failure(case, cause))?;
        let actual = controls.execute(&invocation).await;
        let matches = match (&vector.expected, actual) {
            (
                ControlExpected::ActorStatus { status },
                Err(AgentExchangeError::ActorStatus(actual)),
            ) => *status == actual,
            (ControlExpected::Reply { bytes }, actual) => {
                let value = PreparedCommand::decode(&vector.command)
                    .map_err(|cause| failure(case, cause))?;
                let expected =
                    decode_agent_reply(bytes, &value).map_err(|cause| failure(case, cause))?;
                match (expected, actual) {
                    (Ok(expected), Ok(actual)) => {
                        serde_json::to_vec(&expected).map_err(|cause| failure(case, cause))?
                            == serde_json::to_vec(&actual).map_err(|cause| failure(case, cause))?
                    }
                    (Err(expected), Err(AgentExchangeError::Control(actual))) => {
                        same_control(&expected, &actual)
                    }
                    _ => false,
                }
            }
            _ => false,
        };
        if !matches {
            return Err(failure(
                case,
                "public client result differs from the host vector; not retried",
            ));
        }
        passed.push(case);
    }
    Ok(Report { passed })
}
