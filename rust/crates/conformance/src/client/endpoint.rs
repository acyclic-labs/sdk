//! Live agent vectors using only the public endpoint, credential and commands.
use super::{Cause, Failure, Report, failure, stream_failure};
use acyclic_harness_client::{
    AgentBinding, AgentCommand, AgentControls, AgentExchangeError, AgentReply, ConnectionOptions,
    Delivered, IntentId, ModelContent, PreparedCommand, ReadTarget, Reply,
};
use acyclic_stream::{StreamError, session::Client, wire};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};

/// A disposable endpoint's model task and its independent answer oracle.
/// This contains no credential, routing metadata or fabricated host output.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Stable task identity, retained unchanged across retries.
    pub id: IntentId,
    /// A real model task for the served root.
    pub prompt: String,
    /// Required text in the committed output, supplied independently of it.
    pub expected_text: String,
}

async fn execute(
    controls: &AgentControls,
    command: &AgentCommand,
) -> Result<AgentReply, AgentExchangeError> {
    controls
        .execute(&controls.prepare(PreparedCommand::new(command)?)?)
        .await
}

fn exchange_failure(case: &'static str, error: AgentExchangeError) -> Failure {
    match error {
        AgentExchangeError::Session(error) => stream_failure(case, error),
        error => failure(case, error),
    }
}

/// Connect using the shipping selection policy (explicit, environment, default).
/// The opaque account credential is sent unchanged to the endpoint's verifier.
pub async fn connect(options: &ConnectionOptions) -> Result<Client, Failure> {
    use acyclic_harness_client::EndpointTransport;
    let provider = match options.transport() {
        EndpointTransport::Tls => {
            acyclic_stream::grpc::Client::connect_eager(options.endpoint(), options.credential())
                .await
        }
        EndpointTransport::Loopback => {
            acyclic_stream::grpc::Client::connect_loopback(options.endpoint(), options.credential())
                .await
        }
    }
    .map_err(|error| Failure {
        case: "connect",
        detail: error.to_string(),
        cause: Some(Cause::Connect(error)),
    })?;
    let session = provider
        .session_client()
        .map_err(|error| stream_failure("session", error))?;
    session
        .register_credential(0, options.credential().as_bytes().to_vec().into())
        .map_err(|error| stream_failure("credential", error))?;
    Ok(session)
}

/// Discover and run the same vectors against any endpoint. Provision a
/// disposable tree: this sends a real model task and retains its committed turn.
pub async fn verify(session: Client, scenario: &Scenario) -> Result<Report, Failure> {
    verify_with_reply_wait(session, scenario, || {}).await
}

/// The same vectors with an observation at the registered, still-pending reply
/// wait. A host fixture can drop the actual producer at this point to exercise
/// liveness. The observer supplies no reply, cursor or authority.
pub async fn verify_with_reply_wait(
    session: Client,
    scenario: &Scenario,
    waiting: impl Fn(),
) -> Result<Report, Failure> {
    if scenario.prompt.is_empty() || scenario.expected_text.is_empty() {
        return Err(failure(
            "scenario",
            "prompt and independent answer oracle are required",
        ));
    }
    let discovery = AgentControls::new(session.clone(), 0, AgentBinding::discovery());
    let published = execute(&discovery, &AgentCommand::Bind {})
        .await
        .map_err(|error| exchange_failure("bind", error))?;
    let (binding, _) =
        AgentBinding::from_reply(&published).map_err(|error| failure("bind", error))?;
    let controls = AgentControls::new(session.clone(), 0, binding);
    let AgentReply::Root { agent } = execute(&controls, &AgentCommand::Root {})
        .await
        .map_err(|error| exchange_failure("root", error))?
    else {
        return Err(failure("root", "endpoint returned another reply shape"));
    };
    for command in [
        AgentCommand::Children {
            agent: agent.clone(),
            page: Default::default(),
        },
        AgentCommand::Approvals {
            agent: agent.clone(),
            page: Default::default(),
        },
    ] {
        let reply = execute(&controls, &command)
            .await
            .map_err(|error| exchange_failure("discovery_pages", error))?;
        if !matches!(
            (&command, reply),
            (AgentCommand::Children { .. }, AgentReply::Children { .. })
                | (AgentCommand::Approvals { .. }, AgentReply::Approvals { .. })
        ) {
            return Err(failure(
                "discovery_pages",
                "discovery returned another reply shape",
            ));
        }
    }
    let mut foreign = acyclic_harness_client::AgentPath::root(
        &acyclic_harness_client::AgentId::from_bytes([0; 16]),
    );
    if foreign == agent {
        foreign = acyclic_harness_client::AgentPath::root(
            &acyclic_harness_client::AgentId::from_bytes([1; 16]),
        );
    }
    match execute(&controls, &AgentCommand::Settlement { agent: foreign }).await {
        Err(AgentExchangeError::Control(error)) if error.code() == "no_such_agent" => {}
        Err(error) => return Err(exchange_failure("tree_admission", error)),
        Ok(_) => return Err(failure("tree_admission", "endpoint exposed a foreign tree")),
    }
    let command = AgentCommand::Prompt {
        agent: agent.clone(),
        id: scenario.id.clone(),
        body: ModelContent::Text(scenario.prompt.clone()),
    };
    let prepared = controls
        .prepare(PreparedCommand::new(&command).map_err(|error| failure("prompt", error))?)
        .map_err(|error| failure("prompt", error))?;
    let accepted = controls
        .execute(&prepared)
        .await
        .map_err(|error| exchange_failure("prompt", error))?;
    let AgentReply::Delivered {
        delivery: Delivered::Committed { inbox },
    } = &accepted
    else {
        return Err(failure("prompt", "prompt was not committed"));
    };
    let inbox = *inbox;
    let replay = controls
        .execute(&prepared)
        .await
        .map_err(|error| exchange_failure("prompt_retry", error))?;
    if serde_json::to_value(&accepted).map_err(|e| failure("prompt_retry", e))?
        != serde_json::to_value(&replay).map_err(|e| failure("prompt_retry", e))?
    {
        return Err(failure("prompt_retry", "exact retry changed its receipt"));
    }
    let changed = AgentCommand::Prompt {
        agent: agent.clone(),
        id: scenario.id.clone(),
        body: ModelContent::Text(format!("{} changed", scenario.prompt)),
    };
    match execute(&controls, &changed).await {
        Err(AgentExchangeError::Control(error)) if error.code() == "agent_conflict" => {}
        Err(error) => return Err(exchange_failure("changed_prompt", error)),
        result => {
            return Err(failure(
                "changed_prompt",
                format!("changed retry was not agent_conflict: {result:?}"),
            ));
        }
    }
    let answer = wait_reply(&session, &controls, &agent, &scenario.id, inbox, waiting).await?;
    let Reply::Answered { entry, .. } = answer else {
        return Err(failure("model_turn", "accepted prompt was dropped"));
    };
    let descriptor = entry.output.descriptor().clone();
    let target = ReadTarget::File { file: entry.output };
    let mut bytes = Vec::new();
    loop {
        let read = execute(
            &controls,
            &AgentCommand::Read {
                agent: agent.clone(),
                target: target.clone(),
                offset: bytes.len() as u64,
            },
        )
        .await
        .map_err(|error| exchange_failure("output", error))?;
        let (chunk, length) = read.content().map_err(|error| failure("output", error))?;
        if length != descriptor.byte_length() {
            return Err(failure(
                "output",
                "read length differs from the committed descriptor",
            ));
        }
        if chunk.is_empty() && (bytes.len() as u64) < length {
            return Err(failure(
                "output",
                "partial body ended before its declared length",
            ));
        }
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 == length {
            break;
        }
        if bytes.len() as u64 > length {
            return Err(failure("output", "body exceeded its declared length"));
        }
    }
    descriptor
        .verify(&bytes, &mut None)
        .map_err(|error| failure("output", error))?;
    let output: acyclic_harness_core::executor::TurnOutput =
        serde_json::from_slice(&bytes).map_err(|error| failure("output", error))?;
    if !output.text.contains(&scenario.expected_text) {
        return Err(failure(
            "model_turn",
            "committed model output does not satisfy the oracle",
        ));
    }
    Ok(Report {
        passed: vec![
            "bind",
            "root",
            "discovery_pages",
            "tree_admission",
            "prompt",
            "prompt_retry",
            "changed_prompt",
            "model_turn",
            "output",
        ],
    })
}

/// Wait at an explicit status cut, register before rechecking the reply, then
/// recheck only after actual committed data. Closed producers are typed errors.
async fn wait_reply(
    session: &Client,
    controls: &AgentControls,
    agent: &acyclic_harness_client::AgentPath,
    id: &IntentId,
    inbox: u64,
    waiting: impl Fn(),
) -> Result<Reply, Failure> {
    let AgentReply::Settlement { statuses, .. } = execute(
        controls,
        &AgentCommand::Settlement {
            agent: agent.clone(),
        },
    )
    .await
    .map_err(|error| exchange_failure("reply_cut", error))?
    else {
        return Err(failure("reply_cut", "missing committed status cut"));
    };
    let mut follow = session
        .subscribe(wire::SessionSubscription {
            credential: 0,
            resume: Some(wire::SessionCursor {
                next: statuses,
                ..Default::default()
            }),
            kind: Some(wire::session_subscription::Kind::Follow(
                wire::SessionFollow {
                    resource: acyclic_stream::wire_codec::session::stream_resource(
                        agent
                            .status()
                            .map_err(|error| failure("reply_cut", error))?
                            .as_str(),
                    ),
                    delivery: wire::SessionDelivery::All as i32,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await
        .map_err(|error| stream_failure("reply_follow", error))?;
    loop {
        let AgentReply::Reply { reply } = execute(
            controls,
            &AgentCommand::Reply {
                agent: agent.clone(),
                id: id.clone(),
                inbox,
            },
        )
        .await
        .map_err(|error| exchange_failure("reply", error))?
        else {
            return Err(failure("reply", "endpoint returned another reply shape"));
        };
        if !matches!(reply, Reply::Pending) {
            return Ok(reply);
        }
        loop {
            let frame = futures::future::poll_fn(|context| {
                let next = follow.poll_next_unpin(context);
                if next.is_pending() {
                    waiting();
                }
                next
            })
            .await
            .ok_or_else(|| stream_failure("reply_liveness", StreamError::Unavailable))?
            .map_err(|error| stream_failure("reply_liveness", error))?;
            match frame.body {
                Some(wire::session_frame::Body::Data(_)) => break,
                Some(wire::session_frame::Body::Closed(closed)) => {
                    return Err(stream_failure(
                        "reply_liveness",
                        StreamError::from_session_close(&closed.code),
                    ));
                }
                _ => {}
            }
        }
    }
}
