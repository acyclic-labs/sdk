//! G1 declared_equals_effective: the effective prompt, tools, model and
//! environment every agent runs with equal what its role declared.
//!
//! One world opens the production local harness with a worker, advisor,
//! reviewer and forked child, makes every agent use the home and effective
//! volumes, exchanges parent steering through the real send tool, and checks
//! each recorded request against its declared role.
use super::*;
use acyclic_harness::{
    LocalHarnessConfig,
    agent::{AgentPath, ConversationEntry, TurnOutcome},
    context::TASK_TOOLS,
    local::{Event, agent_conversation::Conversation},
    model::{
        Model, ModelContent, ModelContentPart, ModelEvent, ModelMessage, ModelProvider,
        ModelRequest, ModelResolver, ModelRole, ToolCall, ToolResultContent,
    },
    test_support::{
        PromptOutcome, ScriptedEvent, ScriptedModelProvider, ScriptedTurn, prompt_settled,
    },
    tools::shell::PLATFORM,
};
use acyclic_harness_client::{AgentCommand, AgentReply};
use acyclic_stream::OwnedBytes;
use serde_json::{Value, json};
use sha2::Digest as _;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

pub(super) const SEEDS: &[u64] = &[0, 1, 2, 3];

const MARKER: &str = "g1-node:";
const ROOT_MARKER: &str = "Understand the user's intent";
const WORKER_MARKER: &str = "G1-WORKER-DIRECTIVE";
const ADVISOR_MARKER: &str = "G1-ADVISOR-DIRECTIVE";
const REVIEWER_MARKER: &str = "G1-REVIEWER-DIRECTIVE";
const DELEGATION: &str = "decide in order: atomic";
const NONE_FOUND: &str = "report that negative result";
const PATH_FILE: &str = "g1-path.txt";
const PATH_MARKER: &str = "G1-PATH-";
const PARENT_AUTHORITY: &str = "Instruction from your parent";
const PEER_AUTHORITY: &str = "Peer input only";
/// The one tool that runs the command-line executables a prompt names.
const COMMAND_TOOL: &str = PLATFORM.name;

const ITEMS: [&str; 9] = [
    "tool",
    "path_or_volume",
    "command",
    "role_template",
    "model",
    "output_channel",
    "none_found_outcome",
    "message_provenance",
    "environment_constraint",
];
const AGENTS: [&str; 4] = ["root", "worker", "advisor", "reviewer"];
const NODES: [&str; 4] = ["root", "worker", "advisor", "reviewer"];

fn brief(node: &str) -> String {
    format!("{MARKER}{node}")
}

fn template(context: &str, may_spawn: &str, allowed: &str, body: &str) -> String {
    format!(
        "---\nversion: 1\ncontext: {context}\nmay_spawn: [{may_spawn}]\n\
         allowed_tools: [{allowed}]\nmodel: scripted/role-model\n---\n{body}\n"
    )
}

/// A seed's step: tool calls, or the final answer.
enum Step {
    Calls(Vec<(&'static str, Value)>),
    Text(String),
}

#[derive(Default)]
struct Plan {
    root: Option<AgentPath>,
    children: BTreeMap<String, AgentPath>,
    parents: BTreeMap<String, String>,
    focus: String,
}

fn steering(plan: &Plan, node: &str) -> Vec<(&'static str, Value)> {
    let direct = |child: &String| plan.parents.get(child).is_some_and(|parent| parent == node);
    let mut calls = Vec::new();
    if node != "root" && plan.focus == "root" {
        let root = plan.root.as_ref().expect("root path before spawn");
        calls.push((
            "send",
            json!({"agent": root.as_str(), "message": format!("report {node}")}),
        ));
    }
    for (child, path) in &plan.children {
        if direct(child) {
            calls.push((
                "send",
                json!({"agent": path.as_str(), "message": format!("steer {child}")}),
            ));
        }
    }
    calls
}

fn path_steps(node: &str) -> Vec<Step> {
    let content = format!("{PATH_MARKER}{node}");
    vec![Step::Calls(vec![
        (
            "write",
            json!({
                "path": format!("volume:///home/{PATH_FILE}"),
                "content": content,
                "ifVersion": "absent",
            }),
        ),
        (
            "read",
            json!({"path": "volume:///home/templates/worker/AGENT.md"}),
        ),
        ("read", json!({"path": "volume:///effective/AGENT.md"})),
    ])]
}

fn steps(plan: &Plan, node: &str) -> Vec<Step> {
    let mut steps = if node == plan.focus {
        path_steps(node)
    } else {
        Vec::new()
    };
    if node != "root" {
        steps.push(Step::Text(format!("ready {node}")));
        return steps;
    }
    steps.push(Step::Calls(steering(plan, node)));
    steps.push(Step::Text(format!("done {node}")));
    steps
}

fn named(text: &str) -> Option<String> {
    let start = text.find(MARKER)? + MARKER.len();
    Some(
        text[start..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect(),
    )
}

/// The node whose brief is newest in `request`; a steering message is newer
/// but does not change the agent it belongs to.
fn node_of(request: &ModelRequest) -> Option<(String, usize)> {
    let digests: BTreeMap<[u8; 32], String> = NODES
        .iter()
        .map(|node| (sha2::Sha256::digest(brief(node)).into(), (*node).to_owned()))
        .collect();
    request
        .messages
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, message)| message.role == ModelRole::User)
        .find_map(|(index, message)| {
            let node = match &message.content {
                ModelContent::Text(text) => named(text),
                content => content.parts().iter().find_map(|part| match part {
                    ModelContentPart::Text { text, .. } => named(text),
                    ModelContentPart::File { file, .. } => {
                        digests.get(file.descriptor().sha256()).cloned()
                    }
                    _ => None,
                }),
            };
            node.map(|node| (node, index))
        })
}

fn notice(message: &ModelMessage, kind: &str) -> bool {
    message
        .content
        .notice_parts()
        .is_some_and(|(notices, _)| notices.iter().any(|notice| notice.kind == kind))
}

fn respond(plan: &Plan, request: &ModelRequest) -> Result<ScriptedTurn> {
    let Some((node, briefed)) = node_of(request) else {
        return Ok(ScriptedTurn::Text("ack root".into()));
    };
    // A parent's steering message is a new turn after the agent's last answer.
    if node != "root"
        && let Some(parent) = request
            .messages
            .iter()
            .rposition(|message| notice(message, "parent.message"))
    {
        let calls = steering(plan, &node);
        if calls.is_empty() {
            return Ok(ScriptedTurn::Text(format!("ack {node}")));
        }
        let made = request.messages[parent..]
            .iter()
            .filter(|message| message.role == ModelRole::Assistant)
            .flat_map(|message| message.content.parts())
            .filter(|part| matches!(part, ModelContentPart::ToolCall(_)))
            .count();
        if made == 0 {
            let mut events: Vec<ScriptedEvent> = calls
                .into_iter()
                .enumerate()
                .map(|(index, (name, arguments))| {
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        format!("report{index}"),
                        name.into(),
                        arguments,
                    )))
                })
                .collect();
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }));
            return Ok(ScriptedTurn::Events(events));
        }
        return Ok(ScriptedTurn::Text(format!("ack {node}")));
    }
    if node == "root"
        && request
            .messages
            .iter()
            .any(|message| notice(message, "peer.message"))
    {
        return Ok(ScriptedTurn::Text("ack root".into()));
    }
    let mut made = request.messages[briefed..]
        .iter()
        .filter(|message| message.role == ModelRole::Assistant)
        .flat_map(|message| message.content.parts())
        .filter(|part| matches!(part, ModelContentPart::ToolCall(_)))
        .count();
    let steps = steps(plan, &node);
    let mut step = 0;
    while let Some(Step::Calls(calls)) = steps.get(step) {
        if made < calls.len() {
            break;
        }
        made -= calls.len();
        step += 1;
    }
    match steps.get(step) {
        Some(Step::Text(text)) => Ok(ScriptedTurn::Text(text.clone())),
        Some(Step::Calls(calls)) => {
            let mut events: Vec<ScriptedEvent> = calls
                .iter()
                .enumerate()
                .map(|(index, (name, arguments))| {
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        format!("s{step}c{index}"),
                        (*name).into(),
                        arguments.clone(),
                    )))
                })
                .collect();
            events.push(ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }));
            Ok(ScriptedTurn::Events(events))
        }
        None => Ok(ScriptedTurn::Text(format!("done {node}"))),
    }
}

fn system_text(request: &ModelRequest) -> String {
    let mut texts = Vec::new();
    for message in &request.messages {
        if message.role != ModelRole::System {
            continue;
        }
        match &message.content {
            ModelContent::Text(text) => texts.push(text.as_str()),
            content => {
                for part in content.parts() {
                    if let ModelContentPart::Text { text, .. } = part {
                        texts.push(text.as_str());
                    }
                }
            }
        }
    }
    texts.join("\n")
}

fn environment_text(request: &ModelRequest) -> Option<String> {
    request
        .messages
        .iter()
        .find_map(|message| match &message.content {
            ModelContent::Text(text) if text.contains("<environment_context>") => {
                Some(text.clone())
            }
            content => content.parts().iter().find_map(|part| match part {
                ModelContentPart::Text { text, .. } if text.contains("<environment_context>") => {
                    Some(text.clone())
                }
                _ => None,
            }),
        })
}

fn commands(system: &str) -> BTreeSet<&str> {
    let mut found = BTreeSet::new();
    for (index, part) in system.split('`').enumerate() {
        if index % 2 == 1
            && let Some(line) = part.strip_prefix("$ ")
        {
            found.insert(line.split_whitespace().next().unwrap_or_default());
        }
    }
    found
}

fn expected_tools(node: &str) -> BTreeSet<String> {
    let allowed: &[&str] = match node {
        "root" => {
            return acyclic_harness::tools::default_names()
                .into_iter()
                .collect();
        }
        "worker" | "advisor" | "reviewer" => {
            &["read", "write", "send", "spawn", COMMAND_TOOL]
        }
        _ => unreachable!("known node"),
    };
    allowed.iter().map(|name| (*name).to_owned()).collect()
}

fn expected_marker(node: &str) -> &'static str {
    match node {
        "root" => ROOT_MARKER,
        "worker" => WORKER_MARKER,
        "advisor" => ADVISOR_MARKER,
        "reviewer" => REVIEWER_MARKER,
        _ => unreachable!("known node"),
    }
}

fn expected_model(node: &str) -> &'static str {
    if node == "root" { "dst" } else { "role-model" }
}

fn check_request(
    node: &str,
    first: &ModelRequest,
    request: &ModelRequest,
) -> std::result::Result<(), String> {
    let system = system_text(request);
    let marker = expected_marker(node);
    if !system.contains(marker) {
        return Err(format!("{node} request lacks its role directive {marker}"));
    }
    for other in NODES {
        let other_marker = expected_marker(other);
        if other != node && other_marker != marker && system.contains(other_marker) {
            return Err(format!(
                "{node} request renders another role's directive {other_marker}"
            ));
        }
    }
    if node == "root" {
        if !system.contains(DELEGATION) {
            return Err("root request lacks the declared delegation policy".into());
        }
    } else if system.contains(DELEGATION) {
        return Err(format!("{node} request appends the root delegation policy"));
    }
    if !system.contains(NONE_FOUND) {
        return Err(format!(
            "{node} request lacks the declared none-found outcome"
        ));
    }
    for path in ["volume:///home", "volume:///effective/"] {
        if !system.contains(path) {
            return Err(format!("{node} prompt does not name {path}"));
        }
    }
    let names: BTreeSet<String> = request.tools.iter().map(|tool| tool.name.clone()).collect();
    let expected = expected_tools(node);
    if names != expected {
        return Err(format!(
            "{node} declares tools {expected:?} but the request offers {names:?}"
        ));
    }
    if request.model.name != expected_model(node) {
        return Err(format!(
            "{node} declared model {} but the request uses {}",
            expected_model(node),
            request.model.name
        ));
    }
    let declared = commands(&system);
    if !declared.is_empty() && !names.contains(COMMAND_TOOL) {
        return Err(format!(
            "{node} prompt names commands {declared:?} but its tools have no {COMMAND_TOOL} tool"
        ));
    }
    for command in &declared {
        if !TASK_TOOLS.contains(command) {
            return Err(format!(
                "{node} prompt names `{command}`, which no task environment guarantees"
            ));
        }
    }
    let environment = environment_text(first)
        .ok_or_else(|| format!("{node} request lacks the environment context"))?;
    for tag in ["<cwd>.", "<os>", "<shell>", "<tooling>", "<scratch>"] {
        if !environment.contains(tag) {
            return Err(format!("{node} environment context lacks {tag}"));
        }
    }
    for tool in TASK_TOOLS {
        if !environment.contains(&format!("{tool}: available"))
            && !environment.contains(&format!("{tool}: absent"))
        {
            return Err(format!(
                "{node} environment context does not report {tool} availability"
            ));
        }
    }
    for message in &request.messages {
        for part in message.content.parts() {
            if let ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Error { message },
                ..
            } = part
            {
                return Err(format!("{node} tool {name} failed: {message}"));
            }
        }
    }
    Ok(())
}

fn path_result<'a>(
    request: &'a ModelRequest,
    name: &str,
    path: &str,
) -> Option<&'a ToolResultContent> {
    let mut calls = BTreeMap::new();
    for message in &request.messages {
        for part in message.content.parts() {
            if let ModelContentPart::ToolCall(call) = part {
                calls.insert(call.call_id.as_str(), call);
            }
        }
    }
    for message in &request.messages {
        for part in message.content.parts() {
            if let ModelContentPart::ToolResult {
                call_id, content, ..
            } = part
                && let Some(call) = calls.get(call_id.as_str())
                && call.name == name
                && call.arguments.get("path").and_then(Value::as_str) == Some(path)
            {
                return Some(content);
            }
        }
    }
    None
}

fn check_paths(node: &str, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let home = format!("volume:///home/{PATH_FILE}");
    let mut wrote = false;
    let mut read_home = false;
    let mut read_effective = false;
    for request in requests {
        if let Some(result) = path_result(request, "write", &home) {
            if let ToolResultContent::Error { message } = result {
                return Err(format!("{node} could not write {home}: {message}"));
            }
            wrote = true;
        }
        if let Some(result) =
            path_result(request, "read", "volume:///home/templates/worker/AGENT.md")
        {
            match result {
                ToolResultContent::Json { value }
                    if value["text"]
                        .as_str()
                        .is_some_and(|text| text.contains(WORKER_MARKER)) =>
                {
                    read_home = true;
                }
                ToolResultContent::Error { message } => {
                    return Err(format!(
                        "{node} could not read volume:///home/templates/worker/AGENT.md: {message}"
                    ));
                }
                other => {
                    return Err(format!(
                        "{node} read volume:///home/templates/worker/AGENT.md as {other:?}"
                    ));
                }
            }
        }
        if let Some(result) = path_result(request, "read", "volume:///effective/AGENT.md") {
            match result {
                ToolResultContent::Json { value }
                    if value["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("volume:///home")) =>
                {
                    read_effective = true;
                }
                ToolResultContent::Error { message } => {
                    return Err(format!(
                        "{node} could not read volume:///effective/AGENT.md: {message}"
                    ));
                }
                other => {
                    return Err(format!(
                        "{node} read volume:///effective/AGENT.md as {other:?}"
                    ));
                }
            }
        }
    }
    if !wrote || !read_home || !read_effective {
        return Err(format!(
            "{node} did not exercise its declared paths: write={wrote}, home={read_home}, effective={read_effective}"
        ));
    }
    Ok(())
}

fn check_node(node: &str, requests: &[ModelRequest]) -> std::result::Result<(), String> {
    let first = requests
        .first()
        .ok_or_else(|| format!("{node} made no model request"))?;
    for request in requests {
        check_request(node, first, request)?;
    }
    check_paths(node, requests)?;
    let mut parent_seen = false;
    let mut peer_seen = false;
    for request in requests {
        for message in &request.messages {
            if let Some((notices, _)) = message.content.notice_parts() {
                for notice in notices {
                    if notice.kind == "parent.message" {
                        parent_seen = true;
                        let authority = notice.body["authority"].as_str().unwrap_or_default();
                        if !authority.contains(PARENT_AUTHORITY) {
                            return Err(format!(
                                "{node} parent steering has authority {authority:?}"
                            ));
                        }
                    }
                    if notice.kind == "peer.message" {
                        peer_seen = true;
                        let authority = notice.body["authority"].as_str().unwrap_or_default();
                        if !authority.contains(PEER_AUTHORITY) {
                            return Err(format!("{node} peer message has authority {authority:?}"));
                        }
                    }
                }
            }
        }
    }
    if node == "root" {
        if !peer_seen {
            return Err("root never saw a peer message from its child".into());
        }
    } else if !parent_seen {
        return Err(format!(
            "{node} never saw its parent's steering as parent input"
        ));
    }
    Ok(())
}

async fn conversation_turns(
    connection: &acyclic_harness::local::Connection,
    path: &AgentPath,
) -> Result<Vec<ConversationEntry>> {
    let conversation = Conversation::open(connection.select(path.clone()).await?);
    let settled = conversation.settled().await?;
    conversation.entries(0, settled).await
}

async fn check_output(
    harness: &Arc<LocalHarness>,
    paths: &BTreeMap<String, AgentPath>,
) -> Result<()> {
    let connection = harness.connect().await?;
    for (node, path) in paths {
        let entries = conversation_turns(&connection, path).await?;
        let conversation = Conversation::open(connection.select(path.clone()).await?);
        let mut outputs = Vec::new();
        for entry in &entries {
            if let TurnOutcome::Failed { message } = &entry.outcome {
                return Err(Error::Conflict(format!("{node} turn failed: {message}")));
            }
            outputs.push(conversation.output_text(entry).await?);
        }
        let expected = format!("done {node}");
        let ack = format!("ack {node}");
        if !outputs
            .iter()
            .any(|output| output.contains(&expected) || output.contains(&ack))
        {
            return Err(Error::Conflict(format!(
                "{node} output channel never published its answer: {outputs:?}"
            )));
        }
    }
    Ok(())
}

async fn settle(
    harness: &Arc<LocalHarness>,
    paths: &BTreeMap<String, AgentPath>,
    expected: &[&str],
) -> Result<()> {
    let connection = harness.connect().await?;
    for (node, path) in paths {
        let conversation = Conversation::open(connection.select(path.clone()).await?);
        let mut observer = conversation.observe(Some(0)).await?;
        let wants: Vec<String> = expected
            .iter()
            .map(|prefix| format!("{prefix} {node}"))
            .collect();
        loop {
            match observer.next().await? {
                Event::Turn { entry, .. } => {
                    let text = conversation.output_text(&entry).await?;
                    if wants.iter().any(|want| text.contains(want)) {
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// The agent this seed makes prove its effective view.
fn focus(seed: u64) -> &'static str {
    usize::try_from(seed)
        .ok()
        .and_then(|index| AGENTS.get(index))
        .copied()
        .unwrap_or("root")
}

/// Opens the production harness with the scripted model and the declared role
/// templates. Kept apart from the world so building the configuration does not
/// consume the stack the first spawn needs.
async fn open(
    directory: &Path,
) -> Result<(
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
    Arc<Mutex<Plan>>,
)> {
    let reference = ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
    let volume = VolumeRef::new(
        reference.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("g1".into()),
        &mut None,
    )?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
        .await
        .map_err(storage)?;
    FilesystemHost::new(fs, &reference, None)?
        .create_volume(&volume)
        .await?;
    let plan = Arc::new(Mutex::new(Plan::default()));
    let provider = Arc::new(
        ScriptedModelProvider::responding({
            let plan = Arc::clone(&plan);
            move |request| {
                let plan = plan
                    .lock()
                    .map_err(|_| Error::Storage("g1 plan lock poisoned".into()))?;
                respond(&plan, request)
            }
        })
        .reading_content(),
    );
    let settings =
        acyclic_harness::native_settings::SettingsConfig::for_test(directory.join("filesystem"))
            .map_err(storage)?;
    let models = Arc::new(ModelResolver::new("scripted/dst", {
        let provider = Arc::clone(&provider);
        move |selection: &str| {
            let name = selection.strip_prefix("scripted/").unwrap_or(selection);
            let model = Model::new("scripted".into(), name.into(), "1".into())?;
            Ok((model, Arc::clone(&provider) as Arc<dyn ModelProvider>))
        }
    }));
    let config = LocalHarnessConfig::resolving(
        directory.join("filesystem"),
        models,
        Some("scripted/dst".into()),
    )
    .settings(settings)
    .without_user_settings()
    .volume(volume)
    .state_directory(directory.join("state"))
    .hermetic()
    .approval_policy(acyclic_harness::approvals::ApprovalPolicy::accept_all("g1"))
    .home_file(
        "templates/worker/AGENT.md",
        OwnedBytes::from(template(
            "fork",
            "worker",
            &format!("read, write, send, spawn, {COMMAND_TOOL}"),
            &format!(
                "{WORKER_MARKER} Use read, write, send, spawn and {COMMAND_TOOL}. Run `$ git status` before editing."
            ),
        )),
    )
    .home_file(
        "templates/advisor/AGENT.md",
        OwnedBytes::from(template(
            "fresh",
            "worker",
            &format!("read, write, send, spawn, {COMMAND_TOOL}"),
            &format!(
                "{ADVISOR_MARKER} Use read, write, send, spawn and {COMMAND_TOOL}. Run `$ git status` before advising."
            ),
        )),
    )
    .home_file(
        "templates/reviewer/AGENT.md",
        OwnedBytes::from(template(
            "fresh",
            "worker",
            &format!("read, write, send, spawn, {COMMAND_TOOL}"),
            &format!(
                "{REVIEWER_MARKER} Use read, write, send, spawn and {COMMAND_TOOL}. Run `$ git status` before reviewing."
            ),
        )),
    );
    let harness = Arc::new(entry(LocalHarness::open(config)).await?);
    Ok((harness, provider, plan))
}

async fn spawn_child(
    connection: &acyclic_harness::local::Connection,
    parent: &AgentPath,
    parent_node: &str,
    node: &str,
    role: &str,
    plan: &Arc<Mutex<Plan>>,
) -> Result<AgentPath> {
    let AgentReply::Spawned { agent, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: parent.clone(),
            id: acyclic_harness::agent::IntentId::try_from(format!("g1-{node}"))
                .map_err(storage)?,
            brief: ModelContent::Text(brief(node)),
            role: role.into(),
        })
        .await?
    else {
        return Err(Error::Invalid(format!(
            "{node} spawn returned another reply"
        )));
    };
    let mut plan = plan
        .lock()
        .map_err(|_| Error::Storage("g1 plan lock poisoned".into()))?;
    plan.children.insert(node.to_owned(), agent.clone());
    plan.parents.insert(node.to_owned(), parent_node.to_owned());
    Ok(agent)
}

/// One seed opens the root and the agent it makes prove its effective view.
fn world(seed: u64) -> std::result::Result<bool, String> {
    let runtime =
        acyclic_native_runtime::testing::seeded_runtime(seed).map_err(|error| error.to_string())?;
    runtime.block_on(async {
        tokio::time::pause();
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        entry(world_at(seed, directory.path()))
            .await
            .map_err(|error| error.to_string())
    })
}

/// Splitting by agent keeps each world's spawn chain shallow enough to stay
/// inside the test thread's stack and the per-case limit.
async fn world_at(seed: u64, directory: &Path) -> Result<bool> {
    let focus = focus(seed);
    let (harness, provider, plan) = open(directory).await?;
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    {
        let mut plan = plan
            .lock()
            .map_err(|_| Error::Storage("g1 plan lock poisoned".into()))?;
        plan.root = Some(root.clone());
        plan.focus = focus.to_owned();
    }
    if focus == "advisor" {
        spawn_child(&connection, &root, "root", "advisor", "advisor", &plan).await?;
    } else if focus == "reviewer" {
        spawn_child(&connection, &root, "root", "reviewer", "reviewer", &plan).await?;
    } else {
        spawn_child(&connection, &root, "root", "worker", "worker", &plan).await?;
    }
    let children = plan
        .lock()
        .map_err(|_| Error::Storage("g1 plan lock poisoned".into()))?
        .children
        .clone();
    let mut paths = children.clone();
    paths.insert("root".to_owned(), root);
    settle(&harness, &children, &["ready"]).await?;
    match entry(prompt_settled(&harness, "g1-root", &brief("root"))).await? {
        PromptOutcome::Settled(_, _) => {}
        PromptOutcome::Failed(message) => {
            return Err(Error::Conflict(format!("root prompt failed: {message}")));
        }
    }
    settle(&harness, &children, &["ack", "done"]).await?;
    check(&harness, &provider, focus, &paths).await?;
    Ok(true)
}

async fn check(
    harness: &Arc<LocalHarness>,
    provider: &Arc<ScriptedModelProvider>,
    focus: &str,
    paths: &BTreeMap<String, AgentPath>,
) -> Result<()> {
    let requests = provider.requests();
    let mut by_node: BTreeMap<String, Vec<ModelRequest>> = BTreeMap::new();
    for request in requests {
        if let Some((node, _)) = node_of(&request) {
            by_node.entry(node).or_default().push(request);
        }
    }
    let requests = by_node
        .get(focus)
        .ok_or_else(|| Error::Conflict(format!("{focus} made no model request")))?;
    check_node(focus, requests).map_err(Error::Conflict)?;
    check_output(harness, paths).await
}

pub(super) fn variants(seed: u64) -> Vec<family::Variant> {
    let agent = focus(seed);
    ITEMS
        .iter()
        .map(|item| family::Variant::from([("item", *item), ("agent", agent)]))
        .collect()
}

pub(super) const GENERATOR: family::Generator = family::Generator {
    class: "G1",
    seeds: SEEDS,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_declared_role_runs_with_its_effective_configuration,
    values: u64,
    runner = super::run,
    cases = [seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3, seed_4 => 4],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}
