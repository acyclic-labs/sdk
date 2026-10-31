//! Pinned profile files, resolved once per epoch without I/O or agent lookup.
//!
//! Tool revisions describe the model's ordered declarations. They do not bind
//! executors or grant authority. Roles select availability in the appended
//! overlay; they never rewrite the model's declaration list or system prefix.

use crate::{Error, Result, approvals::ApprovalGate, context::ModelContextCapacity};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, io::Write};

#[cfg(test)]
mod tests;

/// One capability's model-specific shape, in declaration order.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRevision {
    /// Canonical capability, independent of provider wire spelling.
    pub capability: String,
    /// Model-trained declaration shape selected by the file.
    pub variant: String,
    /// Immutable schema/behavior revision.
    pub revision: String,
}

/// Semantic reasoning baseline; provider encoding belongs to the renderer.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "mode",
    content = "effort",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Reasoning {
    /// Model has no admitted reasoning control.
    Disabled,
    /// Effort-based reasoning, with summary visibility handled by the renderer.
    Effort(Effort),
    /// Adaptive reasoning with the selected effort.
    Adaptive(Effort),
    /// Provider thinking-level control.
    Level(Effort),
}

/// Profile effort, independent of the provider/model identity.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effort {
    /// Low effort.
    Low,
    /// Medium effort.
    Medium,
    /// High effort.
    High,
    /// Adaptive effort chosen through the model's admitted control.
    Adaptive,
}

/// Cache semantics selected by the model file, encoded by the renderer.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cache {
    /// Provider-managed implicit cache.
    Automatic,
    /// Short-lived explicit breakpoint.
    Ephemeral,
    /// Hour-lived explicit breakpoint.
    Hour,
    /// Model-specific retained cache object.
    Explicit,
}

/// Context carried by a role. `TopLevel` refuses child admission.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpawnContext {
    /// A root role that cannot be admitted as a child.
    TopLevel,
    /// Only the self-contained brief and explicitly sent artifacts.
    Fresh,
    /// A stream fork at the parent's conversation cut plus the brief.
    Fork,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelLayer {
    version: u32,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    tools: Option<Vec<ToolRevision>>,
    #[serde(default)]
    reasoning: Option<Reasoning>,
    #[serde(default)]
    cache: Option<Cache>,
    #[serde(default)]
    budget_tokens: Option<u32>,
    #[serde(default)]
    watermark_tokens: Option<u32>,
    #[serde(default)]
    output_budget: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Role {
    version: u32,
    name: String,
    model: String,
    context: SpawnContext,
    may_spawn: Vec<String>,
    /// Capabilities, not a replacement declaration list. Absent means all.
    #[serde(default)]
    allowed_tools: Option<Vec<String>>,
    #[serde(default)]
    effort: Option<Effort>,
    #[serde(default)]
    max_output_tokens: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Override {
    version: u32,
    #[serde(default)]
    budget_tokens: Option<u32>,
    #[serde(default)]
    watermark_tokens: Option<u32>,
}

struct File<T> {
    metadata: T,
    body: String,
    digest: [u8; 32],
}

impl<T: DeserializeOwned> File<T> {
    fn parse(source: &str) -> Result<Self> {
        let (metadata, body) = crate::context::parse_frontmatter_with_body(source.as_bytes())?;
        Ok(Self {
            metadata,
            body: source
                .get(body..)
                .ok_or_else(|| invalid("invalid body boundary"))?
                .to_owned(),
            digest: *blake3::hash(source.as_bytes()).as_bytes(),
        })
    }
}

/// Family and model files plus the original provider-verified hard capacities.
/// Files are immutable after parsing; edits become a new epoch's input.
pub struct ModelProfile {
    family: File<ModelLayer>,
    model: File<ModelLayer>,
    capacity: ModelContextCapacity,
    renderer_revision: String,
}

impl ModelProfile {
    /// Parses `models/<family>/_family.md` and its selected model file.
    /// The caller supplies the actual renderer revision and verified capacity;
    /// profile text cannot advertise a larger provider capacity.
    pub fn parse(
        family: &str,
        model: &str,
        capacity: ModelContextCapacity,
        renderer_revision: &str,
    ) -> Result<Self> {
        capacity.validate()?;
        nonempty(renderer_revision)?;
        let family = File::<ModelLayer>::parse(family)?;
        let model = File::<ModelLayer>::parse(model)?;
        version(family.metadata.version)?;
        version(model.metadata.version)?;
        let key = model
            .metadata
            .key
            .as_deref()
            .ok_or_else(|| invalid("model key is absent"))?;
        nonempty(key)?;
        for layer in [&family.metadata, &model.metadata] {
            if let Some(tools) = &layer.tools {
                let mut seen = BTreeSet::new();
                for tool in tools {
                    nonempty(&tool.capability)?;
                    nonempty(&tool.variant)?;
                    nonempty(&tool.revision)?;
                    if !seen.insert(&tool.capability) {
                        return Err(invalid("duplicate capability declaration"));
                    }
                }
            }
        }
        Ok(Self {
            family,
            model,
            capacity,
            renderer_revision: renderer_revision.into(),
        })
    }
}

/// A role file selects a model/profile and describes policy and context.
pub struct RoleFile(File<Role>);

impl RoleFile {
    /// Parses a versioned role file without resolving or opening its model.
    pub fn parse(source: &str) -> Result<Self> {
        let file = File::<Role>::parse(source)?;
        version(file.metadata.version)?;
        nonempty(&file.metadata.name)?;
        nonempty(&file.metadata.model)?;
        unique(&file.metadata.may_spawn)?;
        if let Some(tools) = &file.metadata.allowed_tools {
            unique(tools)?;
        }
        Ok(Self(file))
    }

    /// Profile or alloy selection to resolve through the existing provider owner.
    pub fn model(&self) -> &str {
        &self.0.metadata.model
    }

    /// Context policy is data from the role file, not a role-name switch.
    pub fn context(&self) -> SpawnContext {
        self.0.metadata.context
    }

    /// Called at the existing child admission boundary, before a fork is made.
    pub fn admit_child(&self, child: &Self) -> Result<SpawnContext> {
        if child.context() == SpawnContext::TopLevel
            || !self.0.metadata.may_spawn.contains(&child.0.metadata.name)
        {
            return Err(invalid("role refuses this spawn"));
        }
        Ok(child.context())
    }
}

/// `project/.harness/profile.md`: project instruction and capacity layer.
pub struct ProjectLayer(File<Override>);
/// Agent-local versioned override with the owner's already bound approval gate.
/// Agent-writable profile text cannot replace the owner's policy.
pub struct AgentOverride<'a> {
    file: File<Override>,
    approvals: &'a ApprovalGate,
}

impl ProjectLayer {
    /// Parses the project layer; tool/approval replacement fields refuse.
    pub fn parse(source: &str) -> Result<Self> {
        let file = File::<Override>::parse(source)?;
        version(file.metadata.version)?;
        Ok(Self(file))
    }
}
impl<'a> AgentOverride<'a> {
    /// Parses agent text while retaining the owner's original admission gate.
    pub fn parse(source: &str, approvals: &'a ApprovalGate) -> Result<Self> {
        let file = File::<Override>::parse(source)?;
        version(file.metadata.version)?;
        Ok(Self { file, approvals })
    }
}

/// Total context admission and summary-preparation boundary, in tokens.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct Capacity {
    /// Hard capacities from the original selected provider.
    pub model: ModelContextCapacity,
    /// User-admitted total input plus output reservation.
    pub budget_tokens: u32,
    /// Input boundary for summary preparation, below the output-reserved budget.
    pub watermark_tokens: u32,
    /// Model profile's generated-output reservation.
    pub output_budget: u32,
}

/// Frozen model-facing values. Role directives/availability are excluded.
#[derive(Debug, PartialEq, Serialize)]
pub struct PrefixProfile<'a> {
    /// Logical model profile key.
    pub key: &'a str,
    /// Base, family, selected model and project instructions, in order.
    pub system_layers: [&'a str; 4],
    /// Fixed ordered declaration revisions, identical across roles.
    pub tools: &'a [ToolRevision],
    /// Epoch baseline; role effort is a separate appended overlay.
    pub reasoning: Reasoning,
    /// Epoch cache semantics.
    pub cache: Cache,
    /// Verified hard bounds and admitted context reservations.
    pub capacity: Capacity,
    /// Original renderer revision, never inferred from profile text.
    pub renderer_revision: &'a str,
}

/// Appended role/availability update; it never changes the declaration list.
pub struct RoleOverlay<'a> {
    /// Role identity from its file.
    pub name: &'a str,
    /// Role directive followed by agent-local updates.
    pub directives: [&'a str; 2],
    /// Absent means every declared capability is available.
    pub allowed_tools: Option<&'a [String]>,
    /// Appended effort update, encoded only by a supporting provider binding.
    pub effort: Option<Effort>,
    /// Effective output reservation after the role's optional override.
    pub output_budget: u32,
    /// The existing owner-bound gate; text cannot create a replacement.
    pub approval_policy: &'a ApprovalGate,
}

/// Borrowed resolution result; no layer bodies or declaration vectors are copied.
pub struct AgentProfile<'a> {
    /// Pins every input layer, the ordered revisions and renderer revision.
    digest: [u8; 32],
    /// Equal for equal rendered prefix values, independently of role overlays.
    prefix_digest: [u8; 32],
    prefix: PrefixProfile<'a>,
    overlay: RoleOverlay<'a>,
}

impl<'a> AgentProfile<'a> {
    /// Identity pinned by the actor's existing epoch/checkpoint publication.
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    /// Frozen prefix identity, excluding the appended role and policy overlay.
    pub const fn prefix_digest(&self) -> [u8; 32] {
        self.prefix_digest
    }
    /// Borrows immutable resolved values; the pinned result cannot be rewritten.
    pub const fn prefix(&self) -> &PrefixProfile<'a> {
        &self.prefix
    }
    /// Borrows the appended directive/availability/policy update.
    pub const fn overlay(&self) -> &RoleOverlay<'a> {
        &self.overlay
    }
}

fn capacity(model: &ModelProfile, project: &Override, agent: &Override) -> Result<Capacity> {
    let family = &model.family.metadata;
    let selected = &model.model.metadata;
    let budget = agent
        .budget_tokens
        .or(project.budget_tokens)
        .or(selected.budget_tokens)
        .or(family.budget_tokens)
        .unwrap_or(model.capacity.context_tokens);
    let watermark = agent
        .watermark_tokens
        .or(project.watermark_tokens)
        .or(selected.watermark_tokens)
        .or(family.watermark_tokens)
        .ok_or_else(|| invalid("profile watermark is absent"))?;
    let output = selected
        .output_budget
        .or(family.output_budget)
        .or(model.capacity.output_tokens)
        .ok_or_else(|| invalid("unknown output capacity requires a profile output budget"))?;
    if budget == 0
        || budget > model.capacity.context_tokens
        || watermark == 0
        || output >= budget
        || watermark >= budget - output
        || !model.capacity.permits_output(output)
    {
        return Err(invalid(
            "profile budget, watermark or output reservation exceeds capacity",
        ));
    }
    Ok(Capacity {
        model: model.capacity,
        budget_tokens: budget,
        watermark_tokens: watermark,
        output_budget: output,
    })
}

/// Resolves pinned inputs once per epoch. Work is bounded by the admitted
/// profile declarations, independent of conversation length and agent count.
/// No executor, filesystem read, wait, model call or token estimate is created.
pub fn resolve<'a>(
    model: &'a ModelProfile,
    role: &'a RoleFile,
    project: &'a ProjectLayer,
    agent: &'a AgentOverride<'a>,
) -> Result<AgentProfile<'a>> {
    let family = &model.family.metadata;
    let selected = &model.model.metadata;
    let role_meta = &role.0.metadata;
    let capacity = capacity(model, &project.0.metadata, &agent.file.metadata)?;
    let role_output = match role_meta.max_output_tokens {
        Some(output) => {
            if output >= capacity.budget_tokens
                || capacity.watermark_tokens >= capacity.budget_tokens - output
                || !model.capacity.permits_output(output)
            {
                return Err(invalid("role output reservation exceeds capacity"));
            }
            output
        }
        None => capacity.output_budget,
    };
    let prefix = PrefixProfile {
        key: selected
            .key
            .as_deref()
            .ok_or_else(|| invalid("model key is absent"))?,
        system_layers: [
            crate::preset::AGENT_PROMPT,
            &model.family.body,
            &model.model.body,
            &project.0.body,
        ],
        tools: selected
            .tools
            .as_deref()
            .or(family.tools.as_deref())
            .ok_or_else(|| invalid("profile tool declarations are absent"))?,
        reasoning: selected
            .reasoning
            .or(family.reasoning)
            .ok_or_else(|| invalid("profile reasoning baseline is absent"))?,
        cache: selected
            .cache
            .or(family.cache)
            .ok_or_else(|| invalid("profile cache strategy is absent"))?,
        capacity,
        renderer_revision: &model.renderer_revision,
    };
    // Hash the actual borrowed prefix, without a temporary JSON buffer. Fixed
    // struct fields and ordered vectors have no map-iteration dependency.
    let mut prefix_hash = HashWriter(blake3::Hasher::new());
    serde_json::to_writer(&mut prefix_hash, &prefix)
        .map_err(|error| invalid(&format!("profile encoding failed: {error}")))?;
    let prefix_digest = *prefix_hash.0.finalize().as_bytes();
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.agent-profile.v1");
    for digest in [
        prefix_digest,
        model.family.digest,
        model.model.digest,
        role.0.digest,
        project.0.digest,
        agent.file.digest,
        agent.approvals.identity_ref().digest,
    ] {
        hash.update(&digest);
    }
    Ok(AgentProfile {
        digest: *hash.finalize().as_bytes(),
        prefix_digest,
        prefix,
        overlay: RoleOverlay {
            name: &role_meta.name,
            directives: [&role.0.body, &agent.file.body],
            allowed_tools: role_meta.allowed_tools.as_deref(),
            effort: role_meta.effort,
            output_budget: role_output,
            approval_policy: agent.approvals,
        },
    })
}

struct HashWriter(blake3::Hasher);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn invalid(reason: &str) -> Error {
    Error::Invalid(format!("profile: {reason}"))
}
fn nonempty(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid("empty identity"));
    }
    Ok(())
}
fn version(value: u32) -> Result<()> {
    if value != 1 {
        return Err(invalid("unsupported profile version; use version 1"));
    }
    Ok(())
}
fn unique(values: &[String]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        nonempty(value)?;
        if !seen.insert(value) {
            return Err(invalid("duplicate role capability or spawn target"));
        }
    }
    Ok(())
}
