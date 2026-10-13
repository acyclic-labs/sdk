use super::*;

/// Provider capabilities visible to task dependency admission.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskDependencyEnvironment {
    /// A model provider is bound for this composition.
    pub model: bool,
    /// A context builder is bound for this composition.
    pub context: bool,
    /// An interaction router is bound for this composition.
    pub interactions: bool,
    /// A policy provider is bound for this composition.
    pub policy: bool,
    /// A durable host is bound for this composition.
    pub host: bool,
    /// Owner-retained task state is bound for this composition.
    pub state: bool,
    /// Durable admission is bound for this composition.
    pub spawner: bool,
    /// Owner-retained content is bound for this composition.
    pub content: bool,
    /// Owner-retained artifacts are bound for this composition.
    pub artifacts: bool,
    /// The content writer and its capability grant are both available.
    pub content_write: bool,
    /// The artifact writer and its capability grant are both available.
    pub artifacts_write: bool,
    /// Exact extension versions selected for this composition.
    pub extensions: BTreeSet<(String, u32)>,
}

/// Validates exact task versions, cycles and provider requirements.
pub(crate) fn validate_task_requirements<'a>(
    definitions: impl IntoIterator<Item = ((&'a str, &'a str), &'a BTreeSet<String>)>,
    has_tool: impl Fn(&str, &str) -> Result<bool>,
    environment: &TaskDependencyEnvironment,
    grants: &Capabilities,
    io: StartupIo<'_>,
) -> Result<()> {
    let mut tasks = BTreeMap::new();
    for (key, requirements) in definitions {
        native_startup::item(io, std::mem::size_of_val(&(key, requirements)))?;
        tasks.insert(key, requirements);
    }
    let mut marks = BTreeMap::new();
    for key in tasks.keys() {
        visit_task_dependency(*key, &mut marks, &tasks, &has_tool, environment, grants, io)?;
    }
    Ok(())
}

fn visit_task_dependency<'a>(
    key: (&'a str, &'a str),
    marks: &mut BTreeMap<(&'a str, &'a str), u8>,
    tasks: &BTreeMap<(&'a str, &'a str), &'a BTreeSet<String>>,
    has_tool: &dyn Fn(&str, &str) -> Result<bool>,
    environment: &TaskDependencyEnvironment,
    grants: &Capabilities,
    io: StartupIo<'_>,
) -> Result<()> {
    native_startup::item(io, 0)?;
    match marks.get(&key) {
        Some(1) => {
            return Err(native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "task dependency cycle",
            ));
        }
        Some(2) => return Ok(()),
        _ => {}
    }
    let requirements = *tasks.get(&key).ok_or_else(|| {
        native_startup::error(
            io,
            crate::wire::ErrorCode::Invalid,
            "unsatisfied task requirement",
        )
    })?;
    native_startup::item(io, std::mem::size_of_val(&(key, 1_u8)))?;
    marks.insert(key, 1);
    for requirement in requirements {
        let available = if let Some(target) = parse_dependency_target(requirement, "task:", io)? {
            visit_task_dependency(target, marks, tasks, has_tool, environment, grants, io)?;
            true
        } else if let Some((name, version)) = parse_dependency_target(requirement, "tool:", io)? {
            has_tool(name, version)?
        } else {
            provider_requirement_satisfied(requirement, environment, grants, io)?
        };
        if !available {
            return Err(native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement",
            ));
        }
    }
    *marks
        .get_mut(&key)
        .expect("task marked before visiting dependencies") = 2;
    Ok(())
}

fn provider_requirement_satisfied(
    requirement: &str,
    environment: &TaskDependencyEnvironment,
    grants: &Capabilities,
    io: StartupIo<'_>,
) -> Result<bool> {
    let available = match requirement {
        "model" => Some(environment.model),
        "context" => Some(environment.context),
        "interactions" => Some(environment.interactions),
        "policy" => Some(environment.policy),
        "host" => Some(environment.host),
        "state" => Some(environment.state),
        "spawner" => Some(environment.spawner),
        "content" => Some(environment.content),
        "artifacts" => Some(environment.artifacts),
        "content:write" => Some(environment.content_write),
        "artifacts:write" => Some(environment.artifacts_write),
        _ => None,
    };
    if let Some(available) = available {
        return Ok(available);
    }
    if let Some(grant) = requirement.strip_prefix("grant:") {
        return Ok(!grant.is_empty() && grants.contains(grant));
    }
    if let Some(extension) = requirement.strip_prefix("extension:") {
        let (name, version) = extension.rsplit_once('@').ok_or_else(|| {
            native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid extension dependency",
            )
        })?;
        let version = version.parse::<u32>().map_err(|_| {
            native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid extension dependency",
            )
        })?;
        return Ok(!name.is_empty()
            && !name.contains('@')
            && environment
                .extensions
                .iter()
                .any(|(selected, revision)| selected == name && *revision == version));
    }
    Ok(false)
}

fn parse_dependency_target<'a>(
    requirement: &'a str,
    prefix: &str,
    io: StartupIo<'_>,
) -> Result<Option<(&'a str, &'a str)>> {
    let Some(value) = requirement.strip_prefix(prefix) else {
        return Ok(None);
    };
    let target = value
        .rsplit_once('@')
        .filter(|(name, version)| !name.is_empty() && !version.is_empty() && !name.contains('@'))
        .ok_or_else(|| {
            native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid task dependency",
            )
        })?;
    Ok(Some(target))
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "validates the complete immutable task dependency graph"
)]
pub(super) fn validate_task_dependencies(
    tasks: &TaskRegistry,
    tools: &ToolRegistry,
    resumable_tools: &ResumableToolRegistry,
    scope: &RuntimeScope,
    has_host: bool,
    has_state: bool,
    has_spawner: bool,
    has_interactions: bool,
    content: Option<&ContentBindings>,
    artifacts: Option<&ArtifactBindings>,
    has_policy: bool,
    io: StartupIo<'_>,
) -> Result<()> {
    let created_default = scope.created_content(None);
    let content = created_default.or(content);
    let artifacts = created_default.or(artifacts);
    let created_read = scope
        .created_access
        .as_ref()
        .is_some_and(|access| !access.content.is_empty());
    let content_write = native_startup::content_write(content, scope, io)?;
    let artifacts_write = native_startup::content_write(artifacts, scope, io)?;
    let mut extensions = BTreeSet::new();
    if let Some(runtime) = scope
        .extension_runtime()
        .filter(|runtime| runtime.accepts_new_admissions())
    {
        for identity in runtime.selected() {
            native_startup::item(io, std::mem::size_of::<(String, u32)>())?;
            extensions.insert((native_startup::text(&identity.name, io)?, identity.version));
        }
    }
    let environment = TaskDependencyEnvironment {
        model: scope.grants.contains(capability::MODEL_GENERATE),
        context: scope.grants.contains(capability::CONTEXT_BUILD),
        interactions: (has_interactions || has_host)
            && scope.grants.contains(capability::INTERACTION_ROUTE),
        policy: has_policy,
        host: has_host,
        state: has_state,
        spawner: has_spawner,
        content: content.is_some() || created_read,
        artifacts: artifacts.is_some() || created_read,
        content_write,
        artifacts_write,
        extensions,
    };
    validate_task_requirements(
        tasks.0.iter().map(|entry| {
            (
                (
                    entry.identity().name.as_str(),
                    entry.identity().version.as_str(),
                ),
                entry.requirements(),
            )
        }),
        |name, version| {
            crate::executor::with_history_io(io, |work| {
                Ok(tools.get(name, Some(version), io)?.is_some()
                    || resumable_tools.get(name, version, work)?.is_some())
            })
        },
        &environment,
        scope.grants(),
        io,
    )
}
