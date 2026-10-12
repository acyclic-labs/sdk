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

/// Validates an immutable task dependency graph and its provider requirements.
pub(crate) fn validate_task_requirements(
    tasks: &BTreeMap<(String, String), BTreeSet<String>>,
    tools: &BTreeSet<(String, String)>,
    environment: &TaskDependencyEnvironment,
    grants: &BTreeSet<String>,
    io: StartupIo<'_>,
) -> Result<()> {
    let mut marks = BTreeMap::new();
    for key in tasks.keys() {
        visit_task_dependency(key, &mut marks, tasks, tools, environment, grants, io)?;
    }
    Ok(())
}

pub(super) fn visit_task_dependency(
    key: &(String, String),
    marks: &mut BTreeMap<(String, String), u8>,
    tasks: &BTreeMap<(String, String), BTreeSet<String>>,
    tools: &BTreeSet<(String, String)>,
    environment: &TaskDependencyEnvironment,
    grants: &BTreeSet<String>,
    io: StartupIo<'_>,
) -> Result<()> {
    native_startup::item(io, 0)?;
    match marks.get(key).copied() {
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
    let requirements = tasks.get(key).ok_or_else(|| {
        native_startup::error(
            io,
            crate::wire::ErrorCode::Invalid,
            "unsatisfied task requirement",
        )
    })?;
    native_startup::item(io, std::mem::size_of::<((String, String), u8)>())?;
    marks.insert(
        (
            native_startup::text(&key.0, io)?,
            native_startup::text(&key.1, io)?,
        ),
        1,
    );
    for requirement in requirements {
        validate_requirement(requirement, marks, tasks, tools, environment, grants, io)?;
    }
    *marks
        .get_mut(key)
        .expect("the original task was marked before walking its dependencies") = 2;
    Ok(())
}

pub(super) fn validate_requirement(
    requirement: &str,
    marks: &mut BTreeMap<(String, String), u8>,
    tasks: &BTreeMap<(String, String), BTreeSet<String>>,
    tools: &BTreeSet<(String, String)>,
    environment: &TaskDependencyEnvironment,
    grants: &BTreeSet<String>,
    io: StartupIo<'_>,
) -> Result<()> {
    native_startup::item(io, 0)?;
    if let Some(target) = parse_dependency_target(requirement, "task:", io)? {
        if !tasks.contains_key(&target) {
            return Err(native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement",
            ));
        }
        return visit_task_dependency(&target, marks, tasks, tools, environment, grants, io);
    }
    if let Some(target) = parse_dependency_target(requirement, "tool:", io)? {
        return tools.contains(&target).then_some(()).ok_or_else(|| {
            native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied tool requirement",
            )
        });
    }
    if provider_requirement_satisfied(requirement, environment, grants, io)? {
        return Ok(());
    }
    Err(native_startup::error(
        io,
        crate::wire::ErrorCode::Invalid,
        "unsatisfied task requirement",
    ))
}

pub(super) fn provider_requirement_satisfied(
    requirement: &str,
    environment: &TaskDependencyEnvironment,
    grants: &BTreeSet<String>,
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
        return if available {
            Ok(true)
        } else {
            Err(native_startup::error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement",
            ))
        };
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
                .contains(&(native_startup::text(name, io)?, version)));
    }
    Ok(false)
}

pub(super) fn parse_dependency_target(
    requirement: &str,
    prefix: &str,
    io: StartupIo<'_>,
) -> Result<Option<(String, String)>> {
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
    Ok(Some((
        native_startup::text(target.0, io)?,
        native_startup::text(target.1, io)?,
    )))
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
    let mut task_requirements = BTreeMap::new();
    for entry in &tasks.0 {
        native_startup::item(
            io,
            std::mem::size_of::<((String, String), BTreeSet<String>)>(),
        )?;
        let mut requirements = BTreeSet::new();
        for requirement in entry.requirements() {
            native_startup::item(io, std::mem::size_of::<String>())?;
            requirements.insert(native_startup::text(requirement, io)?);
        }
        task_requirements.insert(
            (
                native_startup::text(&entry.identity().name, io)?,
                native_startup::text(&entry.identity().version, io)?,
            ),
            requirements,
        );
    }
    let mut available_tools = BTreeSet::new();
    for requirement in task_requirements.values().flat_map(BTreeSet::iter) {
        let Some((name, version)) = requirement
            .strip_prefix("tool:")
            .and_then(|value| value.rsplit_once('@'))
        else {
            continue;
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut history = io.map(|io| io.history_io());
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let preparation = io;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let preparation = None;
        let available = tools.get_version(name, version, preparation)?.is_some()
            || resumable_tools.get(name, version, &mut work)?.is_some();
        if available {
            native_startup::item(io, std::mem::size_of::<(String, String)>())?;
            available_tools.insert((
                native_startup::text(name, io)?,
                native_startup::text(version, io)?,
            ));
        }
    }
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
    let mut grants = BTreeSet::new();
    for grant in scope.grants.iter() {
        native_startup::item(io, std::mem::size_of::<String>())?;
        grants.insert(native_startup::text(grant, io)?);
    }
    validate_task_requirements(
        &task_requirements,
        &available_tools,
        &environment,
        &grants,
        io,
    )
}
