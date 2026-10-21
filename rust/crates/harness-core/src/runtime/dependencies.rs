use super::*;

#[doc(hidden)]
pub fn validate_task_dependencies<'a>(
    count: usize,
    task: impl Fn(usize) -> Result<(&'a str, &'a str, &'a BTreeSet<String>)>,
    available: impl Fn(&str) -> Result<bool>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let _marks_hold;
    let mut marks = crate::model::vec_with_capacity(count, io)?;
    #[cfg(feature = "filesystem")]
    {
        _marks_hold = io
            .map(|io| io.guard_owned(marks.capacity() as u64))
            .transpose()?;
    }
    marks.resize(count, 0);
    for index in 0..marks.len() {
        visit(index, &task, &available, &mut marks, io)?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn dependency_target(value: &str) -> Option<(&str, &str)> {
    value
        .rsplit_once('@')
        .filter(|(name, version)| !name.is_empty() && !version.is_empty() && !name.contains('@'))
}

fn visit<'a>(
    index: usize,
    task: &impl Fn(usize) -> Result<(&'a str, &'a str, &'a BTreeSet<String>)>,
    available: &impl Fn(&str) -> Result<bool>,
    marks: &mut [u8],
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::model::charge_items(io, 1)?;
    let invalid_index = || {
        crate::conversation::content_error(
            io,
            crate::wire::ErrorCode::Invalid,
            "task dependency index is invalid",
        )
    };
    match marks.get_mut(index).ok_or_else(invalid_index)? {
        1 => {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "task dependency cycle",
            ));
        }
        2 => return Ok(()),
        mark => *mark = 1,
    }
    for requirement in task(index)?.2 {
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(requirement, work)
        })?;
        if let Some(value) = requirement.strip_prefix("task:") {
            let (name, version) = dependency_target(value).ok_or_else(|| {
                crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "invalid task dependency",
                )
            })?;
            let mut dependency = None;
            for index in 0..marks.len() {
                crate::model::charge_items(io, 1)?;
                let (registered_name, registered_version, _) = task(index)?;
                if registered_name == name && registered_version == version {
                    dependency = Some(index);
                    break;
                }
            }
            let dependency = dependency.ok_or_else(|| {
                crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "unsatisfied task requirement",
                )
            })?;
            visit(dependency, task, available, marks, io)?;
            continue;
        }
        if !available(requirement)? {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement",
            ));
        }
    }
    *marks.get_mut(index).ok_or_else(invalid_index)? = 2;
    Ok(())
}

pub(super) fn requirement_available(
    bindings: &Bindings,
    requirement: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<bool> {
    let scope = &bindings.scope;
    let has_host = bindings.durable_host.is_some();
    let has_state = has_host || bindings.state.is_some() || bindings.execution.is_some();
    let has_spawner = has_host || bindings.spawner.is_some() || bindings.execution.is_some();
    let durable = has_host || (has_state && has_spawner);
    Ok(if let Some(value) = requirement.strip_prefix("tool:") {
        let (name, version) = dependency_target(value).ok_or_else(|| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid tool dependency",
            )
        })?;
        bindings.tools.get(name, Some(version), io)?.is_some()
            || crate::executor::with_history_io(io, |work| {
                bindings
                    .resumable_tools
                    .get(name, version, work)
                    .map(|tool| tool.is_some())
            })?
    } else if let Some(grant) = requirement.strip_prefix("grant:") {
        !grant.is_empty() && scope.grants.contains(grant)
    } else if let Some(value) = requirement.strip_prefix("extension:") {
        let (name, version) = dependency_target(value).ok_or_else(|| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid extension dependency",
            )
        })?;
        let version = version.parse::<u32>().map_err(|_| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "invalid extension dependency",
            )
        })?;
        scope.extension_runtime().is_some_and(|runtime| {
            runtime.accepts_new_admissions()
                && runtime
                    .selected()
                    .any(|identity| identity.name == name && identity.version == version)
        })
    } else {
        let created = scope.created_content(None);
        let content = created.or(bindings.content.as_ref());
        let artifacts = created.or(bindings.artifacts.as_ref());
        match requirement {
            "model" => scope.grants.contains(capability::MODEL_GENERATE),
            "context" => scope.grants.contains(capability::CONTEXT_BUILD),
            "interactions" => {
                (bindings.interactions.is_some() || durable)
                    && scope.grants.contains(capability::INTERACTION_ROUTE)
            }
            "policy" => bindings.policy.is_some(),
            "host" => durable,
            "state" => has_state,
            "spawner" => has_spawner,
            "content" => {
                content.is_some()
                    || scope
                        .created_access
                        .as_ref()
                        .is_some_and(|access| !access.content.is_empty())
            }
            "artifacts" => {
                artifacts.is_some()
                    || scope
                        .created_access
                        .as_ref()
                        .is_some_and(|access| !access.content.is_empty())
            }
            "content:write" | "artifacts:write" => {
                let binding = if requirement == "content:write" {
                    content
                } else {
                    artifacts
                };
                match binding.and_then(|binding| binding.writer.as_ref()) {
                    Some(writer) => crate::executor::with_history_io(io, |work| {
                        writer
                            .volume()
                            .capability(VolumeOperation::Write, work)
                            .map(|grant| scope.grants.contains(&grant))
                    })?,
                    None => false,
                }
            }
            _ => false,
        }
    })
}
