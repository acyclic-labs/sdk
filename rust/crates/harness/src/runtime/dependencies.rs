use super::*;

pub(super) fn validate_task_dependencies(
    bindings: &Bindings,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let mut marks = crate::model::vec_with_capacity(bindings.tasks.0.len(), io)?;
    marks.resize(bindings.tasks.0.len(), 0);
    for index in 0..marks.len() {
        visit(index, bindings, &mut marks, io)?;
    }
    Ok(())
}

fn target(value: &str) -> Option<(&str, &str)> {
    value
        .rsplit_once('@')
        .filter(|(name, version)| !name.is_empty() && !version.is_empty() && !name.contains('@'))
}

fn visit(
    index: usize,
    bindings: &Bindings,
    marks: &mut [u8],
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::model::charge_items(io, 1)?;
    match marks[index] {
        1 => {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "task dependency cycle",
            ));
        }
        2 => return Ok(()),
        _ => marks[index] = 1,
    }
    let scope = &bindings.scope;
    let has_host = bindings.durable_host.is_some();
    let has_state = has_host || bindings.state.is_some() || bindings.execution.is_some();
    let has_spawner = has_host || bindings.spawner.is_some() || bindings.execution.is_some();
    let durable = has_host || (has_state && has_spawner);
    for requirement in bindings.tasks.0[index].requirements() {
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(requirement, work)
        })?;
        if let Some(value) = requirement.strip_prefix("task:") {
            let (name, version) = target(value).ok_or_else(|| {
                crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "invalid task dependency",
                )
            })?;
            let mut dependency = None;
            for (index, entry) in bindings.tasks.0.iter().enumerate() {
                crate::model::charge_items(io, 1)?;
                if entry.identity().name == name && entry.identity().version == version {
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
            visit(dependency, bindings, marks, io)?;
            continue;
        }
        let available = if let Some(value) = requirement.strip_prefix("tool:") {
            let (name, version) = target(value).ok_or_else(|| {
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
            let (name, version) = target(value).ok_or_else(|| {
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
            match requirement.as_str() {
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
        };
        if !available {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsatisfied task requirement",
            ));
        }
    }
    marks[index] = 2;
    Ok(())
}
