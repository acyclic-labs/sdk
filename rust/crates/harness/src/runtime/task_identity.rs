use super::*;

#[derive(Serialize)]
pub(super) struct TaskDefinitionDigest<'a> {
    pub(super) contract: &'static str,
    pub(super) name: &'a str,
    pub(super) version: &'a str,
    pub(super) input_schema: &'a Value,
    pub(super) output_schema: &'a Value,
    pub(super) requirements: &'a BTreeSet<String>,
    pub(super) machine_digest: Option<[u8; 32]>,
}

/// Computes the pinned task identity using the caller's IO context.
pub(crate) fn task_definition_digest(
    name: &str,
    version: &str,
    input_schema: &Value,
    output_schema: &Value,
    requirements: &BTreeSet<String>,
    machine_digest: Option<[u8; 32]>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<[u8; 32]> {
    validate_identity(name, version, io)?;
    crate::executor::with_history_io(io, |work| {
        validate_task_schemas(input_schema, output_schema, machine_digest.is_some(), work)
    })?;
    let mut invalid = machine_digest == Some([0; 32]);
    if !invalid {
        for value in requirements {
            inspect_task_metadata(io, 1)?;
            if value.is_empty() || value.chars().any(char::is_whitespace) {
                invalid = true;
                break;
            }
        }
    }
    if invalid {
        return Err(task_workflow_error(
            task_original_work(io)?,
            crate::wire::ErrorCode::Invalid,
            "task dependency or machine digest is invalid",
        ));
    }
    crate::executor::preparation_digest(
        &TaskDefinitionDigest {
            contract: "harness.task.v2",
            name,
            version,
            input_schema,
            output_schema,
            requirements,
            machine_digest,
        },
        io,
    )
}

pub(crate) fn scope_metadata_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

/// Builds the two identities that durable hosts retain for one resumable task.

pub fn task_admission_identities(
    name: &str,
    version: &str,
    input_schema: &Value,
    output_schema: &Value,
    requirements: &BTreeSet<String>,
    machine_digest: &[u8],
) -> Result<(ComponentIdentity, MachineIdentity)> {
    let machine_digest: [u8; 32] = machine_digest
        .try_into()
        .map_err(|_| Error::Invalid("machine digest must contain exactly 32 bytes".into()))?;
    let task_digest = task_definition_digest(
        name,
        version,
        input_schema,
        output_schema,
        requirements,
        Some(machine_digest),
        None,
    )?;
    Ok((
        ComponentIdentity {
            name: name.to_owned(),
            version: version.to_owned(),
            digest: task_digest,
        },
        MachineIdentity {
            name: name.to_owned(),
            version: version.to_owned(),
            digest: machine_digest,
        },
    ))
}

pub(crate) fn validate_task_schemas(
    input: &Value,
    output: &Value,
    resumable: bool,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    for (label, schema, refusal) in [
        (
            "task input",
            input,
            "resumable task input schema cannot accept every value",
        ),
        (
            "task output",
            output,
            "resumable task output schema cannot accept every value",
        ),
    ] {
        crate::executor::compile_json_schema(schema, label, work)?;
        if resumable
            && (schema == &Value::Bool(true)
                || schema.as_object().is_some_and(serde_json::Map::is_empty))
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                refusal,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_identity(
    name: &str,
    version: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() {
        inspect_task_metadata(io, 2)?;
        if !crate::contract::is_valid_component_label(name)
            || !crate::contract::is_valid_component_label(version)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task name or version is invalid",
            });
        }
    } else {
        validate_component_label(name, "task name")?;
        validate_component_label(version, "task version")?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        validate_component_label(name, "task name")?;
        validate_component_label(version, "task version")?;
    }
    if name.contains('@') || version.contains('@') {
        return Err(task_workflow_error(
            task_original_work(io)?,
            crate::wire::ErrorCode::Invalid,
            "task name and version cannot contain the version separator",
        ));
    }
    Ok(())
}

/// Runs closed physical field producers against the original preparation ledger.

pub(super) fn task_copy_with_io<T>(
    io: Option<&crate::executor::PreparationIo>,
    copy: impl FnOnce(&mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
) -> Result<T> {
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(|io| io.history_io());
    #[cfg(feature = "filesystem")]
    let mut work = observer
        .as_mut()
        .map(|io| io as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work = {
        let _ = io;
        None
    };
    let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
    let value = copy(&mut quota)?;
    quota.transfer();
    Ok(value)
}

pub(super) fn task_copy_text(
    source: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<String> {
    task_copy_with_io(io, |quota| crate::core::copy_text(source, quota))
}

pub(super) fn task_copy_value(
    source: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Value> {
    task_copy_with_io(io, |quota| crate::core::copy_value(source, quota))
}

pub(super) fn task_copy_component(
    source: &ComponentIdentity,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<ComponentIdentity> {
    Ok(ComponentIdentity {
        name: crate::core::copy_text(&source.name, quota)?,
        version: crate::core::copy_text(&source.version, quota)?,
        digest: source.digest,
    })
}

pub(super) fn task_copy_machine(
    source: &MachineIdentity,
    quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<MachineIdentity> {
    Ok(MachineIdentity {
        name: crate::core::copy_text(&source.name, quota)?,
        version: crate::core::copy_text(&source.version, quota)?,
        digest: source.digest,
    })
}
