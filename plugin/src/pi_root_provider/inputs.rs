//! Immutable inputs on the original task's private volume.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

pub(super) struct NativeInputCoordinates {
    pub(super) path: (String, NativeAllocation),
    pub(super) display_name: (String, NativeAllocation),
    pub(super) key: (IdempotencyKey, NativeAllocation),
}

fn maximum(owner: &PiOwnerConfig) -> u64 {
    owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES)
}

pub(super) fn write_grant(
    owner: &PiOwnerConfig,
    work: &NativeMaterializationWork,
) -> Result<(ContentGrant, NativeAllocation)> {
    let authority = owner.authority(work)?;
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    let grant = ContentGrant::verify(
        &authority.verifier,
        &authority.signed,
        &owner.policy().journal,
        VolumeOperation::Write,
        owner.policy().maximum_payload_bytes,
        &mut Some(&mut io),
    )?;
    Ok((grant, io.into_custody()?))
}

fn validate_input(
    owner: &PiOwnerConfig,
    file: &FileRef,
    coordinates: &NativeInputCoordinates,
    media: &str,
) -> Result<()> {
    owner.policy().limits.validate_file(file, &mut None)?;
    if file.volume() != &owner.policy().journal
        || file.path() != coordinates.path.0
        || file.display_name() != coordinates.display_name.0
        || file.descriptor().media_type() != media
        || file.descriptor().byte_length() > maximum(owner)
    {
        return Err(Error::Invalid(
            "Pi original input operation returned another immutable file".into(),
        ));
    }
    Ok(())
}

fn coordinates(
    owner: &PiOwnerConfig,
    operation: OperationId,
    label: &str,
    directory: &'static str,
    purpose: &'static str,
    work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    work.cancellation()
        .check()
        .map_err(|_| NativeTreeMaterializationError::Cancelled)?;
    if label.is_empty()
        || label.len() > 64
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "Pi input label must be one bounded path component",
        });
    }
    let display_name = work.display(&format_args!("{label}.json"))?;
    let path = work.display(&format_args!("{directory}/{operation}/{}", display_name.0))?;
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "Pi input path exceeds its original owner limit",
        });
    }
    let key = work.display(&format_args!(
        "acyclic.pi/{purpose}/{}/{operation}/{label}",
        owner.policy().root_task
    ))?;
    Ok(NativeInputCoordinates {
        path,
        display_name,
        key: (IdempotencyKey::new(key.0)?, key.1),
    })
}

pub(crate) fn stage_pi_input<'a, T: Serialize + Sync>(
    owner: &'a PiOwnerConfig,
    filesystem: &'a Host,
    operation: OperationId,
    label: &'a str,
    value: &'a T,
    work: &'a NativeMaterializationWork,
) -> impl std::future::Future<Output = Result<(FileRef, NativeAllocation)>> + Send + 'a {
    let coordinates = coordinates(owner, operation, label, ROOT_INPUT_DIRECTORY, "input", work);
    async move {
        publish_pi_input_with_native_coordinates(
            owner,
            filesystem,
            value,
            "application/json",
            coordinates?,
            work,
        )
        .await
    }
}

pub(crate) async fn stage_pi_operator_input<T: Serialize + Sync>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    let coordinates = coordinates(
        owner,
        operation,
        label,
        OPERATOR_INPUT_DIRECTORY,
        "operator-input",
        work,
    )?;
    publish_pi_input_with_native_coordinates(
        owner,
        filesystem,
        value,
        "application/json",
        coordinates,
        work,
    )
    .await
}

pub(crate) async fn observe_pi_input(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let coordinates = coordinates(owner, operation, label, ROOT_INPUT_DIRECTORY, "input", work)?;
    let grant = write_grant(owner, work)?;
    observe_pi_input_at(
        owner,
        filesystem,
        coordinates,
        "application/json",
        &grant.0,
        work,
    )
    .await
}

fn root_media(name: &str) -> Result<&'static str> {
    match name {
        "root" | "preparation" => Ok("application/json"),
        "allocation-schema" => Ok("application/schema+json"),
        _ => Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "unknown original Pi root artifact",
        }),
    }
}

fn root_coordinates(
    owner: &PiOwnerConfig,
    name: &str,
    work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    root_media(name)?;
    let display_name = work.display(&format_args!("{name}.json"))?;
    let path = work.display(&format_args!("{ROOT_INPUT_DIRECTORY}/{}", display_name.0))?;
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "original Pi root artifact path exceeds owner limit",
        });
    }
    let key = work.display(&format_args!(
        "acyclic.pi/{name}-input/{}",
        owner.policy().root_task
    ))?;
    Ok(NativeInputCoordinates {
        path,
        display_name,
        key: (IdempotencyKey::new(key.0)?, key.1),
    })
}

pub(crate) async fn stage_pi_root_input_with_native_work<T: Serialize + Sync>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    name: &str,
    value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    publish_pi_input_with_native_coordinates(
        owner,
        filesystem,
        value,
        root_media(name)?,
        root_coordinates(owner, name, work)?,
        work,
    )
    .await
}

pub(super) async fn observe_pi_input_at(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    coordinates: NativeInputCoordinates,
    media: &str,
    grant: &ContentGrant,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let io = PreparationIo::from_native_work(work)?;
    let Some(reference) = filesystem
        .content_for_operation(
            &owner.policy().journal,
            grant,
            &coordinates.key.0,
            &coordinates.path.0,
            maximum(owner),
            None,
            Some(&io),
        )
        .await?
    else {
        return Ok(None);
    };
    validate_input(owner, &reference, &coordinates, media)?;
    Ok(Some((
        reference,
        acyclic_harness::store::HistoryWork::new(io).into_custody()?,
    )))
}

pub(super) fn publish_pi_input_with_native_coordinates<'a, T: Serialize + Sync>(
    owner: &'a PiOwnerConfig,
    filesystem: &'a Host,
    value: &'a T,
    media: &'a str,
    coordinates: NativeInputCoordinates,
    work: &'a NativeMaterializationWork,
) -> impl std::future::Future<Output = Result<(FileRef, NativeAllocation)>> + Send + 'a {
    async move {
        let grant = write_grant(owner, work)?;
        let encoded = guarded_pi_bytes(value, maximum(owner), work)?;
        let length = encoded.0.len() as u64;
        let io = PreparationIo::from_native_work(work)?;
        let result = filesystem
            .put_content(
                &owner.policy().journal,
                &grant.0,
                &coordinates.path.0,
                &encoded.0,
                media,
                &coordinates.display_name.0,
                maximum(owner),
                &coordinates.key.0,
                None,
                None,
                Some(&io),
            )
            .await;
        drop(encoded);
        let reference = result?;
        validate_input(owner, &reference, &coordinates, media)?;
        if reference.descriptor().byte_length() != length {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Storage,
                reason: "Pi input original body length differs",
            });
        }
        Ok((
            reference,
            acyclic_harness::store::HistoryWork::new(io).into_custody()?,
        ))
    }
}

pub(super) async fn read_root_artifact_with_native_work<T: DeserializeOwned + Serialize>(
    owner: &PiOwnerConfig,
    context: &TaskContext,
    reference: &FileRef,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    if reference.descriptor().byte_length() > maximum(owner)
        || reference.path().len() > owner.policy().limits.path_bytes
        || !matches!(
            reference.descriptor().media_type(),
            "application/json" | "application/schema+json"
        )
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "root artifact exceeds original header bounds or media type",
        });
    }
    let io = PreparationIo::from_native_work(work)?;
    let bytes = context.read_file(reference, Some(&io)).await?;
    let schema = if reference.descriptor().media_type() == "application/schema+json" {
        JsonValueSchema::Root
    } else {
        JsonValueSchema::None
    };
    let decoded = guarded_pi_decode(
        bytes.as_ref(),
        maximum(owner),
        std::mem::size_of::<T>() as u64,
        ErrorCode::Invalid,
        schema,
        ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    if guarded_pi_bytes(&decoded.0, maximum(owner), work)?
        .0
        .as_slice()
        != bytes.as_ref()
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "root artifact is not its immutable first-party encoding",
        });
    }
    Ok(decoded)
}

pub(crate) async fn load_original_preparation_with_native_work(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    expected_binding: &crate::PiTaskBinding,
    expected_root: &PiRootIntent,
    context: Arc<TaskContext>,
    work: &NativeMaterializationWork,
) -> Result<PiRootPreparation> {
    let missing = || Error::NativeBoundary {
        code: ErrorCode::Unauthorized,
        reason: "original root preparation upload operation is absent",
    };
    let grant = write_grant(owner, work)?;
    let mut files = [None, None, None];
    for (slot, name) in files
        .iter_mut()
        .zip(["preparation", "root", "allocation-schema"])
    {
        let reference = observe_pi_input_at(
            owner,
            filesystem,
            root_coordinates(owner, name, work)?,
            root_media(name)?,
            &grant.0,
            work,
        )
        .await?
        .ok_or_else(missing)?;
        *slot = Some(reference);
    }
    let [mut payload, root, schema_reference] =
        files.map(|file| file.expect("three original inputs"));
    let mut plan = read_root_artifact_with_native_work::<
        acyclic_harness::effect_host::TaskEffectPlan,
    >(owner, &context, &payload.0, work)
    .await?;
    if plan.0.provider != ROOT_ALLOCATION_PROVIDER
        || plan.0.effect_kind != ROOT_ALLOCATION_EFFECT
        || plan.0.guarantee != EffectGuarantee::AtMostOnce
        || plan.0.request != root.0
        || plan.0.result_schema != schema_reference.0
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "original root plan differs from canonical input operations",
        });
    }
    let mut request =
        read_root_artifact_with_native_work::<PiRootRequest>(owner, &context, &root.0, work)
            .await?;
    if &request.0.binding != expected_binding || &request.0.root != expected_root {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "root request differs from original canonical input",
        });
    }
    let schema = read_root_artifact_with_native_work::<serde_json::Value>(
        owner,
        &context,
        &schema_reference.0,
        work,
    )
    .await?;
    if schema.0 != PiRootAllocation::schema(&request.0.binding, work)?.0 {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "original root allocation schema differs",
        });
    }
    let mut kind = work.display(acyclic_harness::filesystem::EFFECT_TASK_COMMAND_KIND)?;
    for incoming in [&mut kind.1, &mut plan.1, &mut request.1] {
        payload.1.absorb(incoming)?;
    }
    Ok(PiRootPreparation {
        command: acyclic_harness::workflow::WorkflowCommand {
            operation_id: crate::pi_root_inputs::preparation_operation(
                owner.policy().root_task,
                work,
            )?,
            kind: kind.0,
            payload: payload.0,
        },
        plan: plan.0,
        request: request.0,
        context,
        _custody: payload.1,
    })
}
