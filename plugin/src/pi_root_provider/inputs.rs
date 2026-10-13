//! Immutable uploads and their original task bindings.
use super::*;

fn pi_input_write_grant_with_native_work(
    owner: &PiOwnerConfig,
    work: &NativeMaterializationWork,
) -> Result<(ContentGrant, NativeAllocation)> {
    let authority = owner.authority(work)?;
    let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
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

fn validate_pi_input_media(
    owner: &PiOwnerConfig,
    file: &FileRef,
    path: &str,
    display_name: &str,
    media_type: &str,
) -> Result<()> {
    owner.policy().limits.validate_file(file)?;
    if file.volume() != &owner.policy().journal
        || file.path() != path
        || file.display_name() != display_name
        || file.descriptor().media_type() != media_type
        || file.descriptor().byte_length()
            > owner
                .policy()
                .maximum_payload_bytes
                .min(crate::MAXIMUM_ADAPTER_STATE_BYTES)
    {
        return Err(Error::Invalid(
            "Pi original input operation returned another immutable file".into(),
        ));
    }
    Ok(())
}

/// Same original upload key and protected bootstrap grant as stage_pi_input, with the caller's original native ledger owning both codec and publication.
pub(crate) async fn stage_pi_input<T: Serialize>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(
        owner,
        filesystem,
        operation,
        label,
        value,
        work,
        ROOT_INPUT_DIRECTORY,
        "input",
    )
    .await
}

pub(crate) async fn stage_pi_operator_input<T: Serialize>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    stage_pi_input_namespace_with_native_work(
        owner,
        filesystem,
        operation,
        label,
        value,
        work,
        OPERATOR_INPUT_DIRECTORY,
        "operator-input",
    )
    .await
}

/// Readonly original operation metadata, never a mutable path or new upload.
pub(crate) async fn observe_pi_input(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let coordinates = pi_input_coordinates_with_native_work(
        owner,
        operation,
        label,
        ROOT_INPUT_DIRECTORY,
        "input",
        work,
    )?;
    observe_pi_input_at(owner, filesystem, coordinates, "application/json", work).await
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixed trusted namespace shares the original publisher engine"
)]
async fn stage_pi_input_namespace_with_native_work<T: Serialize>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    operation: OperationId,
    label: &str,
    value: &T,
    work: &NativeMaterializationWork,
    directory: &'static str,
    purpose: &'static str,
) -> Result<(FileRef, NativeAllocation)> {
    let coordinates =
        pi_input_coordinates_with_native_work(owner, operation, label, directory, purpose, work)?;
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

/// Fixed original root artifact coordinates, including the original schema media type; this is not an operation/label input alias or a new upload key.
pub(crate) async fn stage_pi_root_input_with_native_work<T: Serialize>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    name: &str,
    value: &T,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    let media_type = root_input_media(name)?;
    let coordinates = root_input_coordinates_with_native_work(owner, name, work)?;
    publish_pi_input_with_native_coordinates(
        owner,
        filesystem,
        value,
        media_type,
        coordinates,
        work,
    )
    .await
}

fn root_input_media(name: &str) -> Result<&'static str> {
    match name {
        "root" | "preparation" => Ok("application/json"),
        "allocation-schema" => Ok("application/schema+json"),
        _ => Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "unknown original Pi root artifact",
        }),
    }
}

fn root_input_coordinates_with_native_work(
    owner: &PiOwnerConfig,
    name: &str,
    work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    root_input_media(name)?;
    let (display_name, custody) = work.display(&format_args!("{name}.json"))?;
    let display_name = (display_name, custody);
    let (path, custody) =
        work.display(&format_args!("{ROOT_INPUT_DIRECTORY}/{}", display_name.0))?;
    let path = (path, custody);
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "original Pi root artifact path exceeds owner limit",
        });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{name}-input/{}",
        owner.policy().root_task
    ))?;
    let key = IdempotencyKey::new(key)?;
    let key = (key, custody);
    Ok(NativeInputCoordinates {
        path,
        display_name,
        key,
    })
}

/// Same fixed canonical root upload operation as staging, without creating or trusting the current file at its path.
pub(crate) async fn observe_pi_root_input_with_native_work(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    name: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let media_type = root_input_media(name)?;
    let coordinates = root_input_coordinates_with_native_work(owner, name, work)?;
    observe_pi_input_at(owner, filesystem, coordinates, media_type, work).await
}

pub(super) async fn observe_pi_input_at(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    coordinates: NativeInputCoordinates,
    media_type: &str,
    work: &NativeMaterializationWork,
) -> Result<Option<(FileRef, NativeAllocation)>> {
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    let result = filesystem
        .content_for_operation_with_native_work(
            &owner.policy().journal,
            &grant.0,
            &coordinates.key.0,
            &coordinates.path.0,
            maximum,
            work,
        )
        .await;
    match result {
        Ok(Some((reference, custody))) => {
            if let Err(error) = validate_pi_input_media(
                owner,
                &reference,
                &coordinates.path.0,
                &coordinates.display_name.0,
                media_type,
            ) {
                drop(reference);
                drop(custody);
                Err(error)
            } else {
                Ok(Some((reference, custody)))
            }
        }
        result => result,
    }
}

pub(super) async fn publish_pi_input_with_native_coordinates<T: Serialize>(
    owner: &PiOwnerConfig,
    filesystem: &Host,
    value: &T,
    media_type: &str,
    coordinates: NativeInputCoordinates,
    work: &NativeMaterializationWork,
) -> Result<(FileRef, NativeAllocation)> {
    let grant = pi_input_write_grant_with_native_work(owner, work)?;
    let maximum = owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    let (bytes, bytes_custody) = guarded_pi_bytes(value, maximum, work)?;
    let encoded = (bytes, bytes_custody);
    let length = encoded.0.len() as u64;
    let result = filesystem
        .put_content_with_native_work(
            &owner.policy().journal,
            &grant.0,
            &coordinates.path.0,
            &encoded.0,
            media_type,
            &coordinates.display_name.0,
            maximum,
            &coordinates.key.0,
            work,
        )
        .await;
    drop(encoded);
    let result = match result {
        Ok((reference, custody)) => {
            let validation = validate_pi_input_media(
                owner,
                &reference,
                &coordinates.path.0,
                &coordinates.display_name.0,
                media_type,
            )
            .and_then(|()| {
                if reference.descriptor().byte_length() == length {
                    Ok(())
                } else {
                    Err(Error::NativeBoundary {
                        code: ErrorCode::Storage,
                        reason: "Pi input original body length differs",
                    })
                }
            });
            match validation {
                Ok(()) => Ok((reference, custody)),
                Err(error) => {
                    drop(reference);
                    drop(custody);
                    Err(error)
                }
            }
        }
        result => result,
    };
    drop(coordinates);
    result
}

pub(super) async fn read_root_artifact_with_native_work<T: DeserializeOwned + Serialize>(
    owner: &PiOwnerConfig,
    context: &TaskContext,
    reference: &FileRef,
    work: &NativeMaterializationWork,
) -> Result<(T, NativeAllocation)> {
    let maximum = owner
        .policy()
        .maximum_payload_bytes
        .min(crate::MAXIMUM_ADAPTER_STATE_BYTES);
    if reference.descriptor().byte_length() > maximum
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
    let bytes = context.read_file(reference, Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)).await?;
    let maximum_owned = std::mem::size_of::<T>() as u64;
    let decoded = guarded_pi_decode(
        bytes.as_ref(),
        maximum,
        maximum_owned,
        acyclic_harness::wire::ErrorCode::Invalid,
        if reference.descriptor().media_type() == "application/schema+json" {
            JsonValueSchema::Root
        } else {
            JsonValueSchema::None
        },
        ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    let encoded = guarded_pi_bytes(&decoded.0, maximum, work)?;
    if encoded.0.as_slice() != bytes.as_ref() {
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
    context: Arc<acyclic_harness::runtime::TaskContext>,
    work: &NativeMaterializationWork,
) -> Result<PiRootPreparation> {
    let missing = || Error::NativeBoundary {
        code: ErrorCode::Unauthorized,
        reason: "original root preparation upload operation is absent",
    };
    let mut payload =
        observe_pi_root_input_with_native_work(owner, filesystem, "preparation", work)
            .await?
            .ok_or_else(missing)?;
    let plan = read_root_artifact_with_native_work::<acyclic_harness::effect_host::TaskEffectPlan>(
        owner,
        context.as_ref(),
        &payload.0,
        work,
    )
    .await?;
    let root = observe_pi_root_input_with_native_work(owner, filesystem, "root", work)
        .await?
        .ok_or_else(missing)?;
    let schema_reference =
        observe_pi_root_input_with_native_work(owner, filesystem, "allocation-schema", work)
            .await?
            .ok_or_else(missing)?;
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
    let request = read_root_artifact_with_native_work::<PiRootRequest>(
        owner,
        context.as_ref(),
        &root.0,
        work,
    )
    .await?;
    if &request.0.binding != expected_binding || &request.0.root != expected_root {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "root request differs from original canonical input",
        });
    }
    let schema = read_root_artifact_with_native_work::<serde_json::Value>(
        owner,
        context.as_ref(),
        &schema_reference.0,
        work,
    )
    .await?;
    let expected_schema = PiRootAllocation::schema(&request.0.binding, work)?;
    if schema.0 != expected_schema.0 {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "original root allocation schema differs",
        });
    }
    let mut kind = work.display(acyclic_harness::filesystem::EFFECT_TASK_COMMAND_KIND)?;
    payload.1.absorb(&mut kind.1)?;
    let mut plan_custody = plan.1;
    let mut request_custody = request.1;
    let mut preparation = PiRootPreparation {
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
        custody: payload.1,
    };
    preparation.custody.absorb(&mut plan_custody)?;
    preparation.custody.absorb(&mut request_custody)?;
    Ok(preparation)
}

pub(super) struct NativeInputCoordinates {
    pub(super) path: (String, NativeAllocation),
    pub(super) display_name: (String, NativeAllocation),
    pub(super) key: (IdempotencyKey, NativeAllocation),
}

fn pi_input_coordinates_with_native_work(
    owner: &PiOwnerConfig,
    operation: OperationId,
    label: &str,
    directory: &'static str,
    purpose: &'static str,
    work: &NativeMaterializationWork,
) -> Result<NativeInputCoordinates> {
    work.cancellation()
        .check()
        .map_err(|_| acyclic_harness::Error::from(NativeTreeMaterializationError::Cancelled))?;
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
    let (display_name, custody) = work.display(&format_args!("{label}.json"))?;
    let display_name = (display_name, custody);
    let (path, custody) =
        work.display(&format_args!("{directory}/{operation}/{}", display_name.0))?;
    let path = (path, custody);
    if path.0.len() > owner.policy().limits.path_bytes {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "Pi input path exceeds its original owner limit",
        });
    }
    let (key, custody) = work.display(&format_args!(
        "acyclic.pi/{purpose}/{}/{operation}/{label}",
        owner.policy().root_task
    ))?;
    let key = IdempotencyKey::new(key)?;
    let key = (key, custody);
    Ok(NativeInputCoordinates {
        path,
        display_name,
        key,
    })
}
