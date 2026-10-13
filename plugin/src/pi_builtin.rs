//! Pi builtin IO is mediated by the original task owner, not by its cwd.
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{Error, OperationId, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub(crate) const PI_BUILTIN_COMMAND_KIND: &str = "acyclic.pi.builtin";
pub(crate) const MAXIMUM_BUILTIN_BYTES: u64 = 262_144;
pub(crate) const PI_BUILTIN_TOOL_REVISION: &str = "1.1.0";

/// Static contracts are decoded by the original guarded SDK constructor, not assembled as unobserved JSON values and adopted after allocation.
pub(crate) fn builtin_tool_definition_with_native_work(
    work: &NativeMaterializationWork,
) -> Result<(acyclic_harness::tool::ToolDefinition, NativeAllocation)> {
    acyclic_harness::tool::ToolDefinition::new(
        PI_BUILTIN_COMMAND_KIND,
        PI_BUILTIN_TOOL_REVISION,
        "Pi builtin IO through the original task's declared SOURCE, PROJECT or SCRATCH",
        r#"{
          "type":"object","additionalProperties":false,
          "required":["caller","sdk_operation","path","io"],
          "properties":{
            "caller":{"type":"string","minLength":1,"maxLength":128},
            "sdk_operation":{"type":"string","minLength":1,"maxLength":512},
            "path":{"type":"string","minLength":1,"maxLength":4096},
            "io":{"oneOf":[
              {"type":"object","additionalProperties":false,"required":["kind","maximum_bytes"],
               "properties":{"kind":{"const":"read"},
                 "maximum_bytes":{"type":"integer","minimum":1,"maximum":262144},
                 "variants":{"type":"array","maxItems":4,
                   "items":{"type":"string","minLength":1,"maxLength":4096}}}},
              {"type":"object","additionalProperties":false,"required":["kind","content"],
               "properties":{"kind":{"const":"write"},"content":{"type":"string","maxLength":262144}}},
              {"type":"object","additionalProperties":false,"required":["kind","maximum_bytes"],
               "properties":{"kind":{"const":"edit_read"},
                 "maximum_bytes":{"type":"integer","minimum":1,"maximum":262144}}},
              {"type":"object","additionalProperties":false,"required":["kind","content","expected_sha256"],
               "properties":{"kind":{"const":"edit_write"},"content":{"type":"string","maxLength":262144},
                 "expected_sha256":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"}}}
            ]}
          }
        }"#,
        r#"{
          "oneOf":[
            {"type":"object","additionalProperties":false,"required":["content_base64","sha256","path","volume_role"],
             "properties":{"content_base64":{"type":"string","maxLength":349528},
               "sha256":{"type":"string","pattern":"^[0-9a-f]{64}$"},
               "path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["SOURCE","PROJECT","SCRATCH"]}}},
            {"type":"object","additionalProperties":false,"required":["written","path","volume_role"],
             "properties":{"written":{"const":true},"path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["PROJECT","SCRATCH"]}}}
          ]
        }"#,
        r#"{
          "type":"object","additionalProperties":false,"required":["kind","value"],
          "properties":{"kind":{"const":"json"},"value":{"oneOf":[
            {"type":"object","additionalProperties":false,"required":["content_base64","sha256","path","volume_role"],
             "properties":{"content_base64":{"type":"string","maxLength":349528},
               "sha256":{"type":"string","pattern":"^[0-9a-f]{64}$"},
               "path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["SOURCE","PROJECT","SCRATCH"]}}},
            {"type":"object","additionalProperties":false,"required":["written","path","volume_role"],
             "properties":{"written":{"const":true},"path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["PROJECT","SCRATCH"]}}}
          ]}}
        }"#,
        16_384,
        work,
    )
}
const BUILTIN_DIAGNOSTIC_LABEL_BYTES: u64 =
    acyclic_harness::preparation_serde_diagnostic_label_bytes()
        + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "PiBuiltinRequest",
            "caller",
            "sdk_operation",
            "path",
            "io",
            "PiBuiltinIo",
            "kind",
            "read",
            "write",
            "edit_read",
            "edit_write",
            "maximum_bytes",
            "variants",
            "content",
            "expected_sha256",
        ]);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiBuiltinIo {
    Read {
        maximum_bytes: u64,
        #[serde(default)]
        variants: Vec<String>,
    },
    Write {
        content: String,
    },
    EditRead {
        maximum_bytes: u64,
    },
    EditWrite {
        content: String,
        expected_sha256: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiBuiltinRequest {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) path: String,
    pub(crate) io: PiBuiltinIo,
}

impl PiBuiltinRequest {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.sdk_operation.is_empty()
            || self.sdk_operation.len() > 512
            || self.sdk_operation.as_bytes().contains(&0)
        {
            return Err(invalid(
                "builtin requires the original bounded SDK tool call identity",
            ));
        }
        validate_path(&self.path)?;
        match &self.io {
            PiBuiltinIo::Read {
                maximum_bytes,
                variants,
            } => {
                validate_maximum(*maximum_bytes)?;
                if variants.len() > 4 {
                    return Err(invalid("builtin path variant bound exceeded"));
                }
                for path in variants {
                    validate_path(path)?;
                }
            }
            PiBuiltinIo::EditRead { maximum_bytes } => validate_maximum(*maximum_bytes)?,
            PiBuiltinIo::Write { content } => validate_content(content)?,
            PiBuiltinIo::EditWrite {
                content,
                expected_sha256,
            } => {
                validate_content(content)?;
                expected_digest(expected_sha256)?;
            }
        }
        Ok(())
    }

    pub(crate) fn operation(
        &self,
        root: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<OperationId> {
        let original =
            crate::pi_fork::original_operation(root, self.caller, &self.sdk_operation, work)?;
        crate::pi_fork::derived(
            original,
            match self.io {
                PiBuiltinIo::Read { .. } => "builtin-read",
                PiBuiltinIo::Write { .. } => "builtin-write",
                PiBuiltinIo::EditRead { .. } => "builtin-edit-read",
                PiBuiltinIo::EditWrite { .. } => "builtin-edit-write",
            },
            work,
        )
    }

    pub(crate) fn require_root_source_read(&self, original_root: TaskId) -> Result<()> {
        if self.caller != original_root || !matches!(self.io, PiBuiltinIo::Read { .. }) {
            return Err(denied(
                "model builtins cannot mutate the original physical Root source",
            ));
        }
        Ok(())
    }
}

fn validate_maximum(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid(
            "builtin whole-file read exceeds its original byte allowance",
        ));
    }
    Ok(())
}

fn validate_content(content: &str) -> Result<()> {
    if content.len() as u64 > MAXIMUM_BUILTIN_BYTES {
        return Err(invalid("builtin write exceeds its original byte allowance"));
    }
    Ok(())
}

pub(crate) fn expected_digest(value: &str) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    if value.len() != 64 || hex::decode_to_slice(value, &mut digest).is_err() {
        return Err(invalid(
            "builtin edit requires its exact bounded preimage digest",
        ));
    }
    Ok(digest)
}

fn validate_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 4096
        || value.as_bytes().contains(&0)
        || (value.starts_with("\\\\") && !windows_local_verbatim(value))
        || value.starts_with("//")
        || value.split(['/', '\\']).any(|part| part == "..")
        || (value.as_bytes().get(1) == Some(&b':') && !Path::new(value).is_absolute())
        || Path::new(value)
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(denied("builtin path escapes its declared native volume"));
    }
    Ok(())
}

/// Lexical selection only.
pub(crate) fn relative_path<'a>(path: &'a str, root: &Path) -> Result<&'a Path> {
    validate_path(path)?;
    if !root.is_absolute() {
        return Err(denied(
            "builtin volume has no original absolute native mapping",
        ));
    }
    let selected = Path::new(path);
    let relative = if selected.is_absolute() {
        strip_native_prefix(selected, root)
            .ok_or_else(|| denied("builtin path is outside its original native view"))?
    } else {
        selected
    };
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(denied(
            "builtin path is not a regular leaf below its original native volume",
        ));
    }
    Ok(relative)
}

#[cfg(not(windows))]
fn windows_local_verbatim(_: &str) -> bool {
    false
}

#[cfg(windows)]
fn windows_local_verbatim(value: &str) -> bool {
    matches!(Path::new(value).components().next(),
        Some(Component::Prefix(prefix)) if matches!(prefix.kind(), std::path::Prefix::VerbatimDisk(_)))
}

#[cfg(not(windows))]
fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    path.strip_prefix(root).ok()
}

#[cfg(windows)]
fn strip_native_prefix<'a>(path: &'a Path, root: &Path) -> Option<&'a Path> {
    use std::path::Prefix;
    let mut selected = path.components();
    for expected in root.components() {
        let actual = selected.next()?;
        let equivalent = match (actual, expected) {
            (Component::Prefix(actual), Component::Prefix(expected)) => {
                let disk = |prefix| match prefix {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        Some(drive.to_ascii_uppercase())
                    }
                    _ => None,
                };
                disk(actual.kind()).is_some() && disk(actual.kind()) == disk(expected.kind())
            }
            _ => actual == expected,
        };
        if !equivalent {
            return None;
        }
    }
    Some(selected.as_path())
}

pub(crate) fn invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}

pub(crate) fn read_response(
    bytes: &[u8],
    path: &str,
    volume_role: &str,
    work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    use base64::Engine as _;
    use sha2::{Digest as _, Sha256};
    let size = bytes
        .len()
        .checked_add(2)
        .and_then(|size| size.checked_div(3))
        .and_then(|size| size.checked_mul(4))
        .ok_or_else(|| invalid("builtin response size overflow"))?;
    let _encoded = work.claim(size as u64 + 64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    work.charge(WorkCounters {
        bytes_hashed: bytes.len() as u64,
        bytes_encoded: bytes.len() as u64,
        ..WorkCounters::default()
    })?;
    let content_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    let sha256 = hex::encode(Sha256::digest(bytes));
    #[derive(Serialize)]
    struct Response<'a> {
        content_base64: &'a str,
        sha256: &'a str,
        path: &'a str,
        volume_role: &'a str,
    }
    crate::pi_root_provider::guarded_pi_value(
        &Response {
            content_base64: &content_base64,
            sha256: &sha256,
            path,
            volume_role,
        },
        MAXIMUM_BUILTIN_BYTES * 2,
        work,
    )
}

fn decoded_tool_request(
    context: &acyclic_harness::runtime::ToolContext,
    invocation: &acyclic_harness::tool::ToolInvocation,
    work: &NativeMaterializationWork,
) -> Result<(PiBuiltinRequest, NativeAllocation)> {
    work.remaining()?;
    if invocation.name != PI_BUILTIN_COMMAND_KIND
        || invocation.operation_id != context.operation_id()
        || invocation.call_id != context.call_id()
    {
        return Err(denied(
            "builtin invocation differs from its original SDK command",
        ));
    }
    let encoded = crate::pi_root_provider::guarded_pi_bytes(
        &invocation.arguments,
        MAXIMUM_BUILTIN_BYTES * 2,
        work,
    )?;
    let decoded = crate::pi_root_provider::guarded_pi_decode::<PiBuiltinRequest>(
        &encoded.0,
        MAXIMUM_BUILTIN_BYTES * 2,
        MAXIMUM_BUILTIN_BYTES * 4,
        acyclic_harness::wire::ErrorCode::Invalid,
        acyclic_harness::JsonValueSchema::None,
        BUILTIN_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    drop(encoded.0);
    drop(encoded.1);
    decoded.0.validate()?;
    if context.task().durable_task_id() != Some(decoded.0.caller) {
        return Err(denied(
            "builtin input substitutes its original accepted task actor",
        ));
    }
    Ok(decoded)
}

/// This executor owns no session, runtime, provider, credentials, or view.
struct PiBuiltinToolExecutor;

pub(crate) fn builtin_tool_executor_with_native_work(
    work: &NativeMaterializationWork,
) -> Result<std::sync::Arc<dyn acyclic_harness::tool::ToolExecutor>> {
    work.charge(WorkCounters {
        allocation_operations: 1,
        ..WorkCounters::default()
    })?;
    Ok(std::sync::Arc::new(PiBuiltinToolExecutor))
}

fn validate_declared_working_roles(
    binding: &crate::PiTaskBinding,
    manifest: &acyclic_harness::filesystem::NativeViewManifest,
) -> Result<()> {
    binding.validate(
        None,
        binding.project_volumes.keys().map(String::as_str),
        false,
    )?;
    if manifest.volumes.len() != binding.project_volumes.len() + 1
        || !manifest
            .volumes
            .iter()
            .any(|mapping| mapping.volume == binding.scratch.volume)
        || binding.project_volumes.values().any(|volume| {
            !manifest
                .volumes
                .iter()
                .any(|mapping| &mapping.volume == volume)
        })
        || manifest.volumes.iter().any(|mapping| {
            mapping.volume != binding.scratch.volume
                && !binding
                    .project_volumes
                    .values()
                    .any(|volume| volume == &mapping.volume)
        })
        || binding.results.as_ref().is_some_and(|result| {
            manifest
                .volumes
                .iter()
                .any(|mapping| mapping.volume == result.volume)
        })
    {
        return Err(denied(
            "prepared working view differs from its original declared PROJECT/SCRATCH roles",
        ));
    }
    Ok(())
}

pub(crate) fn mounted_relative_path(
    manifest: &acyclic_harness::filesystem::NativeViewManifest,
    mapping: &acyclic_harness::filesystem::NativeVolumeMapping,
    leaf: &Path,
    work: &NativeMaterializationWork,
) -> Result<(std::path::PathBuf, NativeAllocation)> {
    let prefix = mapping
        .path
        .strip_prefix(&manifest.options.root)
        .map_err(|_| denied("mounted volume is outside its original namespace root"))?;
    if prefix.as_os_str().is_empty()
        || prefix
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
        || leaf.as_os_str().is_empty()
        || prefix
            .components()
            .chain(leaf.components())
            .any(|part| match part {
                Component::Normal(value) => value
                    .to_str()
                    .is_none_or(|value| value.contains(['\\', '\0'])),
                _ => true,
            })
    {
        return Err(denied(
            "mounted path is not a regular original namespace leaf",
        ));
    }
    let capacity = prefix
        .as_os_str()
        .len()
        .checked_add(leaf.as_os_str().len())
        .and_then(|length| length.checked_add(1))
        .ok_or_else(|| invalid("mounted path size overflow"))?;
    // Covers either native byte or UTF-16 backing before PathBuf growth.
    let bytes = (capacity as u64)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<std::path::PathBuf>() as u64))
        .ok_or_else(|| invalid("mounted path ownership overflow"))?;
    let custody = work.claim(bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut path = std::path::PathBuf::with_capacity(capacity);
    path.push(prefix);
    path.push(leaf);
    Ok((path, custody))
}

fn require_builtin_mapping_access(
    mapping: &acyclic_harness::filesystem::NativeVolumeMapping,
    io: &PiBuiltinIo,
) -> Result<()> {
    if !matches!(io, PiBuiltinIo::Read { .. }) && !mapping.writable {
        return Err(denied(
            "builtin native volume is not an original writable destination",
        ));
    }
    Ok(())
}

async fn accepted_builtin_io(
    context: &acyclic_harness::runtime::ToolContext,
    invocation: &acyclic_harness::tool::ToolInvocation,
    work: &NativeMaterializationWork,
) -> Result<(acyclic_harness::tool::ToolResult, NativeAllocation)> {
    use acyclic_harness::conversation::VolumeOperation;
    let decoded = decoded_tool_request(context, invocation, work)?;
    let _request_custody = decoded.1;
    let request = decoded.0;
    let admission = context.original_admission_with_native_work(work)?.await?;
    let encoded = crate::pi_root_provider::guarded_pi_bytes(
        &admission.0.input,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        work,
    )?;
    let input = crate::pi_root_provider::guarded_pi_decode::<crate::pi_task::PiTaskInput>(
        &encoded.0,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        crate::MAXIMUM_ADAPTER_STATE_BYTES,
        acyclic_harness::wire::ErrorCode::Invalid,
        acyclic_harness::JsonValueSchema::None,
        crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        work,
    )?;
    drop(encoded.0);
    drop(encoded.1);
    let _input_custody = input.1;
    let input = input.0;
    if admission.0.parent.is_none()
        || input.binding.task_id != request.caller
        || !matches!(
            input.execution,
            crate::pi_task::PiTaskExecution::Child { .. }
        )
    {
        return Err(denied(
            "working builtin does not belong to its original declared child task",
        ));
    }
    let manifest = context.native_working_manifest_with_native_work(work)?;
    validate_declared_working_roles(&input.binding, manifest)?;
    let (mapping, leaf) = select_native_path(&request.path, &input.binding, manifest)?;
    require_builtin_mapping_access(mapping, &request.io)?;
    let variants = match &request.io {
        PiBuiltinIo::Read { variants, .. } => variants.as_slice(),
        _ => &[],
    };
    let leaves = scoped_variants(variants, &mapping.path)?;
    let volume_role = if mapping.volume == input.binding.scratch.volume {
        "SCRATCH"
    } else {
        "PROJECT"
    };
    let response = match &request.io {
        PiBuiltinIo::Read { maximum_bytes, .. } | PiBuiltinIo::EditRead { maximum_bytes } => {
            let mut last_absence = None;
            let mut found = None;
            for (leaf, candidate) in std::iter::once(leaf)
                .chain(leaves.into_iter().flatten())
                .zip(std::iter::once(&request.path).chain(variants.iter()))
            {
                let path = mounted_relative_path(manifest, mapping, leaf, work)?;
                if matches!(request.io, PiBuiltinIo::EditRead { .. }) {
                    context
                        .authorize_working_file_with_native_work(
                            &path.0,
                            VolumeOperation::Write,
                            work,
                        )
                        .await?;
                }
                match context
                    .read_working_file_with_native_work(&path.0, *maximum_bytes as usize, work)
                    .await
                {
                    Ok(bytes) => {
                        let result = match context
                            .authorize_working_file_with_native_work(
                                &path.0,
                                VolumeOperation::Read,
                                work,
                            )
                            .await
                        {
                            Ok(()) => read_response(bytes.as_ref(), candidate, volume_role, work),
                            Err(error) => Err(error),
                        };
                        drop(bytes);
                        found = Some(result?);
                        break;
                    }
                    Err(error) if is_absent(&error) => last_absence = Some(error),
                    Err(error) => return Err(error),
                }
            }
            found.ok_or_else(|| last_absence.expect("the requested candidate is always checked"))?
        }
        PiBuiltinIo::Write { content } | PiBuiltinIo::EditWrite { content, .. } => {
            let path = mounted_relative_path(manifest, mapping, leaf, work)?;
            let expected = match &request.io {
                PiBuiltinIo::EditWrite {
                    expected_sha256, ..
                } => Some(expected_digest(expected_sha256)?),
                _ => None,
            };
            context
                .write_working_file_with_native_work(
                    &path.0,
                    content,
                    matches!(request.io, PiBuiltinIo::Write { .. }),
                    expected,
                    work,
                )
                .await?;
            context
                .authorize_working_file_with_native_work(&path.0, VolumeOperation::Write, work)
                .await?;
            #[derive(Serialize)]
            struct Response<'a> {
                written: bool,
                path: &'a str,
                volume_role: &'a str,
            }
            crate::pi_root_provider::guarded_pi_value(
                &Response {
                    written: true,
                    path: &request.path,
                    volume_role,
                },
                8192,
                work,
            )?
        }
    };
    drop(input);
    drop(admission.0);
    drop(admission.1);
    Ok((
        acyclic_harness::tool::ToolResult { value: response.0 },
        response.1,
    ))
}

impl acyclic_harness::tool::ToolExecutor for PiBuiltinToolExecutor {
    fn execute<'a>(
        &'a self,
        _invocation: acyclic_harness::tool::ToolInvocation,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<acyclic_harness::tool::ToolResult>> {
        Box::pin(async {
            Err(denied(
                "builtin IO requires the original accepted native tool callback",
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: acyclic_harness::tool::ToolInvocation,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<acyclic_harness::tool::ToolResult>>>
    {
        Box::pin(async {
            Err(denied(
                "builtin reconciliation requires the original accepted native tool callback",
            ))
        })
    }

    fn supports_native_execution(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        Ok(())
    }

    fn authorize_with_native_context(
        &self,
        context: &acyclic_harness::runtime::ToolContext,
        invocation: &acyclic_harness::tool::ToolInvocation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let decoded = decoded_tool_request(context, invocation, work)?;
        drop(decoded.0);
        drop(decoded.1);
        Ok(())
    }

    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::ToolContext,
        invocation: &'a acyclic_harness::tool::ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<
            'a,
            (acyclic_harness::tool::ToolResult, NativeAllocation),
        >,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            accepted_builtin_io(context, invocation, work).await
        })
    }

    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a acyclic_harness::runtime::ToolContext,
        invocation: &'a acyclic_harness::tool::ToolInvocation,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<
            'a,
            (Option<acyclic_harness::tool::ToolResult>, NativeAllocation),
        >,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let decoded = decoded_tool_request(context, invocation, work)?;
            let admission = context.original_admission_with_native_work(work)?.await?;
            context.native_working_manifest_with_native_work(work)?;
            drop(decoded.0);
            drop(decoded.1);
            drop(admission.0);
            drop(admission.1);
            let custody = work.claim(
                std::mem::size_of::<Option<acyclic_harness::tool::ToolResult>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            Ok((None, custody))
        })
    }
}

type Owner = acyclic_harness::durable_host::TaskJournalOwner<acyclic_stream::LocalStream>;

/// Borrows original providers and creator handles; it owns no second journal, route registry, root credential, or independent filesystem.
impl<'a> crate::pi_selected_host::PiControlHost<'a> {
    async fn root_read(
        &self,
        native_context: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        owner: std::sync::Arc<Owner>,
        request: &PiBuiltinRequest,
    ) -> Result<(serde_json::Value, NativeAllocation)> {
        request.validate()?;
        request.require_root_source_read(self.protected.policy().root_task)?;
        let context = native_context.as_ref();
        authorize_owner(context, &owner, request.caller, &self.work).await?;
        let work = &self.work;
        let decoded = self.admission_input(request.caller, &self.work).await?;
        let _input_custody = decoded.1;
        let input = decoded.0;
        if input.binding.task_id != request.caller
            || input.binding.agent_id != self.protected.policy().agent
        {
            return Err(denied(
                "builtin Root binding differs from its original admitted owner",
            ));
        }
        let crate::pi_task::PiTaskExecution::Host { root, .. } = input.execution else {
            return Err(denied("builtin Root has no original host admission"));
        };
        let crate::pi_root::PiRootIntent::Project { paths, root_id, .. } = &root else {
            return Err(denied("builtin Root has no original physical SOURCE"));
        };
        let (key, relative) = select_root_path(&request.path, paths, root_id)?;
        let root = Path::new(&paths[key]);
        let PiBuiltinIo::Read {
            maximum_bytes,
            variants,
        } = &request.io
        else {
            unreachable!("read checked")
        };
        let relatives = scoped_variants(variants, root)?;
        let selection = self
            .selected(
                context,
                &native_context,
                &owner,
                request.caller,
                Some(key),
                None,
                request.operation(self.protected.policy().root_task, work)?,
                self.work.cancellation(),
                &self.work,
            )
            .await?;
        let (_, volume, workspace, frozen, inspection, _) = &selection;
        let reader = crate::pi_inspection::PiNativeSelectedSource::select(
            inspection,
            request.caller,
            request.caller,
            volume,
            workspace,
            *frozen,
            &self.work,
        )?;
        let mut last_absence = None;
        for (path, candidate) in std::iter::once(relative)
            .chain(relatives.into_iter().flatten())
            .zip(std::iter::once(&request.path).chain(variants.iter()))
        {
            let converted = logical_path(path, &self.work)?;
            let _path_custody = converted.1;
            let path = converted.0;
            match reader
                .read_with_native_work(context, &owner, &path, *maximum_bytes, &self.work)
                .await
            {
                Ok(bytes) => {
                    let response =
                        match authorize_owner(context, &owner, request.caller, &self.work).await {
                            Ok(()) => {
                                read_response(bytes.as_slice(), candidate, "SOURCE", &self.work)
                            }
                            Err(error) => Err(error),
                        };
                    drop(bytes);
                    return response;
                }
                Err(error) if is_absent(&error) => {
                    last_absence = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_absence.expect("the requested candidate is always checked"))
    }

    pub(crate) async fn run_builtin(
        &self,
        context: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        fence: acyclic_harness::scheduler::LeaseFence,
        command: acyclic_harness::workflow::WorkflowCommand,
        payload: serde_json::Value,
    ) -> Result<acyclic_harness::filesystem::TaskCommandProgress> {
        let work = &self.work;
        let encoded = crate::pi_root_provider::guarded_pi_bytes(
            &payload,
            MAXIMUM_BUILTIN_BYTES * 2,
            &self.work,
        )?;
        let decoded = crate::pi_root_provider::guarded_pi_decode::<PiBuiltinRequest>(
            &encoded.0,
            MAXIMUM_BUILTIN_BYTES * 2,
            MAXIMUM_BUILTIN_BYTES * 4,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            BUILTIN_DIAGNOSTIC_LABEL_BYTES,
            &self.work,
        )?;
        drop(encoded.0);
        drop(encoded.1);
        let _request_custody = decoded.1;
        let request = decoded.0;
        request.validate()?;
        request.require_root_source_read(self.protected.policy().root_task)?;
        if context.durable_task_id() != Some(request.caller) {
            return Err(denied(
                "builtin command differs from its original native task actor",
            ));
        }
        if command.kind != PI_BUILTIN_COMMAND_KIND
            || command.operation_id != request.operation(self.protected.policy().root_task, work)?
        {
            return Err(denied("builtin command changed its original SDK operation"));
        }
        let owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner(request.caller, &fence, Some(&acyclic_harness::executor::PreparationIo::from_native_work(&self.work)?))
            .await?
            .with_invocation_cancellation(self.work.cancellation())
            .into_native_shared(&self.work)?;
        let (value, custody) = self.root_read(context, owner, &request).await?;
        Ok(
            acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody {
                value,
                custody,
            },
        )
    }
}

fn select_native_path<'a, 'p>(
    path: &'p str,
    binding: &crate::PiTaskBinding,
    manifest: &'a acyclic_harness::filesystem::NativeViewManifest,
) -> Result<(
    &'a acyclic_harness::filesystem::NativeVolumeMapping,
    &'p Path,
)> {
    validate_path(path)?;
    manifest.validate()?;
    let allowed = |mapping: &&acyclic_harness::filesystem::NativeVolumeMapping| {
        mapping.volume == binding.scratch.volume
            || binding
                .project_volumes
                .values()
                .any(|volume| volume == &mapping.volume)
    };
    let mut selected = None;
    for mapping in manifest.volumes.iter().filter(allowed) {
        if !Path::new(path).is_absolute()
            && mapping.volume == binding.scratch.volume
            && !binding.project_volumes.is_empty()
        {
            continue;
        }
        if let Ok(relative) = relative_path(path, &mapping.path) {
            if selected.is_some() {
                return Err(denied("builtin native path selection is ambiguous"));
            }
            selected = Some((mapping, relative));
        }
    }
    selected.ok_or_else(|| {
        denied("builtin path is outside its actual PROJECT/SCRATCH view; RESULT is unmounted")
    })
}

pub(crate) async fn authorize_owner(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &Owner,
    caller: TaskId,
    work: &NativeMaterializationWork,
) -> Result<()> {
    if owner.task_id() != caller || context.durable_task_id() != Some(caller) {
        return Err(denied(
            "builtin caller differs from its original task owner/context",
        ));
    }
    owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await
}

fn select_root_path<'p, 'r>(
    path: &'p str,
    roots: &'r std::collections::BTreeMap<String, String>,
    default: &[u8; 16],
) -> Result<(&'r str, &'p Path)> {
    validate_path(path)?;
    if !Path::new(path).is_absolute() {
        let (key, root) = roots
            .iter()
            .find(|(key, _)| {
                let mut identity = [0; 16];
                hex::decode_to_slice(key.as_bytes(), &mut identity).is_ok() && identity == *default
            })
            .ok_or_else(|| denied("builtin original Root cwd is absent"))?;
        return Ok((key, relative_path(path, Path::new(root))?));
    }
    let mut selected = None;
    for (key, root) in roots {
        if let Ok(relative) = relative_path(path, Path::new(root)) {
            if selected.is_some() {
                return Err(denied("builtin original source selection is ambiguous"));
            }
            selected = Some((key.as_str(), relative));
        }
    }
    selected.ok_or_else(|| denied("builtin path is outside the original physical source"))
}

fn scoped_variants<'a>(variants: &'a [String], root: &Path) -> Result<[Option<&'a Path>; 4]> {
    let mut selected = [None; 4];
    for (index, path) in variants.iter().enumerate() {
        let entry = selected
            .get_mut(index)
            .ok_or_else(|| invalid("builtin variant bound exceeded"))?;
        *entry = Some(relative_path(path, root)?);
    }
    Ok(selected)
}

fn logical_path(
    path: &Path,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    let maximum = path.as_os_str().len();
    let custody = work.claim(maximum as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    let mut value = String::with_capacity(maximum);
    for component in path.components() {
        let part = component
            .as_os_str()
            .to_str()
            .ok_or_else(|| invalid("builtin leaf has no wire representation"))?;
        if !value.is_empty() {
            value.push('/');
        }
        value.push_str(part);
    }
    work.charge(WorkCounters {
        bytes_copied: value.len() as u64,
        ..WorkCounters::default()
    })?;
    acyclic_harness::conversation::validate_content_path(&value)?;
    Ok((value, custody))
}

fn is_absent(error: &Error) -> bool {
    match error {
        Error::NotFound(_) => true,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Io(source),
            ..
        } => source.kind() == std::io::ErrorKind::NotFound,
        _ => false,
    }
}
