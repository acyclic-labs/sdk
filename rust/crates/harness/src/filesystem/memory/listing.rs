use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFilesInput {
    path: String,
    expected_generation: Option<GenerationRef>,
    after: Option<String>,
    maximum_entries: u32,
}

pub(super) struct LocalListFilesTool {
    pub(super) verifier:
        Arc<FilesystemContentVerifier<MemoryAuthorityBackend, MemoryObjectBackend>>,
    pub(super) volume: VolumeRef,
    pub(super) maximum_bytes: u64,
    pub(super) observed: tokio::sync::Mutex<BTreeMap<OperationId, ListFilesObservation>>,
}

pub(super) struct ListFilesObservation {
    admission_digest: [u8; 32],
    result: ToolResult,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

#[derive(serde::Serialize)]
struct ListFilesPageWire<'a> {
    generation: &'a GenerationRef,
    entries: ListFilesEntries<'a>,
    has_more: bool,
    next_after: Option<&'a str>,
}

struct ListFilesEntries<'a>(&'a [acyclic_fs::TreeEntry]);

impl serde::Serialize for ListFilesEntries<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{Error as _, SerializeSeq};
        #[derive(serde::Serialize)]
        struct Entry<'a> {
            name: &'a str,
            kind: &'static str,
        }
        let mut entries = serializer.serialize_seq(Some(self.0.len()))?;
        for entry in self.0 {
            let name = std::str::from_utf8(entry.name.as_bytes())
                .map_err(|_| S::Error::custom("local file name is not UTF-8"))?;
            let kind = match entry.kind {
                FileKind::Regular => "file",
                FileKind::Directory => "directory",
                FileKind::SymbolicLink => "symlink",
                FileKind::Fifo => "fifo",
                FileKind::Socket => "socket",
                FileKind::CharacterDevice => "character_device",
                FileKind::BlockDevice => "block_device",
                FileKind::ReparsePoint => "reparse_point",
                FileKind::MountBoundary => "mount_boundary",
            };
            entries.serialize_element(&Entry { name, kind })?;
        }
        entries.end()
    }
}

fn list_files_page_wire<'a>(
    generation: &'a GenerationRef,
    page: &'a DirectoryPage,
) -> Result<ListFilesPageWire<'a>> {
    for entry in &page.entries {
        if entry.name.encoding() != NameEncoding::Utf8
            || std::str::from_utf8(entry.name.as_bytes()).is_err()
        {
            #[cfg(not(target_arch = "wasm32"))]
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "local file name is not UTF-8",
            });
            #[cfg(target_arch = "wasm32")]
            return Err(Error::Unsupported("local file name is not UTF-8".into()));
        }
    }
    Ok(ListFilesPageWire {
        generation,
        entries: ListFilesEntries(&page.entries),
        has_more: page.has_more,
        next_after: page
            .entries
            .last()
            .and_then(|entry| std::str::from_utf8(entry.name.as_bytes()).ok()),
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::filesystem::UploadRecord for ListFilesInput {
    fn diagnostic_label_bytes() -> u64 {
        crate::tool::tool_serde_diagnostic_label_bytes()
    }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for size in [
            self.path.capacity() as u64,
            self.after
                .as_ref()
                .map_or(0, |after| after.capacity() as u64),
            self.expected_generation
                .as_ref()
                .map(GenerationRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
        ] {
            bytes = bytes.checked_add(size).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files input ownership overflow",
            })?;
        }
        Ok(bytes)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl LocalListFilesTool {
    fn native_result_copy(
        result: &ToolResult,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let (value, custody) = crate::tool::clone_value(&result.value, Some(maximum_bytes), work)?;
        Ok((ToolResult { value }, custody))
    }

    async fn native_observe(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(ToolResult, acyclic_fs::NativeAllocation)>> {
        let digest = {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |observer|
                crate::contract::canonical_json_digest_with_work(invocation, io.remaining()?.bytes_encoded, observer))?
        };
        let observed = self.observed.lock().await;
        match observed.get(&invocation.operation_id) {
            Some(original) if original.admission_digest == digest => Self::native_result_copy(
                &original.result,
                self.maximum_bytes
                    .min(context.effective_scope().limits().render_bytes),
                work,
            )
            .map(Some),
            Some(_) => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "list_files operation identity changed",
            }),
            None => Ok(None),
        }
    }

    async fn native_execute(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let digest = {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |observer|
                crate::contract::canonical_json_digest_with_work(invocation, io.remaining()?.bytes_encoded, observer))?
        };
        let limits = context.effective_scope().limits();
        let maximum_bytes = self.maximum_bytes.min(limits.render_bytes);
        let mut observed = self.observed.lock().await;
        if let Some(original) = observed.get(&invocation.operation_id) {
            return if original.admission_digest == digest {
                Self::native_result_copy(&original.result, maximum_bytes, work)
            } else {
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "list_files operation identity changed",
                })
            };
        }
        let _input_custody;
        let (input, custody): (ListFilesInput, _) =
            crate::tool::decode_with_native_work(&invocation.arguments, work)?;
        _input_custody = custody;
        if input.maximum_entries == 0
            || (input.after.is_some() && input.expected_generation.is_none())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files requires a bounded, generation-pinned page",
            });
        }
        let _page_custody;
        let ((generation, page), custody) = self
            .verifier
            .list_private_directory_with_native_work(
                context.task(),
                &self.volume,
                "",
                &input.path,
                input.expected_generation.as_ref(),
                input.after.as_deref(),
                input.maximum_entries,
                work,
            )?
            .await?;
        _page_custody = custody;
        let wire = list_files_page_wire(&generation, &page)?;
        let (value, custody) = crate::tool::value(&wire, maximum_bytes, work)?;
        let output = (ToolResult { value }, custody);
        let mut cached = Self::native_result_copy(&output.0, maximum_bytes, work)?;
        let slot_bytes = (std::mem::size_of::<(OperationId, ListFilesObservation)>()
            - std::mem::size_of::<Value>()) as u64;
        let retained = cached
            .1
            .bytes()
            .checked_add(slot_bytes)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "list_files cache ownership overflow",
            })?;
        cached.1.resize_retained(retained).map_err(Error::from)?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })
        .map_err(Error::from)?;
        observed.insert(
            invocation.operation_id,
            ListFilesObservation {
                admission_digest: digest,
                result: cached.0,
                _custody: Some(cached.1),
            },
        );
        Ok(output)
    }
}

fn require_volume_grant(
    scope: Option<&RuntimeScope>,
    volume: &VolumeRef,
    operation: VolumeOperation,
) -> Result<()> {
    let scope =
        scope.ok_or_else(|| Error::Unauthorized("file tool requires a scoped caller".into()))?;
    let capability = volume.capability(operation, &mut None)?;
    if !scope.grants().contains(&capability) {
        return Err(Error::Unauthorized(format!("scope lacks {capability}")));
    }
    Ok(())
}

impl ToolExecutor for LocalListFilesTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        require_volume_grant(scope, &self.volume, VolumeOperation::Read)
    }

    fn execute<'a>(&'a self, invocation: ToolInvocation) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            let admission_digest = crate::contract::canonical_json_digest(&invocation)?;
            if let Some(original) = observed.get(&invocation.operation_id) {
                return if original.admission_digest == admission_digest {
                    Ok(original.result.clone())
                } else {
                    Err(Error::Conflict(
                        "list_files operation identity changed".into(),
                    ))
                };
            }
            let input: ListFilesInput = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| Error::Invalid(format!("list_files input is invalid: {error}")))?;
            if input.maximum_entries == 0
                || (input.after.is_some() && input.expected_generation.is_none())
            {
                return Err(Error::Invalid(
                    "list_files requires a bounded, generation-pinned page".into(),
                ));
            }
            let after = input
                .after
                .map(|name| {
                    LogicalName::new(NameEncoding::Utf8, name.into_bytes(), u32::MAX)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })
                .transpose()?;
            let (generation, page) = self
                .verifier
                .list_private_directory(
                    &self.volume,
                    "",
                    &input.path,
                    input.expected_generation.as_ref(),
                    after.as_ref(),
                    input.maximum_entries,
                )
                .await?;
            let wire = list_files_page_wire(&generation, &page)?;
            let value =
                serde_json::to_value(wire).map_err(|error| Error::Invalid(error.to_string()))?;
            if serde_json::to_vec(&value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .len() as u64
                > self.maximum_bytes
            {
                return Err(Error::Invalid("directory page exceeds render limit".into()));
            }
            let result = ToolResult { value };
            observed.insert(
                invocation.operation_id,
                ListFilesObservation {
                    admission_digest,
                    result: result.clone(),
                    #[cfg(not(target_arch = "wasm32"))]
                    _custody: None,
                },
            );
            Ok(result)
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let observed = self.observed.lock().await;
            let admission_digest = crate::contract::canonical_json_digest(&invocation)?;
            match observed.get(&invocation.operation_id) {
                Some(original) if original.admission_digest == admission_digest => {
                    Ok(Some(original.result.clone()))
                }
                Some(_) => Err(Error::Conflict(
                    "list_files operation identity changed".into(),
                )),
                None => Ok(None),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.verifier
            .supports_native_directory_list(&self.volume, work)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn authorize_with_native_context(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.supports_native_execution(work)?;
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "list_files tool context identity mismatch",
            });
        }
        let maximum_path_bytes = context.effective_scope().limits().path_bytes;
        for key in ["path", "after"] {
            if invocation
                .arguments
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|path| path.len() > maximum_path_bytes)
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "list_files path exceeds the effective scope path allowance",
                });
            }
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        if !io.contains_capability(
            context.effective_scope().grants(),
            &self.volume,
            "volume:read:",
        )? {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "scope lacks the original volume read capability",
            });
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || self.native_execute(context, invocation, work))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            match self.native_observe(context, invocation, work).await? {
                Some(mut output) => {
                    let retained = output
                        .1
                        .bytes()
                        .checked_add(
                            (std::mem::size_of::<Option<ToolResult>>()
                                - std::mem::size_of::<ToolResult>())
                                as u64,
                        )
                        .ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "list_files observation ownership overflow",
                        })?;
                    output.1.resize_retained(retained).map_err(Error::from)?;
                    Ok((Some(output.0), output.1))
                }
                None => Ok((
                    None,
                    work.claim(
                        std::mem::size_of::<Option<ToolResult>>() as u64,
                        0,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )
                    .map_err(Error::from)?,
                )),
            }
        })
    }
}

impl ToolProjection for LocalListFilesTool {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = _work {
            #[derive(serde::Serialize)]
            struct Projection<'a> {
                kind: &'static str,
                value: &'a Value,
            }
            self.supports_native_projection(Some(work))?;
            return crate::tool::value(
                &Projection {
                    kind: "json",
                    value: &result.value,
                },
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
            .map(|(value, custody)| (value, Some(custody)));
        }
        Ok((
            serde_json::json!({"kind":"json","value":result.value}),
            None,
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        if let Some(work) = work {
            work.remaining().map_err(Error::from)?;
        }
        Ok(())
    }
}
