use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListFilesInput {
    path: String,
    expected_generation: Option<GenerationRef>,
    after: Option<String>,
    maximum_entries: u32,
}

pub(super) struct LocalListFilesTool<A, O> {
    pub(super) verifier: Arc<FilesystemContentVerifier<A, O>>,
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
        crate::tool::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES
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
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> LocalListFilesTool<A, O> {
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
            crate::executor::with_history_io(Some(&io), |observer| {
                crate::executor::json::canonical_json_digest(
                    invocation,
                    io.remaining()?.bytes_encoded,
                    observer,
                )
            })?
        };
        let observed = self.observed.lock().await;
        match observed.get(&invocation.operation_id) {
            Some(original) if original.admission_digest == digest => self
                .native_recheck(&original.result, context, invocation, work)
                .await
                .map(Some),
            Some(_) => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "list_files operation identity changed",
            }),
            None => Ok(None),
        }
    }

    async fn native_query(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
        pinned: Option<&GenerationRef>,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let maximum_bytes = self
            .maximum_bytes
            .min(context.effective_scope().limits().render_bytes);
        let _input_custody;
        let (input, custody): (ListFilesInput, _) =
            crate::tool::decode(&invocation.arguments, Some(work))?;
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
                context,
                &self.volume,
                "",
                &input.path,
                pinned.or(input.expected_generation.as_ref()),
                input.after.as_deref(),
                input.maximum_entries,
                work,
            )?
            .await?;
        _page_custody = custody;
        let wire = list_files_page_wire(&generation, &page)?;
        let (value, custody) = crate::tool::value(&wire, maximum_bytes, Some(work))?;
        let output = (
            ToolResult { value },
            custody.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native list result has no allocation custody",
            })?,
        );
        Ok(output)
    }

    async fn native_recheck(
        &self,
        original: &ToolResult,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let generation = cached_generation(original, Some(&io))?;
        let current = self
            .native_query(context, invocation, work, Some(&generation.0))
            .await?;
        require_same_page(original, &current.0, Some(&io))?;
        Ok(current)
    }

    async fn native_execute(
        &self,
        context: &crate::runtime::ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let digest = {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |observer| {
                crate::executor::json::canonical_json_digest(
                    invocation,
                    io.remaining()?.bytes_encoded,
                    observer,
                )
            })?
        };
        let limits = context.effective_scope().limits();
        let maximum_bytes = self.maximum_bytes.min(limits.render_bytes);
        let mut observed = self.observed.lock().await;
        if let Some(original) = observed.get(&invocation.operation_id) {
            return if original.admission_digest == digest {
                self.native_recheck(&original.result, context, invocation, work)
                    .await
            } else {
                Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "list_files operation identity changed",
                })
            };
        }
        let output = self.native_query(context, invocation, work, None).await?;
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
        cached.1.resize(retained).map_err(Error::from)?;
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

fn cached_generation<'io>(
    result: &ToolResult,
    io: Option<&'io crate::filesystem::PreparationIo>,
) -> Result<(
    GenerationRef,
    Option<crate::filesystem::DecodeReservation<'io>>,
)> {
    let value = result.value.get("generation").ok_or_else(|| {
        crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "cached directory has no generation",
        )
    })?;
    match io {
        Some(io) => io
            .value_decode_preparation(value, crate::wire::ErrorCode::Storage)
            .map(|(value, hold)| (value, Some(hold))),
        None => GenerationRef::deserialize(value)
            .map(|value| (value, None))
            .map_err(|error| Error::Storage(error.to_string())),
    }
}

fn require_same_page(
    original: &ToolResult,
    current: &ToolResult,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<()> {
    if original == current {
        return Ok(());
    }
    Err(crate::executor::stock_boundary(
        io,
        crate::wire::ErrorCode::Unauthorized,
        "cached directory metadata is no longer authorized",
    ))
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> LocalListFilesTool<A, O> {
    async fn query(
        &self,
        invocation: &ToolInvocation,
        context: Option<&crate::runtime::ToolContext>,
        pinned: Option<&GenerationRef>,
    ) -> Result<ToolResult> {
        if let Some(context) = context {
            for key in ["path", "after"] {
                if invocation
                    .arguments
                    .get(key)
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.len() > context.effective_scope().limits().path_bytes)
                {
                    return Err(Error::Unauthorized(
                        "list_files path exceeds its effective scope".into(),
                    ));
                }
            }
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
                crate::filesystem::DirectoryOptions {
                    generation: pinned.or(input.expected_generation.as_ref()),
                    after: after.as_ref(),
                    maximum_entries: input.maximum_entries,
                    ceiling: context.map(|context| context.effective_scope().grants()),
                },
            )
            .await?;
        let wire = list_files_page_wire(&generation, &page)?;
        let maximum_bytes = context.map_or(self.maximum_bytes, |context| {
            self.maximum_bytes
                .min(context.effective_scope().limits().render_bytes)
        });
        let (value, _) = crate::tool::value(&wire, maximum_bytes, None)?;
        Ok(ToolResult { value })
    }

    async fn recheck(
        &self,
        original: &ToolResult,
        invocation: &ToolInvocation,
        context: Option<&crate::runtime::ToolContext>,
    ) -> Result<ToolResult> {
        let generation = cached_generation(original, None)?;
        let current = self.query(invocation, context, Some(&generation.0)).await?;
        require_same_page(original, &current, None)?;
        Ok(current)
    }
}

fn require_volume_grant(
    scope: Option<&RuntimeScope>,
    volume: &VolumeRef,
    path: &str,
) -> Result<()> {
    let scope =
        scope.ok_or_else(|| Error::Unauthorized("file tool requires a scoped caller".into()))?;
    if !crate::core::directory_read_granted_with_work(scope.grants(), volume, path, &mut None)? {
        return Err(Error::Unauthorized(
            "scope lacks a directory read grant".into(),
        ));
    }
    Ok(())
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ToolExecutor
    for LocalListFilesTool<A, O>
{
    fn authorize(
        &self,
        scope: Option<&crate::runtime::RuntimeScope>,
        context: Option<&crate::runtime::ToolContext>,
        invocation: &crate::tool::ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> crate::Result<()> {
        let _ = (scope, context, invocation, work);
        let scope = context.map(|context| context.effective_scope()).or(scope);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let context = context.ok_or(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "tool requires its original native context",
            })?;
            return {
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
                if !crate::core::directory_read_granted_with_work(
                    context.effective_scope().grants(),
                    &self.volume,
                    invocation
                        .arguments
                        .get("path")
                        .and_then(Value::as_str)
                        .unwrap_or(""),
                    &mut Some(&mut io.history_io()),
                )? {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "scope lacks the original volume read capability",
                    });
                }
                Ok(())
            };
        }

        require_volume_grant(
            scope,
            &self.volume,
            invocation
                .arguments
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or(""),
        )
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let mut observed = self.observed.lock().await;
            let admission_digest =
                crate::executor::json::canonical_json_digest(&invocation, u64::MAX, &mut None)?;
            if let Some(original) = observed.get(&invocation.operation_id) {
                return if original.admission_digest == admission_digest {
                    self.recheck(&original.result, &invocation, _context.as_ref())
                        .await
                } else {
                    Err(Error::Conflict(
                        "list_files operation identity changed".into(),
                    ))
                };
            }
            let result = self.query(&invocation, _context.as_ref(), None).await?;
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
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let observed = self.observed.lock().await;
            let admission_digest =
                crate::executor::json::canonical_json_digest(&invocation, u64::MAX, &mut None)?;
            match observed.get(&invocation.operation_id) {
                Some(original) if original.admission_digest == admission_digest => self
                    .recheck(&original.result, &invocation, _context.as_ref())
                    .await
                    .map(Some),
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
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a crate::runtime::ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.authorize(None, Some(context), invocation, Some(work))?;
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
        self.authorize(None, Some(context), invocation, Some(work))?;
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
                    output.1.resize(retained).map_err(Error::from)?;
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

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ToolProjection
    for LocalListFilesTool<A, O>
{
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
                Some(work),
            );
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
