use super::*;

impl FileTool {
    pub(in crate::tool) fn preflight(
        &self,
        scope: &RuntimeScope,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<(Option<FileRef>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<&crate::executor::PreparationIo> = None;
        crate::executor::with_history_io(io, |history| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), history)
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            io.check_json_byte_bound(&invocation.arguments, scope.limits().file_bytes)?;
        } else {
            crate::executor::json::json_byte_length(
                &invocation.arguments,
                scope.limits().file_bytes,
                &mut None,
            )?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        crate::executor::json::json_byte_length(
            &invocation.arguments,
            scope.limits().file_bytes,
            &mut None,
        )?;
        let (file, custody) = self.decode_source(scope, invocation, work, io)?;
        let Some(file) = file else {
            return Ok((None, custody));
        };
        self.public_path(file.path(), io)?;
        crate::executor::with_history_io(io, |history| {
            scope.limits().validate_file(&file, history)?;
            if !crate::runtime::read_granted_with_work(scope.grants(), &file, history)? {
                return Err(crate::store::guarded_boundary(
                    history,
                    crate::wire::ErrorCode::Unauthorized,
                    "file tool requires original source read grant",
                ));
            }
            Ok(())
        })?;
        Ok((Some(file), custody))
    }

    /// Validates a model-supplied workspace path before any resolution.
    fn workspace_path(
        &self,
        path: &str,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.public_path(path, io)?;
        if path.len() > scope.limits().path_bytes {
            return Err(crate::executor::with_history_io(io, |history| {
                crate::store::guarded_boundary(
                    history,
                    crate::wire::ErrorCode::Invalid,
                    "file path exceeds original task bound",
                )
            }));
        }
        Ok(())
    }

    fn public_path(&self, path: &str, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        crate::executor::with_history_io(io, |history| {
            crate::conversation::validate_content_path(path, history)?;
            if crate::conversation::is_internal_path(path) {
                return Err(crate::store::guarded_boundary(
                    history,
                    crate::wire::ErrorCode::Unauthorized,
                    "file tools cannot access internal storage",
                ));
            }
            Ok(())
        })
    }

    fn decode_source(
        &self,
        scope: &RuntimeScope,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Option<FileRef>, crate::NativeCustody)> {
        let error = |code, reason| {
            crate::executor::with_history_io(io, |history| {
                crate::store::guarded_boundary(history, code, reason)
            })
        };
        let (file, custody) = match *self {
            Self::Read => {
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                self.workspace_path(&input.0.path, scope, io)?;
                let maximum = scope.limits().render_bytes.min(scope.limits().file_bytes);
                if input.0.offset == Some(0)
                    || input.0.limit == Some(0)
                    || maximum == 0
                    || maximum == u64::MAX
                {
                    return Err(error(
                        crate::wire::ErrorCode::Invalid,
                        "read offset and limit are one-based and need a finite rendering bound",
                    ));
                }
                return Ok((None, input.1));
            }
            Self::Write => {
                let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
                self.workspace_path(&input.0.path, scope, io)?;
                return Ok((None, input.1));
            }
            Self::Edit => {
                let input = decode::<EditFileInput>(&invocation.arguments, work)?;
                self.workspace_path(&input.0.path, scope, io)?;
                if input.0.old_string.is_empty() {
                    return Err(error(
                        crate::wire::ErrorCode::Invalid,
                        "edit old_string must be nonempty",
                    ));
                }
                return Ok((None, input.1));
            }
            Self::Search {
                options,
                maximum_result_bytes,
            } => {
                let input =
                    decode::<super::super::text_files::SearchInput>(&invocation.arguments, work)?;
                options.validate()?;
                if input.0.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.0.query.is_empty()
                    || input.0.query.len() as u64 > options.maximum_query_bytes
                    || u64::from(options.maximum_matches)
                        * std::mem::size_of::<super::text::TextRange>() as u64
                        > maximum_result_bytes.min(scope.limits().file_bytes)
                {
                    return Err(error(
                        crate::wire::ErrorCode::Invalid,
                        "search exceeds its admitted input or retained-position bounds",
                    ));
                }
                (input.0.file, input.1)
            }
            _ => decode::<FileRef>(
                invocation.arguments.get("file").ok_or_else(|| {
                    error(
                        crate::wire::ErrorCode::Invalid,
                        "file tool requires an original source reference",
                    )
                })?,
                work,
            )?,
        };
        Ok((Some(file), custody))
    }
}
