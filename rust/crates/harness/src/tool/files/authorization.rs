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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Error::ToolRejected { code, reason };
            }
            crate::executor::with_history_io(io, |history| {
                crate::store::guarded_boundary(history, code, reason)
            })
        };
        let (file, custody) = match *self {
            Self::Read => {
                let input = decode::<ReadFileInput>(&invocation.arguments, work)?;
                let maximum = scope.limits().render_bytes.min(scope.limits().file_bytes);
                let length = match input.0.range {
                    Some(range) if range.end <= input.0.file.descriptor().byte_length() => {
                        range.end.checked_sub(range.start)
                    }
                    Some(_) => None,
                    None => Some(input.0.file.descriptor().byte_length()),
                };
                if maximum == 0
                    || maximum == u64::MAX
                    || length.is_none_or(|length| length > maximum)
                {
                    return Err(error(
                        crate::wire::ErrorCode::Invalid,
                        "file selection exceeds text rendering bound",
                    ));
                }
                (input.0.file, input.1)
            }
            Self::Write => {
                let input = decode::<WriteFileInput>(&invocation.arguments, work)?;
                self.public_path(&input.0.path, io)?;
                if input.0.path.len() > scope.limits().path_bytes {
                    return Err(error(
                        crate::wire::ErrorCode::Invalid,
                        "file path exceeds original task bound",
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
