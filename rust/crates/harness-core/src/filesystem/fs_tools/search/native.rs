use super::response::{Reply, Rows, SearchResult};
use super::*;
use crate::executor::{PreparationIo, stock_boundary};
use crate::wire::ErrorCode;
use acyclic_stream::search::{Cursor, CursorState};

fn invalid(io: &PreparationIo, reason: &'static str) -> Error {
    stock_boundary(Some(io), ErrorCode::Invalid, reason)
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    fn native_search_selection(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
        io: &PreparationIo,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(acyclic_stream::NativeStreamOutput<Cursor>, NonZeroU32)> {
        let query = command.grep_with_io(Some(io))?;
        let (current, _) = context.task().agent().ok_or_else(|| {
            stock_boundary(
                Some(io),
                ErrorCode::Unauthorized,
                "search requires an agent binding",
            )
        })?;
        if *current != self.agent {
            return Err(stock_boundary(
                Some(io),
                ErrorCode::Unauthorized,
                "search agent differs from the original tool binding",
            ));
        }
        let root = match query.scope {
            Some(".") => current.as_str(),
            Some(root) => root,
            None => {
                return Err(stock_boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "native mounted-file search requires its original file cursor",
                ));
            }
        };
        if !current
            .contains_path(root)
            .map_err(|_| invalid(io, "invalid agent search scope"))?
        {
            return Err(stock_boundary(
                Some(io),
                ErrorCode::Unauthorized,
                "search scope is outside the bound agent subtree",
            ));
        }
        crate::executor::with_history_io(Some(io), |history| {
            self.verifier.verify(&self.scope, history)
        })?;
        let meter = work.runtime_meter();
        let cursor = match query.cursor {
            None => Cursor::new_native(root, query.pattern.as_bytes(), &meter)
                .map_err(|error| work.runtime_error(error))?
                .map_err(|_| invalid(io, "invalid search cursor"))?,
            Some(token) => {
                let mut bytes = io.buffer(base64::decoded_len_estimate(token.len()))?;
                io.charged(acyclic_fs::WorkCounters {
                    items_examined: token.len() as u64,
                    ..Default::default()
                })?;
                let length = STANDARD
                    .decode_slice(token, &mut bytes.0)
                    .map_err(|_| invalid(io, "invalid search cursor"))?;
                bytes.0.truncate(length);
                let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work,
                    cancellation: work.cancellation(),
                };
                let state = accounting
                    .decode_json::<CursorState>(&bytes.0)
                    .map_err(|error| match error {
                        acyclic_fs::kernel::MetadataDecodeError::Work(error) => Error::from(error),
                        acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                            invalid(io, "invalid search cursor")
                        }
                    })?;
                state
                    .to_native(&meter)
                    .map_err(|error| work.runtime_error(error))?
                    .map_err(|_| invalid(io, "invalid search cursor"))?
            }
        };
        if cursor.value().selection().is_none_or(|(path, pattern)| {
            path.as_str() != root || pattern != query.pattern.as_bytes()
        }) {
            return Err(invalid(io, "search cursor differs from selection"));
        }
        Ok((cursor, query.size))
    }

    pub(in crate::filesystem::fs_tools) async fn search_native(
        &self,
        invocation: &ToolInvocation,
        context: &ToolContext,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        self.authorize(
            Some(context.effective_scope()),
            Some(context),
            invocation,
            Some(work),
        )?;
        let command = invocation
            .arguments
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| invalid(&io, "fs requires a command string"))?;
        let parsed = crate::tools::fs_command::parse_fs_command_with_io(command, Some(&io))?;
        let parsed_hold = io.guard_owned(io.owned_bytes()?)?;
        let (cursor, size) = self.native_search_selection(&parsed, context, &io, work)?;
        let reader = super::super::super::FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &self.verifier,
            &self.scope,
            None,
            Some(&io),
        )?;
        let matcher = crate::search::ContentMatcher::with_native_work(
            reader.as_ref(),
            Some(context.effective_scope().grants()),
            work,
        )?;
        let provider = self.host.filesystem.stream_provider();
        let page = cursor
            .value()
            .native_page_with(provider.as_ref(), size, &matcher, work.runtime_meter())
            .await
            .map_err(|error| work.runtime_error(error))??;
        crate::executor::with_history_io(Some(&io), |history| {
            self.verifier.verify(&self.scope, history)
        })?;
        let encoded = page
            .value()
            .next
            .as_ref()
            .map(|next| io.encode(next, None, false))
            .transpose()?;
        let token = if let Some(encoded) = &encoded {
            let length = encoded
                .len()
                .div_ceil(3)
                .checked_mul(4)
                .ok_or_else(|| invalid(&io, "search cursor size overflow"))?;
            let mut bytes = io.buffer(length)?;
            io.charged(acyclic_fs::WorkCounters {
                bytes_encoded: length as u64,
                ..Default::default()
            })?;
            let written = STANDARD
                .encode_slice(encoded, &mut bytes.0)
                .map_err(|_| invalid(&io, "search cursor encoding failed"))?;
            bytes.0.truncate(written);
            let text = String::from_utf8(bytes.0)
                .map_err(|_| invalid(&io, "search cursor encoding is invalid"))?;
            Some((text, bytes.1))
        } else {
            None
        };
        let start = io.owned_bytes()?;
        let value = io.value_json(
            &Reply {
                result: SearchResult {
                    kind: "grep",
                    matches: Rows(&page.value().matches),
                    work: page.value().work,
                    next: token.as_ref().map(|token| token.0.as_str()),
                },
            },
            None,
        )?;
        let custody = io.take_native_custody(io.owned_bytes()? - start)?;
        drop(parsed);
        drop(parsed_hold);
        Ok((ToolResult { value }, custody))
    }
}
