use super::response::{Reply, Rows, SearchResult};
use super::*;
use crate::executor::{PreparationIo, stock_boundary};
use crate::wire::ErrorCode;
use acyclic_fs::kernel::Retained;
use acyclic_stream::search::{Cursor, CursorState};
pub(super) enum NativeSelection {
    Records(acyclic_stream::NativeStreamOutput<Cursor>),
    Files(Retained<crate::search::FileCursor>),
}

fn invalid(io: &PreparationIo, reason: &'static str) -> Error {
    stock_boundary(Some(io), ErrorCode::Invalid, reason)
}

fn decode_cursor<T: serde::de::DeserializeOwned>(
    token: &str,
    io: &PreparationIo,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<Retained<T>> {
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
    accounting
        .decode_json(&bytes.0)
        .map_err(|error| match error {
            acyclic_fs::kernel::MetadataDecodeError::Work(error) => Error::from(error),
            acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                invalid(io, "invalid search cursor")
            }
        })
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    fn native_file_selection(
        &self,
        command: &crate::tools::FsCommand,
        query: &crate::tools::fs_command::Grep<'_>,
        io: &PreparationIo,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Retained<crate::search::FileCursor>> {
        let name = command
            .volume
            .as_deref()
            .or(self.primary.as_deref())
            .ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    ErrorCode::NotFound,
                    "primary filesystem workspace",
                )
            })?;
        let binding = self.volumes.get(name).ok_or_else(|| {
            stock_boundary(
                Some(io),
                ErrorCode::NotFound,
                "filesystem volume is not bound",
            )
        })?;
        let volume = binding
            .as_volume()
            .ok_or_else(|| invalid(io, "filesystem workspace is a subtree view"))?;
        if volume.provider() != &self.host.provider {
            return Err(stock_boundary(
                Some(io),
                ErrorCode::Unauthorized,
                "filesystem volume belongs to another provider",
            ));
        }
        let start = io.owned_bytes()?;
        let root = crate::executor::with_history_io(Some(io), |history| {
            crate::wire_codec::CodecQuota::produce(history, |quota| {
                Ok(crate::context::ContextRoot {
                    volume: volume.clone_in_quota(quota)?,
                    directory: crate::core::copy_text(&binding.root, quota)?,
                })
            })
            .map(|(root, _)| root)
        })?;
        let (kind, relative) = query
            .path
            .unwrap_or((crate::tool::ReadPathKind::Directory, ""));
        let mut path = crate::executor::stock_string_copy(&root.directory, Some(io))?;
        if !path.is_empty() && !relative.is_empty() {
            crate::executor::stock_string_push(
                &mut path,
                "/",
                None,
                "search path size overflow",
                Some(io),
            )?;
        }
        crate::executor::stock_string_push(
            &mut path,
            relative,
            None,
            "search path size overflow",
            Some(io),
        )?;
        let selection = (root, path, io.guard_owned(io.owned_bytes()? - start)?);
        let cursor = match query.cursor {
            None => crate::search::FileCursor::new_with_native_work(
                &selection.0,
                &selection.1,
                kind,
                query.pattern.as_bytes(),
                work,
            )?,
            Some(token) => decode_cursor(token, io, work)?,
        };
        if cursor.selection()
            != (
                &selection.0,
                selection.1.as_str(),
                kind,
                query.pattern.as_bytes(),
            )
        {
            return Err(invalid(io, "search cursor differs from selection"));
        }
        Ok(cursor)
    }

    pub(super) fn native_search_selection(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
        io: &PreparationIo,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(NativeSelection, NonZeroU32)> {
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
        crate::executor::with_history_io(Some(io), |history| {
            self.verifier.verify(&self.scope, history)
        })?;
        let root = match query.scope {
            Some(".") => current.as_str(),
            Some(root) => root,
            None => {
                return self
                    .native_file_selection(command, &query, io, work)
                    .map(|cursor| (NativeSelection::Files(cursor), query.size));
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
        let meter = work.runtime_meter();
        let cursor = match query.cursor {
            None => Cursor::new_native(root, query.pattern.as_bytes(), &meter)
                .map_err(|error| work.runtime_error(error))?
                .map_err(|_| invalid(io, "invalid search cursor"))?,
            Some(token) => {
                let state = decode_cursor::<CursorState>(token, io, work)?;
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
        Ok((NativeSelection::Records(cursor), query.size))
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
        let output = match cursor {
            NativeSelection::Records(cursor) => {
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
                self.native_search_result(
                    Rows(&page.value().matches),
                    page.value().work,
                    page.value().next.as_ref(),
                    &io,
                )
            }
            NativeSelection::Files(cursor) => {
                let page = cursor
                    .page_with_native_work(
                        reader.as_ref(),
                        Some(context.effective_scope().grants()),
                        size,
                        work,
                    )?
                    .await?;
                self.native_search_result(&page.matches, page.work, page.next.as_ref(), &io)
            }
        };
        drop(parsed);
        drop(parsed_hold);
        output
    }

    fn native_search_result<M: serde::Serialize, C: serde::Serialize>(
        &self,
        matches: M,
        progress: u32,
        next: Option<&C>,
        io: &PreparationIo,
    ) -> Result<(ToolResult, acyclic_fs::NativeAllocation)> {
        crate::executor::with_history_io(Some(io), |history| {
            self.verifier.verify(&self.scope, history)
        })?;
        let encoded = next.map(|next| io.encode(next, None, false)).transpose()?;
        let token = if let Some(encoded) = &encoded {
            let length = encoded
                .len()
                .div_ceil(3)
                .checked_mul(4)
                .ok_or_else(|| invalid(io, "search cursor size overflow"))?;
            let mut bytes = io.buffer(length)?;
            io.charged(acyclic_fs::WorkCounters {
                bytes_encoded: length as u64,
                ..Default::default()
            })?;
            let written = STANDARD
                .encode_slice(encoded, &mut bytes.0)
                .map_err(|_| invalid(io, "search cursor encoding failed"))?;
            bytes.0.truncate(written);
            let text = String::from_utf8(bytes.0)
                .map_err(|_| invalid(io, "search cursor encoding is invalid"))?;
            Some((text, bytes.1))
        } else {
            None
        };
        let start = io.owned_bytes()?;
        let value = io.value_json(
            &Reply {
                result: SearchResult {
                    kind: "grep",
                    matches,
                    work: progress,
                    next: token.as_ref().map(|token| token.0.as_str()),
                },
            },
            None,
        )?;
        let custody = io.take_native_custody(io.owned_bytes()? - start)?;
        Ok((ToolResult { value }, custody))
    }
}
