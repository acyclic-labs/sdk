//! Recheck the original sources before exposing a saved query result.
use super::*;
use crate::conversation::{ContentAccess, ContentResidencyVerifier, FileRef};
use crate::executor::{PreparationIo, stock_boundary};
use crate::wire::ErrorCode;
use serde::Deserialize;

enum Source<'a> {
    Records(&'a str),
    Files(
        &'a crate::context::ContextRoot,
        &'a str,
        crate::tool::ReadPathKind,
    ),
}

fn file_source(cursor: &crate::search::FileCursor) -> Source<'_> {
    let (root, path, kind, _) = cursor.selection();
    Source::Files(root, path, kind)
}

fn portable_source(cursor: &Selection) -> Result<Source<'_>> {
    Ok(match cursor {
        Selection::Records(cursor) => Source::Records(
            cursor
                .selection()
                .ok_or_else(|| invalid(None, "search cursor has no root"))?
                .0
                .as_str(),
        ),
        Selection::Files(cursor) => file_source(cursor),
    })
}
fn invalid(io: Option<&PreparationIo>, reason: &'static str) -> Error {
    stock_boundary(io, ErrorCode::Invalid, reason)
}

fn within(root: &str, path: &str) -> bool {
    root.is_empty()
        || root == path
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    pub(in crate::filesystem::fs_tools) async fn authorize_search_result(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
        result: &ToolResult,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        // Rejected input never selected or disclosed a source.
        if result
            .value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .is_some()
        {
            return Ok(());
        }
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        let portable;
        #[cfg(not(target_arch = "wasm32"))]
        let native;
        #[cfg(not(target_arch = "wasm32"))]
        let source = if let Some(work) = work {
            native = self
                .native_search_selection(
                    command,
                    context,
                    io.ok_or(Error::NativeBoundary {
                        code: ErrorCode::Storage,
                        reason: "native replay lacks original accounting",
                    })?,
                    work,
                )?
                .0;
            match &native {
                native::NativeSelection::Records(cursor) => Source::Records(
                    cursor
                        .value()
                        .selection()
                        .ok_or_else(|| invalid(io, "search cursor has no root"))?
                        .0
                        .as_str(),
                ),
                native::NativeSelection::Files(cursor) => file_source(cursor),
            }
        } else {
            portable = self.search_selection(command, context)?.0;
            portable_source(&portable)?
        };
        #[cfg(target_arch = "wasm32")]
        let source = {
            portable = self.search_selection(command, context)?.0;
            portable_source(&portable)?
        };
        let rows = result
            .value
            .get("result")
            .filter(|result| result.get("kind").and_then(serde_json::Value::as_str) == Some("grep"))
            .and_then(|result| result.get("matches"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| invalid(io, "saved search result has no matches"))?;
        let reader = &self.reader;
        let ceiling = Some(context.effective_scope().grants());
        let mut last_path = None;
        for row in rows {
            match &source {
                Source::Records(root) => {
                    let path = row
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| invalid(io, "saved search match has no source path"))?;
                    if !within(root, path) {
                        return Err(stock_boundary(
                            io,
                            ErrorCode::Unauthorized,
                            "saved search match is outside its original scope",
                        ));
                    }
                    if last_path != Some(path) {
                        self.recheck_stream(path, io).await?;
                        last_path = Some(path);
                    }
                    if let Some(file) = row.get("file").filter(|file| !file.is_null()) {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(work) = work {
                            let mut accounting =
                                acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                                    work,
                                    cancellation: work.cancellation(),
                                };
                            let file =
                                accounting
                                    .deserialize::<FileRef, _>(file)
                                    .map_err(|error| match error {
                                        acyclic_fs::kernel::MetadataDecodeError::Work(error) => {
                                            Error::from(error)
                                        }
                                        acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                                            invalid(io, "invalid saved file address")
                                        }
                                    })?;
                            reader
                                .read_range(
                                    &file,
                                    acyclic_fs::ByteRange {
                                        offset: 0,
                                        length: 0,
                                    },
                                    ceiling,
                                    io,
                                )?
                                .await?;
                            continue;
                        }
                        let file = FileRef::deserialize(file)
                            .map_err(|_| invalid(io, "invalid saved file address"))?;
                        reader
                            .read_range(
                                &file,
                                acyclic_fs::ByteRange {
                                    offset: 0,
                                    length: 0,
                                },
                                ceiling,
                                io,
                            )?
                            .await?;
                    }
                }
                Source::Files(root, path, kind) => {
                    let ordinary;
                    #[cfg(not(target_arch = "wasm32"))]
                    let retained;
                    #[cfg(not(target_arch = "wasm32"))]
                    let found = if let Some(work) = work {
                        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                            work,
                            cancellation: work.cancellation(),
                        };
                        retained = accounting
                            .deserialize::<crate::search::FileMatch, _>(row)
                            .map_err(|error| match error {
                                acyclic_fs::kernel::MetadataDecodeError::Work(error) => {
                                    Error::from(error)
                                }
                                acyclic_fs::kernel::MetadataDecodeError::Codec(_) => {
                                    invalid(io, "invalid saved file match")
                                }
                            })?;
                        &*retained
                    } else {
                        ordinary = crate::search::FileMatch::deserialize(row)
                            .map_err(|_| invalid(io, "invalid saved file match"))?;
                        &ordinary
                    };
                    #[cfg(target_arch = "wasm32")]
                    let found = {
                        ordinary = crate::search::FileMatch::deserialize(row)
                            .map_err(|_| invalid(io, "invalid saved file match"))?;
                        &ordinary
                    };
                    if found.file.volume != root.volume
                        || !within(path, &found.file.path)
                        || (*kind == crate::tool::ReadPathKind::File
                            && found.file.path.as_str() != *path)
                    {
                        return Err(stock_boundary(
                            io,
                            ErrorCode::Unauthorized,
                            "saved file match is outside its original scope",
                        ));
                    }
                    reader
                        .read_private_range(
                            &root.volume,
                            &root.directory,
                            &found.file.path,
                            Some(&found.file.generation),
                            acyclic_fs::ByteRange {
                                offset: 0,
                                length: 0,
                            },
                            ContentAccess { ceiling, io },
                        )?
                        .await?;
                }
            }
        }
        crate::executor::with_history_io(io, |history| self.verifier.verify(&self.scope, history))
    }

    async fn recheck_stream(&self, path: &str, io: Option<&PreparationIo>) -> Result<()> {
        let provider = self.host.filesystem.stream_provider();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            let path = acyclic_stream::StreamPath::new_native(path, &work.runtime_meter())
                .map_err(|error| work.runtime_error(error))?
                .map_err(|_| invalid(io, "invalid saved source path"))?;
            let request = stream_request(path);
            let observed = provider
                .native_heads(request.clone(), work.runtime_meter())
                .await
                .map_err(|error| work.runtime_error(error))?
                .map_err(|source| Error::NativeStream {
                    code: if source.category() == acyclic_stream::StreamError::AccessDenied {
                        ErrorCode::Unauthorized
                    } else {
                        ErrorCode::Storage
                    },
                    source,
                    custody: None,
                })?;
            observed
                .value()
                .validate_for(&request)
                .map_err(|source| Error::NativeStream {
                    code: ErrorCode::Storage,
                    source,
                    custody: None,
                })?;
            return Ok(());
        }
        let request = stream_request(acyclic_stream::StreamPath::new(path)?);
        use acyclic_stream::StreamProviderExt as _;
        provider
            .heads(request.clone())
            .await?
            .validate_for(&request)?;
        Ok(())
    }
}

fn stream_request(path: acyclic_stream::StreamPath) -> acyclic_stream::HeadsRequest {
    acyclic_stream::HeadsRequest {
        paths: std::iter::once(path).collect(),
        projection: acyclic_stream::HeadProjection::Tail,
        limit: 1,
        at: None,
        retained: Default::default(),
        durable_through: None,
        children: None,
        at_least: Default::default(),
    }
}
