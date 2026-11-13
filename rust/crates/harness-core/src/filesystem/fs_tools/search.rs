//! Stream scope selection for the existing read-only filesystem grep facade.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::num::NonZeroU32;
#[cfg(not(target_arch = "wasm32"))]
mod native;
mod replay;
mod response;

pub(super) fn selected(command: &crate::tools::FsCommand) -> bool {
    command
        .arguments
        .first()
        .is_some_and(|command| command == "grep")
}

pub(super) enum Selection {
    Records(acyclic_stream::search::Cursor),
    Files(crate::search::FileCursor),
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    pub(super) fn search_selection(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
    ) -> Result<(Selection, NonZeroU32)> {
        let query = command.grep()?;
        let pattern = query.pattern;
        let (current, _) = context
            .task()
            .agent()
            .ok_or_else(|| Error::Unauthorized("search requires an agent binding".into()))?;
        if *current != self.agent {
            return Err(Error::Unauthorized(
                "search agent differs from the original tool binding".into(),
            ));
        }
        let Some(root) = query.scope else {
            let name = command
                .volume
                .as_deref()
                .or(self.primary.as_deref())
                .ok_or_else(|| Error::NotFound("primary filesystem workspace".into()))?;
            let binding = self
                .volumes
                .get(name)
                .ok_or_else(|| Error::NotFound(format!("filesystem volume {name}")))?;
            let volume = binding.volume()?.clone();
            if volume.provider() != &self.host.provider {
                return Err(Error::Unauthorized(
                    "filesystem volume belongs to another provider".into(),
                ));
            }
            self.verifier.verify(&self.scope, &mut None)?;
            let root = crate::context::ContextRoot {
                volume,
                directory: binding.root.clone(),
            };
            let (kind, relative) = query
                .path
                .unwrap_or((crate::tool::ReadPathKind::Directory, ""));
            let path = if root.directory.is_empty() {
                relative.to_owned()
            } else if relative.is_empty() {
                root.directory.clone()
            } else {
                format!("{}/{relative}", root.directory)
            };
            let cursor = match query.cursor {
                None => crate::search::FileCursor::new(
                    root.clone(),
                    path.clone(),
                    kind,
                    pattern.as_bytes().to_vec(),
                )?,
                Some(token) => {
                    let bytes = STANDARD
                        .decode(token)
                        .map_err(|_| Error::Invalid("invalid search cursor".into()))?;
                    let cursor: crate::search::FileCursor = serde_json::from_slice(&bytes)
                        .map_err(|_| Error::Invalid("invalid search cursor".into()))?;
                    if cursor.selection() != (&root, path.as_str(), kind, pattern.as_bytes()) {
                        return Err(Error::Invalid(
                            "search cursor differs from selection".into(),
                        ));
                    }
                    cursor
                }
            };
            return Ok((Selection::Files(cursor), query.size));
        };
        let root = if root == "." {
            current.clone()
        } else {
            crate::agent::AgentPath::try_from(root.to_owned())
                .map_err(|error| Error::Invalid(error.to_string()))?
        };
        if !current.contains(&root) {
            return Err(Error::Unauthorized(
                "search scope is outside the bound agent subtree".into(),
            ));
        }
        self.verifier.verify(&self.scope, &mut None)?;
        let root = acyclic_stream::StreamPath::new(root.as_str())?;
        let cursor = match query.cursor {
            None => acyclic_stream::search::Cursor::new(root.clone(), pattern.as_bytes().to_vec()),
            Some(token) => {
                let bytes = STANDARD
                    .decode(token)
                    .map_err(|_| Error::Invalid("invalid search cursor".into()))?;
                let cursor: acyclic_stream::search::Cursor = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Invalid("invalid search cursor".into()))?;
                if cursor.selection() != Some((&root, pattern.as_bytes())) {
                    return Err(Error::Invalid(
                        "search cursor differs from selection".into(),
                    ));
                }
                cursor
            }
        };
        Ok((Selection::Records(cursor), query.size))
    }

    pub(super) async fn search(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
    ) -> Result<ToolResult> {
        let (cursor, size) = self.search_selection(command, context)?;
        let provider = self.host.filesystem.stream_provider();
        let reader = super::super::FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &self.verifier,
            &self.scope,
            u64::MAX,
            None,
        )?;
        let cursor = match cursor {
            Selection::Records(cursor) => cursor,
            Selection::Files(cursor) => {
                let page = read_before_effect(&mut context.model_cancellation(None)?, async {
                    cursor
                        .page(
                            reader.as_ref(),
                            Some(context.effective_scope().grants()),
                            size,
                        )
                        .await
                })
                .await?;
                self.verifier.verify(&self.scope, &mut None)?;
                let next = page
                    .next
                    .map(|cursor| serde_json::to_vec(&cursor).map(|bytes| STANDARD.encode(bytes)))
                    .transpose()
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                return Ok(ToolResult {
                    value: serde_json::json!({"result": {
                        "kind": "grep", "matches": page.matches, "work": page.work, "next": next
                    }}),
                });
            }
        };
        let matcher = crate::search::ContentMatcher::new(
            reader.as_ref(),
            Some(context.effective_scope().grants()),
        );
        let page = read_before_effect(&mut context.model_cancellation(None)?, async {
            cursor.page_with(provider.as_ref(), size, &matcher).await
        })
        .await?;
        self.verifier.verify(&self.scope, &mut None)?;
        let next = page
            .next
            .as_ref()
            .map(|cursor| serde_json::to_vec(&cursor).map(|bytes| STANDARD.encode(bytes)))
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(ToolResult {
            value: serde_json::to_value(response::Reply {
                result: response::SearchResult {
                    kind: "grep",
                    matches: response::Rows(&page.matches),
                    work: page.work,
                    next: next.as_deref(),
                },
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        })
    }
}
