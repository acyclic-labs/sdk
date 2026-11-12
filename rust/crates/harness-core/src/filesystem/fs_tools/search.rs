//! Stream scope selection for the existing read-only filesystem grep facade.
use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::num::NonZeroU32;

pub(super) fn selected(command: &crate::tools::FsCommand) -> bool {
    matches!(command.arguments.as_slice(), [grep, scope, ..] if grep == "grep" && scope == "--scope")
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    pub(super) fn search_selection(
        &self,
        command: &crate::tools::FsCommand,
        context: &ToolContext,
    ) -> Result<(acyclic_stream::search::Cursor, NonZeroU32)> {
        let [_, _, root, pattern, options @ ..] = command.arguments.as_slice() else {
            return Err(Error::Invalid(
                "fs grep --scope AGENT PATTERN [--page-size N] [--cursor TOKEN]".into(),
            ));
        };
        if command.volume.is_some() {
            return Err(Error::Invalid(
                "scope grep selects streams; use --volume for file grep".into(),
            ));
        }
        let (current, _) = context
            .task()
            .agent()
            .ok_or_else(|| Error::Unauthorized("scope grep requires an agent binding".into()))?;
        if *current != self.agent {
            return Err(Error::Unauthorized(
                "search agent differs from the original tool binding".into(),
            ));
        }
        let root = if root == "." {
            current.clone()
        } else {
            crate::agent::AgentPath::try_from(root.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?
        };
        if !current.contains(&root) {
            return Err(Error::Unauthorized(
                "search scope is outside the bound agent subtree".into(),
            ));
        }
        self.verifier.verify(&self.scope, &mut None)?;
        let mut size = None;
        let mut token = None;
        for option in options.chunks(2) {
            match option {
                [name, value] if name == "--page-size" && size.is_none() => {
                    size =
                        Some(value.parse::<NonZeroU32>().map_err(|_| {
                            Error::Invalid("search page size must be nonzero".into())
                        })?);
                }
                [name, value] if name == "--cursor" && token.is_none() => token = Some(value),
                _ => return Err(Error::Invalid("invalid scope grep option".into())),
            }
        }
        let root = acyclic_stream::StreamPath::new(root.as_str())?;
        let cursor = match token {
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
        // This is only the default chunk size; next continues until exhaustion.
        Ok((
            cursor,
            size.unwrap_or(
                NonZeroU32::new(64).ok_or_else(|| Error::Invalid("invalid search chunk".into()))?,
            ),
        ))
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
        let matcher = crate::search::ContentMatcher::new(
            reader.as_ref(),
            Some(context.effective_scope().grants()),
        );
        let page = read_before_effect(&mut context.model_cancellation(None)?, async {
            cursor.page_with(provider.as_ref(), size, &matcher).await
        })
        .await?;
        self.verifier.verify(&self.scope, &mut None)?;
        let matches = page
            .matches
            .into_iter()
            .map(|found| {
                serde_json::json!({
                    "path": found.path.as_str(),
                    "sequence": found.record.sequence,
                    "commit": hex::encode(found.record.commit_id.as_bytes()),
                    "origin": found.record.origin.as_str(),
                    "file": found.source,
                })
            })
            .collect::<Vec<_>>();
        let next = page
            .next
            .map(|cursor| serde_json::to_vec(&cursor).map(|bytes| STANDARD.encode(bytes)))
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(ToolResult {
            value: serde_json::json!({"result": {"kind": "grep", "matches": matches, "work": page.work, "next": next}}),
        })
    }
}
