//! `fs grep --scope AGENT LITERAL`: one paged search over an agent subtree's
//! committed records and the immutable files they reference. It reuses the
//! stream search frontier and the authorized content reader; nothing is indexed.
use super::*;
use crate::tools::fs_command::ScopeGrep;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as TOKEN};

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> FilesystemTools<P, O> {
    /// The cursor this query selects, after checking that its scope is inside
    /// the bound agent's own subtree. A token is data, never authority.
    pub(super) fn search_selection(
        &self,
        query: &ScopeGrep<'_>,
        context: &ToolContext,
    ) -> Result<acyclic_stream::search::Cursor> {
        let (current, _) = context
            .task()
            .agent()
            .ok_or_else(|| Error::Unauthorized("search requires an agent binding".into()))?;
        if *current != self.agent {
            return Err(Error::Unauthorized(
                "search agent differs from the original tool binding".into(),
            ));
        }
        let root = if query.scope == "." {
            current.clone()
        } else {
            crate::agent::AgentPath::try_from(query.scope.to_owned())
                .map_err(|error| Error::Invalid(error.to_string()))?
        };
        if !current.contains(&root) {
            return Err(Error::Unauthorized(
                "search scope is outside the bound agent subtree".into(),
            ));
        }
        self.verifier.verify(&self.scope, &mut None)?;
        let root = acyclic_stream::StreamPath::new(root.as_str())?;
        let pattern = query.pattern.as_bytes();
        let Some(token) = query.cursor else {
            return Ok(acyclic_stream::search::Cursor::new(root, pattern.to_vec()));
        };
        let cursor: acyclic_stream::search::Cursor = TOKEN
            .decode(token)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| Error::Invalid("invalid search cursor".into()))?;
        if cursor.selection() != Some((&root, pattern)) {
            return Err(Error::Invalid(
                "search cursor differs from selection".into(),
            ));
        }
        Ok(cursor)
    }

    pub(super) async fn search(
        &self,
        query: &ScopeGrep<'_>,
        context: &ToolContext,
    ) -> Result<ToolResult> {
        let cursor = self.search_selection(query, context)?;
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
            cursor
                .page_with(provider.as_ref(), query.size, &matcher)
                .await
        })
        .await?;
        // Authority is rechecked after the reads, so a revoked scope returns nothing.
        self.verifier.verify(&self.scope, &mut None)?;
        let next = page
            .next
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|error| Error::Invalid(error.to_string()))?
            .map(|bytes| TOKEN.encode(bytes));
        let matches = page
            .matches
            .iter()
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
        Ok(ToolResult {
            value: serde_json::json!({"result": {
                "kind": "grep",
                "matches": matches,
                "work": page.work,
                "next": next,
            }}),
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
