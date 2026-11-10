use super::McpToolDefinition;
use crate::{Error, Result};
use futures::{Stream, stream};
use std::{collections::BTreeSet, future::Future};

/// Lazily traverses an MCP tools/list catalog, one server-sized page per demand.
///
/// The first request has no cursor. Each continuation is passed through unchanged;
/// an absent continuation ends the stream, including after an empty page. Empty
/// or repeated continuation cursors are protocol errors. Dropping the stream
/// cancels an in-flight request and never fetches another page. Transports enforce
/// their protective frame bound on each response; catalog size is unbounded.
pub fn tool_pages<F, Fut>(list: F) -> impl Stream<Item = Result<Vec<McpToolDefinition>>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<rmcp::model::ListToolsResult>>,
{
    stream::try_unfold(
        (list, None, BTreeSet::new(), false),
        |(mut list, cursor, mut seen, done)| async move {
            if done {
                return Ok(None);
            }
            let page = list(cursor).await?;
            if let Some(next) = &page.next_cursor {
                if next.is_empty() || !seen.insert(next.clone()) {
                    return Err(Error::Invalid(
                        "MCP discovery repeated an invalid cursor".into(),
                    ));
                }
            }
            let done = page.next_cursor.is_none();
            let tools = page
                .tools
                .into_iter()
                .map(McpToolDefinition::from)
                .collect();
            Ok(Some((tools, (list, page.next_cursor, seen, done))))
        },
    )
}

#[cfg(test)]
mod tests;
