use super::McpToolDefinition;
use crate::{Error, Result};
use futures::{Stream, stream};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, future::Future};

/// Independent caller reservations for one advertised tools/list traversal.
/// Absent policies are unbounded; filtered-out tools still consume reservations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryLimits {
    /// Total advertised tools, before local selection.
    pub tools: Option<u32>,
    /// Requests in one traversal, including empty pages.
    pub pages: Option<u32>,
    /// Sum of the original serialized pages, before local selection.
    pub bytes: Option<u32>,
}

impl McpDiscoveryLimits {
    /// Reject invalid reservations before opening a connection or requesting a page.
    pub fn validate(self) -> Result<()> {
        if [self.tools, self.pages, self.bytes].contains(&Some(0)) {
            return Err(Error::Invalid(
                "MCP catalog reservations must be positive".into(),
            ));
        }
        Ok(())
    }

    fn begin_page(&mut self) -> Result<()> {
        if self.bytes == Some(0) {
            return Err(Error::Invalid(
                "MCP discovery exceeds byte reservation".into(),
            ));
        }
        Self::consume(&mut self.pages, 1, "MCP discovery exceeds page reservation")
    }

    pub(super) fn admit_tools(&mut self, tools: usize) -> Result<()> {
        Self::consume(
            &mut self.tools,
            tools as u64,
            "MCP discovery exceeds tool reservation",
        )
    }

    pub(super) fn admit_bytes<T: Serialize + ?Sized>(&mut self, page: &T) -> Result<()> {
        if let Some(bytes) = self.bytes {
            let length =
                crate::executor::json::json_byte_length(page, Some(u64::from(bytes)), &mut None)?;
            Self::consume(
                &mut self.bytes,
                length,
                "MCP discovery exceeds byte reservation",
            )?;
        }
        Ok(())
    }

    fn consume(remaining: &mut Option<u32>, amount: u64, reason: &'static str) -> Result<()> {
        if let Some(value) = remaining {
            let rest = u64::from(*value)
                .checked_sub(amount)
                .ok_or_else(|| Error::Invalid(reason.into()))?;
            *value = u32::try_from(rest).map_err(|_| Error::Invalid(reason.into()))?;
        }
        Ok(())
    }
}

/// Lazily traverses an MCP tools/list catalog, one server-sized page per demand.
///
/// Reservations charge the original advertised pages before selecting tools.
/// A page reservation refuses before the next request. Absent reservations impose
/// no aggregate counter or ceiling. The first request has no cursor; continuations
/// pass through unchanged. Empty or repeated cursors are protocol errors. Dropping
/// the stream cancels its current request and never fetches another page.
pub fn tool_pages<F, Fut, Selected>(
    list: F,
    limits: McpDiscoveryLimits,
    selected: Selected,
) -> Result<impl Stream<Item = Result<Vec<McpToolDefinition>>>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: Future<Output = Result<rmcp::model::ListToolsResult>>,
    Selected: FnMut(&str) -> bool,
{
    limits.validate()?;
    Ok(stream::try_unfold(
        (list, selected, limits, None, BTreeSet::new(), false),
        |(mut list, mut selected, mut remaining, cursor, mut seen, done)| async move {
            if done {
                return Ok(None);
            }
            remaining.begin_page()?;
            let page = list(cursor).await?;
            if let Some(next) = &page.next_cursor {
                if next.is_empty() || !seen.insert(next.clone()) {
                    return Err(Error::Invalid(
                        "MCP discovery repeated an invalid cursor".into(),
                    ));
                }
            }
            remaining.admit_tools(page.tools.len())?;
            remaining.admit_bytes(&page)?;
            let done = page.next_cursor.is_none();
            let tools = page
                .tools
                .into_iter()
                .filter(|tool| selected(&tool.name))
                .map(McpToolDefinition::from)
                .collect();
            Ok(Some((
                tools,
                (list, selected, remaining, page.next_cursor, seen, done),
            )))
        },
    ))
}

#[cfg(test)]
mod tests;
