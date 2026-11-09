//! A disposable ordering of the visible LATEST status follow. The caller owns
//! subscription/page demand; this function never discovers or scans agents.
use crate::{AgentPath, StatusRecord, reader::Want};
use pulldown_cmark::{Event, Parser, Tag};
use std::collections::BTreeMap;

/// Demand the LATEST status of only agents in the visible subtree pages. Use
/// these same status paths with `Reader::set_follows`; child discovery remains
/// the view's ordinary paged `children_page` demand.
pub fn attention_wants<'a>(
    visible: impl IntoIterator<Item = &'a AgentPath>,
) -> super::Result<Vec<Want>> {
    visible
        .into_iter()
        .map(|agent| {
            Ok(Want::Last {
                stream: agent.status()?,
                count: 1,
            })
        })
        .collect()
}

/// Filter followed statuses by the same lossless attention flag the publisher
/// uses. An authored queue ranks matching agent-path links; unranked entries
/// retain stream-path order. Links are data and are never fetched.
pub fn attention<'a>(
    visible: impl IntoIterator<Item = (&'a AgentPath, &'a StatusRecord)>,
    queue: Option<&str>,
) -> Vec<&'a AgentPath> {
    let mut entries = visible
        .into_iter()
        .filter(|(_, s)| s.phase.attention())
        .map(|(path, _)| (path.as_str(), path))
        .collect::<BTreeMap<_, _>>();
    let mut ordered = Vec::new();
    if let Some(queue) = queue.filter(|s| s.len() <= super::blocks::MAX_DOCUMENT_BYTES) {
        for event in Parser::new(queue) {
            if let Event::Start(Tag::Link { dest_url, .. }) = event
                && let Some(path) = entries.remove(dest_url.as_ref())
            {
                ordered.push(path);
            }
        }
    }
    ordered.extend(entries.into_values());
    ordered
}
