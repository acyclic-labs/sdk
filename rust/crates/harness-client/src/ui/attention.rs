//! A disposable ordering of the visible LATEST status follow. The caller owns
//! subscription/page demand; this function never discovers or scans agents.
use crate::{AgentPath, StatusRecord, reader::Want};
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
/// uses. Retain stream-path order; questions go directly to the human.
pub fn attention<'a>(
    visible: impl IntoIterator<Item = (&'a AgentPath, &'a StatusRecord)>,
) -> Vec<&'a AgentPath> {
    visible
        .into_iter()
        .filter(|(_, s)| s.phase.attention())
        .map(|(path, _)| (path.as_str(), path))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect()
}
