//! Demand-selected trace windows on the ordinary authenticated file query.
use super::{FileRoot, FileView, QueryFile};
use crate::{Error, Result, agent::AgentPath, supervision::Position};
use acyclic_stream::{ReadRequest, StreamPath, StreamProvider, trace::TraceQuery};

/// Cursor windows, `live/agents/self/trace/<from>/<count>.json`. Counts split
/// work; there is no default history scan or server-selected retention bound.
pub const PREFIX: &str = "live/agents/self/trace/";

/// One agent's original trace subtree. Children own their own trace histories.
pub fn source(agent: &AgentPath) -> Result<StreamPath> {
    StreamPath::new(format!("{agent}/trace")).map_err(Error::from)
}

/// Pure window projection installed on the existing local and remote file host.
pub struct TraceFiles<P: ?Sized> {
    registry: acyclic_stream::trace::Registry<P>,
}

impl<P: StreamProvider + ?Sized> TraceFiles<P> {
    /// Bind the host's shared admitted-interest registry without IO.
    pub fn new(registry: acyclic_stream::trace::Registry<P>) -> Self {
        Self { registry }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("trace window {from}..{end} ended at {received_through}")]
struct IncompleteWindow {
    from: u64,
    end: u64,
    received_through: u64,
}

fn window(path: &str) -> Option<(u64, u32)> {
    let (from, count) = path
        .strip_prefix(PREFIX)?
        .strip_suffix(".json")?
        .split_once('/')?;
    let start: u64 = from.parse().ok()?;
    let length: u32 = count.parse().ok()?;
    if length == 0
        || start.checked_add(u64::from(length)).is_none()
        || from != start.to_string()
        || count != length.to_string()
    {
        return None;
    }
    Some((start, length))
}

impl<P: StreamProvider + ?Sized> FileView<P> for TraceFiles<P> {
    fn identity(&self) -> &'static str {
        "trace-window/v1"
    }

    fn interest(
        &self,
        agent: &AgentPath,
        stream: &StreamPath,
    ) -> Result<Option<Box<dyn acyclic_stream::ProviderPlatform>>> {
        if *stream == source(agent)? {
            Ok(Some(Box::new(self.registry.subscribe(
                StreamPath::new(agent.as_str())?,
                stream.clone(),
            )?)))
        } else {
            Ok(None)
        }
    }

    fn source(&self, agent: &AgentPath, root: FileRoot, path: &str) -> Result<Option<StreamPath>> {
        if matches!(root, FileRoot::Private {}) && window(path).is_some() {
            source(agent).map(Some)
        } else {
            Ok(None)
        }
    }

    fn read<'a>(
        &'a self,
        provider: &'a P,
        agent: &'a AgentPath,
        paths: &'a [&'a str],
        stream: &'a StreamPath,
        through: u64,
        at_least: acyclic_stream::RevisionCut,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Vec<QueryFile>>> {
        Box::pin(async move {
            if *stream != source(agent)? {
                return Err(Error::Invalid("trace file source differs".into()));
            }
            let mut files = Vec::new();
            for path in paths
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
            {
                let (from, count) = window(path)
                    .ok_or_else(|| Error::Invalid("invalid trace cursor window".into()))?;
                if from >= through {
                    continue;
                }
                let end = (from + u64::from(count)).min(through);
                let mut query = TraceQuery::new((), Default::default());
                let mut received_through = from;
                while received_through < end {
                    let limit = u32::try_from(
                        (end - received_through).min(acyclic_stream::MAX_ITEMS as u64),
                    )
                    .map_err(|error| {
                        acyclic_stream::StreamError::InvalidResponse.with_source(error)
                    })?;
                    let received = query
                        .read_page(
                            provider,
                            ReadRequest {
                                path: stream.clone(),
                                from: received_through,
                                limit,
                                key: None,
                                at_least: at_least.clone(),
                            },
                        )
                        .await
                        .map_err(acyclic_stream::StreamError::from)?;
                    received_through += u64::from(received);
                    if received != limit {
                        return Err(acyclic_stream::StreamError::InvalidResponse
                            .with_source(IncompleteWindow {
                                from,
                                end,
                                received_through,
                            })
                            .into());
                    }
                }
                files.push(QueryFile {
                    path: path.to_owned(),
                    input: Position {
                        stream: stream.clone(),
                        sequence: end - 1,
                    },
                    text: serde_json::to_string(&query.snapshot(from, end, Some(through), 0))
                        .map_err(|error| {
                            acyclic_stream::StreamError::InvalidResponse.with_source(error)
                        })?,
                    media_type: "application/json".into(),
                });
            }
            Ok(files)
        })
    }
}

#[cfg(test)]
mod tests;
