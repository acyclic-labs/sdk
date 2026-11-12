//! Pure supervision files on the harness's shared demand-selected file path.
use super::{Kind, streams};
use crate::{
    Result,
    agent::AgentPath,
    client_files::{FileRoot, FileView, QueryFile},
};
use acyclic_stream::{BoxProviderFuture, StreamPath, StreamProvider};
use serde::Deserialize as _;

pub(crate) struct SignalFiles;

fn kind(path: &str) -> Option<Kind> {
    let name = path
        .strip_prefix("status/supervision/")?
        .strip_suffix(".json")?;
    let value = serde::de::value::StrDeserializer::<serde::de::value::Error>::new(name);
    Kind::deserialize(value).ok()
}

impl<P: StreamProvider + ?Sized> FileView<P> for SignalFiles {
    fn identity(&self) -> &'static str {
        "supervision-signals/v1"
    }

    fn source(&self, agent: &AgentPath, root: FileRoot, path: &str) -> Result<Option<StreamPath>> {
        if matches!(root, FileRoot::Private {}) && kind(path).is_some() {
            streams::path(agent).map(Some)
        } else {
            Ok(None)
        }
    }

    fn read<'a>(
        &'a self,
        provider: &'a P,
        agent: &'a AgentPath,
        paths: &'a [&'a str],
        source: &'a StreamPath,
        through: u64,
        at_least: acyclic_stream::RevisionCut,
    ) -> BoxProviderFuture<'a, Result<Vec<QueryFile>>> {
        Box::pin(async move {
            if *source != streams::path(agent)? {
                return Err(crate::Error::Invalid("signal file source differs".into()));
            }
            let Some(run) =
                streams::active_run_before(provider, agent, through, at_least.clone()).await?
            else {
                return Ok(Vec::new());
            };
            let queries = streams::Queries::new(provider, agent, run);
            futures::future::try_join_all(paths.iter().map(|path| {
                let queries = &queries;
                let at_least = at_least.clone();
                async move {
                    let kind = kind(path)
                        .ok_or_else(|| crate::Error::Invalid("unknown signal file".into()))?;
                    queries
                        .read_signal_source(kind, through, at_least)
                        .await?
                        .map(|(signal, input)| {
                            Ok(QueryFile {
                                path: (*path).to_owned(),
                                input,
                                media_type: "application/json".into(),
                                text: serde_json::to_string(&signal)
                                    .map_err(|error| crate::Error::Invalid(error.to_string()))?,
                            })
                        })
                        .transpose()
                }
            }))
            .await
            .map(|files| files.into_iter().flatten().collect())
        })
    }
}
