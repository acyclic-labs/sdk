//! Layout selection at one published filesystem cut.

use super::ClientInterests;
use crate::{
    Error, Result,
    context::{ContextRoot, PinnedContextPath},
    conversation::ContentResidencyVerifier,
    resources::GenerationRef,
};
use acyclic_stream::StreamPath;
use serde::{Deserialize, Serialize};

/// Contents of layout.json and open-views.json in the user's config volume.
/// The two files share a schema; their selections are combined at one fs cut.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterestDocument {
    /// Agents currently rendered.
    #[serde(default)]
    pub in_view: Vec<StreamPath>,
    /// Agents followed ahead of viewport movement.
    #[serde(default)]
    pub prefetch: Vec<StreamPath>,
}

impl InterestDocument {
    /// Decodes one bounded file, rejecting invalid paths and unknown fields.
    pub fn decode(bytes: &[u8], maximum_bytes: usize) -> Result<Self> {
        if bytes.len() > maximum_bytes {
            return Err(Error::Invalid("interest file exceeds byte bound".into()));
        }
        serde_json::from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("invalid interest file: {error}")))
    }
}

/// One publication yielded by the config-volume snapshot plus cursor follow.
/// Both selected files are read at this generation, never at a moving head.
#[derive(Clone, Debug)]
pub struct InterestFiles {
    /// Authorized config directory containing layout.json and open-views.json.
    pub root: ContextRoot,
    /// Exact publication cut supplied by the filesystem live query.
    pub generation: GenerationRef,
}

impl InterestFiles {
    pub(super) async fn read(
        &self,
        reader: &dyn ContentResidencyVerifier,
        swarm: StreamPath,
        maximum_selected: usize,
        maximum_bytes: usize,
    ) -> Result<ClientInterests> {
        let mut documents = [InterestDocument::default(), InterestDocument::default()];
        let mut remaining = maximum_bytes as u64;
        for (name, document) in ["layout.json", "open-views.json"].into_iter().zip(&mut documents) {
            let path = if self.root.directory.is_empty() {
                name.into()
            } else {
                format!("{}/{name}", self.root.directory)
            };
            let source = PinnedContextPath {
                root: self.root.clone(),
                generation: self.generation.clone(),
                path,
            };
            match source.read_remaining(reader, remaining).await {
                Ok((_, bytes)) => {
                    remaining -= bytes.len() as u64;
                    *document = InterestDocument::decode(&bytes, maximum_bytes)?;
                }
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        let [layout, views] = documents;
        ClientInterests::from_layout(
            swarm,
            layout.in_view.into_iter().chain(views.in_view),
            layout.prefetch.into_iter().chain(views.prefetch),
            maximum_selected,
        )
    }
}
