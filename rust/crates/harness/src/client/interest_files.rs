//! Layout selection from one task-volume snapshot and live query.

use super::ClientInterests;
use crate::{
    Error, Result,
    task_volume::{ChangeStream, FileKey, Query, TaskVolume},
};
use acyclic_stream::{StreamPath, StreamProvider};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};

/// Contents of workspace/{layout,open-views}.json in the user's config volume.
/// Both files share a schema and are folded through the same live query.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct InterestDocument {
    /// Agents currently rendered.
    #[serde(default)]
    #[cfg_attr(feature = "wasm", tsify(type = "string[]"))]
    pub in_view: Vec<StreamPath>,
    /// Agents followed ahead of viewport movement.
    #[serde(default)]
    #[cfg_attr(feature = "wasm", tsify(type = "string[]"))]
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

/// Disposable two-file fold of the user config volume's single live query.
pub(super) struct InterestFiles {
    root: StreamPath,
    keys: [FileKey; 2],
    documents: [InterestDocument; 2],
    sizes: [usize; 2],
    maximum_bytes: usize,
    maximum_selected: usize,
    initial: Option<ClientInterests>,
    changes: ChangeStream,
}

impl InterestFiles {
    pub(super) async fn observe<P: StreamProvider + ?Sized>(
        volume: &TaskVolume<P>,
        swarm: StreamPath,
        maximum_selected: usize,
        maximum_bytes: usize,
        maximum_files: usize,
    ) -> Result<Self> {
        let observed = volume
            .observe(Query::Inputs, None, maximum_files)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Storage("missing initial layout snapshot".into()))?;
        let mut files = Self {
            root: swarm,
            keys: [
                FileKey::parse("workspace/layout.json")
                    .map_err(|e| Error::Invalid(e.to_string()))?,
                FileKey::parse("workspace/open-views.json")
                    .map_err(|e| Error::Invalid(e.to_string()))?,
            ],
            documents: Default::default(),
            sizes: [0; 2],
            maximum_bytes,
            maximum_selected,
            initial: None,
            changes: observed.changes,
        };
        for file in snapshot.files {
            files.apply(file)?;
        }
        files.initial = Some(files.select([&files.documents[0], &files.documents[1]])?);
        Ok(files)
    }

    fn select(&self, documents: [&InterestDocument; 2]) -> Result<ClientInterests> {
        ClientInterests::from_layout(
            self.root.clone(),
            documents.iter().flat_map(|doc| doc.in_view.iter().cloned()),
            documents
                .iter()
                .flat_map(|doc| doc.prefetch.iter().cloned()),
            self.maximum_selected,
        )
    }

    fn apply(&mut self, file: crate::task_volume::FileValue) -> Result<Option<ClientInterests>> {
        let Some(index) = self.keys.iter().position(|key| key == &file.key) else {
            return Ok(None);
        };
        if self.sizes[1 - index]
            .checked_add(file.bytes.len())
            .is_none_or(|bytes| bytes > self.maximum_bytes)
        {
            return Err(Error::Invalid(
                "combined interest files exceed byte bound".into(),
            ));
        }
        let document = InterestDocument::decode(&file.bytes, self.maximum_bytes)?;
        let mut documents = [&self.documents[0], &self.documents[1]];
        documents[index] = &document;
        let selected = self.select(documents)?;
        self.documents[index] = document;
        self.sizes[index] = file.bytes.len();
        Ok(Some(selected))
    }

    pub(super) async fn next(&mut self) -> Result<Option<ClientInterests>> {
        if let Some(initial) = self.initial.take() {
            return Ok(Some(initial));
        }
        let change = self
            .changes
            .next()
            .await
            .ok_or_else(|| Error::Storage("layout query ended; reobserve".into()))?
            .map_err(|error| Error::Storage(error.to_string()))?;
        if let Some(file) = change.file {
            self.apply(file)
        } else {
            Ok(None)
        }
    }
}
