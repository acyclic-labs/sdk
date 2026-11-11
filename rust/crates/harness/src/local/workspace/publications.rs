//! Durable fork publication debt: one stream and one lazy cursor per workspace.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, ReadRequest, StreamError, StreamPath,
    StreamProvider, StreamProviderExt,
};
use futures::TryStreamExt as _;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::local) struct Publication {
    version: u32,
    pub(in crate::local) operation: OperationId,
    pub(in crate::local) branch: String,
    pub(in crate::local) source: acyclic_fs::WorkspaceId,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    next: u64,
}

fn path(workspace: acyclic_fs::WorkspaceId, part: &str) -> Result<StreamPath> {
    StreamPath::new(format!(
        "harness/workspace-publications-v1/{}/{part}",
        hex::encode(workspace.into_bytes())
    ))
    .map_err(storage)
}

impl LocalWorkspace {
    pub(in crate::local) async fn defer_publication(
        &self,
        operation: OperationId,
        branch: String,
        source: acyclic_fs::WorkspaceId,
    ) -> Result<()> {
        let workspace = self.view().workspace().id();
        let publication = Publication {
            version: 1,
            operation,
            branch,
            source,
        };
        self.append_publication_record(
            path(workspace, "pending")?,
            serde_json::to_vec(&publication).map_err(storage)?,
            format!(
                "workspace-publication:{}:{operation}:enqueue",
                hex::encode(workspace.into_bytes())
            ),
        )
        .await
    }

    pub(in crate::local) async fn next_publication(&self) -> Result<Option<(u64, Publication)>> {
        let workspace = self.view().workspace().id();
        let stream = self.filesystem.stream_provider();
        let next = match stream.head(path(workspace, "cursor")?).await {
            Ok(head) => match head.last {
                Some(record) => {
                    let cursor: Cursor = serde_json::from_slice(&record.value).map_err(storage)?;
                    if cursor.version != 1 {
                        return Err(Error::Invalid("unsupported publication cursor".into()));
                    }
                    cursor.next
                }
                None => 0,
            },
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(storage(error)),
        };
        let mut records = match stream
            .read(ReadRequest {
                key: None,
                path: path(workspace, "pending")?,
                from: next,
                limit: 1,
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(storage(error)),
        };
        let Some(record) = records.try_next().await.map_err(storage)? else {
            return Ok(None);
        };
        let publication: Publication = serde_json::from_slice(&record.value).map_err(storage)?;
        if publication.version != 1 {
            return Err(Error::Invalid("unsupported deferred publication".into()));
        }
        Ok(Some((next, publication)))
    }

    pub(in crate::local) async fn acknowledge_publication(&self, sequence: u64) -> Result<()> {
        let workspace = self.view().workspace().id();
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("publication cursor overflow".into()))?;
        self.append_publication_record(
            path(workspace, "cursor")?,
            serde_json::to_vec(&Cursor { version: 1, next }).map_err(storage)?,
            format!(
                "workspace-publication:{}:{sequence}:ack",
                hex::encode(workspace.into_bytes())
            ),
        )
        .await
    }

    async fn append_publication_record(
        &self,
        path: StreamPath,
        value: Vec<u8>,
        key: String,
    ) -> Result<()> {
        let stream = self.filesystem.stream_provider();
        match stream
            .append(AppendRequest {
                path,
                records: vec![value.into()],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(key).map_err(storage)?,
            })
            .await
            .map_err(storage)?
        {
            AppendOutcome::Committed(_) => {
                stream.durability_barrier().await.map_err(storage)?;
                Ok(())
            }
            AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
                "deferred publication append was refused".into(),
            )),
        }
    }
}
