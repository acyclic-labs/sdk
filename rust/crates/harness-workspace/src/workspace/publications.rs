//! Durable fork publication debt: one stream and one lazy cursor per workspace.
use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, IdempotencyKey, ReadRequest, StreamCategory, StreamPath,
    StreamProvider, StreamProviderExt,
};
use futures::TryStreamExt as _;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    version: u32,
    pub(crate) operation: OperationId,
    pub(crate) branch: String,
    pub(crate) source: acyclic_fs::GitGenerationRef,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    next: u64,
    completed: OperationId,
}

fn decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| {
        Error::Unsupported(format!(
            "publication journal requires format v2; incompatible records are retained; start a fresh session: {error}"
        ))
    })
}

/// Builds the stream path of one workspace publication part.
pub fn path(workspace: acyclic_fs::WorkspaceId, part: &str) -> Result<StreamPath> {
    StreamPath::new(format!(
        "harness/workspace-publications-v1/{}/{part}",
        hex::encode(workspace.into_bytes())
    ))
    .map_err(storage)
}

async fn cursor(
    filesystem: &acyclic_fs::LocalFs,
    workspace: acyclic_fs::WorkspaceId,
) -> Result<Option<Cursor>> {
    let stream = filesystem.stream_provider();
    let record = match stream.head(path(workspace, "cursor")?).await {
        Ok(head) => head.last,
        Err(error) if error.category() == StreamCategory::NotFound => None,
        Err(error) => return Err(storage(error)),
    };
    let Some(record) = record else {
        return Ok(None);
    };
    let cursor: Cursor = decode_record(&record.value)?;
    if cursor.version != 2 {
        return Err(Error::Unsupported(
            "publication cursor requires format v2; start a fresh session".into(),
        ));
    }
    Ok(Some(cursor))
}

async fn pending(
    filesystem: &acyclic_fs::LocalFs,
    workspace: acyclic_fs::WorkspaceId,
    next: u64,
) -> Result<Option<Publication>> {
    let stream = filesystem.stream_provider();
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
        Err(error) if error.category() == StreamCategory::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    let Some(record) = records.try_next().await.map_err(storage)? else {
        return Ok(None);
    };
    let publication: Publication = decode_record(&record.value)?;
    if publication.version != 2 {
        return Err(Error::Unsupported(
            "deferred publication requires format v2; start a fresh session".into(),
        ));
    }
    Ok(Some(publication))
}

impl LocalWorkspace {
    /// Records a publication to hand back at this checkout's next command.
    pub async fn defer_publication(
        self: &Arc<Self>,
        operation: OperationId,
        branch: String,
        source: acyclic_fs::GitGenerationRef,
    ) -> Result<()> {
        let workspace = Arc::clone(self);
        acyclic_native_runtime::TaskSpawner::current()
            .spawn(async move {
                let result = workspace
                    .enqueue_publication(operation, branch, source)
                    .await;
                // Publish the retry signal in the original append's completion owner,
                // even if its caller stopped observing before the durable acknowledgement.
                workspace
                    .publication_debt
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                result
            })
            .await
            .map_err(storage)?
    }

    async fn enqueue_publication(
        &self,
        operation: OperationId,
        branch: String,
        source: acyclic_fs::GitGenerationRef,
    ) -> Result<()> {
        let workspace = self.view().workspace().id();
        let key = format!(
            "workspace-publication-v2:{}:{operation}:enqueue",
            hex::encode(workspace.into_bytes())
        );
        let stream = self.filesystem.stream_provider();
        if let Some(observed) = stream
            .inspect_idempotency(IdempotencyKey::new(key.clone()).map_err(storage)?)
            .await
            .map_err(storage)?
        {
            if !matches!(
                observed.outcome,
                acyclic_stream::IdempotencyOutcome::Append(AppendOutcome::Committed(_))
            ) {
                return Err(Error::Conflict(
                    "publication retry identity was reused".into(),
                ));
            }
            // A completion retry owns the first durable cut, even if a
            // background writer has since advanced the child's workspace.
            stream.durability_barrier().await.map_err(storage)?;
            return Ok(());
        }
        let publication = Publication {
            version: 2,
            operation,
            branch,
            source,
        };
        self.append_publication_record(
            path(workspace, "pending")?,
            serde_json::to_vec(&publication).map_err(storage)?,
            key,
        )
        .await
    }

    pub(crate) async fn next_publication(
        &self,
        repository: &acyclic_fs::GitCompatRepository<LocalCoreStateStore>,
    ) -> Result<Option<(u64, Publication)>> {
        let workspace = self.view().workspace().id();
        let next = match cursor(&self.filesystem, workspace).await? {
            Some(cursor) => {
                // The cursor is the durable withdrawal. Recover a crash
                // after acknowledging debt but before releasing its reply.
                repository
                    .acknowledge_request(acyclic_fs_format::identity::OperationId::from_bytes(
                        cursor.completed.into_bytes(),
                    ))
                    .await
                    .map_err(storage)?;
                cursor.next
            }
            None => 0,
        };
        Ok(pending(&self.filesystem, workspace, next)
            .await?
            .map(|publication| (next, publication)))
    }

    /// Whether `workspace` holds a handback no command has published yet,
    /// read without binding the workspace.
    pub async fn owes_publication(
        filesystem: &acyclic_fs::LocalFs,
        workspace: acyclic_fs::WorkspaceId,
    ) -> Result<bool> {
        let next = cursor(filesystem, workspace)
            .await?
            .map_or(0, |cursor| cursor.next);
        Ok(pending(filesystem, workspace, next).await?.is_some())
    }

    pub(crate) async fn acknowledge_publication(
        &self,
        sequence: u64,
        completed: OperationId,
        repository: &acyclic_fs::GitCompatRepository<LocalCoreStateStore>,
    ) -> Result<()> {
        let workspace = self.view().workspace().id();
        let next = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("publication cursor overflow".into()))?;
        self.append_publication_record(
            path(workspace, "cursor")?,
            serde_json::to_vec(&Cursor {
                version: 2,
                next,
                completed,
            })
            .map_err(storage)?,
            format!(
                "workspace-publication-v2:{}:{sequence}:ack",
                hex::encode(workspace.into_bytes())
            ),
        )
        .await?;
        repository
            .acknowledge_request(acyclic_fs_format::identity::OperationId::from_bytes(
                completed.into_bytes(),
            ))
            .await
            .map_err(storage)
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
