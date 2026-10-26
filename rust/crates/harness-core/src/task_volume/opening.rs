use super::*;
use futures::StreamExt as _;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Opens or atomically initializes a volume, then reconstructs its logical index.
    pub async fn open(provider: Arc<P>, path: StreamPath) -> Result<Self> {
        let initialize = match provider.watermarks(path.clone()).await {
            Ok(acyclic_stream::StreamWatermarks { accepted: 0, .. })
            | Err(StreamError::NotFound) => true,
            Ok(_) => false,
            Err(error) => return Err(error.into()),
        };
        if initialize {
            let header = Header {
                volume: crate::identity::fresh_uuid(),
                epoch: crate::identity::fresh_uuid(),
                schema: SCHEMA,
            };
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![
                        serde_json::to_vec(&header)
                            .map_err(|_| Error::InvalidSegment)?
                            .into(),
                    ],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await?;
        }
        Self::open_existing(provider, path).await
    }

    /// Opens a published volume without creating or writing any state.
    pub async fn open_existing(provider: Arc<P>, path: StreamPath) -> Result<Self> {
        let mut records = provider
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
            })
            .await?;
        let record = records.next().await.ok_or(Error::ResetRequired)??;
        if record.sequence != 0 {
            return Err(Error::InvalidSegment);
        }
        let header: Header =
            serde_json::from_slice(&record.value).map_err(|_| Error::InvalidSegment)?;
        if !matches!(header.schema, 2 | 3 | SCHEMA) {
            return Err(Error::InvalidSegment);
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(path.as_str().as_bytes());
        hasher.update(header.volume.as_bytes());
        let scope = *hasher.finalize().as_bytes();
        let volume = Self {
            provider,
            path,
            index: Arc::new(Mutex::new(Index {
                volume: scope,
                epoch: header.epoch,
                next: 1,
                commit: record.commit_id,
                ..Index::default()
            })),
        };
        volume.refresh().await?;
        Ok(volume)
    }
}
