//! Last-record projections and optimistic publication on canonical Streams.
use super::*;
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, ReadRequest, StreamError,
    StreamPath, StreamProvider,
};
use futures::TryStreamExt;
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub(super) struct Journal {
    pub provider: Arc<dyn StreamProvider>,
    pub root: String,
}

pub(super) struct Row<T> {
    pub path: StreamPath,
    pub tail: u64,
    pub anchor: Option<acyclic_stream::CommitId>,
    pub value: Option<T>,
}

impl<T> Row<T> {
    pub fn condition(&self) -> CommitCondition {
        match self.anchor {
            Some(commit_id) => CommitCondition::Head {
                path: self.path.clone(),
                expected: self.tail,
                commit_id,
            },
            None => CommitCondition::Tail {
                path: self.path.clone(),
                expected: 0,
            },
        }
    }
}

pub(super) fn storage(error: StreamError) -> ProviderError {
    tracing::error!(
        error = error.code(),
        "Machines Streams persistence failed; recovery: restore the configured Streams backend, then call recover with the original Machines idempotency key before any new dispatch"
    );
    ProviderError::Unavailable
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Blob {
    digest: [u8; 32],
    records: u64,
    bytes: u64,
    anchor: [u8; 32],
}

impl Journal {
    pub fn append<T: Serialize>(
        &self,
        suffix: &str,
        value: &T,
    ) -> Result<CommitMutation, ProviderError> {
        Ok(CommitMutation::Append {
            path: self.path(suffix)?,
            records: vec![
                serde_json::to_vec(value)
                    .map_err(|_| storage(StreamError::InvalidArgument))?
                    .into(),
            ],
        })
    }

    /// One atomic publication attempt. Only a definite conflict permits a new
    /// read/compute attempt; a lost reply must never redispatch a native action.
    pub async fn publish(
        &self,
        conditions: Vec<CommitCondition>,
        mutations: Vec<CommitMutation>,
        durable_path: StreamPath,
        durable_tail: u64,
    ) -> Result<bool, ProviderError> {
        match self
            .provider
            .commit(CommitRequest {
                conditions,
                mutations,
                idempotency_key: acyclic_stream::IdempotencyKey::new(
                    uuid::Uuid::new_v4().as_bytes().to_vec(),
                )
                .map_err(storage)?,
            })
            .await
            .map_err(storage)?
        {
            CommitOutcome::Committed(_) => {
                if self
                    .provider
                    .durable_receipt(durable_path, durable_tail)
                    .await
                    .map_err(storage)?
                    .is_none()
                {
                    return Err(storage(StreamError::Unavailable));
                }
                Ok(true)
            }
            CommitOutcome::Conflict(_) => Ok(false),
        }
    }

    pub fn new(provider: Arc<dyn StreamProvider>, account: &str) -> Result<Self, ProviderError> {
        let root = StreamPath::new(format!("machines/{account}")).map_err(|_| ProviderError::Invalid("Machines account admission failed; recovery: supply one canonical account path segment".into()))?;
        if account.is_empty() || account.contains('/') {
            return Err(ProviderError::Invalid("Machines account admission failed; recovery: supply one canonical account path segment".into()));
        }
        Ok(Self {
            provider,
            root: root.to_string(),
        })
    }

    pub fn path(&self, suffix: &str) -> Result<StreamPath, ProviderError> {
        let path = if suffix.is_empty() {
            self.root.clone()
        } else {
            format!("{}/{suffix}", self.root)
        };
        StreamPath::new(path).map_err(storage)
    }

    pub async fn load<T: DeserializeOwned>(&self, suffix: &str) -> Result<Row<T>, ProviderError> {
        let path = self.path(suffix)?;
        let tail = match self.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(storage(error)),
        };
        if tail == 0 {
            return Ok(Row {
                path,
                tail,
                anchor: None,
                value: None,
            });
        }
        let mut records = self
            .provider
            .read(ReadRequest {
                path: path.clone(),
                from: tail - 1,
                limit: 1,
            })
            .await
            .map_err(storage)?;
        let record = records
            .try_next()
            .await
            .map_err(storage)?
            .filter(|record| record.sequence == tail - 1)
            .ok_or_else(|| storage(StreamError::InvalidResponse))?;
        if self
            .provider
            .durable_receipt(path.clone(), tail)
            .await
            .map_err(storage)?
            .is_none()
        {
            return Err(storage(StreamError::Unavailable));
        }
        let value = serde_json::from_slice(&record.value)
            .map_err(|_| storage(StreamError::InvalidResponse))?;
        Ok(Row {
            path,
            tail,
            anchor: Some(record.commit_id),
            value,
        })
    }

    /// Stage immutable outcome bytes in bounded canonical records. Publication
    /// happens only when the operation points at this fully durable prefix.
    pub async fn store_outcome(
        &self,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<Blob, ProviderError> {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let suffix = format!("ops/{operation}/results/{}", hex::encode(digest));
        let path = self.path(&suffix)?;
        let chunks = bytes.chunks(acyclic_stream::MAX_RECORD_BYTES);
        for (index, chunk) in chunks.enumerate() {
            let index = index as u64;
            let mut key = Sha256::new();
            key.update(path.as_str().as_bytes());
            key.update(index.to_be_bytes());
            let key: Vec<u8> = key.finalize().to_vec();
            match self
                .provider
                .commit(CommitRequest {
                    conditions: vec![CommitCondition::Tail {
                        path: path.clone(),
                        expected: index,
                    }],
                    mutations: vec![CommitMutation::Append {
                        path: path.clone(),
                        records: vec![chunk.to_vec().into()],
                    }],
                    idempotency_key: acyclic_stream::IdempotencyKey::new(key).map_err(storage)?,
                })
                .await
                .map_err(storage)?
            {
                CommitOutcome::Committed(_) => {}
                CommitOutcome::Conflict(_) => {
                    if self.provider.tail(path.clone()).await.map_err(storage)? <= index {
                        return Err(storage(StreamError::InvalidResponse));
                    }
                }
            }
        }
        let records = bytes.len().div_ceil(acyclic_stream::MAX_RECORD_BYTES) as u64;
        let mut last = self
            .provider
            .read(ReadRequest {
                path: path.clone(),
                from: records.saturating_sub(1),
                limit: 1,
            })
            .await
            .map_err(storage)?;
        let last = last
            .try_next()
            .await
            .map_err(storage)?
            .ok_or_else(|| storage(StreamError::InvalidResponse))?;
        if self
            .provider
            .durable_receipt(path, records)
            .await
            .map_err(storage)?
            .is_none()
        {
            return Err(storage(StreamError::Unavailable));
        }
        Ok(Blob {
            digest,
            records,
            bytes: bytes.len() as u64,
            anchor: *last.commit_id.as_bytes(),
        })
    }

    pub async fn outcome(
        &self,
        operation: OperationId,
        blob: &Blob,
    ) -> Result<MutationOutcome, ProviderError> {
        let path = self.path(&format!(
            "ops/{operation}/results/{}",
            hex::encode(blob.digest)
        ))?;
        let mut from = 0;
        let mut bytes = Vec::new();
        let mut anchor = None;
        while from < blob.records {
            let before = from;
            let limit = (blob.records - from).min(acyclic_stream::MAX_ITEMS as u64) as u32;
            let mut records = self
                .provider
                .read(ReadRequest {
                    path: path.clone(),
                    from,
                    limit,
                })
                .await
                .map_err(storage)?;
            while let Some(record) = records.try_next().await.map_err(storage)? {
                if from >= blob.records
                    || record.sequence != from
                    || record.value.len() > acyclic_stream::MAX_RECORD_BYTES
                {
                    return Err(storage(StreamError::InvalidResponse));
                }
                bytes.extend_from_slice(&record.value);
                if bytes.len() as u64 > blob.bytes {
                    return Err(storage(StreamError::InvalidResponse));
                }
                anchor = Some(record.commit_id);
                from += 1;
            }
            if from == before {
                return Err(storage(StreamError::InvalidResponse));
            }
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() as u64 != blob.bytes
            || digest != blob.digest
            || anchor.map(|id| *id.as_bytes()) != Some(blob.anchor)
        {
            return Err(storage(StreamError::InvalidResponse));
        }
        serde_json::from_slice(&bytes).map_err(|_| storage(StreamError::InvalidResponse))
    }

    /// Re-executes only the pure state transition after a definite CAS conflict.
    /// A lost commit response is returned as an error, never as a fresh dispatch.
    pub async fn update<T, R>(
        &self,
        suffix: &str,
        mut change: impl FnMut(&mut Option<T>) -> Result<R, ProviderError>
        + acyclic_stream::ProviderTask,
    ) -> Result<R, ProviderError>
    where
        T: Serialize + DeserializeOwned + acyclic_stream::ProviderTask,
    {
        self.update_with(suffix, move |entry| {
            change(entry).map(|result| (result, Vec::new()))
        })
        .await
    }

    pub async fn update_with<T, R>(
        &self,
        suffix: &str,
        mut change: impl FnMut(&mut Option<T>) -> Result<(R, Vec<CommitMutation>), ProviderError>
        + acyclic_stream::ProviderTask,
    ) -> Result<R, ProviderError>
    where
        T: Serialize + DeserializeOwned + acyclic_stream::ProviderTask,
    {
        loop {
            let row = self.load::<T>(suffix).await?;
            let before = serde_json::to_vec(&row.value)
                .map_err(|_| storage(StreamError::InvalidArgument))?;
            let condition = row.condition();
            let mut value = row.value;
            let (result, mut mutations) = change(&mut value)?;
            let after =
                serde_json::to_vec(&value).map_err(|_| storage(StreamError::InvalidArgument))?;
            if before == after {
                return Ok(result);
            }
            mutations.push(CommitMutation::Append {
                path: row.path.clone(),
                records: vec![after.into()],
            });
            if self
                .publish(vec![condition], mutations, row.path, row.tail + 1)
                .await?
            {
                return Ok(result);
            }
        }
    }
}
