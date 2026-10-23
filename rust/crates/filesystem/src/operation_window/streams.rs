//! Atomic window and exact-expiry gate transactions on Streams.
use super::*;
use futures::StreamExt as _;

/// Operation-window state stored beside generation authority in one Stream
/// provider. Each lease owns an immutable gate stream: opening creates tail 1,
/// while close/expiry atomically advances it to tail 2 with the window CAS.
/// A generation publication conditioned on tail 1 therefore linearizes
/// exactly against fencing.
#[derive(Clone)]
#[cfg(feature = "distributed")]
pub struct StreamOperationWindowStore<P> {
    provider: Arc<P>,
}

#[cfg(feature = "distributed")]
impl<P> StreamOperationWindowStore<P> {
    /// Binds operation windows to the exact provider used by filesystem
    /// authority publication.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

/// Stream-backed operation-window failure.
#[cfg(feature = "distributed")]
#[derive(Debug, Error)]
pub enum StreamOperationWindowStoreError {
    /// The shared Stream provider rejected or could not durably resolve state.
    #[error("operation-window stream failed: {0}")]
    Stream(#[from] acyclic_stream::StreamError),
    /// Stored state is corrupt or incompatible.
    #[error("operation-window stream state is corrupt: {0}")]
    Corrupt(String),
}

#[cfg(feature = "distributed")]
impl<P: acyclic_stream::StreamProvider> OperationWindowStore for StreamOperationWindowStore<P> {
    type Error = StreamOperationWindowStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        let path = stream_window_path(workspace_id)?;
        let tail = match self.provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if tail == 0 {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "window stream is empty".to_owned(),
            ));
        }
        let mut records = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: tail - 1,
                limit: 1,
            })
            .await?;
        let record = records.next().await.transpose()?.ok_or_else(|| {
            StreamOperationWindowStoreError::Corrupt("window stream tail has no record".to_owned())
        })?;
        let snapshot: OperationWindowSnapshot = serde_json::from_slice(&record.value)
            .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?;
        if snapshot.revision != tail || !snapshot.is_valid_for(workspace_id) {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "window revision does not match its stream tail".to_owned(),
            ));
        }
        Ok(Some(snapshot))
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        if !replacement.is_valid_for(workspace_id)
            || replacement.revision != expected_revision.saturating_add(1)
        {
            return Err(StreamOperationWindowStoreError::Corrupt(
                "replacement identity or revision is invalid".to_owned(),
            ));
        }
        let current = self.load(workspace_id).await?;
        if current.as_ref().map_or(0, |snapshot| snapshot.revision) != expected_revision {
            return Ok(false);
        }
        let before = current
            .as_ref()
            .map_or_else(BTreeMap::new, |snapshot| phase_leases(&snapshot.phase));
        let after = phase_leases(&replacement.phase);
        let state_path = stream_window_path(workspace_id)?;
        let mut conditions = vec![if expected_revision == 0 {
            acyclic_stream::CommitCondition::Absent {
                path: state_path.clone(),
            }
        } else {
            acyclic_stream::CommitCondition::Tail {
                path: state_path.clone(),
                expected: expected_revision,
            }
        }];
        let encoded = serde_json::to_vec(&replacement)
            .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?;
        let mut mutations = vec![acyclic_stream::CommitMutation::Append {
            path: state_path,
            records: vec![encoded.into()],
        }];
        // An expiry is the admission token for this stable lease identity.
        // Closing the old token and opening its successor share the window CAS,
        // so publication can linearize only before or after the renewal.
        for (lease_id, lease) in &after {
            if let Some(previous) = before.get(lease_id) {
                if previous == lease {
                    continue;
                }
                if previous.owner != lease.owner
                    || lease.expires_at_millis <= previous.expires_at_millis
                {
                    return Err(StreamOperationWindowStoreError::Corrupt(
                        "lease replacement must retain owner and increase expiry".to_owned(),
                    ));
                }
            }
            let path = stream_lease_path(workspace_id, *lease_id, lease.expires_at_millis)?;
            conditions.push(acyclic_stream::CommitCondition::Absent { path: path.clone() });
            mutations.push(acyclic_stream::CommitMutation::Append {
                path,
                records: vec![acyclic_stream::OwnedBytes::from_static(b"active").into()],
            });
        }
        for (lease_id, lease) in &before {
            if after.get(lease_id) == Some(lease) {
                continue;
            }
            let path = stream_lease_path(workspace_id, *lease_id, lease.expires_at_millis)?;
            conditions.push(acyclic_stream::CommitCondition::Tail {
                path: path.clone(),
                expected: 1,
            });
            mutations.push(acyclic_stream::CommitMutation::Append {
                path,
                records: vec![acyclic_stream::OwnedBytes::from_static(b"fenced").into()],
            });
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acyclic-operation-window-cas-v1\0");
        hasher.update(&workspace_id.into_bytes());
        hasher.update(&expected_revision.to_le_bytes());
        hasher.update(
            &serde_json::to_vec(&replacement)
                .map_err(|error| StreamOperationWindowStoreError::Corrupt(error.to_string()))?,
        );
        let key = acyclic_stream::IdempotencyKey::new(hasher.finalize().as_bytes().to_vec())?;
        match self
            .provider
            .commit(acyclic_stream::CommitRequest {
                conditions,
                mutations,
                idempotency_key: key,
            })
            .await?
        {
            acyclic_stream::CommitOutcome::Committed(_) => Ok(true),
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
        }
    }
}

#[cfg(feature = "distributed")]
fn phase_leases(phase: &OperationWindowPhase) -> BTreeMap<OperationLeaseId, OperationLease> {
    match phase {
        OperationWindowPhase::Active { leases, .. } => leases.clone(),
        OperationWindowPhase::Idle | OperationWindowPhase::Reconciling { .. } => BTreeMap::new(),
    }
}

#[cfg(feature = "distributed")]
fn stream_window_path(
    workspace_id: WorkspaceId,
) -> Result<acyclic_stream::StreamPath, acyclic_stream::StreamError> {
    acyclic_stream::StreamPath::new(format!(
        "fs/operation-windows-v1/{}/state",
        hex::encode(workspace_id.into_bytes())
    ))
}

#[cfg(feature = "distributed")]
pub(crate) fn stream_lease_path(
    workspace_id: WorkspaceId,
    lease_id: OperationLeaseId,
    expires_at_millis: u64,
) -> Result<acyclic_stream::StreamPath, acyclic_stream::StreamError> {
    let mut text = [0_u8; lease_path::MAX_ENCODED_BYTES];
    let length = lease_path::encode(workspace_id, lease_id, expires_at_millis, &mut text, None)
        .map_err(|_| acyclic_stream::StreamError::InvalidPath)?;
    acyclic_stream::StreamPath::new(
        std::str::from_utf8(
            text.get(..length)
                .ok_or(acyclic_stream::StreamError::InvalidPath)?,
        )
        .map_err(|_| acyclic_stream::StreamError::InvalidPath)?,
    )
}
