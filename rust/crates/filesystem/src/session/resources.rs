//! Filesystem change follows intersect verified workload authority and policy.
use crate::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, VolumeId, WorkBudget};
use crate::access::{Action, PathTrie};
use acyclic_stream::session::{ResourceFollowRequest, ResourceProvider, ResourceSubscription};
use acyclic_stream::{FollowUpdate, StreamError, StreamPath, StreamProvider};
use futures::{StreamExt, future};
use std::sync::{Arc, Mutex};

mod metadata;
mod policy;

/// Canonical filesystem resource for a stable volume identity and Unicode path.
/// Names are encoded component by component; the owner applies volume semantics.
pub fn resource(volume: VolumeId, path: &str) -> Result<String, StreamError> {
    let mut result = format!("fs:/{}", uuid::Uuid::from_bytes(volume.into_bytes()));
    if !path.is_empty() {
        for component in path.split('/') {
            if component.is_empty() || matches!(component, "." | "..") {
                return Err(StreamError::InvalidArgument);
            }
            result.push('/');
            result.push_str(&acyclic_auth::encode_component(component.as_bytes()));
        }
    }
    acyclic_stream::wire_codec::session::validate_resource(&result)?;
    Ok(result)
}

/// Owning filesystem and its exact backing Streams provider. No policy query
/// callback or caller-supplied principal can manufacture an allowing decision.
pub struct ChangeProvider<P, A, O> {
    streams: Arc<P>,
    fs: Fs<A, O>,
}
impl<P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore> ChangeProvider<P, A, O> {
    /// Requires a process-owned policy fence and the same backend owner.
    pub fn new(fs: Fs<A, O>, streams: Arc<P>) -> Result<Self, StreamError> {
        if !fs.has_process_policy_owner()
            || fs.operation_claims().owner_identity() != streams.operation_claims().owner_identity() {
            return Err(StreamError::AccessDenied);
        }
        Ok(Self { streams, fs })
    }
}

struct PolicyState {
    watch: crate::access::projection::PolicyWatch,
    trie: Mutex<Option<Arc<PathTrie>>>,
    authorization: acyclic_stream::session::ResourceAuthorization,
}
impl PolicyState {
    fn validate(&self) -> Result<(), StreamError> {
        let now = self.authorization.now_unix_seconds()?;
        if !self.watch.valid() || !self.trie.lock().map_err(|_| StreamError::AccessDenied)?.as_ref().is_some_and(|trie| trie.valid_at(now)) {
            return Err(StreamError::AccessDenied);
        }
        Ok(())
    }
}

impl<P, A, O> ResourceProvider for ChangeProvider<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn follow(&self, request: ResourceFollowRequest) -> Result<ResourceSubscription, StreamError> {
        let encoded = request.resource.strip_prefix("fs:/").ok_or(StreamError::Unsupported)?;
        let segment = encoded.split('/').next().ok_or(StreamError::InvalidArgument)?;
        let uuid = uuid::Uuid::parse_str(segment).map_err(|_| StreamError::InvalidArgument)?;
        if uuid.to_string() != segment { return Err(StreamError::InvalidArgument); }
        let volume_id = VolumeId::from_bytes(*uuid.as_bytes());
        let state = Arc::new(PolicyState {
            watch: crate::access::projection::observe(self.fs.operation_claims(), volume_id).map_err(|_| StreamError::AccessDenied)?,
            trie: Mutex::new(None),
            authorization: request.authorization.clone(),
        });
        let fs = self.fs.clone();
        let streams = self.streams.clone();
        let opened = state.clone();
        let terminal = state.clone();
        let (deadline_tx, deadline_rx) = tokio::sync::oneshot::channel();
        Ok(ResourceSubscription {
            revalidate: Arc::new(move || state.validate()),
            terminal: Box::pin(async move {
                let deadline = async {
                    match deadline_rx.await {
                        Ok(Some(deadline)) => terminal.authorization.expires_at(deadline).await,
                        Ok(None) => future::pending().await,
                        Err(_) => (),
                    }
                };
                let _ = future::select(Box::pin(terminal.watch.changed()), Box::pin(deadline)).await;
                StreamError::AccessDenied
            }),
            open: Box::pin(async move {
                let cancellation = CancellationToken::new();
                let volume = fs.open_volume(volume_id, WorkBudget::UNBOUNDED, &cancellation).await.map_err(|_| StreamError::AccessDenied)?.value;
                let trie = Arc::new(policy::compile(&volume, &request.authorization, &cancellation).await?);
                // The observation was admitted before the asynchronous policy
                // read. Any concurrent edit invalidated it before its CAS.
                if !opened.watch.valid() { return Err(StreamError::AccessDenied); }
                let _ = deadline_tx.send(trie.valid_until());
                *opened.trie.lock().map_err(|_| StreamError::AccessDenied)? = Some(trie.clone());
                opened.validate()?;
                let path = StreamPath::new(format!(".acyclic/fs/{}", hex::encode(volume_id.into_bytes())))?;
                let filter = request.authorization.filter_paths(&resource(volume_id, "")?, &request.filter)?;
                let records = streams.follow_updates(path, request.from, request.preceding, filter).await?;
                opened.validate()?;
                let output = records.map(move |update| {
                    opened.validate()?;
                    match update? {
                        FollowUpdate::Record(record) => metadata::project(record, volume_id, volume.config, &trie, &request.authorization),
                        progress => Ok(progress),
                    }
                });
                Ok(Box::pin(output) as acyclic_stream::FollowStream)
            }),
        })
    }
}

#[cfg(all(test, feature = "memory", not(acyclic_fs_loom)))]
mod tests;
