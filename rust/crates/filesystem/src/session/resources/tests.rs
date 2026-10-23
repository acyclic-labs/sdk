use super::*;
use crate::{EmbeddedCapabilities, MemoryObjectStore, Workspace};
use crate::distributed::StreamAuthorityStore;
use acyclic_auth::{contract::*, testing as signed};
use acyclic_stream::{FollowFilter, MemoryStream, wire};
use prost::Message;

type Engine = Fs<StreamAuthorityStore<MemoryStream>, MemoryObjectStore>;
type Space = Workspace<StreamAuthorityStore<MemoryStream>, MemoryObjectStore>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture() -> TestResult<(Engine, Arc<MemoryStream>, Space)> {
    let streams = Arc::new(MemoryStream::default());
    let fs = Fs::new(StreamAuthorityStore::new(streams.clone()), MemoryObjectStore::default(), EmbeddedCapabilities::MEMORY);
    let workspace = fs.create_workspace("change-follow", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|error| error.error)?.value;
    let initial = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    crate::access::fixtures::publish_policy(&workspace, &initial).await?;
    Ok((fs, streams, workspace))
}
fn credential(name: &str) -> Vec<u8> {
    signed::root(|block| {
        block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet { grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant { resource: ResourcePattern { name: name.into(), prefix: true }, actions: vec!["fs/list".into()] }),
        }] };
    })
}
async fn subscribe(fs: Engine, streams: Arc<MemoryStream>, volume: VolumeId, path: &str) -> TestResult<ResourceSubscription> {
    let resource = resource(volume, path)?;
    let context = acyclic_stream::session::testing::resource_authorization(&credential(&resource), &resource)?;
    let changes = StreamPath::new(format!(".acyclic/fs/{}", hex::encode(volume.into_bytes())))?;
    let from = streams.tail(changes).await?;
    Ok(ChangeProvider::new(fs, streams)?.follow(ResourceFollowRequest { resource, from, preceding: None, filter: FollowFilter::default(), authorization: context })?)
}
#[tokio::test]
async fn changes_follow_uses_indexed_grants_and_owner_deny_wins() -> TestResult {
    let (fs, streams, workspace) = fixture().await?;
    workspace.write_text("/allowed", "original").await?;
    workspace.write_text("/denied", "private").await?;
    let policy = crate::access::fixtures::denied(Action::List, "denied");
    crate::access::fixtures::publish_policy(&workspace, &policy).await?;
    let prepared = subscribe(fs.clone(), streams.clone(), workspace.id().volume_id(), "allowed").await?;
    let mut follow = prepared.open.await?;
    (prepared.revalidate)()?;
    workspace.write_text("/denied", "must not disclose").await?;
    assert!(matches!(follow.next().await.ok_or("missing skipped cut")??, FollowUpdate::Progress { .. }));
    workspace.write_text("/allowed", "new").await?;
    let FollowUpdate::Record(record) = follow.next().await.ok_or("missing allowed change")?? else { panic!("allowed change was skipped"); };
    let decoded = wire::ChangeRecord::decode(record.value.as_ref())?;
    assert_eq!(decoded.entries.len(), 1);
    assert_eq!(decoded.entries[0].key.as_ref(), b"allowed");
    // With the full-volume cap, the same stored deny must still remove data.
    let prepared = subscribe(fs, streams, workspace.id().volume_id(), "").await?;
    let mut follow = prepared.open.await?;
    workspace.write_text("/denied", "still private").await?;
    assert!(matches!(follow.next().await.ok_or("missing denied cut")??, FollowUpdate::Progress { .. }));
    Ok(())
}
#[tokio::test]
async fn policy_revocation_and_deletion_close_idle_and_queued_changes_without_credit() -> TestResult {
    for deleted in [false, true] {
        let (fs, streams, workspace) = fixture().await?;
        let prepared = subscribe(fs, streams, workspace.id().volume_id(), "allowed").await?;
        let mut follow = prepared.open.await?;
        workspace.write_text("/allowed", "queued metadata").await?;
        assert!(matches!(follow.next().await.ok_or("missing queued record")??, FollowUpdate::Record(_)));
        (prepared.revalidate)()?;
        let mut terminal = prepared.terminal;
        assert!(futures::poll!(terminal.as_mut()).is_pending());
        if deleted {
            workspace.delete(crate::IdempotencyKey::new()).await?;
        } else {
            let policy = crate::access::fixtures::denied(Action::List, "allowed");
            crate::access::fixtures::publish_policy(&workspace, &policy).await?;
        }
        assert!((prepared.revalidate)().is_err(), "queued disclosure retained the former policy");
        assert!(matches!(futures::poll!(terminal.as_mut()), std::task::Poll::Ready(StreamError::AccessDenied)));
    }
    Ok(())
}
#[tokio::test]
async fn missing_policy_and_unrelated_backends_cannot_open_change_follows() -> TestResult {
    let (fs, streams, workspace) = fixture().await?;
    assert!(ChangeProvider::new(fs.clone(), Arc::new(MemoryStream::default())).is_err());
    let missing = fs.create_workspace("no-policy", WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map_err(|error| error.error)?.value;
    let prepared = subscribe(fs, streams, missing.id().volume_id(), "").await?;
    assert!(prepared.open.await.is_err());
    assert_ne!(workspace.id(), missing.id());
    Ok(())
}
