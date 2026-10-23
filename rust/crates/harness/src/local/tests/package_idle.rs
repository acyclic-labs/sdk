//! Idle attachments release the package host, including its final strong owner.
use super::*;
use crate::package::harness::CommandSubscription;
use acyclic_stream::session::runtime::{Executor, PlatformExecutor};

pub(super) struct ObservedExecutor {
    pub(super) finished: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
}
impl Executor for ObservedExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        let finished = self.finished.lock().unwrap().take().unwrap();
        PlatformExecutor.spawn(Box::pin(async move {
            future.await;
            let _ = finished.send(());
        }));
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        PlatformExecutor.sleep(milliseconds)
    }
}

#[tokio::test]
async fn detaching_an_idle_subscription_releases_the_last_runtime_owner() -> Result<()> {
    for only_session in [false, true] {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        let project = volume(directory.path()).await?;
        let local =
            Arc::new(LocalHarness::open(volume_config(directory.path(), project, text())?).await?);
        let owner = Arc::downgrade(&local);
        let (finished, settled) = futures::channel::oneshot::channel();
        let executor = ObservedExecutor {
            finished: Mutex::new(Some(finished)),
        };
        let observer =
            CommandSubscription::start(local.clone(), &executor, OperationId::new(), only_session);
        crate::client::disconnect_local(local).await?;
        assert!(
            owner.upgrade().is_some(),
            "runtime must retain its native owner"
        );
        drop(observer);
        settled.await.map_err(super::super::storage)?;
        assert!(
            owner.upgrade().is_none(),
            "idle detach must release its final owner"
        );
    }
    Ok(())
}
