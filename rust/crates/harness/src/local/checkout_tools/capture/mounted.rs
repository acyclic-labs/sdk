//! A mounted checkout's native sync, whose frame is funded on the mount's own
//! native work before it is built.
use super::*;

pub(super) async fn sync(
    mount: &acyclic_fs::LazyMount<
        LocalAuthorityBackend,
        LocalObjectBackend,
        acyclic_fs::demand::native::NativeDemandSource,
        acyclic_fs::LocalCoreStateStore,
    >,
    permit: PublicationPermit,
    owner: Option<(
        Arc<crate::durable_host::TaskJournalOwner<LocalStream>>,
        bool,
    )>,
) -> Result<()> {
    let work = mount.native_work()?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    crate::conversation::BoundedOperationFuture::new(Some(&io), || {
        mount.sync_with_permit(permit, &work, move |_| {
            let owner = owner.clone();
            async move {
                if let Some((owner, settlement)) = owner {
                    owner.verify(settlement, &mut None).await?;
                }
                Ok::<(), Error>(())
            }
        })
    })?
    .await
    .map_err(|failure| failure.cause)?;
    Ok(())
}
