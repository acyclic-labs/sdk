use super::*;
use acyclic_fs::NativeMaterializationWork;
use acyclic_stream::OwnedBytes;

fn require_original<P: StreamProvider + 'static>(
    mailbox: &TaskJournalMailbox<P>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    work.charge(acyclic_fs::WorkCounters::default())
        .map_err(Error::from)?;
    if !mailbox
        .native_custody
        .as_ref()
        .is_some_and(|guard| guard.work().shares_allocation_context(work))
        || !mailbox.owner.as_ref().native_shared_work_matches(work)
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "mailbox differs from its original SDK metadata and owner ledger",
        });
    }
    Ok(())
}

pub(super) fn read_received<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>,
    receive: OperationId,
    item: &'a crate::scheduler::InboxItem,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, OwnedBytes>> {
    require_original(mailbox, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        mailbox
            .host
            .read_received_mail_owned(mailbox.owner.as_ref(), receive, item, Some(&io))
            .await
    })
}
