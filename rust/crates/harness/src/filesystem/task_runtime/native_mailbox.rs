use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_stream::OwnedBytes;

fn require_original<P: StreamProvider + 'static>(
    mailbox: &TaskJournalMailbox<P>, work: &NativeMaterializationWork,
) -> Result<()> {
    work.charge(acyclic_fs::WorkCounters::default())
        .map_err(Error::from)?;
    if !mailbox.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work))
        || !mailbox.owner.as_ref().native_shared_work_matches(work)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "mailbox differs from its original SDK metadata and owner ledger" });
    }
    Ok(())
}

pub(super) fn send<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, recipient: TaskId, operation: OperationId,
    payload: &'a crate::conversation::FileRef, work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (crate::conversation::FileRef, NativeAllocation)>> {
    require_original(mailbox, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        let io = super::super::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<crate::conversation::FileRef>() as u64)?;
        let payload = payload.copy(&mut Some(&mut io.history_io()))?;
        let ack = mailbox.host.send_from_owner(
            mailbox.owner.as_ref(), recipient, operation, payload, Some(&io),
        ).await?;
        let owned = (std::mem::size_of::<crate::conversation::FileRef>() as u64)
            .checked_add(ack.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "mail acknowledgement retained capacity overflows" })?;
        let custody = io.into_native_custody(owned)?;
        Ok((ack, custody))
    })
}

pub(super) fn inbox<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, after: u64, maximum: usize,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (Vec<crate::scheduler::InboxItem>, NativeAllocation)>> {
    require_original(mailbox, work)?;
    if maximum == 0 {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "inbox page bound is invalid" });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let owner = mailbox.owner.as_ref();
        owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        let items = mailbox.host.inbox_with_native_work(owner.task_id(), after, maximum, work).await?;
        owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await?;
        Ok(items)
    })
}

pub(super) fn read_received<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, receive: OperationId,
    item: &'a crate::scheduler::InboxItem, work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, OwnedBytes>> {
    require_original(mailbox, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        mailbox.host.read_received_from_owner_with_native_work(
            mailbox.owner.as_ref(), receive, item, work,
        ).await
    })
}
