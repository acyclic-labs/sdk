use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_stream::OwnedBytes;

fn require_original<P: StreamProvider + 'static>(
    mailbox: &TaskJournalMailbox<P>, work: &NativeMaterializationWork,
) -> Result<()> {
    work.charge(acyclic_fs::WorkCounters::default())
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if !mailbox.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work))
        || !mailbox.owner.borrowed().native_shared_work_matches(work)
    {
        return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "mailbox differs from its original SDK metadata and owner ledger" });
    }
    Ok(())
}

pub(super) fn send<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, recipient: TaskId, operation: OperationId,
    payload: &'a crate::conversation::FileRef, work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (crate::durable_host::MailSendAck, NativeAllocation)>> {
    require_original(mailbox, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        let io = super::super::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<crate::durable_host::MailSendAck>() as u64)?;
        let payload = payload.clone_with_io(&io)?;
        let ack = mailbox.host.send_from_owner_with_io(
            mailbox.owner.borrowed(), recipient, operation, payload, Some(&io),
        ).await?;
        let owned = (std::mem::size_of::<crate::durable_host::MailSendAck>() as u64)
            .checked_add(ack.payload.logical_owned_bytes()?)
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
        let owner = mailbox.owner.borrowed();
        owner.verify_read_access_with_native_work(work).await?;
        let items = mailbox.host.inbox_with_native_work(owner.task_id(), after, maximum, work).await?;
        owner.verify_read_access_with_native_work(work).await?;
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
            mailbox.owner.borrowed(), receive, item, work,
        ).await
    })
}

pub(super) fn verify_read_access<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, scope: &'a RuntimeScope,
    work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, ()>> {
    require_original(mailbox, work)?;
    mailbox.owner.borrowed().require_mail_scope_with_native_work(scope, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        mailbox.owner.borrowed().verify_read_access_with_native_work(work).await
    })
}

pub(super) fn read_received_for_model<'a, P: StreamProvider + 'static>(
    mailbox: &'a TaskJournalMailbox<P>, scope: &'a RuntimeScope, volume: &'a VolumeRef,
    model: OperationId, receive: OperationId, work: &'a NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (OwnedBytes, crate::runtime::ReceivedCopyReadWitness<'a>)>> {
    require_original(mailbox, work)?;
    mailbox.owner.borrowed().require_mail_scope_with_native_work(scope, work)?;
    crate::NativeFutureBundle::new(work, || async move {
        mailbox.host.read_received_for_model_from_owner_native(
            mailbox.owner.borrowed(), mailbox, scope, volume, model, receive, work,
        ).await
    })
}
