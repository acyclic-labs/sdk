//! Mailbox for the filesystem task runtime.

use super::*;

impl<P: StreamProvider + 'static> crate::runtime::OwnedTaskMailbox for TaskJournalMailbox<P> {
    fn validate_scope(&self, scope: &RuntimeScope) -> Result<()> {
        self.owner.as_ref().require_mail_scope(scope, &mut None)
    }
    fn task_binding(&self) -> (TaskId, &LeaseFence) {
        self.owner.as_ref().task_binding()
    }
    fn matches_task_binding(&self, task: TaskId, fence: &LeaseFence) -> bool {
        self.owner.as_ref().matches_task_binding(task, fence)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_scope_with_native_work(
        &self,
        scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self
            .native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "mailbox metadata differs from original work",
            });
        }
        self.owner
            .as_ref()
            .require_mail_scope_with_native_work(scope, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn send_with_native_work<'a>(
        &'a self,
        recipient: TaskId,
        operation: OperationId,
        payload: &'a crate::conversation::FileRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (crate::conversation::FileRef, acyclic_fs::NativeAllocation)>,
    > {
        native_mailbox::send(self, recipient, operation, payload, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn inbox_with_native_work<'a>(
        &'a self,
        after: u64,
        maximum: usize,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Vec<crate::scheduler::InboxItem>,
                acyclic_fs::NativeAllocation,
            ),
        >,
    > {
        native_mailbox::inbox(self, after, maximum, work)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_received_with_native_work<'a>(
        &'a self,
        receive_operation: OperationId,
        item: &'a crate::scheduler::InboxItem,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        native_mailbox::read_received(self, receive_operation, item, work)
    }
    fn send<'a>(
        &'a self,
        recipient: TaskId,
        operation: OperationId,
        payload: crate::conversation::FileRef,
    ) -> BoxFuture<'a, Result<crate::conversation::FileRef>> {
        Box::pin(async move {
            let (task, fence) = self.owner.as_ref().task_binding();
            self.host
                .send_mail(task, fence, recipient, operation, payload, None)
                .await
        })
    }
    fn inbox<'a>(
        &'a self,
        after: u64,
        maximum: usize,
    ) -> BoxFuture<'a, Result<Vec<crate::scheduler::InboxItem>>> {
        Box::pin(async move {
            self.owner.as_ref().verify(true, &mut None).await?;
            let (task, _) = self.owner.as_ref().task_binding();
            let items = self.host.inbox(task, after, maximum).await?;
            self.owner.as_ref().verify(true, &mut None).await?;
            Ok(items)
        })
    }
    fn read_received<'a>(
        &'a self,
        receive_operation: OperationId,
        item: &'a crate::scheduler::InboxItem,
    ) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            self.host
                .read_received_mail_owned(self.owner.as_ref(), receive_operation, item, None)
                .await
        })
    }
}
