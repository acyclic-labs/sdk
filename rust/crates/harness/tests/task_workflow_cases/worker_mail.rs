use super::*;

pub(super) async fn assert_mail<P, A, O>(
    runtime: &FilesystemTaskRuntime<P, A, O>,
    task: TaskId,
    reader: &dyn ContentResidencyVerifier,
    with_mail_send: bool,
    lease: &acyclic_harness::distributed::WorkLease,
) -> Result<()>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let inbox = runtime.task_host().inbox(task, 0, 1024).await?;
    assert_eq!(inbox.len(), if with_mail_send { 72 } else { 2 });
    let first = inbox
        .first()
        .ok_or_else(|| Error::NotFound("committed mail item".into()))?;
    assert_eq!(
        reader
            .read(&first.payload, 65_536, None, None)?
            .await?
            .as_ref(),
        b"7"
    );
    if with_mail_send {
        assert!(
            runtime
                .task_host()
                .send_mail(
                    task,
                    &LeaseFence::from(&lease.reservation),
                    task,
                    OperationId::from_bytes([99; 16]),
                    first.payload.clone(),
                    None
                )
                .await
                .is_err()
        );
    }

    Ok(())
}
