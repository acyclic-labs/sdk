//! One funded metadata composer whose lifetime belongs to the authority task.
use super::*;
use crate::kernel::allocation::retained::OwnedAccounting;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

struct Completion {
    result: Mutex<Option<(Result<ObjectId, crate::FsError>, OwnedAccounting)>>,
    signal: tokio::sync::Notify,
}

struct Job<P> {
    owner: P,
    core: Arc<ProposalCore>,
    predecessor: ObjectId,
    limits: crate::kernel::DecodeLimits,
    completion: RetainedArc<Completion>,
    accounting: Option<OwnedAccounting>,
}

impl<P> Job<P> {
    fn finish(&mut self, result: Result<ObjectId, crate::FsError>) {
        if let Some(accounting) = self.accounting.take() {
            *self
                .completion
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((result, accounting));
            self.completion.signal.notify_one();
        }
    }
}

impl<P> Drop for Job<P> {
    fn drop(&mut self) {
        self.finish(Err(PublicationError::GroupAborted.into()));
    }
}

impl<P: PublicationOwner> Job<P> {
    async fn run(mut self) {
        let accounting = self
            .accounting
            .as_mut()
            .unwrap_or_else(|| unreachable!("running work"));
        let cancellation = accounting.cancellation();
        let result = super::plan::compose_metadata(
            self.owner.objects(),
            self.predecessor,
            Composition {
                delta: &self.core.delta.value,
                retained_merge: self.core.retained_merge,
            },
            self.limits,
            &mut accounting.borrow(),
            &cancellation,
        )
        .await;
        self.finish(result);
    }
}

pub(super) async fn compose<P: PublicationOwner>(
    owner: P,
    core: Arc<ProposalCore>,
    predecessor: ObjectId,
    limits: crate::kernel::DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ObjectId, crate::FsError> {
    let cancelled = accounting.cancellation_wait(cancellation)?;
    tokio::pin!(cancelled);
    let completion = RetainedArc::new(
        Completion {
            result: Mutex::new(None),
            signal: tokio::sync::Notify::new(),
        },
        accounting,
    )?;
    let job = Job {
        owner,
        core,
        predecessor,
        limits,
        completion: completion.clone(),
        accounting: Some(OwnedAccounting::take_branch(accounting, cancellation)?),
    };
    let task = crate::OwnedAuthorityTask::admit(
        job,
        |job, bytes| {
            let result = RetainedAllocation::claim(
                bytes,
                u64::from(bytes != 0),
                &mut job
                    .accounting
                    .as_mut()
                    .unwrap_or_else(|| unreachable!("undispatched work"))
                    .borrow(),
            );
            match result {
                Ok(allocation) => Ok(allocation),
                Err(error) => {
                    job.finish(Err(error));
                    Err(())
                }
            }
        },
        Job::run,
    );
    if let Ok(task) = task {
        acyclic_stream::runtime::PlatformExecutor.spawn_owned(task);
    }
    loop {
        let notified = completion.signal.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let finished = completion
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some((result, completed)) = finished {
            completed.restore(accounting)?;
            return result;
        }
        wait_for_notification(notified.as_mut(), cancelled.as_mut()).await?;
    }
}
