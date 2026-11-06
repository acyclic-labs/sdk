//! Browser adapters share the original ledger and the Objects batch driver.
use crate::async_storage::{admit_read_batch, object_error};
use crate::kernel::allocation::{
    KernelAccounting,
    retained::{Retained, RetainedAllocation},
};
use crate::{
    AuthorityAllocation, AuthorityWork, CancellationToken, ObjectRead, ObjectReadRequest,
    ObjectStoreError, WorkCounters,
};
use std::{cell::RefCell, future::Future};

/// One complete browser member, from metadata lookup through materialization.
/// Implementations borrow the original accounting only around synchronous work;
/// they never hold a `RefCell` borrow across a Promise wait.
pub trait Reader {
    /// Reads one member without charging a batch's returned-item count.
    fn read(
        &self,
        request: &ObjectReadRequest,
        accounting: &RefCell<KernelAccounting<'_>>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ObjectRead, ObjectStoreError>>;
}

/// Runs one complete member against the caller's existing accounting frame.
pub async fn read_one(
    store: &impl Reader,
    request: &ObjectReadRequest,
    context: &mut AuthorityWork<'_>,
) -> Result<ObjectRead, ObjectStoreError> {
    let cancellation = context.cancellation;
    store
        .read(request, &RefCell::new(context.accounting()), cancellation)
        .await
}

/// Overlaps complete members through the canonical bounded ordered driver.
/// Output storage and every in-flight frame are admitted on the same ledger.
pub async fn read_many(
    store: &impl Reader,
    requests: &[ObjectReadRequest],
    window: usize,
    context: &mut AuthorityWork<'_>,
) -> Result<(Vec<ObjectRead>, AuthorityAllocation), ObjectStoreError> {
    let cancellation = context.cancellation;
    let mut scope = context.scope()?;
    let count = admit_read_batch(requests.len(), &mut scope)?;
    let mut values =
        Retained::allocate(requests.len(), &mut scope.accounting()).map_err(object_error)?;
    {
        let accounting = RefCell::new(scope.accounting());
        let mut error = None;
        acyclic_objects::batch::independent(
            requests,
            window,
            |bytes| {
                RetainedAllocation::claim(bytes as u64, 1, &mut accounting.borrow_mut())
                    .map_err(object_error)
            },
            |request| store.read(request, &accounting, cancellation),
            |result| match result {
                Ok(value) => values.value.push(value),
                Err(cause) if error.is_none() => error = Some(cause),
                Err(_) => {}
            },
        )
        .await?;
        if let Some(error) = error {
            return Err(error);
        }
        accounting
            .borrow_mut()
            .charge(WorkCounters {
                items_returned: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(object_error)?;
    }
    Ok((values.value, Some(values.allocation)))
}
