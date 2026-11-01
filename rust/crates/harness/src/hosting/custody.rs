//! Original policy and admitted operation owners for selected enrollment.
use super::{CommandPolicy, CredentialCustody, StreamError};
use acyclic_auth::operation::{Context, Stopped};
use std::{future::Future, sync::Arc};

pub(super) fn stopped(reason: Stopped) -> StreamError {
    match reason {
        Stopped::Deadline => StreamError::DeadlineElapsed,
        Stopped::Cancelled => StreamError::Unavailable,
    }
}

struct Selected {
    policy: Arc<dyn CredentialCustody>,
    context: Context,
}

/// A native policy is already bound to its original runtime lifetime. A
/// platform admission also retains its original invocation's stop context.
pub(super) fn retain(
    policy: CommandPolicy,
    context: Option<&Context>,
) -> Result<Arc<dyn CredentialCustody>, StreamError> {
    let policy = policy.into_custody()?;
    match context {
        None => Ok(policy),
        Some(context) => {
            context.check().map_err(stopped)?;
            Ok(Arc::new(Selected {
                policy,
                context: context.clone(),
            }))
        }
    }
}

impl CredentialCustody for Selected {
    fn check(&self) -> Result<(), StreamError> {
        self.context.check().map_err(stopped)?;
        self.policy.check()
    }

    fn terminated(&self) -> acyclic_stream::BoxProviderFuture<'static, StreamError> {
        let policy = self.policy.terminated();
        let context = self.context.clone();
        Box::pin(async move {
            let operation = async {
                match context
                    .run(std::future::pending::<std::convert::Infallible>())
                    .await
                {
                    Err(reason) => stopped(reason),
                    Ok(never) => match never {},
                }
            };
            futures::pin_mut!(operation);
            match futures::future::select(policy, operation).await {
                futures::future::Either::Left((error, _))
                | futures::future::Either::Right((error, _)) => error,
            }
        })
    }
}

/// Register the selected owner before polling source IO. The session already
/// supervises the original caller. Same-poll owner loss also fences completion.
pub(super) async fn run<T>(
    custody: &dyn CredentialCustody,
    work: impl Future<Output = Result<T, StreamError>>,
) -> Result<T, StreamError> {
    let terminated = custody.terminated();
    custody.check()?;
    futures::pin_mut!(work);
    match futures::future::select(terminated, work).await {
        futures::future::Either::Left((error, _)) => Err(error),
        futures::future::Either::Right((result, _)) => {
            custody.check()?;
            result
        }
    }
}
