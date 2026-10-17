//! Push cancellation stops execution and waits for its effects to settle.
use crate::Result;

pub(crate) async fn settle<T, C>(
    execution: impl Future<Output = Result<T>>,
    cancelled: impl Future<Output = Result<C>>,
    stop: impl FnOnce(),
) -> Result<(T, Option<C>)> {
    tokio::pin!(execution, cancelled);
    tokio::select! {
        biased;
        cancelled = &mut cancelled => {
            stop();
            // Dropping an async waiter does not stop an already-submitted
            // native process. Retain its custody through kill and reap, even
            // when the cancellation follow itself failed.
            let settled = execution.await;
            let cancelled = cancelled?;
            settled.map(|value| (value, Some(cancelled)))
        }
        settled = &mut execution => settled.map(|value| (value, None)),
    }
}

pub(crate) async fn prepare<P: acyclic_stream::StreamProvider + ?Sized>(
    requests: &crate::task_volume::TaskVolume<P>,
    request: &super::ExternalRequest,
    task: crate::task_volume::Component,
) -> Result<acyclic_stream::BoxProviderFuture<'static, Result<super::ExternalRequest>>> {
    let super::ExternalAction::RunTool { call_id, .. } = &request.action else {
        return Err(crate::Error::Invalid(
            "only a running tool has call cancellation".into(),
        ));
    };
    let operation = super::normalize::operation(
        request.host,
        &request.session_id,
        &request.caller,
        &super::ExternalAction::CancelTool {
            call_id: call_id.clone(),
        },
    );
    let key = crate::task_volume::FileKey::Task {
        task: task.clone(),
        file: crate::task_volume::TaskFile::Request(
            crate::task_volume::Component::new(operation.to_string())
                .map_err(super::publication::storage)?,
        ),
    };
    let file = super::client::follow_file(requests, key)
        .await
        .map_err(super::publication::storage)?;
    // Retain only identity while the effect runs, never a copy of tool arguments.
    let (host, session, caller, call_id) = (
        request.host,
        request.session_id.clone(),
        request.caller.clone(),
        call_id.clone(),
    );
    Ok(Box::pin(async move {
        let file = file.await.map_err(super::publication::storage)?;
        let cancel =
            super::ExternalRequest::from_file(&task, &file).map_err(super::publication::storage)?;
        if cancel.host != host
            || cancel.session_id != session
            || cancel.caller != caller
            || !matches!(&cancel.action, super::ExternalAction::CancelTool {call_id: original} if original == &call_id)
        {
            return Err(crate::Error::Unauthorized(
                "tool cancellation differs from its original call".into(),
            ));
        }
        Ok(cancel)
    }))
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
