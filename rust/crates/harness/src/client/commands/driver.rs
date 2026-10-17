//! Input, effects and display over the session's ordinary file query.
use super::*;
use crate::OperationId;
use acyclic_stream::StreamProvider;
use futures::{Stream, StreamExt as _};
use serde_json::Value;
use std::sync::Arc;

async fn consume<P, D, Fut>(
    session: &mut CommandSession<P>,
    request: &crate::task_volume::FileValue,
    execute: D,
) -> Result<Option<Value>>
where
    P: StreamProvider + ?Sized,
    D: Fn(OperationId, Option<crate::TaskId>, CommandResolution, Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let settings = session.request_settings(request).await?;
    session
        .consume(request, |operation, task, resolution| {
            execute(operation, task, resolution, settings)
        })
        .await
}

/// Run the shared client control loop. EOF drains submitted effects while file
/// follows remain live; explicit quit cancels workers. Headless errors propagate.
/// Rendering is acknowledged independently of file observation and submission.
pub async fn run_command_client<P, I, D, Fut, R>(
    mut session: CommandSession<P>,
    mut input: I,
    reader: &dyn ContentResidencyVerifier,
    execute: D,
    mut render: R,
    interactive: bool,
) -> Result<CommandSession<P>>
where
    P: StreamProvider + ?Sized + 'static,
    I: Stream<Item = Result<String>> + Unpin,
    D: Fn(OperationId, Option<crate::TaskId>, CommandResolution, Value) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
    R: FnMut(&str) -> Result<()>,
{
    let mut effects = tokio::task::JoinSet::new();
    let mut closed = false;
    let mut displayed = session.rendered_sequence()?;
    let mut documents = CommandViewFiles::default();
    let mut follows: crate::task_volume::ChangeStream = Box::pin(futures::stream::pending());
    let mut last_view = None;
    loop {
        if closed {
            session.refresh_files().await?;
        }
        while let Some(request) = session.take_request() {
            let resolution: CommandResolution = serde_json::from_slice(&request.bytes)
                .or_else(|_| {
                    serde_json::from_slice(&request.bytes)
                        .map(|request| CommandResolution::Submit { request })
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let consumer = session.reopen();
            let worker_request = request.clone();
            let worker_execute = execute.clone();
            let effect = Box::pin(async move {
                let mut worker = consumer.await?;
                loop {
                    match consume(&mut worker, &worker_request, worker_execute.clone()).await {
                        Err(Error::Conflict(message))
                            if message == "command query advanced; retry from the new cut" =>
                        {
                            continue;
                        }
                        result => return result,
                    }
                }
            });
            let result = schedule_command_effect(
                &resolution,
                Box::pin(consume(&mut session, &request, execute.clone())),
                || {
                    effects.spawn(effect);
                },
            )
            .await;
            if let Some(Err(error)) = result {
                if matches!(&error, Error::Conflict(message) if message == "command query advanced; retry from the new cut")
                {
                    continue;
                }
                if interactive {
                    render(&format!("{error}\n"))?;
                } else {
                    return Err(error);
                }
            }
        }
        loop {
            let record = session.output_since(displayed).next().transpose()?;
            let Some((sequence, value)) = record else {
                break;
            };
            render(&render_command_record(&value))?;
            session.acknowledge_output(sequence).await?;
            displayed = sequence;
        }
        if command_loop_finished(session.exit_requested()?, closed, !effects.is_empty()) {
            effects.abort_all();
            return Ok(session);
        }
        let queries = session.view_queries()?;
        if !documents.selects(&queries) {
            let (maximum_files, maximum_bytes) = session.view_bounds();
            match CommandViewFiles::observe(
                session.volume().as_ref(),
                &queries,
                maximum_files,
                maximum_bytes,
            )
            .await
            {
                Ok((view, changes)) => {
                    documents = view;
                    follows = changes;
                }
                Err(error) => {
                    render(&format!("view: {error}\n"))?;
                    follows = Box::pin(futures::stream::pending());
                }
            }
        }
        let view = session
            .render_view(&documents)
            .unwrap_or_else(|error| format!("view: {error}\n"));
        let request = session
            .state(CommandFileRoot::Session, "layout.json")?
            .and_then(|layout| layout.get("view_request").cloned());
        if last_view.as_ref() != Some(&(request.clone(), view.clone())) {
            render(&view)?;
            last_view = Some((request, view));
        }
        tokio::select! {
            line = input.next(), if !closed => {
                let Some(line) = line else { closed = true; continue; };
                let line = line?;
                loop {
                    match session.submit(&line, reader).await {
                        Err(Error::Conflict(message)) if matches!(message.as_str(), "command query advanced; retry from the new cut" | "command query advanced during resolution; retry from the new cut") => continue,
                        Err(error) if interactive => { render(&format!("{error}\n"))?; break; }
                        result => { result?; break; }
                    }
                }
            }
            change = session.observe_change() => { change?; }
            result = effects.join_next(), if !effects.is_empty() => {
                match result.ok_or_else(|| Error::Conflict("command worker missing".into()))?.map_err(|error| Error::Storage(error.to_string()))? {
                    Ok(_) => {}
                    Err(error) if interactive => render(&format!("{error}\n"))?,
                    Err(error) => return Err(error),
                }
            }
            change = follows.next() => {
                match change {
                    Some(Ok(change)) => if let Err(error) = documents.apply(change) { render(&format!("view: {error}\n"))?; follows = Box::pin(futures::stream::pending()); },
                    Some(Err(error)) => { render(&format!("view: {error}\n"))?; follows = Box::pin(futures::stream::pending()); }
                    None => follows = Box::pin(futures::stream::pending()),
                }
            }
        }
    }
}

#[cfg(feature = "filesystem-local")]
impl<P: StreamProvider + ?Sized + 'static> CommandSession<P> {
    /// Native owner binding for the shared input/effect/render loop.
    pub async fn run_local<I, R>(
        self,
        input: I,
        reader: &dyn ContentResidencyVerifier,
        local: Arc<crate::LocalHarness>,
        render: R,
        interactive: bool,
    ) -> Result<Self>
    where
        I: Stream<Item = Result<String>> + Unpin,
        R: FnMut(&str) -> Result<()>,
    {
        run_command_client(
            self,
            input,
            reader,
            move |operation, task, resolution, settings| {
                let local = local.clone();
                async move {
                    local
                        .dispatch_configured_command(operation, task, resolution, &settings)
                        .await
                }
            },
            render,
            interactive,
        )
        .await
    }
}
