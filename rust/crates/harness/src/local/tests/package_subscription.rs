//! The stock package, original filesystem journal and real command-file path.
use super::package_idle::ObservedExecutor;
use super::*;
use crate::{
    client::{CommandResolution, CommandSession},
    package::harness::{CommandSubscription, execute_request},
};

struct GateTool {
    started: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    release: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
}
impl ToolExecutor for GateTool {
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let started = self.started.lock().unwrap().take().expect("one execution");
        let release = self.release.lock().unwrap().take().expect("one execution");
        Box::pin(async move {
            started.send(()).unwrap();
            release.await.map_err(super::super::storage)?;
            Ok(ToolResult {
                value: json!({"text":"released"}),
            })
        })
    }
}

async fn history(runtime_owned: bool, only_session: bool, join_shutdown: bool) -> Result<bool> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
                "gate".into(),
                "test.gated".into(),
                json!({}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("finished after detach".into()),
    ]);
    let (started, boundary) = futures::channel::oneshot::channel();
    let (release, released) = futures::channel::oneshot::channel();
    let mut gated = tool("test.gated");
    gated.executor = Arc::new(GateTool {
        started: Mutex::new(Some(started)),
        release: Mutex::new(Some(released)),
    });
    let local = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?.tools(vec![gated]),
        )
        .await?,
    );
    let session = OperationId::new();
    let mut commands = CommandSession::open_local(
        &local,
        uuid::Uuid::from_bytes(session.into_bytes()),
        65_536,
        4096,
    )
    .await?;
    commands
        .publish_resolution(CommandResolution::Prompt {
            text: "run gated tool".into(),
            source: None,
            mentions: Vec::new(),
        })
        .await?;
    let request = commands.next_request().await?;
    if join_shutdown {
        let (closing, closed) = futures::channel::oneshot::channel();
        let presentation = || async move {
            boundary.await.unwrap();
            closing.send(()).unwrap();
            Ok::<(), Error>(())
        };
        let work = Box::pin(crate::client::with_local_commands(
            local.clone(),
            session,
            presentation,
        ));
        let futures::future::Either::Right((Ok(()), mut work)) =
            futures::future::select(work, closed).await
        else {
            panic!("shutdown must first reach the blocked effect");
        };
        // The old wrapper returned here and let the CLI stop Tokio. The actual
        // client boundary must remain pending until its worker publishes.
        use futures::FutureExt;
        assert!(work.as_mut().now_or_never().is_none());
        release.send(()).unwrap();
        work.await?;
        commands.observe_change().await?;
        while commands.output_page(0)?.is_none() {
            commands.observe_change().await?;
        }
        assert_eq!(provider.requests().len(), 2);
        drop(commands);
        crate::client::disconnect_local(local).await?;
        return Ok(true);
    }
    if runtime_owned {
        let (finished, settled) = futures::channel::oneshot::channel();
        let executor = ObservedExecutor {
            finished: Mutex::new(Some(finished)),
        };
        let observer = CommandSubscription::start(local.clone(), &executor, session, only_session);
        boundary.await.unwrap();
        // This is the client disconnect, after a real effect began.
        drop(observer);
        release.send(()).unwrap();
        loop {
            if let Some(output) = commands.output_page(0)? {
                assert_eq!(output["ok"]["text"], "finished after detach");
                break;
            }
            commands.observe_change().await?;
        }
        assert_eq!(provider.requests().len(), 2);
        drop(commands);
        crate::client::disconnect_local(local).await?;
        settled.await.map_err(super::super::storage)?;
        Ok(true)
    } else {
        // The old direct execution shape loses the in-flight effect when its
        // observation future is dropped at this exact same boundary.
        let work = Box::pin(execute_request(&mut commands, &local, &request));
        match futures::future::select(work, boundary).await {
            futures::future::Either::Right((Ok(()), work)) => drop(work),
            _ => panic!("effect must reach its release boundary"),
        }
        let survived = release.send(()).is_ok();
        assert_eq!(provider.requests().len(), 1);
        drop(commands);
        Arc::into_inner(local)
            .expect("direct owner released")
            .disconnect_unfinished()
            .await?;
        Ok(survived)
    }
}

#[tokio::test]
async fn command_subscriptions_finish_admitted_work_after_client_detach() -> Result<()> {
    for only_session in [false, true] {
        assert!(Box::pin(history(true, only_session, false)).await?);
    }
    Ok(())
}

#[tokio::test]
async fn negative_control_presentation_owned_execution_drops_the_effect() -> Result<()> {
    assert!(!Box::pin(history(false, true, false)).await?);
    Ok(())
}

#[tokio::test]
async fn client_shutdown_joins_the_blocked_tool_before_returning() -> Result<()> {
    assert!(Box::pin(history(true, false, true)).await?);
    Ok(())
}
