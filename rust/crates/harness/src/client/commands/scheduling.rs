//! The input owner awaits control publications and detaches effects that can park.
use super::{CommandRequest, CommandResolution, CommandTool};
use std::future::Future;

/// EOF drains admitted effects; explicit quit cancels them immediately.
pub fn command_loop_finished(quit: bool, input_closed: bool, effects_pending: bool) -> bool {
    quit || input_closed && !effects_pending
}

/// Schedule the real owner effect without parking the input/follow loop. The
/// caller supplies its executor; all admission decisions live here.
pub async fn schedule_command_effect<F: Future>(
    resolution: &CommandResolution,
    effect: F,
    spawn: impl FnOnce(),
) -> Option<F::Output> {
    let detached = matches!(
        resolution,
        CommandResolution::Tool {
            tool: CommandTool::Bash | CommandTool::WebSearch | CommandTool::UrlFetch,
            ..
        } | CommandResolution::Submit {
            request: CommandRequest::Compact { .. } | CommandRequest::ClearAll
        }
    );
    schedule(detached, effect, spawn).await
}

pub(super) async fn schedule<F: Future>(
    detached: bool,
    effect: F,
    spawn: impl FnOnce(),
) -> Option<F::Output> {
    if detached {
        spawn();
        None
    } else {
        Some(effect.await)
    }
}

// One race owns both futures until presentation exits. Neither event handling
// nor an awaited file write can stop polling the owner that may unblock it.
#[cfg(any(
    all(test, not(acyclic_harness_loom)),
    all(feature = "filesystem-local", not(target_arch = "wasm32"))
))]
pub(super) async fn drive_command_owner<T, E: From<crate::Error>>(
    owner: impl Future<Output = crate::Result<()>>,
    presentation: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    futures::pin_mut!(owner, presentation);
    match futures::future::select(owner, presentation).await {
        futures::future::Either::Left((result, _)) => {
            result.map_err(E::from)?;
            Err(crate::Error::Conflict("local command owner stopped".into()).into())
        }
        futures::future::Either::Right((result, _)) => result,
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod owner_models {
    // Model the real race with a publication waiting for owner progress. The
    // channel is a model input, not a replacement for a production lock: this
    // orchestration function adds no synchronization or spawned tasks.
    fn model<const DRIVE: bool>() {
        for readiness in 0..4 {
            shuttle::check_dfs(
                move || {
                    shuttle::future::block_on(async move {
                        let (release, waiting) = futures::channel::oneshot::channel();
                        let owner = async move {
                            if readiness & 1 != 0 {
                                shuttle::future::yield_now().await;
                            }
                            release.send(()).unwrap();
                            futures::future::pending::<crate::Result<()>>().await
                        };
                        let presentation = async move {
                            if readiness & 2 != 0 {
                                shuttle::future::yield_now().await;
                            }
                            waiting.await.unwrap();
                            Ok::<_, crate::Error>(())
                        };
                        if DRIVE {
                            super::drive_command_owner(owner, presentation)
                                .await
                                .unwrap();
                        } else {
                            // Broken original: retain the owner without polling it
                            // while a presentation-side publication awaits progress.
                            let _retained = owner;
                            presentation.await.unwrap();
                        }
                    })
                },
                None,
            );
        }
    }
    #[test]
    fn owner_progress_survives_awaited_publications() {
        model::<true>();
    }
    #[test]
    fn negative_control_parked_owner_deadlocks() {
        let failure =
            std::panic::catch_unwind(model::<false>).expect_err("parked owner must deadlock");
        let text = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(text.contains("deadlock"), "{text}");
    }
}
