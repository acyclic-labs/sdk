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
            tool: CommandTool::Shell | CommandTool::WebSearch | CommandTool::UrlFetch,
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

#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(feature = "filesystem-local", all(test, not(acyclic_harness_loom))))]
mod owner;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub(super) use owner::drive_command_owner;
