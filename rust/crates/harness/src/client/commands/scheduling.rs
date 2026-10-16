//! The input owner awaits control publications and detaches effects that can park.
use super::{CommandRequest, CommandResolution, CommandTool};
use std::future::Future;

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
