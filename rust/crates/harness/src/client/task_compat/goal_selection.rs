use super::{ClientViews, FileQueryView};
use crate::{
    Error, Result,
    task_volume::{ChangeStream, FileKey, FileQuerySource, Observation, Query},
};
use futures::StreamExt as _;

impl ClientViews {
    /// Selects the private goal and retained request head, with independent live cuts.
    /// Both snapshots must validate before either replaces the visible selection.
    pub async fn select_goal_view(
        &mut self,
        goal: &(impl FileQuerySource + ?Sized),
        request: &(impl FileQuerySource + ?Sized),
        maximum_bytes: usize,
    ) -> Result<ChangeStream> {
        let goal_query = Query::File(FileKey::Goal);
        let request_query = Query::File(FileKey::RequestHead);
        let (goal, request) = futures::future::try_join(
            goal.observe_files(goal_query.clone(), None, 1, maximum_bytes),
            request.observe_files(request_query.clone(), None, 1, maximum_bytes),
        )
        .await?;
        let prepare = |query, observation: Observation| {
            let snapshot = observation
                .snapshot
                .ok_or_else(|| Error::Conflict("initial goal source has no snapshot".into()))?;
            let view = FileQueryView::from_snapshot(query, snapshot, 1, maximum_bytes)?;
            Ok::<_, Error>((view, required_follow(observation.changes)))
        };
        let (goal, goal_changes) = prepare(goal_query, goal)?;
        let (request, request_changes) = prepare(request_query, request)?;
        let goal_id = (goal.cursor().volume, goal.cursor().query);
        let request_id = (request.cursor().volume, request.cursor().query);
        self.queries.insert(goal_id, goal);
        self.queries.insert(request_id, request);
        self.bind_goal_sources(goal_id, request_id)?;
        Ok(Box::pin(futures::stream::select(
            goal_changes,
            request_changes,
        )))
    }
}

fn required_follow(changes: ChangeStream) -> ChangeStream {
    Box::pin(changes.chain(futures::stream::iter([Err(
        crate::task_volume::Error::ResetRequired,
    )])))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
