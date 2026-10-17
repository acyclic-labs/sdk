//! Ordinary document queries feed the one shared file fold.
use super::ClientViews;
use crate::{
    Error, Result,
    task_volume::{ChangeStream, FileKey, FileQuerySource, Query},
};
/// Install each atomic snapshot before following its exact cursor.
pub async fn document_files(
    files: &(impl FileQuerySource + ?Sized),
    views: &mut ClientViews,
    maximum_plans: usize,
    maximum_bytes: usize,
) -> Result<ChangeStream> {
    let query = Query::File(FileKey::Goal);
    let goal = files
        .observe_files(query.clone(), None, 1, maximum_bytes)
        .await?;
    let plans = files
        .observe_files(Query::Plans, None, maximum_plans, maximum_bytes)
        .await?;
    let goal_snapshot = goal
        .snapshot
        .ok_or_else(|| Error::Conflict("goal snapshot missing".into()))?;
    let plan_snapshot = plans
        .snapshot
        .ok_or_else(|| Error::Conflict("plans snapshot missing".into()))?;
    views.query_snapshot(query, goal_snapshot, 1, maximum_bytes)?;
    views.query_snapshot(Query::Plans, plan_snapshot, maximum_plans, maximum_bytes)?;
    Ok(Box::pin(futures::stream::select(
        goal.changes,
        plans.changes,
    )))
}
