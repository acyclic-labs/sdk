use crate::{
    client::{ClientViews, FileQueryView},
    task_volume::{Component, FileKey, Query},
    wire,
};

pub(super) fn selected<'a, const ACTIVE: bool>(
    views: &'a ClientViews,
    authority: &wire::Authority,
) -> Option<(&'a FileQueryView, &'a Component)> {
    if !ACTIVE {
        return views
            .selections()
            .find_map(|(_, files)| match files.query() {
                Query::TaskView { task, .. } | Query::Task(task) => Some((files, task)),
                _ => None,
            });
    }
    let active = || {
        views
            .selections()
            .filter_map(|(_, files)| match files.query() {
                Query::TaskView { task, .. } => Some((files, task)),
                _ => None,
            })
    };
    active()
        .find(|(_, task)| task.as_str() == authority.id)
        .or_else(|| active().next())
        .or_else(|| {
            views
                .selections()
                .find_map(|(_, files)| match files.query() {
                    Query::Task(task) if task.as_str() == authority.id => Some((files, task)),
                    _ => None,
                })
        })
        .or_else(|| {
            views.selections().find_map(|(_, files)| {
                if !matches!(files.query(), Query::Tasks | Query::Root) {
                    return None;
                }
                files.files().keys().find_map(|key| match key {
                    FileKey::Task { task, .. } if task.as_str() == authority.id => {
                        Some((files, task))
                    }
                    _ => None,
                })
            })
        })
}
