//! Approval prompts are disposable projections of request/decision files.
use super::*;
use crate::{
    client::TaskQueryView,
    task_volume::{FileApproval, FileKey, TaskFile},
};
fn bytes(file: &crate::task_volume::HeadFile) -> Vec<u8> {
    file.segments
        .iter()
        .flat_map(|bytes| bytes.iter().copied())
        .collect()
}
pub(super) fn rows(files: Option<TaskQueryView<'_>>) -> Vec<Line<'static>> {
    let Some(files) = files else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for file in files.files() {
        if !matches!(
            file.key,
            FileKey::Task {
                file: TaskFile::ApprovalRequest(_),
                ..
            }
        ) {
            continue;
        }
        let request = match FileApproval::decode(&crate::task_volume::FileValue::new(
            file.key.clone(),
            bytes(file),
        )) {
            Ok(request) if request.key().is_ok_and(|key| key == file.key) => request,
            _ => {
                rows.push(Line::raw("invalid approval request"));
                break;
            }
        };
        let resolved = files
            .files()
            .filter(|file| {
                matches!(
                    &file.key,
                    FileKey::Task {
                        file: TaskFile::ApprovalDecision { .. },
                        ..
                    }
                )
            })
            .any(|file| {
                request
                    .decision(&file.key, &bytes(file))
                    .is_ok_and(|decision| decision.is_some())
            });
        if !resolved
            && let crate::interaction::Interaction::Approval {
                prompt, subject, ..
            } = request.request
        {
            let Ok(subject) = serde_json::to_string_pretty(&subject) else {
                rows.push(Line::raw("invalid approval subject"));
                break;
            };
            rows.push(Line::raw(format!(
                "Approval: {}",
                super::super::display_text(&prompt)
            )));
            rows.extend(
                subject
                    .lines()
                    .map(|line| Line::raw(super::super::display_text(line).into_owned())),
            );
            rows.push(Line::raw("Approve this action? [y/n]"));
            // The command session answers the first unresolved request in this
            // task's canonical file order. Show only that same request.
            break;
        }
    }
    rows
}

#[cfg(test)]
mod tests;
