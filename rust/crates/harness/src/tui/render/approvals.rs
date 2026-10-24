//! Approval prompts are disposable projections of request/decision files.
use super::*;
use crate::{
    client::FileQueryView,
    task_volume::{FileApproval, FileKey, TaskFile},
};
fn bytes(file: &crate::task_volume::HeadFile) -> Vec<u8> {
    file.segments
        .iter()
        .flat_map(|bytes| bytes.iter().copied())
        .collect()
}
pub(super) fn rows(files: Option<&FileQueryView>) -> Vec<Line<'static>> {
    let Some(files) = files else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for file in files.files().values() {
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
                continue;
            }
        };
        let resolved = files
            .files()
            .values()
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
            && let crate::interaction::Interaction::Approval { prompt, .. } = request.request
        {
            rows.push(Line::raw(format!(
                "Approval: {} [y/n]",
                super::super::display_text(&prompt)
            )));
        }
    }
    rows
}
