use super::{Line, StreamText};
use crate::{
    client::{FileQueryView, OutputOwner},
    task_volume::{FileKey, Query},
};
use std::sync::Arc;

#[derive(Default)]
pub(super) struct Archive {
    source: Option<Arc<Query>>,
    first: Option<FileKey>,
    last: Option<(u64, FileKey)>,
    after: u64,
    stream: StreamText,
    rows: Vec<Line<'static>>,
}
impl Archive {
    pub(super) fn after(&self) -> u64 {
        self.after
    }
    pub(super) fn rows(&self) -> &[Line<'static>] {
        &self.rows
    }
    #[cfg(test)]
    pub(super) fn work(&self) -> usize {
        self.stream.work
    }
    pub(super) fn update(&mut self, source: Option<(&FileQueryView, OutputOwner)>) -> usize {
        let Some((files, owner)) = source else {
            if self.source.is_some() {
                *self = Self::default();
                return 0;
            }
            return self.rows.len();
        };
        let after = match &owner {
            OutputOwner::Session(session) => {
                let key = FileKey::parse(&format!(".ui/session-{}/layout.json", session.as_str()))
                    .expect("canonical session");
                files
                    .files()
                    .get(&key)
                    .and_then(|file| {
                        let [bytes] = file.segments.as_slice() else {
                            return None;
                        };
                        serde_json::from_slice::<serde_json::Value>(bytes).ok()?["history_after"]
                            .as_u64()
                    })
                    .unwrap_or(0)
            }
            OutputOwner::Task(_) => 0,
        };
        let mut changed = self.rows.len();
        if !self
            .source
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, files.source()))
            || self.after != after
            || self
                .first
                .as_ref()
                .is_some_and(|key| !files.files().contains_key(key))
        {
            *self = Self {
                source: Some(files.source().clone()),
                after,
                ..Self::default()
            };
            changed = 0;
        }
        let last = self.last.clone();
        let initial = last
            .is_none()
            .then(|| files.outputs(&owner, after))
            .into_iter()
            .flatten();
        let tail = last
            .as_ref()
            .map(|(seq, key)| files.outputs_from(&owner, Some((*seq, key))))
            .into_iter()
            .flatten();
        for (key, row) in initial.chain(tail) {
            self.last = Some((row.sequence, key.clone()));
            if row.sequence <= after {
                continue;
            }
            self.first.get_or_insert_with(|| key.clone());
            let Some(text) = &row.text else {
                continue;
            };
            changed = changed.min(self.stream.boundary());
            changed = changed.min(self.stream.push(text.as_bytes()));
        }
        self.rows.truncate(changed);
        self.rows.extend(self.stream.rows(changed));
        changed
    }
}
