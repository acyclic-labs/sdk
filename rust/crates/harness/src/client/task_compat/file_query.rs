//! Disposable folds over the shared volume snapshot and cursor follow.

use crate::{
    Error, Result,
    goal::GoalState,
    task_volume::{Change, Cursor, FileKey, FileValue, Query, SCHEMA, Snapshot},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

fn goal_revision(file: Option<&FileValue>) -> Option<crate::OperationId> {
    file.and_then(|file| file.publication)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod goal_lines_tests;

/// Disposable goal rendering at the file query's current cursor.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GoalFileView {
    /// The observed selection contains no GOAL.md.
    Missing,
    /// The ordinary file exists but cannot be interpreted as an accepted goal.
    Invalid {
        /// Parser diagnostic; the exact file bytes remain in the query.
        reason: String,
    },
    /// Validated state from the ordinary private file.
    Present {
        /// Shared runtime and renderer interpretation.
        goal: GoalState,
    },
}

impl GoalFileView {
    fn from_file(file: Option<&FileValue>, maximum_bytes: usize) -> Self {
        match file {
            None => Self::Missing,
            Some(file) => match GoalState::from_bytes(&file.bytes, maximum_bytes as u64) {
                Ok(goal) => Self::Present { goal },
                Err(error) => Self::Invalid {
                    reason: error.to_string(),
                },
            },
        }
    }
}

#[cfg(feature = "test-support")]
type ViewIndex<V> = crate::test_support::TaskVolumeFiles<V>;
#[cfg(not(feature = "test-support"))]
type ViewIndex<V> = BTreeMap<FileKey, V>;

/// Bounded, shared file slices from one accepted volume query.
#[derive(Clone, Debug, PartialEq)]
pub struct FileQueryView {
    query: Query,
    files: ViewIndex<FileValue>,
    rows: BTreeMap<FileKey, Arc<[String]>>,
    models: ViewIndex<super::ModelFileView>,
    rejected_models: BTreeSet<FileKey>,
    goal: GoalFileView,
    goal_revision: Option<crate::OperationId>,
    goal_line_starts: Arc<[usize]>,
    progress: Option<super::GoalProgress>,
    cursor: Cursor,
    bytes: usize,
    maximum_files: usize,
    maximum_bytes: usize,
}

impl FileQueryView {
    /// Replaces a disposable view at the snapshot's atomic cut.
    pub fn from_snapshot(
        query: Query,
        snapshot: Snapshot,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<Self> {
        let invalid = || Error::Conflict("file query snapshot requires reset".into());
        let digest = blake3::hash(
            &serde_json::to_vec(&query).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        let cursor = snapshot.cursor;
        if maximum_files == 0
            || maximum_bytes == 0
            || maximum_files as u64 > crate::conversation::MAX_PORTABLE_COUNT as u64
            || maximum_bytes as u64 > crate::conversation::MAX_EXACT_JS_INTEGER
            || snapshot.files.len() > maximum_files
            || cursor.query != *digest.as_bytes()
            || cursor.schema != SCHEMA
            || cursor.position == 0
            || cursor.position > cursor.accepted
            || cursor.durable > cursor.accepted
            || cursor.volume != snapshot.generation.volume
            || cursor.epoch != snapshot.generation.epoch
            || cursor.position != snapshot.generation.position
            || cursor.commit != snapshot.generation.commit
        {
            return Err(invalid());
        }
        let mut files = ViewIndex::default();
        let mut rejected_models = BTreeSet::new();
        let mut bytes = 0usize;
        for file in snapshot.files {
            if !query.matches(&file.key)
                || files.contains_key(&file.key)
                || rejected_models.contains(&file.key)
            {
                return Err(invalid());
            }
            if rejected_model(&file, maximum_bytes) {
                rejected_models.insert(file.key);
                continue;
            }
            bytes = bytes
                .checked_add(file.bytes.len())
                .filter(|size| *size <= maximum_bytes)
                .ok_or_else(invalid)?;
            files.insert(file.key.clone(), file);
        }
        let rows = files
            .iter()
            .filter(|(key, _)| matches!(key, FileKey::Plan(_)))
            .map(|(key, file)| (key.clone(), plan_rows(file)))
            .collect();
        let models = files
            .iter()
            .filter(|(key, _)| super::model_view::turn_request(key).is_some())
            .map(|(key, file)| (key.clone(), super::ModelFileView::from_file(file)))
            .chain(
                rejected_models
                    .iter()
                    .map(|key| (key.clone(), super::ModelFileView::Invalid)),
            )
            .collect();
        let goal = GoalFileView::from_file(files.get(&FileKey::Goal), maximum_bytes);
        let goal_revision = goal_revision(files.get(&FileKey::Goal));
        let goal_line_starts = goal_line_starts(&goal);
        Ok(Self {
            query,
            rows,
            models,
            rejected_models,
            goal,
            goal_revision,
            goal_line_starts,
            progress: super::GoalProgress::from_file(files.get(&FileKey::RequestHead)),
            files,
            cursor,
            bytes,
            maximum_files,
            maximum_bytes,
        })
    }

    /// File selection represented by this fold, including an empty snapshot.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Exact resume cut; it advances even for nonmatching publications.
    pub fn cursor(&self) -> &Cursor {
        &self.cursor
    }

    /// Admitted file slices, sharing immutable query payloads. Oversized model
    /// requests have an Invalid model row and retain no payload in this cache.
    pub fn files(&self) -> &BTreeMap<FileKey, FileValue> {
        #[cfg(feature = "test-support")]
        {
            self.files.as_map()
        }
        #[cfg(not(feature = "test-support"))]
        {
            &self.files
        }
    }

    /// Borrows model identities; rendering never reparses request bytes.
    pub fn models(&self) -> impl Iterator<Item = super::ModelQueryView<'_>> {
        self.models.iter().filter_map(|(key, model)| {
            let (task, turn) = super::model_view::turn_request(key)?;
            Some(super::ModelQueryView {
                cursor: &self.cursor,
                task,
                turn,
                model,
            })
        })
    }

    /// Borrows parsed goal state; rendering never reparses the file.
    pub fn goal(&self) -> &GoalFileView {
        &self.goal
    }

    pub(in crate::client) fn goal_line_starts(&self) -> &Arc<[usize]> {
        &self.goal_line_starts
    }

    pub(in crate::client) fn progress(&self) -> Option<&super::GoalProgress> {
        self.progress.as_ref()
    }

    pub(in crate::client) fn goal_revision(&self) -> Option<crate::OperationId> {
        self.goal_revision
    }

    /// Borrows cached rows; iteration and clipping never parse or format a file.
    pub fn plan_rows(&self) -> impl Iterator<Item = &str> {
        self.rows
            .values()
            .flat_map(|rows| rows.iter().map(String::as_str))
    }

    /// Collects plain rows for consumers that require an owned export.
    pub fn plan_lines(&self) -> Vec<String> {
        self.plan_rows().map(str::to_owned).collect()
    }

    fn matches_file(&self, file: &FileValue) -> bool {
        if self.rejected_models.contains(&file.key) {
            // Rejected payloads have no observable content in this bounded view.
            // The exact publication cursor still identifies the source revision.
            rejected_model(file, self.maximum_bytes)
        } else {
            self.files.get(&file.key) == Some(file)
        }
    }

    /// Folds the next accepted record atomically. Gaps require a new snapshot.
    pub fn apply(&mut self, change: Change) -> Result<bool> {
        let invalid = || Error::Conflict("file query follow requires reset".into());
        let cursor = &change.cursor;
        let files = change.file.iter().chain(&change.files);
        let changed_files = usize::from(change.file.is_some())
            .checked_add(change.files.len())
            .and_then(|count| count.checked_add(change.removed.len()))
            .ok_or_else(invalid)?;
        if changed_files
            > self
                .maximum_files
                .saturating_add(self.files.len())
                .saturating_add(self.rejected_models.len())
        {
            return Err(invalid());
        }
        let mut keys = std::collections::BTreeSet::new();
        for key in files.clone().map(|file| &file.key).chain(&change.removed) {
            if !self.query.matches(key) || !keys.insert(key) {
                return Err(invalid());
            }
        }
        if *cursor == self.cursor {
            return if files.clone().all(|file| self.matches_file(file))
                && change
                    .removed
                    .iter()
                    .all(|key| !self.files.contains_key(key) && !self.rejected_models.contains(key))
            {
                Ok(false)
            } else {
                Err(invalid())
            };
        }
        if cursor.volume != self.cursor.volume
            || cursor.query != self.cursor.query
            || cursor.epoch != self.cursor.epoch
            || cursor.schema != self.cursor.schema
            || self.cursor.position.checked_add(1) != Some(cursor.position)
            || cursor.accepted < self.cursor.accepted
            || cursor.accepted < cursor.position
            || cursor.durable < self.cursor.durable
            || cursor.durable > cursor.accepted
        {
            return Err(invalid());
        }
        let mut bytes = self.bytes;
        let mut count = self.files.len() + self.rejected_models.len();
        for key in &change.removed {
            let old = self.files.get(key);
            if old.is_none() && !self.rejected_models.contains(key) {
                return Err(invalid());
            }
            bytes = bytes
                .checked_sub(old.map_or(0, |file| file.bytes.len()))
                .ok_or_else(invalid)?;
            count -= 1;
        }
        let mut changed = !change.removed.is_empty();
        for file in files.clone() {
            let old = self.files.get(&file.key);
            bytes = bytes
                .checked_sub(old.map_or(0, |file| file.bytes.len()))
                .ok_or_else(invalid)?;
            count = count
                .checked_add(usize::from(
                    old.is_none() && !self.rejected_models.contains(&file.key),
                ))
                .ok_or_else(invalid)?;
            changed |= !self.matches_file(file);
        }
        for file in files {
            if !rejected_model(file, self.maximum_bytes) {
                bytes = bytes.checked_add(file.bytes.len()).ok_or_else(invalid)?;
            }
        }
        if bytes > self.maximum_bytes || count > self.maximum_files {
            return Err(invalid());
        }
        for key in change.removed {
            if key == FileKey::Goal {
                self.goal = GoalFileView::Missing;
                self.goal_revision = None;
                self.goal_line_starts = Arc::default();
            }
            if key == FileKey::RequestHead {
                self.progress = None;
            }
            self.files.remove(&key);
            self.rows.remove(&key);
            self.models.remove(&key);
            self.rejected_models.remove(&key);
        }
        for file in change.file.into_iter().chain(change.files) {
            if rejected_model(&file, self.maximum_bytes) {
                self.files.remove(&file.key);
                self.models
                    .insert(file.key.clone(), super::ModelFileView::Invalid);
                self.rejected_models.insert(file.key);
                continue;
            }
            self.rejected_models.remove(&file.key);
            if file.key == FileKey::RequestHead && self.files.get(&file.key) != Some(&file) {
                self.progress = super::GoalProgress::from_file(Some(&file));
            }
            if file.key == FileKey::Goal && self.files.get(&file.key) != Some(&file) {
                self.goal = GoalFileView::from_file(Some(&file), self.maximum_bytes);
                self.goal_revision = goal_revision(Some(&file));
                self.goal_line_starts = goal_line_starts(&self.goal);
            }
            if matches!(file.key, FileKey::Plan(_)) && self.files.get(&file.key) != Some(&file) {
                self.rows.insert(file.key.clone(), plan_rows(&file));
            }
            if super::model_view::turn_request(&file.key).is_some()
                && self.files.get(&file.key) != Some(&file)
            {
                self.models
                    .insert(file.key.clone(), super::ModelFileView::from_file(&file));
            }
            self.files.insert(file.key.clone(), file);
        }
        self.bytes = bytes;
        self.cursor = change.cursor;
        Ok(changed)
    }
}

fn rejected_model(file: &FileValue, maximum_bytes: usize) -> bool {
    file.bytes.len() > maximum_bytes && super::model_view::turn_request(&file.key).is_some()
}

pub(in crate::client) fn goal_line_starts(goal: &GoalFileView) -> Arc<[usize]> {
    let GoalFileView::Present { goal } = goal else {
        return Arc::default();
    };
    if goal.markdown.is_empty() {
        return Arc::default();
    }
    std::iter::once(0)
        .chain(
            goal.markdown
                .match_indices('\n')
                .map(|(index, _)| index + 1)
                .filter(|index| *index < goal.markdown.len()),
        )
        .collect()
}

fn plan_rows(file: &FileValue) -> Arc<[String]> {
    let mut lines = vec![file.key.path()];
    let markdown = String::from_utf8_lossy(&file.bytes);
    if let Some(list) = super::parse_task_list(&markdown) {
        for task in list.items() {
            for (index, line) in task.text.lines().enumerate() {
                lines.push(format!(
                    "{} {}",
                    if index == 0 {
                        task.state.marker()
                    } else {
                        "   "
                    },
                    safe(line)
                ));
            }
        }
    } else {
        lines.extend(markdown.lines().map(safe));
    }
    lines.into()
}

fn safe(text: &str) -> String {
    let mut result = String::new();
    for character in text.chars() {
        if character.is_control() {
            result.extend(character.escape_debug());
        } else {
            result.push(character);
        }
    }
    result
}

impl super::ClientViews {
    /// Goal rendering for one followed volume selection, absent before its snapshot.
    pub fn goal(&self, volume: [u8; 32], query: [u8; 32]) -> Option<&GoalFileView> {
        self.queries
            .get(&(volume, query))
            .filter(|view| view.query.matches(&FileKey::Goal))
            .map(FileQueryView::goal)
    }

    /// Publishes a replacement file query after initial observation or reset.
    pub fn query_snapshot(
        &mut self,
        query: Query,
        snapshot: Snapshot,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<()> {
        let view = FileQueryView::from_snapshot(query, snapshot, maximum_files, maximum_bytes)?;
        super::task_projection::snapshot(self, &view);
        self.queries
            .insert((view.cursor.volume, view.cursor.query), view);
        Ok(())
    }

    /// Updates the same shared views consumed by terminal, headless and browser renderers.
    pub fn query_change(&mut self, change: Change) -> Result<bool> {
        let key = (change.cursor.volume, change.cursor.query);
        let affected: BTreeSet<_> = change
            .removed
            .iter()
            .filter_map(super::task_projection::projected_task)
            .collect();
        let updates: Vec<_> = change.file.iter().chain(&change.files).cloned().collect();
        let view = self
            .queries
            .get_mut(&key)
            .ok_or_else(|| Error::Conflict("file query snapshot is missing".into()))?;
        let changed = view.apply(change)?;
        if changed && !affected.is_empty() {
            let mut retained = Vec::new();
            for task in &affected {
                for range in Query::Task(task.clone()).ranges() {
                    retained.extend(view.files().range(range).map(|(_, file)| file.clone()));
                }
            }
            super::task_projection::replace_tasks(self, affected.clone(), retained);
        }
        if changed {
            for file in updates {
                if !super::task_projection::projected_task(&file.key)
                    .is_some_and(|task| affected.contains(&task))
                {
                    super::task_projection::append(self, &file);
                }
            }
        }
        Ok(changed)
    }
}
