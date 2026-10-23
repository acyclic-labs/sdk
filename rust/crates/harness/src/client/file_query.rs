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

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod goal_lines_tests;

/// Disposable goal rendering at the file query's current cursor.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
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
    query: Arc<Query>,
    pub(super) outputs: super::command_output::Outputs,
    storage: super::FileViews,
    rows: BTreeMap<FileKey, Arc<[String]>>,
    models: ViewIndex<super::ModelFileView>,
    pub(super) notifications: BTreeMap<FileKey, super::NotificationFileView>,
    rejected_models: BTreeSet<FileKey>,
    goal: GoalFileView,
    goal_revision: Option<crate::OperationId>,
    #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
    goal_line_starts: Arc<[usize]>,
    progress: Option<super::GoalProgress>,
    materialized: bool,
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
        let mut files = BTreeMap::<FileKey, FileValue>::new();
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
        let storage = super::FileViews::ordinary(
            crate::task_volume::HeadSnapshot {
                generation: snapshot.generation,
                cursor,
                files: files
                    .into_iter()
                    .map(|(_, file)| crate::task_volume::HeadFile {
                        key: file.key,
                        publication: file.publication,
                        channel: file.channel,
                        length: file.bytes.len() as u64,
                        segments: vec![file.bytes],
                        through_sequence: None,
                    })
                    .collect(),
            },
            super::QueryViewLimits {
                files: maximum_files,
                segments: maximum_files,
                bytes: maximum_bytes as u64,
            },
        )
        .map_err(super::query_error)?;
        Self::from_storage(
            query,
            storage,
            rejected_models,
            maximum_files,
            maximum_bytes,
            false,
        )
    }

    fn from_storage(
        query: Query,
        storage: super::FileViews,
        rejected_models: BTreeSet<FileKey>,
        maximum_files: usize,
        maximum_bytes: usize,
        materialized: bool,
    ) -> Result<Self> {
        let models = rejected_models
            .iter()
            .map(|key| (key.clone(), super::ModelFileView::Invalid))
            .collect();
        let mut view = Self {
            query: Arc::new(query),
            outputs: super::command_output::Outputs::default(),
            storage,
            rows: BTreeMap::new(),
            models,
            notifications: BTreeMap::new(),
            rejected_models,
            goal: GoalFileView::Missing,
            goal_revision: None,
            #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
            goal_line_starts: Arc::default(),
            progress: None,
            materialized,
            maximum_files,
            maximum_bytes,
        };
        // Parse only semantic files, directly from the sole segment index.
        // Text/tool heads keep every shared segment; no temporary view is built.
        for head in view.storage.files().filter(|head| semantic(&head.key)) {
            let [bytes] = head.segments.as_slice() else {
                return Err(Error::Invalid("semantic file must have one segment".into()));
            };
            let mut value = FileValue::new(head.key.clone(), bytes.clone());
            value.channel = head.channel;
            value.publication = head.publication;
            match &value.key {
                FileKey::Goal => {
                    view.goal = GoalFileView::from_file(Some(&value), maximum_bytes);
                    view.goal_revision = value.publication;
                }
                FileKey::RequestHead => {
                    view.progress = super::GoalProgress::from_file(Some(&value))
                }
                FileKey::Task {
                    file: crate::task_volume::TaskFile::Notification(_),
                    ..
                } => {
                    view.notifications.insert(
                        value.key.clone(),
                        super::NotificationFileView::from_file(&value),
                    );
                }
                FileKey::Plan(_) => {
                    view.rows.insert(value.key.clone(), plan_rows(&value));
                }
                _ if super::command_output::Outputs::tracks(&value.key) => {
                    view.outputs.insert(&value);
                }
                _ => {
                    view.models
                        .insert(value.key.clone(), super::ModelFileView::from_file(&value));
                }
            }
        }
        #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
        {
            view.goal_line_starts = goal_line_starts(&view.goal);
        }
        Ok(view)
    }

    /// File selection represented by this fold, including an empty snapshot.
    pub fn query(&self) -> &Query {
        &self.query
    }

    #[cfg(feature = "tui")]
    pub(crate) fn source(&self) -> &Arc<Query> {
        &self.query
    }

    /// Exact resume cut; it advances even for nonmatching publications.
    pub fn cursor(&self) -> &Cursor {
        self.storage.cursor()
    }

    /// Admitted file slices, sharing immutable query payloads. Oversized model
    /// requests have an Invalid model row and retain no payload in this cache.
    pub fn files(&self) -> &BTreeMap<FileKey, crate::task_volume::HeadFile> {
        #[cfg(feature = "test-support")]
        {
            self.storage.files.as_map()
        }
        #[cfg(not(feature = "test-support"))]
        {
            &self.storage.files
        }
    }

    /// Borrows model identities; rendering never reparses request bytes.
    pub fn models(&self) -> impl Iterator<Item = super::ModelQueryView<'_>> {
        self.models.iter().filter_map(|(key, model)| {
            let (task, turn) = super::model_view::turn_request(key)?;
            Some(super::ModelQueryView {
                cursor: self.storage.cursor(),
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

    #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
    pub(super) fn goal_line_starts(&self) -> &Arc<[usize]> {
        &self.goal_line_starts
    }

    pub(super) fn progress(&self) -> Option<&super::GoalProgress> {
        self.progress.as_ref()
    }

    pub(super) fn goal_revision(&self) -> Option<crate::OperationId> {
        self.goal_revision
    }

    /// Borrows cached rows; iteration and clipping never parse or format a file.
    pub fn plan_rows(&self) -> impl Iterator<Item = &str> {
        self.rows
            .values()
            .flat_map(|rows| rows.iter().map(String::as_str))
    }

    #[cfg(feature = "tui")]
    pub(crate) fn plan_files(&self) -> impl Iterator<Item = &crate::task_volume::HeadFile> {
        self.rows.keys().filter_map(|key| self.storage.file(key))
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
            self.matches_head(file)
        }
    }

    fn matches_head(&self, file: &FileValue) -> bool {
        self.storage.file(&file.key).is_some_and(|head| {
            head.segments.len() == 1
                && head.segments[0] == file.bytes
                && head.publication == file.publication
                && head.channel == file.channel
        })
    }

    fn project(&mut self, change: &Change) {
        for key in &change.removed {
            if *key == FileKey::Goal {
                self.goal = GoalFileView::Missing;
                self.goal_revision = None;
                #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
                {
                    self.goal_line_starts = Arc::default();
                }
            }
            if *key == FileKey::RequestHead {
                self.progress = None;
            }
            self.outputs.remove(key);
            self.rows.remove(key);
            self.models.remove(key);
            self.notifications.remove(key);
            self.rejected_models.remove(key);
        }
        for file in change.file.iter().chain(&change.files) {
            if rejected_model(&file, self.maximum_bytes) {
                self.models
                    .insert(file.key.clone(), super::ModelFileView::Invalid);
                self.rejected_models.insert(file.key.clone());
                continue;
            }
            self.rejected_models.remove(&file.key);
            if super::command_output::Outputs::tracks(&file.key) && !self.matches_head(file) {
                self.outputs.insert(file);
            }
            if file.key == FileKey::RequestHead && !self.matches_head(&file) {
                self.progress = super::GoalProgress::from_file(Some(&file));
            }
            if file.key == FileKey::Goal
                && (!self.matches_head(file) || self.goal_revision != file.publication)
            {
                self.goal = GoalFileView::from_file(Some(&file), self.maximum_bytes);
                self.goal_revision = goal_revision(Some(&file));
                #[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
                {
                    self.goal_line_starts = goal_line_starts(&self.goal);
                }
            }
            if matches!(file.key, FileKey::Plan(_)) && !self.matches_head(&file) {
                self.rows.insert(file.key.clone(), plan_rows(&file));
            }
            if super::notification_view::identity(&file.key).is_some() && !self.matches_head(&file)
            {
                self.notifications.insert(
                    file.key.clone(),
                    super::NotificationFileView::from_file(&file),
                );
            }
            if super::model_view::turn_request(&file.key).is_some() && !self.matches_head(&file) {
                self.models
                    .insert(file.key.clone(), super::ModelFileView::from_file(&file));
            }
        }
    }

    pub(super) fn from_heads(
        snapshot: crate::task_volume::HeadSnapshot,
        limits: super::QueryViewLimits,
    ) -> Result<Self> {
        let maximum_bytes = usize::try_from(limits.bytes).unwrap_or(usize::MAX);
        let storage = super::FileViews::snapshot(snapshot, limits).map_err(super::query_error)?;
        Self::from_storage(
            Query::Root,
            storage,
            BTreeSet::new(),
            limits.files,
            maximum_bytes,
            true,
        )
    }

    fn apply_materialized(&mut self, change: &Change) -> Result<()> {
        let apply = self.storage.prepare(change).map_err(super::query_error)?;
        self.project(change);
        apply(&mut self.storage).map_err(super::query_error)
    }

    pub(super) fn session_change(
        &mut self,
        data: &acyclic_stream::wire::SessionData,
    ) -> Result<Change> {
        let change = self
            .storage
            .session_change(data)
            .map_err(super::query_error)?;
        self.apply_materialized(&change)?;
        Ok(change)
    }
    pub(super) fn session_watermark(
        &mut self,
        value: &acyclic_stream::wire::SessionWatermark,
    ) -> Result<()> {
        self.storage
            .session_watermark(value)
            .map_err(super::query_error)
    }
    pub(super) fn head(&self, key: &FileKey) -> Option<&crate::task_volume::HeadFile> {
        self.storage.file(key)
    }
    pub(super) fn heads(&self) -> impl Iterator<Item = &crate::task_volume::HeadFile> {
        self.storage.files()
    }

    /// Folds the next accepted record atomically. Gaps require a new snapshot.
    pub fn apply(&mut self, change: Change) -> Result<bool> {
        if self.materialized {
            self.apply_materialized(&change)?;
            return Ok(change.file.is_some());
        }
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
                .saturating_add(self.storage.files.len())
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
        if *cursor == self.storage.cursor {
            return if files.clone().all(|file| self.matches_file(file))
                && change.removed.iter().all(|key| {
                    !self.storage.files.contains_key(key) && !self.rejected_models.contains(key)
                }) {
                Ok(false)
            } else {
                Err(invalid())
            };
        }
        if cursor.volume != self.storage.cursor.volume
            || cursor.query != self.storage.cursor.query
            || cursor.epoch != self.storage.cursor.epoch
            || cursor.schema != self.storage.cursor.schema
            || self.storage.cursor.position.checked_add(1) != Some(cursor.position)
            || cursor.accepted < self.storage.cursor.accepted
            || cursor.accepted < cursor.position
            || cursor.durable < self.storage.cursor.durable
            || cursor.durable > cursor.accepted
        {
            return Err(invalid());
        }
        let mut bytes = usize::try_from(self.storage.bytes).map_err(|_| invalid())?;
        let mut count = self.storage.files.len() + self.rejected_models.len();
        for key in &change.removed {
            let old = self.storage.files.get(key);
            if old.is_none()
                && !self.rejected_models.contains(key)
                && *self.query != Query::ActiveAgents
            {
                return Err(invalid());
            }
            bytes = bytes
                .checked_sub(old.map_or(0, |file| file.length as usize))
                .ok_or_else(invalid)?;
            count -= usize::from(old.is_some() || self.rejected_models.contains(key));
        }
        let mut changed = !change.removed.is_empty();
        for file in files.clone() {
            let old = self.storage.files.get(&file.key);
            bytes = bytes
                .checked_sub(old.map_or(0, |file| file.length as usize))
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
            return Err(crate::task_volume::Error::LimitExceeded.into());
        }
        self.project(&change);
        self.storage.replace(&change, &self.rejected_models);
        Ok(changed)
    }
}

fn semantic(key: &FileKey) -> bool {
    matches!(key, FileKey::Goal | FileKey::Plan(_) | FileKey::RequestHead)
        || super::model_view::turn_request(key).is_some()
        || super::notification_view::identity(key).is_some()
        || super::command_output::Outputs::tracks(key)
}

fn rejected_model(file: &FileValue, maximum_bytes: usize) -> bool {
    file.bytes.len() > maximum_bytes && super::model_view::turn_request(&file.key).is_some()
}

#[cfg(any(feature = "tui", all(test, not(target_arch = "wasm32"))))]
pub(super) fn goal_line_starts(goal: &GoalFileView) -> Arc<[usize]> {
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
        self.selection(&(volume, query))
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
        self.install_selection(view);
        Ok(())
    }

    /// Updates the same shared views consumed by terminal, headless and browser renderers.
    pub fn query_change(&mut self, change: Change) -> Result<bool> {
        self.selection_mut(&(change.cursor.volume, change.cursor.query))
            .ok_or_else(|| Error::Conflict("file query snapshot is missing".into()))?
            .apply(change)
    }
}

impl super::ClientViews {
    /// Resolves a file-backed view selection and starts its atomic snapshot/follow.
    /// The host folds returned records with `query_change` and redraws shared views.
    pub async fn select_file_view(
        &mut self,
        selection: super::CommandQuery,
        volume: &(impl crate::task_volume::FileQuerySource + ?Sized),
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<crate::task_volume::ChangeStream> {
        let query = match selection {
            super::CommandQuery::Plan => crate::task_volume::Query::Plans,
            super::CommandQuery::Goal => {
                crate::task_volume::Query::File(crate::task_volume::FileKey::Goal)
            }
            _ => {
                return Err(Error::Unsupported(
                    "view does not select a private file query".into(),
                ));
            }
        };
        let observation = volume
            .observe_files(query.clone(), None, maximum_files, maximum_bytes)
            .await
            .map_err(|error| match error {
                #[cfg(feature = "filesystem")]
                crate::task_volume::Error::FileRead(error) => error,
                error => Error::Storage(error.to_string()),
            })?;
        let snapshot = observation
            .snapshot
            .ok_or_else(|| Error::Conflict("initial file query has no snapshot".into()))?;
        self.query_snapshot(query, snapshot, maximum_files, maximum_bytes)?;
        Ok(observation.changes)
    }
}
