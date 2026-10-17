//! Command composition over the existing volume snapshot and push follow.
use super::*;
use crate::task_volume::command_query::related;
use crate::{OperationId, TaskId, task_volume as files};
use acyclic_stream::StreamProvider;
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc};
#[path = "session_activity.rs"]
mod activity;
#[path = "session_archive.rs"]
mod archive;
#[path = "session_confirmation.rs"]
mod confirmation;
#[path = "session_consume.rs"]
mod consumption;
#[path = "session_init.rs"]
mod init;
#[cfg(all(test, acyclic_harness_loom))]
#[path = "session_init_models.rs"]
mod init_models;
#[path = "session_input.rs"]
mod input;
#[path = "session_publish.rs"]
mod publication;
#[path = "session_view.rs"]
mod view;

/// Disposable command/UI fold. All authoritative contents and execution markers
/// are ordinary files in the supplied, already-authorized private/config volume.
pub struct CommandSession<P: ?Sized> {
    volume: Arc<files::TaskVolume<P>>,
    writer: Option<Arc<files::TaskFiles<P>>>,
    changes: files::ChangeStream,
    next_change: Option<files::Change>,
    generation: files::Generation,
    files: BTreeMap<files::FileKey, files::FileValue>,
    pending: VecDeque<files::FileValue>,
    pending_session: usize,
    pending_tasks: BTreeMap<String, usize>,
    outputs: BTreeMap<u64, files::FileKey>,
    activity_cursor: Option<(u64, String)>,
    catalog: CommandCatalog,
    loaded_task: Option<Option<TaskId>>,
    #[cfg(test)]
    settings_visits: usize,
    session: uuid::Uuid,
    maximum_bytes: u64,
    maximum_files: usize,
}

fn storage(error: files::Error) -> Error {
    Error::Storage(error.to_string())
}
fn codec(error: serde_json::Error) -> Error {
    Error::Invalid(error.to_string())
}
fn component(text: impl AsRef<str>) -> Result<files::Component> {
    files::Component::new(text).map_err(storage)
}
fn ui(directory: &str, name: &str) -> Result<files::FileKey> {
    if directory == "config" && matches!(name, "commands.json" | "themes.json") {
        let path = if name == "commands.json" {
            "commands/catalog.json"
        } else {
            "commands/themes.json"
        };
        return files::FileKey::parse(path).map_err(storage);
    }
    Ok(files::FileKey::Ui {
        directory: component(directory)?,
        name: component(name)?,
    })
}

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    /// Open one atomic snapshot and its exact cursor follow. Reopening discovers
    /// unconsumed requests and never reruns a claimed external effect.
    pub async fn open(
        volume: Arc<files::TaskVolume<P>>,
        session: uuid::Uuid,
        maximum_bytes: u64,
        maximum_files: usize,
    ) -> Result<Self> {
        Self::open_bound(volume, None, session, maximum_bytes, maximum_files).await
    }

    /// Open the caller-authorized file binding; execution-owned writes stay denied.
    pub async fn open_authorized(
        files: Arc<files::TaskFiles<P>>,
        session: uuid::Uuid,
        maximum_bytes: u64,
        maximum_files: usize,
    ) -> Result<Self> {
        let volume = Arc::new(files.inner.reopen().await.map_err(storage)?);
        Self::open_bound(volume, Some(files), session, maximum_bytes, maximum_files).await
    }

    async fn open_bound(
        volume: Arc<files::TaskVolume<P>>,
        writer: Option<Arc<files::TaskFiles<P>>>,
        session: uuid::Uuid,
        maximum_bytes: u64,
        maximum_files: usize,
    ) -> Result<Self> {
        if maximum_bytes == 0 || maximum_files == 0 {
            return Err(Error::Invalid("zero command-session bound".into()));
        }
        let observed = volume
            .observe(
                files::Query::CommandSession {
                    session: component(session.to_string())?,
                    recent: u32::try_from((maximum_files / 4).max(1))
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                },
                None,
                maximum_files,
            )
            .await
            .map_err(storage)?;
        let snapshot = observed
            .snapshot
            .ok_or_else(|| Error::Conflict("command snapshot missing".into()))?;
        let mut client = Self {
            volume,
            writer,
            changes: observed.changes,
            next_change: None,
            generation: snapshot.generation,
            files: BTreeMap::new(),
            pending: VecDeque::new(),
            pending_session: 0,
            pending_tasks: BTreeMap::new(),
            outputs: BTreeMap::new(),
            activity_cursor: None,
            catalog: CommandCatalog::builtins()?,
            loaded_task: None,
            #[cfg(test)]
            settings_visits: 0,
            session,
            maximum_bytes,
            maximum_files,
        };
        for file in snapshot.files {
            client.fold(file)?;
        }
        client.load_task_settings().await?;
        for (key, bytes) in [
            (
                ui("config", "commands.json")?,
                DEFAULT_COMMANDS_JSON.to_vec(),
            ),
            (
                ui("config", "settings.json")?,
                serde_json::to_vec(
                    &json!({"diff_mode":"auto","diff_modes":["auto","unified","split"]}),
                )
                .map_err(codec)?,
            ),
            (
                client.state_key(CommandFileRoot::Session, "layout.json")?,
                serde_json::to_vec(&json!({"selected_task":null})).map_err(codec)?,
            ),
            (
                client.state_key(CommandFileRoot::Task, "settings.json")?,
                serde_json::to_vec(&json!({"agent_modes":["code","plan"],"agent_mode":"code"}))
                    .map_err(codec)?,
            ),
        ] {
            client.initialize_file::<true>(key, bytes).await?;
        }
        Ok(client)
    }

    pub(super) fn volume(&self) -> &Arc<files::TaskVolume<P>> {
        &self.volume
    }

    /// Open an independent observer of this authorized command volume.
    pub fn reopen(&self) -> acyclic_stream::BoxProviderFuture<'static, Result<Self>>
    where
        P: 'static,
    {
        let volume = self.volume.clone();
        let (session, maximum_bytes, maximum_files) =
            (self.session, self.maximum_bytes, self.maximum_files);
        Box::pin(async move { Self::open(volume, session, maximum_bytes, maximum_files).await })
    }

    pub(super) fn position(&self) -> u64 {
        self.generation.position
    }

    /// Catalog from the latest observed command-definition file.
    pub fn catalog(&self) -> &CommandCatalog {
        &self.catalog
    }

    /// Observe one pushed file publication, including presentation-only changes.
    pub async fn observe_change(&mut self) -> Result<()> {
        self.advance().await?;
        self.load_task_settings().await
    }

    /// Observe one publication and report whether it changes presentation.
    pub(crate) async fn observe_change_visible(&mut self) -> Result<bool> {
        let visible = self.advance_visible().await?;
        self.load_task_settings().await?;
        Ok(visible)
    }

    /// Pending request derived from observed immutable file identities.
    pub fn take_request(&mut self) -> Option<files::FileValue> {
        self.pending.pop_front()
    }

    /// One archive page from immutable outcome files, newest first. Selection
    /// lives in layout.json; the index is reconstructed from file-query snapshots.
    pub fn output_page(&self, back: usize) -> Result<Option<Value>> {
        let after = self
            .state(CommandFileRoot::Session, "layout.json")?
            .and_then(|value| value.get("history_after").and_then(Value::as_u64))
            .unwrap_or(0);
        self.outputs
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .rev()
            .nth(back)
            .map(|(_, key)| key)
            .and_then(|key| self.files.get(key))
            .map(|file| serde_json::from_slice(&file.bytes).map_err(codec))
            .transpose()
    }

    /// Latest immutable outcome ordinal at the observed query cut.
    pub fn output_sequence(&self) -> u64 {
        self.outputs.keys().next_back().copied().unwrap_or(0)
    }

    /// Every retained result after the renderer's own cursor, oldest first.
    pub fn output_since(&self, after: u64) -> impl Iterator<Item = Result<(u64, Value)>> + '_ {
        self.outputs
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .map(|(sequence, key)| {
                let file = self
                    .files
                    .get(key)
                    .ok_or_else(|| Error::Conflict("output file missing".into()))?;
                Ok((
                    *sequence,
                    serde_json::from_slice(&file.bytes).map_err(codec)?,
                ))
            })
    }

    /// Follow through the currently accepted file cut, including results folded
    /// while another input or worker was being processed.
    pub async fn refresh_files(&mut self) -> Result<()> {
        self.read_file(&ui("config", "settings.json")?).await?;
        Ok(())
    }

    fn request_id(&self, operation: OperationId) -> Result<files::Component> {
        component(format!("command-{}.{operation}", self.session))
    }

    /// SDK activity selection derived from its immutable outcome files.
    pub fn activity_cursor(&self) -> Option<&str> {
        self.activity_cursor
            .as_ref()
            .map(|(_, cursor)| cursor.as_str())
    }

    /// Borrow one authoritative UI/config file at the latest query cut.
    pub fn state_file(
        &self,
        root: CommandFileRoot,
        name: &str,
    ) -> Result<Option<&files::FileValue>> {
        Ok(self.files.get(&self.state_key(root, name)?))
    }

    /// Read one ordinary UI/config file from the latest query cut.
    pub fn state(&self, root: CommandFileRoot, name: &str) -> Result<Option<Value>> {
        let value: Option<Value> = self
            .files
            .get(&self.state_key(root, name)?)
            .map(|file| serde_json::from_slice(&file.bytes).map_err(codec))
            .transpose()?;
        if matches!(
            name,
            "settings.json" | "layout.json" | "theme.json" | "attention.json"
        ) && value.as_ref().is_some_and(|value| !value.is_object())
        {
            return Err(Error::Invalid(format!("{name} must be a JSON object")));
        }
        Ok(value)
    }

    /// Settings bound to the request's immutable target, independently of any
    /// subsequent navigation in the consuming UI.
    pub async fn request_settings(&mut self, request: &files::FileValue) -> Result<Value> {
        let key = match &request.key {
            files::FileKey::Task { task, .. } => {
                ui(&format!("task-{}", task.as_str()), "settings.json")?
            }
            files::FileKey::Session { .. } => {
                ui(&format!("session-{}", self.session), "task-settings.json")?
            }
            _ => return Err(Error::Invalid("not a command request".into())),
        };
        let task: Value = self
            .read_file(&key)
            .await?
            .as_ref()
            .map(|file| serde_json::from_slice(&file.bytes))
            .transpose()
            .map_err(codec)?
            .unwrap_or_else(|| json!({}));
        if !task.is_object() {
            return Err(Error::Invalid(
                "request settings must be a JSON object".into(),
            ));
        }
        let mut value = self
            .state(CommandFileRoot::Config, "settings.json")?
            .unwrap_or_else(|| json!({}));
        value
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("shared settings must be an object".into()))?
            .extend(
                task.as_object()
                    .ok_or_else(|| Error::Invalid("task settings must be an object".into()))?
                    .clone(),
            );
        Ok(value)
    }

    /// Payload space in an ordinary UI file, after its exact record framing.
    pub fn state_byte_limit(&self, root: CommandFileRoot, name: &str) -> Result<usize> {
        self.publication_byte_limit(self.state_key(root, name)?)
    }

    /// Publish an ordinary UI/config file, then follow its accepted cut.
    pub async fn write_state(
        &mut self,
        root: CommandFileRoot,
        name: &str,
        value: &Value,
    ) -> Result<()> {
        self.publish_rebased::<true, true>(
            self.state_key(root, name)?,
            serde_json::to_vec(value).map_err(codec)?,
            false,
        )
        .await?;
        if root == CommandFileRoot::Session && name == "layout.json" {
            self.load_task_settings().await?;
        }
        Ok(())
    }

    /// Publish owner-provided definitions at the current query cut.
    pub async fn install_catalog(&mut self, bytes: Vec<u8>) -> Result<()> {
        let _ = CommandCatalog::from_bytes(&bytes, self.maximum_bytes)?;
        let key = ui("config", "commands.json")?;
        let expected = self.files.get(&key).map(|file| file.bytes.clone());
        let current = self.read_file(&key).await?;
        if current.as_ref().map(|file| &file.bytes) != expected.as_ref() {
            return Err(Error::Conflict(
                "command catalog changed; review the new revision".into(),
            ));
        }
        self.publish_rebased::<true, true>(key, bytes, false).await
    }

    /// The selected task is persisted in the session layout, not the input loop.
    pub fn selected_task(&self) -> Result<Option<TaskId>> {
        let value = self
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or(Value::Null);
        match value.get("selected_task") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(task)) => TaskId::parse(task).map(Some),
            _ => Err(Error::Invalid("selected_task must be a task ID".into())),
        }
    }

    fn state_key(&self, root: CommandFileRoot, name: &str) -> Result<files::FileKey> {
        if root == CommandFileRoot::Task && self.selected_task()?.is_none() {
            return ui(
                &format!("session-{}", self.session),
                &format!("task-{name}"),
            );
        }
        let directory = match root {
            CommandFileRoot::Config => "config".into(),
            CommandFileRoot::Session => format!("session-{}", self.session),
            CommandFileRoot::Task => format!(
                "task-{}",
                self.selected_task()?
                    .ok_or_else(|| Error::Invalid("no selected task".into()))?
            ),
        };
        ui(&directory, name)
    }

    fn snapshot(&self, resolution: &CommandResolution) -> Result<CommandFileSnapshot> {
        let mut snapshot = BTreeMap::new();
        for &(root, name) in resolution.file_inputs() {
            if let Some(file) = self.files.get(&self.state_key(root, name)?) {
                snapshot.entry(root).or_insert_with(BTreeMap::new).insert(
                    name.into(),
                    serde_json::from_slice(&file.bytes).map_err(codec)?,
                );
            }
        }
        Ok(snapshot)
    }

    /// Publishes an already-resolved key action at its observed file cut.
    pub async fn publish_resolution(
        &mut self,
        resolution: CommandResolution,
    ) -> Result<OperationId> {
        self.publish_resolution_with_retry::<false>(resolution, OperationId::new())
            .await
    }

    pub(super) async fn publish_resolution_with_retry<const RETRY: bool>(
        &mut self,
        resolution: CommandResolution,
        operation: OperationId,
    ) -> Result<OperationId> {
        for change in resolution.file_changes(&self.snapshot(&resolution)?, operation)? {
            let key = if change.create_only {
                let task = if change.root == CommandFileRoot::Session {
                    None
                } else {
                    self.selected_task()?
                };
                let file = files::TaskFile::Request(self.request_id(operation)?);
                match task {
                    Some(task) => files::FileKey::Task {
                        task: component(task.to_string())?,
                        file,
                    },
                    None => files::FileKey::Session {
                        session: component(self.session.to_string())?,
                        file,
                    },
                }
            } else {
                self.state_key(change.root, &change.path)?
            };
            self.publish_rebased::<RETRY, true>(
                key,
                serde_json::to_vec(&change.contents).map_err(codec)?,
                change.create_only,
            )
            .await?;
        }
        Ok(operation)
    }

    fn fold(&mut self, file: files::FileValue) -> Result<()> {
        if matches!(&file.key, files::FileKey::Task { .. })
            && !files::command_query::matches(&component(self.session.to_string())?, &file.key)
        {
            return Ok(());
        }
        if matches!(&file.key, files::FileKey::Session { session, .. } if session.as_str() != self.session.to_string())
        {
            return Ok(());
        }
        if let files::FileKey::Ui { directory, .. } = &file.key {
            if directory.as_str().starts_with("task-")
                && self
                    .selected_task()?
                    .is_none_or(|task| directory.as_str() != format!("task-{task}"))
            {
                return Ok(());
            }
        }
        if file.bytes.len() as u64 > self.maximum_bytes {
            return Err(Error::Invalid("command file exceeds byte bound".into()));
        }
        self.make_room(usize::from(!self.files.contains_key(&file.key)))?;
        self.fold_activity_file(&file)?;
        if file.key == ui("config", "commands.json")? {
            let mut catalog = CommandCatalog::from_bytes(&file.bytes, self.maximum_bytes)?;
            catalog.set_themes(self.catalog.themes.clone())?;
            self.catalog = catalog;
        }
        if file.key == ui("config", "themes.json")? {
            self.catalog
                .set_themes(serde_json::from_slice::<Vec<String>>(&file.bytes).map_err(codec)?)?;
        }
        if matches!(
            file.key,
            files::FileKey::Task {
                file: files::TaskFile::ExecutionResult(_),
                ..
            } | files::FileKey::Session {
                file: files::TaskFile::ExecutionResult(_),
                ..
            }
        ) {
            if let Some(request) = related(&file.key, files::TaskFile::Request) {
                self.pending.retain(|pending| pending.key != request);
            }
            if let Some(claim) = related(&file.key, files::TaskFile::ExecutionClaim) {
                self.files.remove(&claim);
            }
            #[derive(Deserialize)]
            struct Order<'a> {
                sequence: u64,
                #[serde(borrow)]
                ok: Option<&'a serde_json::value::RawValue>,
            }
            let order: Order<'_> = serde_json::from_slice(&file.bytes).map_err(codec)?;
            if order.sequence == 0 {
                return Err(Error::Invalid("zero command output sequence".into()));
            }
            if self
                .outputs
                .get(&order.sequence)
                .is_some_and(|key| key != &file.key)
            {
                return Err(Error::Conflict("duplicate command output sequence".into()));
            }
            self.outputs.insert(order.sequence, file.key.clone());
            if let files::FileKey::Session {
                session,
                file: files::TaskFile::ExecutionResult(id),
            } = &file.key
            {
                #[derive(Deserialize)]
                struct Operation {
                    kind: String,
                    command: String,
                }
                #[derive(Deserialize)]
                struct Cursor {
                    activity_cursor: Option<String>,
                }
                let request = files::FileKey::Session {
                    session: session.clone(),
                    file: files::TaskFile::Request(id.clone()),
                };
                let operation = self
                    .files
                    .get(&request)
                    .and_then(|file| serde_json::from_slice::<Operation>(&file.bytes).ok());
                if operation.is_some_and(|op| {
                    op.kind == "sdk" && matches!(op.command.as_str(), "next" | "cursor" | "reset")
                }) && self
                    .activity_cursor
                    .as_ref()
                    .is_none_or(|(sequence, _)| *sequence < order.sequence)
                {
                    if let Some(cursor) = order
                        .ok
                        .and_then(|raw| serde_json::from_str::<Cursor>(raw.get()).ok())
                        .and_then(|value| value.activity_cursor)
                    {
                        self.activity_cursor = Some((order.sequence, cursor));
                    }
                }
            }
        }
        if matches!(
            file.key,
            files::FileKey::Task {
                file: files::TaskFile::Request(_),
                ..
            } | files::FileKey::Session {
                file: files::TaskFile::Request(_),
                ..
            }
        ) && !self.files.contains_key(&file.key)
        {
            self.pending.push_back(file.clone());
        }
        self.fold_pending_execution(&file.key);
        self.files.insert(file.key.clone(), file);
        self.trim_archive();
        Ok(())
    }

    async fn advance(&mut self) -> Result<()> {
        self.advance_visible().await.map(|_| ())
    }

    async fn advance_visible(&mut self) -> Result<bool> {
        if self.next_change.is_none() {
            self.next_change = Some(
                self.changes
                    .next()
                    .await
                    .ok_or_else(|| Error::Conflict("command follow ended; reopen snapshot".into()))?
                    .map_err(storage)?,
            );
        }
        if let Some(file) = self
            .next_change
            .as_ref()
            .and_then(|change| change.file.clone())
        {
            self.fold(file)?;
        }
        let change = self
            .next_change
            .take()
            .ok_or_else(|| Error::Conflict("command change missing".into()))?;
        let visible = !change.is_checkpoint();
        self.generation = files::Generation {
            volume: change.cursor.volume,
            epoch: change.cursor.epoch,
            position: change.cursor.position,
            commit: change.cursor.commit,
        };
        Ok(visible)
    }

    /// Wait on the existing push follow; no timer, watcher or polling loop.
    /// Non-request file changes refresh the fold before the next pending request.
    pub async fn next_request(&mut self) -> Result<files::FileValue> {
        loop {
            if let Some(request) = self.pending.pop_front() {
                return Ok(request);
            }
            self.advance().await?;
        }
    }

    /// Query-derived exit flag used by native and renderer clients alike.
    pub fn exit_requested(&self) -> Result<bool> {
        let key = self.state_key(CommandFileRoot::Session, "layout.json")?;
        let value: Value = self
            .files
            .get(&key)
            .map(|file| serde_json::from_slice(&file.bytes))
            .transpose()
            .map_err(codec)?
            .unwrap_or(Value::Null);
        Ok(value
            .get("exit_requested")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }
}

#[cfg(all(test, acyclic_loom))]
#[path = "session_models.rs"]
mod models;
