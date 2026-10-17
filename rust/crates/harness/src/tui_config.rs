//! User/workspace configuration is followed with the existing file query. The
//! only authority is the selected volume; UI actions publish ordinary file writes.
use crate::{
    task_volume::{ChangeStream, Cursor, FileKey, Generation, Query, TaskVolume, Write},
    tui::{
        AppAction, PromptInput,
        config::{ConfigFile, ConfigView, Diagnostic, Scope, fold, parse, parse_bytes},
        term::Capabilities,
    },
};
use acyclic_stream::StreamProvider;
use futures::StreamExt;
use serde_json::Value;

/// File-query or typed configuration failure.
#[derive(Debug)]
pub enum ConfigError {
    /// Underlying live-query or fenced publication error.
    Volume(crate::task_volume::Error),
    /// Rejected configuration fields.
    Diagnostics(Vec<Diagnostic>),
    /// A higher-precedence file prevents a selected-scope shortcut taking effect.
    ShadowedSetting,
}
impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ConfigError {}
impl From<crate::task_volume::Error> for ConfigError {
    fn from(e: crate::task_volume::Error) -> Self {
        Self::Volume(e)
    }
}
impl From<Vec<Diagnostic>> for ConfigError {
    fn from(e: Vec<Diagnostic>) -> Self {
        Self::Diagnostics(e)
    }
}
type Result<T> = std::result::Result<T, ConfigError>;

/// Two config-volume observations and their disposable merged projection.
pub struct ConfigFiles {
    /// Current defaults/user/workspace fold.
    pub view: ConfigView,
    /// Latest followed cut for user and workspace, respectively.
    pub cursors: [Cursor; 2],
    changes: [ChangeStream; 2],
}
impl ConfigFiles {
    /// Observe both finite snapshots and their cursor-follow streams.
    pub async fn observe<P: StreamProvider + ?Sized>(
        user: &TaskVolume<P>,
        workspace: &TaskVolume<P>,
        input: &mut PromptInput,
        profile: &Capabilities,
    ) -> Result<Self> {
        let mut view = ConfigView::default();
        let mut cuts = vec![];
        let mut streams = vec![];
        for (scope, volume) in [(Scope::User, user), (Scope::Workspace, workspace)] {
            let observation = volume.observe(Query::Config, None, 3).await?;
            if let Some(snapshot) = observation.snapshot {
                for file in snapshot.files {
                    if let Some(kind) = ConfigFile::from_path(&file.key.path()) {
                        view.set_bytes(scope, kind, &file.bytes);
                    }
                }
                cuts.push(snapshot.cursor);
            }
            streams.push(observation.changes);
        }
        view.apply(input, profile)?;
        let cursors: [Cursor; 2] = cuts
            .try_into()
            .map_err(|_| crate::task_volume::Error::ResetRequired)?;
        let changes: [ChangeStream; 2] = streams
            .try_into()
            .map_err(|_| crate::task_volume::Error::ResetRequired)?;
        Ok(Self {
            view,
            cursors,
            changes,
        })
    }
    /// Wait for either config volume. Both existing cursor-follow streams remain
    /// registered; dropping the losing `next` future does not consume a change.
    pub async fn next_any(
        &mut self,
        input: &mut PromptInput,
        profile: &Capabilities,
    ) -> Result<Scope> {
        let [user, workspace] = &mut self.changes;
        let (scope, change) = match futures::future::select(user.next(), workspace.next()).await {
            futures::future::Either::Left((change, _)) => (Scope::User, change),
            futures::future::Either::Right((change, _)) => (Scope::Workspace, change),
        };
        self.apply_change(scope, change, input, profile)?;
        Ok(scope)
    }

    /// Fold one publication from the selected volume into the mounted input.
    pub async fn next(
        &mut self,
        scope: Scope,
        input: &mut PromptInput,
        profile: &Capabilities,
    ) -> Result<()> {
        let change = self.changes[scope.index()].next().await;
        self.apply_change(scope, change, input, profile)
    }

    fn apply_change(
        &mut self,
        scope: Scope,
        change: Option<crate::task_volume::Result<crate::task_volume::Change>>,
        input: &mut PromptInput,
        profile: &Capabilities,
    ) -> Result<()> {
        let change = change.ok_or(crate::task_volume::Error::ResetRequired)??;
        self.cursors[scope.index()] = change.cursor;
        let mut changed = false;
        for file in change.file.into_iter().chain(change.files) {
            if let Some(kind) = ConfigFile::from_path(&file.key.path()) {
                self.view.set_bytes(scope, kind, &file.bytes);
                changed = true;
            }
        }
        for key in change.removed {
            if let Some(kind) = ConfigFile::from_path(&key.path()) {
                self.view.set_file(scope, kind, None);
                changed = true;
            }
        }
        if changed {
            self.view.apply(input, profile)?;
        }
        Ok(())
    }
    /// Publish a patch against a fresh, fenced file cut. Changes become visible
    /// exclusively through next(); concurrent writers return the volume conflict.
    pub async fn write<P: StreamProvider + ?Sized>(
        &self,
        volume: &TaskVolume<P>,
        scope: Scope,
        file: ConfigFile,
        patch: &Value,
    ) -> Result<Generation> {
        let key = FileKey::parse(file.path())?;
        let snapshot = volume.snapshot(&Query::File(key.clone()), 1).await?;
        let old = match snapshot.files.first() {
            Some(value) => parse_bytes(file, scope, &value.bytes)?,
            None => serde_json::json!({}),
        };
        let value = fold(&old, Some(patch), None);
        let bytes = parse(file, scope, &value.to_string())?
            .to_string()
            .into_bytes();
        publish(volume, snapshot.generation, file, bytes).await
    }

    /// Host setting shortcuts publish files; they do not mutate a private UI flag.
    pub async fn action<P: StreamProvider + ?Sized>(
        &self,
        volume: &TaskVolume<P>,
        scope: Scope,
        action: AppAction,
    ) -> Result<bool> {
        if !matches!(
            action,
            AppAction::ToggleWrap | AppAction::ToggleNotifications
        ) {
            return Ok(false);
        }
        let file = ConfigFile::Settings;
        let snapshot = volume
            .snapshot(&Query::File(FileKey::parse(file.path())?), 1)
            .await?;
        let mut current = self.view.clone();
        current.set_file(scope, file, None);
        if let Some(value) = snapshot.files.first() {
            current.set_bytes(scope, file, &value.bytes);
        }
        let errors: Vec<_> = current
            .diagnostics()
            .filter(|error| error.scope == scope && error.file == file)
            .cloned()
            .collect();
        if !errors.is_empty() {
            return Err(ConfigError::Diagnostics(errors));
        }
        let settings = current.settings()?;
        let patch = match action {
            AppAction::ToggleWrap => serde_json::json!({"wrap":!settings.wrap}),
            AppAction::ToggleNotifications => {
                serde_json::json!({"notifications":if settings.notifications==crate::tui::NotificationPolicy::Off {"on"} else {"off"}})
            }
            _ => return Ok(false),
        };
        if scope == Scope::User
            && current
                .source(Scope::Workspace, file)
                .is_some_and(|workspace| {
                    patch
                        .as_object()
                        .into_iter()
                        .flatten()
                        .any(|(key, _)| workspace.get(key).is_some())
                })
        {
            return Err(ConfigError::ShadowedSetting);
        }
        // edit reuses the parsed snapshot and validates the candidate once. Publish
        // exactly those bytes against the generation from which it was computed.
        let bytes = current.edit(scope, file, &patch)?;
        publish(volume, snapshot.generation, file, bytes).await?;
        Ok(true)
    }
    /// Apply the same followed notification settings to existing client encoding.
    pub fn notification(
        &self,
        notification: &crate::client::AttentionNotification,
        protocol: crate::client::TerminalNotificationProtocol,
        focused: bool,
    ) -> Result<String> {
        let settings = self.view.settings()?;
        let mode = match settings.notifications {
            crate::tui::NotificationPolicy::On => crate::client::NotificationMode::On,
            crate::tui::NotificationPolicy::Off => crate::client::NotificationMode::Off,
            crate::tui::NotificationPolicy::AttentionOnly => {
                crate::client::NotificationMode::AttentionOnly
            }
        };
        Ok(crate::client::terminal_notification(
            notification,
            mode,
            protocol,
            focused && !settings.notify_when_focused,
        ))
    }
}

// Both arbitrary patches and setting shortcuts publish against the exact cut
// used to compute their new value. A concurrent writer causes a fenced conflict.
async fn publish<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    generation: Generation,
    file: ConfigFile,
    bytes: Vec<u8>,
) -> Result<Generation> {
    Ok(volume
        .append(generation, &Write::new(FileKey::parse(file.path())?, bytes))
        .await?)
}

#[cfg(test)]
mod tests;
