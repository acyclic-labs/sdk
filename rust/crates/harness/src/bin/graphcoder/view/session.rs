//! Disposable projections of the selected conversation's UI-session volume.
use super::*;
use acyclic_harness::{
    OperationId,
    client_files::{ClientFileChange, ClientFiles},
    command_request::{CommandFileRoot, CommandFileSnapshot, layout_view, view_changes},
    tui::config::{ConfigFile, ConfigView, Scope},
};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) struct Session {
    pub snapshot: ClientFiles,
    pub config: ConfigView,
    open_view: Option<String>,
}

fn paths() -> Vec<String> {
    std::iter::once("layout.json".to_owned())
        .chain(
            ConfigFile::ALL
                .into_iter()
                .map(|file| file.path().to_owned()),
        )
        .collect()
}

impl Session {
    pub async fn open(conversation: &Conversation) -> Result<Self, Failure> {
        let snapshot = conversation
            .client_files(conversation.client_session(), paths(), None)
            .await?;
        let mut session = Self {
            snapshot,
            config: ConfigView::default(),
            open_view: None,
        };
        session.project()?;
        Ok(session)
    }

    pub fn project(&mut self) -> Result<(), Failure> {
        self.open_view = self
            .snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .map(|file| -> Result<Option<String>, Failure> {
                let layout: Value = serde_json::from_str(&file.text)?;
                Ok(layout_view(&layout)?.map(str::to_owned))
            })
            .transpose()?
            .flatten();
        for file in ConfigFile::ALL {
            self.config.set_file(
                Scope::User,
                file,
                self.snapshot
                    .files
                    .get(file.path())
                    .and_then(Option::as_ref)
                    .map(|file| file.text.as_str()),
            );
        }
        Ok(())
    }

    pub fn open_view(&self) -> Option<&str> {
        self.open_view.as_deref()
    }

    pub async fn select(
        &self,
        conversation: &Conversation,
        query: Option<&CommandQuery>,
    ) -> Result<(), Failure> {
        let mut snapshot = CommandFileSnapshot::new();
        if let Some(file) = self
            .snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
        {
            snapshot.insert(
                CommandFileRoot::Session,
                BTreeMap::from([("layout.json".into(), serde_json::from_str(&file.text)?)]),
            );
        }
        let operation = OperationId::new();
        let changes = view_changes(&snapshot, query, operation)?;
        let changes = changes
            .into_iter()
            .map(|change| {
                let expected = self
                    .snapshot
                    .files
                    .get(&change.path)
                    .and_then(Option::as_ref)
                    .map(|file| file.file.clone());
                Ok(ClientFileChange {
                    path: change.path,
                    expected,
                    text: serde_json::to_string(&change.contents)?,
                })
            })
            .collect::<Result<Vec<_>, serde_json::Error>>()?;
        conversation
            .publish_client_files(conversation.client_session(), operation, changes)
            .await?;
        Ok(())
    }
}

pub(super) async fn follow(
    conversation: Arc<Conversation>,
    after: acyclic_harness::resources::GenerationRef,
) -> acyclic_harness::Result<ClientFiles> {
    conversation
        .client_files(conversation.client_session(), paths(), Some(after))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        LocalHarness,
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };

    #[tokio::test]
    async fn session_files_resume_reopen_and_version_refusal() -> Result<(), Failure> {
        let directory = tempfile::tempdir()?;
        let provider = Arc::new(
            ScriptedModelProvider::new(std::iter::empty::<ScriptedTurn>()).with_context_capacity(
                acyclic_harness::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let harness = Arc::new(
            LocalHarness::open(local_harness_config(
                directory.path(),
                (
                    Model::new("scripted".into(), "test".into(), "1".into())?,
                    provider,
                ),
            )?)
            .await?,
        );
        let connection = harness.connect().await?;
        let conversation = Conversation::open(connection.clone())?;
        let ui = Session::open(&conversation).await?;
        assert!(ui.open_view().is_none());
        // Poll the original live query before publishing through a second call
        // on the same authenticated session. No timing or readiness probe.
        let (changed, published) = futures::join!(
            follow(Arc::clone(&conversation), ui.snapshot.generation.clone()),
            ui.select(&conversation, Some(&CommandQuery::Diff)),
        );
        published?;
        let changed = changed?;
        let original = changed
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .ok_or("published layout is absent")?;
        assert_eq!(
            layout_view(&serde_json::from_str(&original.text)?)?,
            Some("diff")
        );
        assert_eq!(
            original.file.volume().class(),
            acyclic_harness::conversation::VolumeClass::SessionShared
        );
        assert_ne!(original.file.volume(), harness.project());
        assert_ne!(original.file.volume(), harness.scratch());
        if std::env::var_os("ACYCLIC_CAPTURE_CLIENT_FILES").is_some() {
            let command = acyclic_harness_client::AgentCommand::ClientFiles {
                agent: connection.root().clone(),
                session: conversation.client_session(),
                paths: paths(),
                after: None,
            };
            let reply = connection.execute(&command).await?;
            capture("CLIENT_FILES_CORPUS", &command, reply)?;
            println!("SESSION_LAYOUT_CORPUS {}", original.text);
        }
        drop(ui);
        drop(conversation);
        let resumed = Conversation::open(harness.connect().await?)?;
        let reopened = Session::open(&resumed).await?;
        assert_eq!(
            reopened.open_view(),
            Some("diff"),
            "TUI open-view selection is not persisted in the session volume"
        );
        assert!(
            resumed
                .client_files(OperationId::new(), vec!["layout.json".into()], None)
                .await?
                .files
                .get("layout.json")
                .is_some_and(Option::is_none)
        );
        // A stale absent precondition cannot overwrite the committed view.
        assert!(
            ui_create(&resumed, None, original.text.clone())
                .await
                .is_err()
        );
        let replacement = serde_json::json!({"version":2,"open_view":"history"}).to_string();
        let command = acyclic_harness_client::AgentCommand::PublishClientFiles {
            agent: connection.root().clone(),
            session: resumed.client_session(),
            operation: OperationId::new(),
            changes: vec![ClientFileChange {
                path: "layout.json".into(),
                expected: Some(original.file.clone()),
                text: replacement,
            }],
        };
        let reply = connection.execute(&command).await?;
        if std::env::var_os("ACYCLIC_CAPTURE_CLIENT_FILES").is_some() {
            capture("CLIENT_FILES_PUBLISH_CORPUS", &command, reply)?;
        }
        let error = Session::open(&resumed)
            .await
            .err()
            .ok_or("unsupported layout was reopened")?;
        assert!(
            error
                .to_string()
                .contains("unsupported layout.json version")
        );
        Ok(())
    }

    fn capture(
        label: &str,
        command: &acyclic_harness_client::AgentCommand,
        reply: acyclic_harness_client::AgentReply,
    ) -> Result<(), Failure> {
        let mut bytes = acyclic_harness_client::PreparedCommand::new(command)?
            .command()
            .to_vec();
        bytes.push(0);
        bytes.extend(acyclic_harness_client::encode_agent_reply(
            &Ok(reply),
            command,
        )?);
        println!("{label} {}", serde_json::to_string(&bytes)?);
        Ok(())
    }

    async fn ui_create(
        conversation: &Conversation,
        expected: Option<acyclic_harness::conversation::FileRef>,
        text: String,
    ) -> acyclic_harness::Result<acyclic_harness::resources::GenerationRef> {
        conversation
            .publish_client_files(
                conversation.client_session(),
                OperationId::new(),
                vec![ClientFileChange {
                    path: "layout.json".into(),
                    expected,
                    text,
                }],
            )
            .await
    }
}
