//! Disposable projections of the selected conversation's UI-session volume.
use super::*;
use acyclic_harness::{
    OperationId,
    client_files::{ClientFileChange, ClientFiles, FileRoot},
    command_request::{
        CommandFileChange, CommandFileRoot, CommandFileSnapshot, layout_view, view_changes,
    },
    conversation::FileDescriptor,
    tui::config::{ConfigFile, ConfigView, Scope},
};
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) struct Session {
    pub snapshot: ClientFiles,
    pub config: ConfigView,
    pub catalog: CommandCatalog,
    pub prices: Arc<acyclic_harness::executor::ModelPrices>,
    // Reads retain their authenticated generation in snapshot. Repricing
    // depends on the verified contents, not unrelated files at that cut.
    price_source: Option<FileDescriptor>,
    open_view: Option<String>,
    pub tree: Option<bool>,
    pub feedback: Vec<String>,
    pub feedback_page: QueryPage<()>,
    pending_feedback: Option<PendingFeedback>,
}

struct PendingFeedback {
    report: String,
    operation: OperationId,
    change: ClientFileChange,
}

const COMMANDS: &str = "commands/commands.json";
const PRICES: &str = "prices.jsonl";
const FEEDBACK: &str = "feedback.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackFile {
    version: u32,
    reports: Vec<String>,
}

fn root(conversation: &Conversation) -> FileRoot {
    FileRoot::ClientSession {
        session: conversation.client_session(),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandFile {
    version: u32,
    commands: Vec<acyclic_harness::client::commands::CommandSpec>,
}

fn paths() -> Vec<String> {
    ["layout.json", COMMANDS, PRICES, FEEDBACK]
        .into_iter()
        .map(str::to_owned)
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
            .files(root(conversation), paths(), None)
            .await?;
        let mut session = Self {
            snapshot,
            config: ConfigView::default(),
            catalog: CommandCatalog::builtins()?,
            prices: Arc::default(),
            price_source: None,
            open_view: None,
            tree: None,
            feedback: Vec::new(),
            feedback_page: QueryPage::new(acyclic_harness::tui::Feedback::lines(&[]), None, false),
            pending_feedback: None,
        };
        session.project()?;
        Ok(session)
    }

    pub fn project(&mut self) -> Result<(), Failure> {
        let feedback = match self.snapshot.files.get(FEEDBACK).and_then(Option::as_ref) {
            Some(file) => {
                let document: FeedbackFile = serde_json::from_str(&file.text)?;
                if document.version != 1 {
                    return Err(
                        format!("unsupported {FEEDBACK} version {}", document.version).into(),
                    );
                }
                document.reports
            }
            None => Vec::new(),
        };
        if feedback != self.feedback {
            self.feedback_page = QueryPage::new(
                acyclic_harness::tui::Feedback::lines(&feedback),
                None,
                false,
            );
            self.feedback = feedback;
        }
        let source = self.snapshot.files.get(PRICES).and_then(Option::as_ref);
        if source.map(|file| file.descriptor()) != self.price_source.as_ref() {
            let prices = match source {
                Some(file) => {
                    acyclic_harness::executor::ModelPrices::from_jsonl(file.text.as_bytes())?
                }
                None => Default::default(),
            };
            self.prices = Arc::new(prices);
            self.price_source = source.map(|file| file.descriptor().clone());
        }
        self.tree = self
            .snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .map(|file| -> Result<Option<bool>, Failure> {
                let layout: Value = serde_json::from_str(&file.text)?;
                match layout.get("tree") {
                    None => Ok(None),
                    Some(Value::Bool(value)) => Ok(Some(*value)),
                    Some(_) => Err("layout.json tree must be boolean".into()),
                }
            })
            .transpose()?
            .flatten();
        self.catalog = match self.snapshot.files.get(COMMANDS).and_then(Option::as_ref) {
            Some(file) => {
                let document: CommandFile = serde_json::from_str(&file.text)?;
                if document.version != 1 {
                    return Err(
                        format!("unsupported {COMMANDS} version {}", document.version).into(),
                    );
                }
                CommandCatalog::from_specs(document.commands)?
            }
            None => CommandCatalog::builtins()?,
        };
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

    /// Completion consumes the same validated, normalized descriptors as
    /// dispatch and help. Shipped defaults are never written to the volume.
    pub fn complete(&self, input: &mut PromptInput) -> Result<(), Failure> {
        let commands =
            serde_json::to_vec(&capabilities::commands(&self.catalog).collect::<Vec<_>>())?;
        input.completion.set_file(COMMANDS, Some(&commands))?;
        input.refresh_completions();
        Ok(())
    }

    pub fn open_view(&self) -> Option<&str> {
        self.open_view.as_deref()
    }

    pub async fn save_feedback(
        &mut self,
        conversation: &Conversation,
        report: &str,
    ) -> Result<(), Failure> {
        if self.pending_feedback.is_none() {
            self.prepare_feedback(conversation, report).await?;
        }
        let pending = self
            .pending_feedback
            .as_ref()
            .ok_or("feedback request absent")?;
        if pending.report != report {
            return Err(
                "Retry the unchanged feedback submission before starting another report.".into(),
            );
        }
        let published = conversation
            .publish_client_files(
                conversation.client_session(),
                pending.operation,
                vec![pending.change.clone()],
            )
            .await;
        if let Err(error) = published {
            if matches!(error, acyclic_harness::Error::Refused(_)) {
                self.pending_feedback = None;
            }
            return Err(error.into());
        }
        let snapshot = conversation
            .client_files(conversation.client_session(), paths(), None)
            .await?;
        self.snapshot = snapshot;
        self.project()?;
        self.pending_feedback = None;
        Ok(())
    }

    pub fn feedback_draft(&self) -> Option<&str> {
        self.pending_feedback
            .as_ref()
            .map(|pending| pending.report.as_str())
    }

    async fn prepare_feedback(
        &mut self,
        conversation: &Conversation,
        report: &str,
    ) -> Result<(), Failure> {
        // Preparation does not advance the displayed snapshot. A failed edit
        // leaves the original file follow responsible for other UI changes.
        let snapshot = conversation
            .client_files(conversation.client_session(), vec![FEEDBACK.into()], None)
            .await?;
        let source = snapshot.files.get(FEEDBACK).and_then(Option::as_ref);
        let mut document = match source {
            Some(file) => serde_json::from_str::<FeedbackFile>(&file.text)?,
            None => FeedbackFile {
                version: 1,
                reports: Vec::new(),
            },
        };
        if document.version != 1 {
            return Err(format!("unsupported {FEEDBACK} version {}", document.version).into());
        }
        document.reports.push(report.to_owned());
        self.pending_feedback = Some(PendingFeedback {
            report: report.to_owned(),
            operation: OperationId::new(),
            change: ClientFileChange {
                path: FEEDBACK.into(),
                expected: source.map(|file| file.file.clone()),
                text: serde_json::to_string(&document)?,
            },
        });
        Ok(())
    }

    /// Retain an explicitly selected original price file in this session's
    /// existing conditional publication, so reopen keeps the same revision.
    pub async fn prices(
        &mut self,
        conversation: &Conversation,
        text: String,
    ) -> Result<(), Failure> {
        let expected = self.snapshot.files.get(PRICES).and_then(Option::as_ref);
        if expected.is_some_and(|file| file.text == text) {
            return Ok(());
        }
        conversation
            .publish_client_files(
                conversation.client_session(),
                OperationId::new(),
                vec![ClientFileChange {
                    path: PRICES.into(),
                    expected: expected.and_then(|file| file.original().cloned()),
                    text: Some(text),
                }],
            )
            .await?;
        self.snapshot = conversation
            .files(root(conversation), paths(), None)
            .await?;
        self.project()
    }

    pub fn layout_digest(&self) -> Option<[u8; 32]> {
        self.snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .map(|file| *file.descriptor().sha256())
    }

    pub async fn select(
        &mut self,
        conversation: &Conversation,
        query: Option<&CommandQuery>,
    ) -> Result<(), Failure> {
        self.change(conversation, query, None).await
    }

    pub async fn show_tree(
        &mut self,
        conversation: &Conversation,
        shown: bool,
    ) -> Result<(), Failure> {
        self.change(conversation, None, Some(shown)).await
    }

    async fn change(
        &mut self,
        conversation: &Conversation,
        query: Option<&CommandQuery>,
        tree: Option<bool>,
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
        let changes = if let Some(shown) = tree {
            let mut changes = CommandResolution::Select {
                query: CommandQuery::Tree,
            }
            .file_changes(&snapshot, operation)?;
            for change in &mut changes {
                change.contents["tree"] = Value::Bool(shown);
            }
            changes
        } else {
            view_changes(&snapshot, query, operation)?
        };
        self.publish(conversation, operation, changes).await
    }

    pub async fn submit(
        &mut self,
        conversation: &Conversation,
        resolution: &CommandResolution,
    ) -> Result<(), Failure> {
        let mut snapshot = CommandFileSnapshot::new();
        for &(root, path) in resolution.file_inputs() {
            if let Some(file) = self
                .snapshot
                .files
                .get(&session_path(root, path)?)
                .and_then(Option::as_ref)
            {
                snapshot
                    .entry(root)
                    .or_default()
                    .insert(path.into(), serde_json::from_str(&file.text)?);
            }
        }
        if let CommandResolution::Select {
            query: query @ (CommandQuery::Details | CommandQuery::Reasoning),
        } = resolution
        {
            let key = if *query == CommandQuery::Details {
                "tool_details"
            } else {
                "reasoning"
            };
            let settings = self
                .config
                .settings()
                .map_err(|errors| format!("UI configuration: {errors:?}"))?;
            let current = if *query == CommandQuery::Details {
                settings.tool_details
            } else {
                settings.reasoning
            };
            snapshot
                .entry(CommandFileRoot::Config)
                .or_default()
                .entry("settings.json".into())
                .or_insert_with(|| serde_json::json!({}))[key] = Value::Bool(current);
        }
        let operation = OperationId::new();
        let changes = resolution.file_changes(&snapshot, operation)?;
        self.publish(conversation, operation, changes).await
    }

    async fn publish(
        &mut self,
        conversation: &Conversation,
        operation: OperationId,
        changes: Vec<CommandFileChange>,
    ) -> Result<(), Failure> {
        let changes = changes
            .into_iter()
            .map(|change| {
                let path = session_path(change.root, &change.path)?;
                let expected = self
                    .snapshot
                    .files
                    .get(&path)
                    .and_then(Option::as_ref)
                    .and_then(|file| file.original().cloned());
                Ok(ClientFileChange {
                    path,
                    expected,
                    text: Some(serde_json::to_string(&change.contents)?),
                })
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        conversation
            .publish_files(root(conversation), operation, changes)
            .await?;
        // The next conditional edit needs the committed descriptor, including
        // another command in this input batch. Read one consistent query cut
        // while retaining projections of unchanged original file contents.
        self.snapshot = conversation
            .files(root(conversation), paths(), None)
            .await?;
        self.project()
    }
}

fn session_path(root: CommandFileRoot, path: &str) -> Result<String, Failure> {
    match root {
        CommandFileRoot::Session => Ok(path.into()),
        CommandFileRoot::Config => Ok(format!("config/{path}")),
        _ => Err(format!("{root:?} is outside the UI-session files").into()),
    }
}

pub(super) fn follow(
    conversation: Arc<Conversation>,
    after: acyclic_harness::client_files::FileCut,
) -> futures::future::LocalBoxFuture<'static, acyclic_harness::Result<ClientFiles>> {
    super::super::phase(|| async move {
        conversation
            .files(root(&conversation), paths(), Some(after))
            .await
    })
}

pub(super) fn title(snapshot: &acyclic_harness::client_files::FilePreviews) -> String {
    snapshot
        .files
        .get("ui/title")
        .and_then(Option::as_ref)
        .map(|text| text.chars().filter(|ch| !ch.is_control()).collect())
        .unwrap_or_default()
}

pub(super) fn follow_title(
    conversation: Arc<Conversation>,
    after: acyclic_harness::resources::GenerationRef,
) -> futures::future::LocalBoxFuture<
    'static,
    acyclic_harness::Result<acyclic_harness::client_files::FilePreviews>,
> {
    super::super::phase(|| async move {
        conversation
            .agent_file_previews(
                vec!["ui/title".into()],
                Some(after),
                acyclic_harness::client::MAX_READ_BYTES,
            )
            .await
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        LocalHarness,
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };
    use futures::TryFutureExt as _;

    #[tokio::test]
    async fn feedback_survives_reopen_and_follows_other_writers() -> Result<(), Failure> {
        let directory = tempfile::tempdir()?;
        let harness = Arc::new(
            LocalHarness::open(local_harness_config(
                directory.path(),
                (
                    Model::new("scripted".into(), "test".into(), "1".into())?,
                    Arc::new(ScriptedModelProvider::new(
                        std::iter::empty::<ScriptedTurn>(),
                    )),
                ),
            )?)
            .await?,
        );
        let connection = harness.connect().await?;
        let conversation = Arc::new(Conversation::open(connection.clone()));
        let mut writer = Session::open(&conversation).await?;
        let mut reader = Session::open(&conversation).await?;
        let mut stale = Session::open(&conversation).await?;
        let mut conflicting = Session::open(&conversation).await?;
        let displayed = conflicting.layout_digest();
        let original_generation = conflicting.snapshot.generation.clone();
        conflicting
            .prepare_feedback(&conversation, "An edit lost its source.")
            .await?;
        writer
            .prepare_feedback(&conversation, "A command returned no explanation.")
            .await?;
        let pending = writer
            .pending_feedback
            .as_ref()
            .ok_or("pending feedback absent")?;
        let (snapshot, ()) = futures::try_join!(
            follow(
                Arc::clone(&conversation),
                reader.snapshot.generation.clone()
            )
            .map_err(Failure::from),
            conversation
                .publish_client_files(
                    conversation.client_session(),
                    pending.operation,
                    vec![pending.change.clone()]
                )
                .map_ok(|_| ())
                .map_err(Failure::from),
        )?;
        // The publication committed, but its caller has not consumed a result
        // or refreshed its view. Resend the exact retained request.
        writer
            .save_feedback(&conversation, "A command returned no explanation.")
            .await?;
        reader.snapshot = snapshot;
        reader.project()?;
        assert_eq!(reader.feedback, ["A command returned no explanation."]);
        let file = reader.snapshot.files[FEEDBACK]
            .as_ref()
            .ok_or("feedback absent")?;
        assert_eq!(
            file.file.volume().class(),
            acyclic_harness::conversation::VolumeClass::SessionShared
        );
        assert_eq!(serde_json::from_str::<FeedbackFile>(&file.text)?.version, 1);
        reader
            .select(&conversation, Some(&CommandQuery::Diff))
            .await?;
        assert!(
            conflicting
                .save_feedback(&conversation, "An edit lost its source.")
                .await
                .is_err()
        );
        assert_eq!(conflicting.snapshot.generation, original_generation);
        assert_eq!(conflicting.layout_digest(), displayed);
        conflicting.snapshot = follow(Arc::clone(&conversation), original_generation).await?;
        conflicting.project()?;
        let mut previous = displayed;
        let mut overlay = None;
        let mut queries = QueryController::<query::Source, query::Context>::default();
        let mut future = std::pin::pin!(None::<std::future::Ready<()>>);
        assert!(
            apply_session_layout(
                &conflicting,
                &mut previous,
                &mut overlay,
                &mut queries,
                future.as_mut(),
                &PromptInput::default()
            )
            .is_some()
        );
        assert!(matches!(queries.source(), Some(query::Source::Diff(None))));
        conflicting
            .save_feedback(&conversation, "A new report after a rejected edit.")
            .await?;
        // A second writer starts from an older view but preserves the first report.
        stale
            .save_feedback(
                &conversation,
                "The result was difficult to read.\nMore detail.",
            )
            .await?;
        drop(writer);
        drop(reader);
        drop(stale);
        drop(conflicting);
        drop(conversation);
        let reopened = Conversation::open(connection);
        let mut session = Session::open(&reopened).await?;
        assert_eq!(
            session.feedback,
            [
                "A command returned no explanation.",
                "A new report after a rejected edit.",
                "The result was difficult to read.\nMore detail."
            ]
        );
        use acyclic_harness::tui::ratatui::{buffer::Buffer, widgets::Widget};
        let area = Rect::new(0, 0, 80, 18);
        let mut buffer = Buffer::empty(area);
        session.feedback_page.wrap(area.width - 2);
        acyclic_harness::tui::Feedback(
            &PromptInput::default().theme,
            &session.feedback_page.lines,
            0,
        )
        .render(area, &mut buffer);
        let rendered = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("A command returned no explanation."));
        assert!(rendered.contains("The result was difficult to read."));
        // One long report spans multiple pages; the shared scroll primitive
        // reaches its final wrapped rows and the older report afterward.
        session
            .save_feedback(
                &reopened,
                &format!("{}\nlast detail", "long report detail ".repeat(100)),
            )
            .await?;
        session.feedback_page.wrap(38);
        let mut top = 0;
        while !session.feedback_page.page_boundary(&mut top, -8, 8) {}
        let mut buffer = Buffer::empty(Rect::new(0, 0, 40, 10));
        acyclic_harness::tui::Feedback(
            &PromptInput::default().theme,
            &session.feedback_page.lines,
            top,
        )
        .render(buffer.area, &mut buffer);
        let bottom = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(bottom.contains("A command returned no explanation."));
        let detail = session
            .feedback_page
            .lines
            .iter()
            .position(|line| line.to_string() == "last detail")
            .ok_or("wrapped report tail absent")?;
        acyclic_harness::tui::Feedback(
            &PromptInput::default().theme,
            &session.feedback_page.lines,
            detail,
        )
        .render(buffer.area, &mut buffer);
        let detail = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(detail.contains("last detail"));
        Ok(())
    }

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
        let conversation = Conversation::open(connection.clone());
        let mut ui = Session::open(&conversation).await?;
        assert!(ui.open_view().is_none());
        // Catalogue publications use the same real volume and follow as
        // layout/configuration. Dispatch, help and an already-open popup must
        // agree on each cut, including removal of a previous alias.
        let mut input = PromptInput::default();
        for alias in ["review", "inspect"] {
            let text = serde_json::json!({"version":1,"commands":[{
                "name": "changes",
                "aliases": [alias],
                "description": "Inspect captured workspace changes",
                "arguments": "none",
                "action": {"kind": "builtin", "command": "diff"}
            }]})
            .to_string();
            let change = ClientFileChange {
                path: COMMANDS.into(),
                expected: ui
                    .snapshot
                    .files
                    .get(COMMANDS)
                    .and_then(Option::as_ref)
                    .and_then(|file| file.original().cloned()),
                text: Some(text),
            };
            let (snapshot, _) = futures::try_join!(
                follow(Arc::clone(&conversation), ui.snapshot.cut.clone()),
                conversation.publish_files(root(&conversation), OperationId::new(), vec![change],),
            )?;
            ui.snapshot = snapshot;
            ui.project()?;
            input.editor = Editor::new(&format!("/{alias}"));
            ui.complete(&mut input)?;
            assert_eq!(
                input
                    .completion
                    .items()
                    .map(|item| item.value.as_str())
                    .collect::<Vec<_>>(),
                [alias]
            );
            assert!(matches!(
                ui.catalog.resolve(&format!("/{alias}"), u64::MAX)?,
                Some(CommandResolution::Select {
                    query: CommandQuery::Diff
                })
            ));
            assert!(
                View::help(&ui.catalog, &input)
                    .iter()
                    .any(|line| line.to_string().contains(&format!("/{alias}")))
            );
        }
        input.editor = Editor::new("/review");
        input.refresh_completions();
        assert!(input.completion.items().next().is_none());
        assert!(ui.catalog.resolve("/review", u64::MAX)?.is_none());
        let reopened = Session::open(&conversation).await?;
        assert!(matches!(
            reopened.catalog.resolve("/inspect", u64::MAX)?,
            Some(CommandResolution::Select {
                query: CommandQuery::Diff
            })
        ));
        let price_text = serde_json::json!({
            "version":1,"revision":"selected-1","provider":"scripted","model":"test",
            "effective_at":1,"currency":"USD","input_nano_units_per_million":1000000,
            "output_nano_units_per_million":2000000,"cached":{"kind":"included"},
            "cache_write":{"kind":"included"},"reasoning":{"kind":"included"}
        })
        .to_string();
        let (price_snapshot, ()) = futures::try_join!(
            follow(Arc::clone(&conversation), ui.snapshot.cut.clone()).map_err(Failure::from),
            ui.prices(&conversation, price_text.clone()),
        )?;
        assert_eq!(
            price_snapshot
                .files
                .get(PRICES)
                .and_then(Option::as_ref)
                .map(|file| file.text.as_str()),
            Some(price_text.as_str())
        );
        let reopened = Session::open(&conversation).await?;
        assert_eq!(reopened.prices.currency(), Some("USD"));
        assert_eq!(
            reopened
                .snapshot
                .files
                .get(PRICES)
                .and_then(Option::as_ref)
                .and_then(|file| file.original()),
            ui.snapshot
                .files
                .get(PRICES)
                .and_then(Option::as_ref)
                .and_then(|file| file.original())
        );
        let operation = OperationId::new();
        ui.publish(
            &conversation,
            operation,
            vec![CommandFileChange {
                operation,
                root: CommandFileRoot::Config,
                path: "theme.json".into(),
                contents: serde_json::json!({"dark":{"accent":"#123456"}}),
                create_only: false,
            }],
        )
        .await?;
        let theme_commands = CommandCatalog::builtins()?;
        for query in [CommandQuery::Details, CommandQuery::Reasoning] {
            let before = ui
                .config
                .settings()
                .map_err(|errors| format!("{errors:?}"))?;
            for toggled in [true, false] {
                ui.submit(
                    &conversation,
                    &CommandResolution::Select {
                        query: query.clone(),
                    },
                )
                .await?;
                let reopened = Session::open(&conversation).await?;
                let settings = reopened
                    .config
                    .settings()
                    .map_err(|errors| format!("{errors:?}"))?;
                let (current, original) = if query == CommandQuery::Details {
                    (settings.tool_details, before.tool_details)
                } else {
                    (settings.reasoning, before.reasoning)
                };
                assert_eq!(current, original ^ toggled);
            }
        }
        for mode in ["light", "dark", "auto"] {
            let resolution = theme_commands
                .resolve(&format!("/theme {mode}"), u64::MAX)?
                .ok_or("theme command is absent")?;
            let (changed, ()) = futures::try_join!(
                follow(Arc::clone(&conversation), ui.snapshot.cut.clone()).map_err(Failure::from),
                ui.submit(&conversation, &resolution),
            )?;
            let file = changed
                .files
                .get("config/theme.json")
                .and_then(Option::as_ref)
                .ok_or("theme publication was not followed")?;
            let theme: Value = serde_json::from_str(&file.text)?;
            assert_eq!(theme["mode"], mode);
            assert_eq!(
                theme["dark"]["accent"], "#123456",
                "theme selection discarded a token override"
            );
            let reopened = Session::open(&conversation).await?;
            assert_eq!(
                reopened
                    .snapshot
                    .files
                    .get("config/theme.json")
                    .and_then(Option::as_ref)
                    .and_then(|file| file.original()),
                file.original()
            );
        }
        // Another UI publishes a panel before this UI's pending follow is
        // consumed. The theme command reopens past that publication.
        let mut displayed = ui.layout_digest();
        let mut other = Session::open(&conversation).await?;
        other
            .select(&conversation, Some(&CommandQuery::Diff))
            .await?;
        let resolution = theme_commands
            .resolve("/theme light", u64::MAX)?
            .ok_or("theme command is absent")?;
        ui.submit(&conversation, &resolution).await?;
        let mut overlay = None;
        let mut queries = QueryController::<query::Source, query::Context>::default();
        let mut pending = std::pin::pin!(None::<std::future::Ready<()>>);
        assert!(
            apply_session_layout(
                &ui,
                &mut displayed,
                &mut overlay,
                &mut queries,
                pending.as_mut(),
                &input
            )
            .is_some_and(|notice| notice.is_none())
        );
        assert!(
            matches!(queries.source(), Some(query::Source::Diff(None))),
            "a reopened theme cut lost the other UI's panel selection"
        );

        // Negative control: another theme-only cut must not reset a retained
        // page when its authoritative layout file is unchanged.
        let retained = std::sync::Arc::new(tokio::sync::Mutex::new(
            conversation
                .diff_query(
                    acyclic_harness::client::AgentPageRequest::default(),
                    acyclic_harness::client::DiffProjection::Patch,
                )
                .await?,
        ));
        queries.select(
            query::Source::Diff(Some(std::sync::Arc::clone(&retained))),
            pending.as_mut(),
        );
        let resolution = theme_commands
            .resolve("/theme dark", u64::MAX)?
            .ok_or("theme command is absent")?;
        ui.submit(&conversation, &resolution).await?;
        assert!(
            apply_session_layout(
                &ui,
                &mut displayed,
                &mut overlay,
                &mut queries,
                pending.as_mut(),
                &input
            )
            .is_none()
        );
        assert!(
            matches!(queries.source(), Some(query::Source::Diff(Some(page))) if std::sync::Arc::ptr_eq(page, &retained))
        );
        // Commands and Escape can arrive in one terminal batch. Each committed
        // selection must already be visible when Escape chooses its target.
        for selected in [CommandQuery::Shortcuts, CommandQuery::Diff] {
            ui.select(&conversation, Some(&selected)).await?;
            apply_session_layout(
                &ui,
                &mut displayed,
                &mut overlay,
                &mut queries,
                pending.as_mut(),
                &input,
            );
            assert!(overlay.is_some() || queries.source().is_some());
            ui.select(&conversation, None).await?;
            apply_session_layout(
                &ui,
                &mut displayed,
                &mut overlay,
                &mut queries,
                pending.as_mut(),
                &input,
            );
            assert!(overlay.is_none() && queries.source().is_none());
        }
        // Poll the original live query before publishing through a second call
        // on the same authenticated session. No timing or readiness probe.
        let (changed, ()) = futures::try_join!(
            follow(Arc::clone(&conversation), ui.snapshot.cut.clone()).map_err(Failure::from),
            ui.select(&conversation, Some(&CommandQuery::Diff)),
        )?;
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
            original.original().unwrap().volume().class(),
            acyclic_harness::conversation::VolumeClass::SessionShared
        );
        assert_ne!(original.original().unwrap().volume(), harness.project());
        assert_ne!(original.original().unwrap().volume(), harness.scratch());
        if std::env::var_os("ACYCLIC_CAPTURE_CLIENT_FILES").is_some() {
            let command = acyclic_harness_client::AgentCommand::ClientFiles {
                agent: connection.root().clone(),
                root: root(&conversation),
                publication: None,
                paths: paths(),
                after: None,
            };
            let reply = connection.execute(&command).await?;
            capture("CLIENT_FILES_CORPUS", &command, reply)?;
            println!("SESSION_LAYOUT_CORPUS {}", original.text);
        }
        // These use the same production command path without yielding to the
        // terminal's background query between selections.
        let selected_prices = Arc::clone(&ui.prices);
        ui.select(&conversation, Some(&CommandQuery::History))
            .await
            .map_err(|error| {
                format!("sequential view selection lost its committed source: {error}")
            })?;
        assert!(
            Arc::ptr_eq(&selected_prices, &ui.prices),
            "layout-only cuts must not restart pricing replay"
        );
        // Another writer publishes prices while the terminal's follow still
        // names its earlier cut. A view command observes the new original file
        // first; the eventual follow must retain that same admitted book.
        let before_prices = ui.snapshot.cut.clone();
        let mut replacement: Value = serde_json::from_str(&price_text)?;
        replacement["revision"] = "selected-2".into();
        replacement["currency"] = "EUR".into();
        conversation
            .publish_client_files(
                conversation.client_session(),
                OperationId::new(),
                vec![ClientFileChange {
                    path: PRICES.into(),
                    expected: ui
                        .snapshot
                        .files
                        .get(PRICES)
                        .and_then(Option::as_ref)
                        .and_then(|file| file.original().cloned()),
                    text: Some(replacement.to_string()),
                }],
            )
            .await?;
        ui.select(&conversation, Some(&CommandQuery::Diff)).await?;
        assert!(!Arc::ptr_eq(&selected_prices, &ui.prices));
        assert_eq!(ui.prices.currency(), Some("EUR"));
        let replacement_prices = Arc::clone(&ui.prices);
        ui.snapshot = follow(Arc::clone(&conversation), before_prices).await?;
        ui.project()?;
        assert!(
            Arc::ptr_eq(&replacement_prices, &ui.prices),
            "the delayed follow must not readmit the same original price file"
        );
        assert_eq!(ui.open_view(), Some("diff"));
        let original = ui
            .snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .ok_or("selected layout is absent")?;
        let mut large: Value = serde_json::from_str(&original.text)?;
        large["description"] = Value::String("x".repeat(32 * 1024 + 1));
        ui_create(
            &conversation,
            Some(original.original().unwrap().clone()),
            large.to_string(),
        )
        .await?;
        let ui = Session::open(&conversation)
            .await
            .map_err(|error| format!("published client file cannot be reopened: {error}"))?;
        let original = ui
            .snapshot
            .files
            .get("layout.json")
            .and_then(Option::as_ref)
            .ok_or("large layout is absent")?
            .clone();
        assert!(original.text.len() > 32 * 1024);
        assert_eq!(ui.open_view(), Some("diff"));
        drop(ui);
        drop(conversation);
        let resumed = Conversation::open(harness.connect().await?);
        let reopened = Session::open(&resumed).await?;
        assert_eq!(
            reopened.open_view(),
            Some("diff"),
            "TUI open-view selection is not persisted in the session volume"
        );
        assert!(
            resumed
                .files(
                    FileRoot::ClientSession {
                        session: OperationId::new()
                    },
                    vec!["layout.json".into()],
                    None
                )
                .await?
                .files
                .get("layout.json")
                .is_some_and(Option::is_none)
        );
        // A stale absent precondition cannot overwrite the committed view.
        // Its error must also end the concurrent follow: that publication
        // will never produce the filesystem event the reader is awaiting.
        assert!(
            futures::try_join!(
                follow(Arc::clone(&resumed), reopened.snapshot.cut.clone()),
                ui_create(&resumed, None, original.text.clone()),
            )
            .is_err()
        );
        let commands = reopened
            .snapshot
            .files
            .get(COMMANDS)
            .and_then(Option::as_ref)
            .ok_or("catalogue was not persisted")?;
        let mut unsupported: Value = serde_json::from_str(&commands.text)?;
        unsupported["version"] = Value::from(2);
        resumed
            .publish_files(
                root(&resumed),
                OperationId::new(),
                vec![ClientFileChange {
                    path: COMMANDS.into(),
                    expected: Some(commands.original().unwrap().clone()),
                    text: Some(unsupported.to_string()),
                }],
            )
            .await?;
        let error = Session::open(&resumed)
            .await
            .err()
            .ok_or("unsupported catalogue was reopened")?;
        assert!(
            error
                .to_string()
                .contains("unsupported commands/commands.json version 2")
        );
        // Restore the exact original catalogue through its new conditional cut
        // before checking the independently versioned layout file.
        let source = resumed
            .files(root(&resumed), vec![COMMANDS.into()], None)
            .await?;
        resumed
            .publish_files(
                root(&resumed),
                OperationId::new(),
                vec![ClientFileChange {
                    path: COMMANDS.into(),
                    expected: source
                        .files
                        .get(COMMANDS)
                        .and_then(Option::as_ref)
                        .and_then(|file| file.original().cloned()),
                    text: Some(commands.text.clone()),
                }],
            )
            .await?;
        let replacement = serde_json::json!({"version":2,"open_view":"history"}).to_string();
        let command = acyclic_harness_client::AgentCommand::PublishClientFiles {
            agent: connection.root().clone(),
            root: root(&resumed),
            operation: OperationId::new(),
            changes: vec![ClientFileChange {
                path: "layout.json".into(),
                expected: Some(original.original().unwrap().clone()),
                text: Some(replacement),
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
            .publish_files(
                root(conversation),
                OperationId::new(),
                vec![ClientFileChange {
                    path: "layout.json".into(),
                    expected,
                    text: Some(text),
                }],
            )
            .await
    }
}
