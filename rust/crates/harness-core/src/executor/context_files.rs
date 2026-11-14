use super::*;
use crate::conversation::{ContentAccess, ContentChange};

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct RewriteFiles {
    version: u32,
    summary: Option<FileRef>,
    drops: Option<FileRef>,
}

impl PreparationRecord for RewriteFiles {}

pub(super) struct RewritePublication<'a> {
    pub(super) files: &'a FileRef,
    pub(super) proof: &'a crate::context::CompactionReference,
}

impl StockExecution<'_> {
    /// Freeze the original file preconditions before the manager starts. The
    /// original immutable intent, not a later head, is used after a restart.
    pub(super) async fn rewrite_files(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
    ) -> Result<Option<FileRef>> {
        let Some(context) = self.original_context() else {
            return Ok(None);
        };
        let destination = context.private_summaries(self.io)?;
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let io = scope.io();
        let (input, step) = turn;
        let key = with_history_io(io, |work| {
            preparation_format(format_args!("manager:{step}:files"), work)
        })?;
        let staged = journal.staged_ref(input.operation_id, &key, io)?.await?;
        match staged {
            Some(file) => {
                drop(key);
                scope.finish(file.logical_owned_bytes()?)?;
                return Ok(Some(file));
            }
            None => {}
        }
        let binding = destination.bindings();
        let access = ContentAccess {
            ceiling: Some(context.scope().grants()),
            io,
        };
        let (generation, generation_hold) = binding
            .reader
            .select_private_generation(&binding.volume, "context", "context", None, access)?
            .await?;
        let mut files = RewriteFiles {
            version: 1,
            summary: None,
            drops: None,
        };
        for (index, path) in ["context/summary.md", "context/drops/selected.json"]
            .into_iter()
            .enumerate()
        {
            let selected = binding
                .reader
                .read_private_path(
                    &binding.volume,
                    "context",
                    path,
                    Some(&generation),
                    self.stock.limits.file_bytes,
                    access,
                )?
                .await?;
            if let Some((file, bytes, hold)) = selected {
                if let Some(hold) = hold {
                    hold.into_retained()?;
                }
                let bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
                drop(bytes);
                drop(bytes_hold);
                if index == 0 {
                    files.summary = Some(file);
                } else {
                    files.drops = Some(file);
                }
            }
        }
        drop(generation);
        drop(generation_hold);
        let bytes: acyclic_stream::OwnedBytes = match io {
            Some(io) => io.encode(&files, io.remaining()?.bytes_encoded, true)?,
            None => crate::contract::canonical_json_bytes(&files)?.into(),
        };
        let bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let file = stage_bytes(
            journal,
            input.operation_id,
            &key,
            (&bytes, "application/json"),
            false,
            io,
        )
        .await?;
        drop(bytes);
        drop(bytes_hold);
        drop(files);
        drop(key);
        scope.finish(file.logical_owned_bytes()?)?;
        Ok(Some(file))
    }

    pub(super) async fn publish_rewrite_files(
        &self,
        journal: &dyn ExecutionJournal,
        turn: (&TurnInput, u32),
        publication: RewritePublication<'_>,
    ) -> Result<()> {
        let context = self.original_context().ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Invalid,
                "context rewrite has no bound home",
            )
        })?;
        let destination = context.private_summaries(self.io)?;
        let binding = destination.bindings();
        let writer = binding.writer.as_deref().ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unauthorized,
                "context rewrite has no home writer",
            )
        })?;
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let io = scope.io();
        let (input, step) = turn;
        let files =
            retain_preparation(load_json::<RewriteFiles>(journal, publication.files, io).await?)?;
        if files.version != 1 {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "unsupported context rewrite intent",
            ));
        }
        let summary = publication.proof.summary.as_ref().ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "context rewrite has no selected summary",
            )
        })?;
        #[derive(Serialize)]
        struct Drops<'a> {
            version: u32,
            operation: OperationId,
            step: u32,
            source_digest: &'a [u8; 32],
            drop_through: u32,
            summary: &'a FileRef,
        }
        let drops = Drops {
            version: 1,
            operation: input.operation_id,
            step,
            source_digest: &publication.proof.source_digest,
            drop_through: summary.source_messages,
            summary: &summary.output,
        };
        let drops_bytes: acyclic_stream::OwnedBytes = match io {
            Some(io) => io.encode(&drops, io.remaining()?.bytes_encoded, true)?,
            None => crate::contract::canonical_json_bytes(&drops)?.into(),
        };
        let drops_hold = io
            .map(|io| io.guard_bytes(&drops_bytes))
            .transpose()?
            .flatten();
        let summary_bytes = journal.load(&summary.output, io)?.await?;
        let summary_hold = io
            .map(|io| io.guard_bytes(&summary_bytes))
            .transpose()?
            .flatten();
        let changes = [
            match &files.summary {
                Some(source) => ContentChange::UpdateExpected {
                    source,
                    bytes: &summary_bytes,
                },
                None => ContentChange::CreateAbsent {
                    path: "context/summary.md",
                    bytes: &summary_bytes,
                    media_type: "text/plain",
                    display_name: "summary.md",
                },
            },
            match &files.drops {
                Some(source) => ContentChange::UpdateExpected {
                    source,
                    bytes: &drops_bytes,
                },
                None => ContentChange::CreateAbsent {
                    path: "context/drops/selected.json",
                    bytes: &drops_bytes,
                    media_type: "application/json",
                    display_name: "selected.json",
                },
            },
        ];
        let digest = preparation_digest(
            &(
                "context-rewrite-files/v1",
                input.operation_id,
                step,
                publication.files,
                publication.proof,
            ),
            io,
        )?;
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        let operation = OperationId::from_bytes(identity);
        let access = ContentAccess {
            ceiling: Some(context.scope().grants()),
            io,
        };
        let (observed, observed_hold) = writer
            .observe_changes(
                &binding.volume,
                operation,
                self.stock.limits.render_bytes,
                access,
            )?
            .await?;
        let (receipt, receipt_hold) = match observed {
            Some(receipt) => (receipt, observed_hold),
            None => {
                drop(observed_hold);
                writer
                    .stage_changes(&binding.volume, operation, &changes, access)?
                    .await?
            }
        };
        {
            let mut published = receipt.files();
            for (path, bytes) in [
                ("context/summary.md", summary_bytes.as_ref()),
                ("context/drops/selected.json", drops_bytes.as_ref()),
            ] {
                let file = published.next().ok_or_else(|| {
                    stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "context rewrite publication is incomplete",
                    )
                })?;
                if file.volume() != &binding.volume || file.path() != path {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "context rewrite publication changed its destination",
                    ));
                }
                with_history_io(io, |work| file.descriptor().verify(bytes, work))?;
            }
            if published.next().is_some() || receipt.members.len() != 2 {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "context rewrite publication changed its members",
                ));
            }
        }
        drop(receipt);
        drop(receipt_hold);
        drop(summary_bytes);
        drop(summary_hold);
        drop(drops_bytes);
        drop(drops_hold);
        drop(files);
        scope.finish(0)?;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities,
        context::{Context, ContextPipeline},
        conversation::VolumeOperation,
        runtime::{Bindings, RuntimeScope, ToolPolicyDecision},
        test_support::{FixedToolPolicy, ScriptedModelProvider},
        tool::ToolRegistry,
    };

    #[tokio::test]
    async fn rewrite_files_are_conditional_atomic_and_replay_the_original_publication() -> Result<()>
    {
        for (existing, race) in [(false, false), (true, false), (true, true)] {
            let home = crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
            let storage =
                crate::filesystem::MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
            let binding = home.content_bindings();
            let writer = binding
                .writer
                .as_ref()
                .ok_or_else(|| Error::Invalid("home writer".into()))?;
            if existing {
                for (path, bytes, media) in [
                    (
                        "context/summary.md",
                        b"original summary".as_slice(),
                        "text/plain",
                    ),
                    (
                        "context/drops/selected.json",
                        b"{}".as_slice(),
                        "application/json",
                    ),
                ] {
                    writer
                        .stage(
                            OperationId::new(),
                            path,
                            bytes,
                            media,
                            path.rsplit('/').next().unwrap(),
                            ContentAccess::default(),
                        )?
                        .await?;
                }
            }
            let grants = Capabilities::new([
                "model:generate".to_owned(),
                binding
                    .volume
                    .capability(VolumeOperation::Write, &mut None)?,
                binding
                    .volume
                    .capability(VolumeOperation::Read, &mut None)?,
                binding.volume.directory_read_capability("", &mut None)?,
            ]);
            let scope = RuntimeScope::new(grants.clone(), Limits::default())?;
            let harness = Bindings {
                scope: scope.clone(),
                artifacts: Some(binding.clone()),
                ..Default::default()
            }
            .build()?;
            let context =
                harness.agent_context(&Capabilities::default(), storage.content_bindings())?;
            let input = TurnInput {
                operation_id: OperationId::new(),
                input: ModelContent::Text("current".into()),
                selected_context: None,
                max_steps: None,
            };
            let executor = StockExecutor::new(
                Model::new("test".into(), "context-files".into(), "1".into())?,
                Arc::new(ScriptedModelProvider::new([])),
                ContextPipeline::default(),
                ToolRegistry::default(),
            )
            .with_tool_authority(
                scope,
                Arc::new(FixedToolPolicy::new(ToolPolicyDecision::Allow)?),
            )?
            .with_task_context(&context, input.operation_id)?;
            let execution = StockExecution::ordinary(&executor);
            let journal = storage.journal();
            let files = execution
                .rewrite_files(journal.as_ref(), (&input, 0))
                .await?
                .ok_or_else(|| Error::Invalid("rewrite intent".into()))?;
            let source = Context {
                messages: vec![
                    ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("old question".into()),
                    },
                    ModelMessage {
                        role: ModelRole::Assistant,
                        content: ModelContent::Text("old answer".into()),
                    },
                    ModelMessage {
                        role: ModelRole::User,
                        content: input.input.clone(),
                    },
                ],
                current_input_index: Some(2),
                ..Default::default()
            };
            let digest = crate::context::prefix_digest(&source, 2, None)?;
            let summary = execution
                .publish_summary(
                    journal.as_ref(),
                    (input.operation_id, 0),
                    (2, digest),
                    ("selected-summary", b"selected"),
                )
                .await?;
            let (_, proof) = crate::context::compact_projection(&source, 2, Some(summary), None)?;
            if race {
                writer
                    .stage(
                        OperationId::new(),
                        "context/summary.md",
                        b"concurrent",
                        "text/plain",
                        "summary.md",
                        ContentAccess::default(),
                    )?
                    .await?;
            }
            let result = execution
                .publish_rewrite_files(
                    journal.as_ref(),
                    (&input, 0),
                    RewritePublication {
                        files: &files,
                        proof: &proof,
                    },
                )
                .await;
            let read = |path: &'static str| {
                binding.reader.read_private_path(
                    &binding.volume,
                    "context",
                    path,
                    None,
                    Some(65_536),
                    ContentAccess::default(),
                )
            };
            let (_, summary_bytes, _) = read("context/summary.md")?
                .await?
                .ok_or_else(|| Error::Invalid("published summary".into()))?;
            let (_, drops_bytes, _) = read("context/drops/selected.json")?
                .await?
                .ok_or_else(|| Error::Invalid("published drops".into()))?;
            if race {
                assert!(result.is_err());
                assert_eq!(summary_bytes.as_ref(), b"concurrent");
                assert_eq!(
                    drops_bytes.as_ref(),
                    b"{}",
                    "a conflict must not publish the other member"
                );
            } else {
                result?;
                assert_eq!(summary_bytes.as_ref(), b"selected");
                let drops: serde_json::Value = crate::contract::json_from_slice(&drops_bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert_eq!(drops["drop_through"], 2);
                assert_eq!(
                    drops["operation"],
                    serde_json::to_value(input.operation_id)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                );
                writer
                    .stage(
                        OperationId::new(),
                        "context/summary.md",
                        b"after",
                        "text/plain",
                        "summary.md",
                        ContentAccess::default(),
                    )?
                    .await?;
                let recovered = execution
                    .rewrite_files(journal.as_ref(), (&input, 0))
                    .await?
                    .ok_or_else(|| Error::Invalid("recovered rewrite intent".into()))?;
                assert_eq!(recovered, files);
                execution
                    .publish_rewrite_files(
                        journal.as_ref(),
                        (&input, 0),
                        RewritePublication {
                            files: &recovered,
                            proof: &proof,
                        },
                    )
                    .await?;
                let (_, bytes, _) = read("context/summary.md")?
                    .await?
                    .ok_or_else(|| Error::Invalid("later summary".into()))?;
                assert_eq!(
                    bytes.as_ref(),
                    b"after",
                    "replay cannot overwrite later edits"
                );
            }
        }
        Ok(())
    }
}
