//! Original committed file-tool read receipts, without an execution observer.
//!
//! This is partial coverage: shell, context, hooks, searches and failed reads
//! are not reconstructed. Neither a request nor hydration proves an actual
//! read. A complete read set needs evidence from those original producers.
use super::{ExecutionEvent, ExecutionJournal, OperationId, export_error};
use crate::{
    Error, Result,
    executor::ExecutionRecordQuery,
    tool::{
        ToolInvocation,
        files::{FileReadSource, ReadPage},
    },
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, io::Write};

// Project only these original fields. The original terminal producer already
// validated the result; ignored arguments/text/media are never rebuilt in a
// second JSON value tree merely to read immutable identity.
#[derive(Deserialize)]
struct Admission<'a> {
    operation_id: OperationId,
    #[serde(borrow)]
    call_id: Cow<'a, str>,
    #[serde(borrow)]
    name: Cow<'a, str>,
}

#[derive(Deserialize)]
struct ResultProjection {
    value: ReadReceipt,
}

#[derive(Deserialize)]
struct ReadReceipt {
    file: FileReadSource,
    page: Option<ReadPage>,
}

/// One immutable source exposed by the original successful read-tool result.
/// Its identity does not claim that every byte was read or shown to the model.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    /// Original journal sequence of the tool's committed completion.
    pub sequence: u64,
    /// Original model step.
    pub step: u32,
    /// Original tool-call identity.
    pub call_id: String,
    /// Verified content reference or authored-generation selection.
    pub source: FileReadSource,
    /// Original returned source interval, when the tool reported one.
    /// Absence does not prove a whole-file read.
    pub page: Option<ReadPage>,
}

/// One bounded page, tied to the original source rather than a current path.
pub struct Page {
    agent: String,
    operation: OperationId,
    after: u64,
    end: u64,
    observations: Vec<Observation>,
}

impl Page {
    /// Exclusive continuation after every source record, including non-reads.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Original read receipts in source order; duplicates remain observations.
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }
}

/// Reads up to one page at a caller-pinned settled cut. The cut must come from
/// the turn's consumed-input fence; a short page or current tail is not finality.
/// Exact tool-chain lookup reuses the journal's authenticated index. An adapter
/// without that index refuses instead of scanning prior history per receipt.
/// All waits are the original replay/index/load results and their typed errors.
pub async fn page(
    journal: &dyn ExecutionJournal,
    agent: &str,
    operation: OperationId,
    after: u64,
    cut: u64,
) -> Result<Page> {
    let remaining = cut
        .checked_sub(after)
        .ok_or_else(|| export_error("read-set cursor exceeds its pinned cut"))?;
    let maximum = remaining.min(32) as u32;
    let records = if maximum == 0 {
        Vec::new()
    } else {
        journal.replay(operation, after, maximum, None)?.await?
    };
    if records.len() != maximum as usize {
        return Err(export_error("read-set pinned journal prefix is incomplete"));
    }
    let mut observations = Vec::new();
    for (offset, record) in records.into_iter().enumerate() {
        if record.operation_id != operation || record.sequence != after + offset as u64 + 1 {
            return Err(export_error(
                "read-set journal record changes its pinned identity",
            ));
        }
        let ExecutionEvent::ToolCompleted {
            step,
            call_id,
            result,
            ..
        } = record.event
        else {
            continue;
        };
        let select = |event: &ExecutionEvent| {
            matches!(event, ExecutionEvent::ToolStarted {
            step: admitted_step, call_id: admitted_call, ..
        } if *admitted_step == step && admitted_call == &call_id)
        };
        // A valid tool chain has one start and one terminal record. Three is
        // the existing structural allowance, not a history or execution cap.
        let query = ExecutionRecordQuery {
            operation,
            step,
            call_id: Some(&call_id),
            include_started: false,
            maximum_step_records: 3,
            maximum: 1,
            select: &select,
        };
        let indexed = journal
            .indexed_records(query, None)?
            .ok_or_else(index_required)?;
        let (_, admitted) = indexed.await?.ok_or_else(index_required)?;
        let [admission] = admitted.as_slice() else {
            return Err(export_error(
                "read-set tool completion lacks one original admission",
            ));
        };
        if admission.operation_id != operation || admission.sequence >= record.sequence {
            return Err(export_error(
                "read-set tool admission changes its original order or operation",
            ));
        }
        let ExecutionEvent::ToolStarted {
            step: admitted_step,
            call_id: admitted_call,
            invocation,
            ..
        } = &admission.event
        else {
            return Err(export_error("read-set index returned another observation"));
        };
        if *admitted_step != step || admitted_call != &call_id {
            return Err(export_error(
                "read-set index returned another call identity",
            ));
        }
        let read = {
            let bytes = journal.load(invocation, None)?.await?;
            let invocation: Admission<'_> = serde_json::from_slice(&bytes).map_err(export_error)?;
            if invocation.call_id != call_id
                || invocation.operation_id
                    != ToolInvocation::model_call_operation(operation, step, &call_id, None)?
            {
                return Err(export_error(
                    "read-set invocation changes its original call identity",
                ));
            }
            invocation.name == crate::tools::catalog::READ.name
        };
        if !read {
            continue;
        }
        let result: ResultProjection =
            serde_json::from_slice(&journal.load(&result, None)?.await?).map_err(export_error)?;
        let result = result.value;
        match &result.file {
            FileReadSource::Content(file) => file.validate(&mut None)?,
            FileReadSource::Authored(file) => file.validate(&mut None)?,
        }
        observations.push(Observation {
            sequence: record.sequence,
            step,
            call_id,
            source: result.file,
            page: result.page,
        });
    }
    Ok(Page {
        agent: agent.into(),
        operation,
        after,
        end: after + u64::from(maximum),
        observations,
    })
}

fn index_required() -> Error {
    Error::Unsupported("read-set projection requires the original tool admission index; recovery: run metrics::read_set::page with the original indexed execution journal".into())
}

/// One version-1 JSONL artifact per turn, streamed a page at a time. Publish the
/// returned writer's bytes once as a FileRef only after successful `finish`.
/// This writer never flushes, fsyncs, publishes, retries or waits. A failed page
/// permanently prevents successful finish; no partial artifact is certified.
pub struct Writer<W: Write> {
    writer: W,
    agent: String,
    operation: OperationId,
    cut: u64,
    next: Option<u64>,
}

fn line(writer: &mut impl Write, value: &impl Serialize) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(export_error)?;
    writer.write_all(b"\n").map_err(export_error)
}

impl<W: Write> Writer<W> {
    /// Starts one artifact at the original turn's settled journal cut.
    pub fn new(mut writer: W, agent: &str, operation: OperationId, cut: u64) -> Result<Self> {
        #[derive(Serialize)]
        struct Header<'a> {
            version: u32,
            kind: &'static str,
            agent_path: &'a str,
            operation: OperationId,
            cut: u64,
            coverage: &'static str,
        }
        line(
            &mut writer,
            &Header {
                version: 1,
                kind: "read_set",
                agent_path: agent,
                operation,
                cut,
                coverage: "committed_read_tool_results_only",
            },
        )?;
        Ok(Self {
            writer,
            agent: agent.into(),
            operation,
            cut,
            next: Some(0),
        })
    }

    /// Consumes each source page once. Foreign bindings, gaps and duplicate
    /// pages refuse before writing any of that page's bytes.
    pub fn append(&mut self, page: &Page) -> Result<()> {
        let next = self
            .next
            .take()
            .ok_or_else(|| export_error("read-set writer has a prior failed page"))?;
        if page.agent != self.agent
            || page.operation != self.operation
            || page.after != next
            || page.end > self.cut
            || page.end <= page.after
        {
            return Err(export_error(
                "read-set page changes its binding, cut or exclusive cursor",
            ));
        }
        for observation in &page.observations {
            line(&mut self.writer, observation)?;
        }
        self.next = Some(page.end);
        Ok(())
    }

    /// Certifies that every source position through the pinned cut was visited.
    /// This proves closure of the declared coverage, never universal read coverage.
    pub fn finish(mut self) -> Result<W> {
        if self.next != Some(self.cut) {
            return Err(export_error(
                "read-set writer has not consumed its complete pinned cut",
            ));
        }
        #[derive(Serialize)]
        struct End {
            kind: &'static str,
            end: u64,
        }
        line(
            &mut self.writer,
            &End {
                kind: "end",
                end: self.cut,
            },
        )?;
        Ok(self.writer)
    }
}

/// Projects one original read receipt into the existing edges table. The
/// enclosing operation identifies the activity that used this generation;
/// the source is an immutable reference, never a current path lookup.
pub fn project(
    agent: &str,
    operation: OperationId,
    observation: &Observation,
) -> Result<super::Batch> {
    let mut row = super::Row::new(
        agent,
        Some(operation),
        observation.sequence,
        "journal",
        "file_read",
    );
    row.source = Some(serde_json::to_string(&observation.source).map_err(export_error)?);
    row.source_type = Some("file_generation");
    row.target = Some(agent.into());
    row.target_type = Some("agent");
    super::batch(super::Table::Edges, &[row])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn committed_reads_keep_original_generations_after_the_path_changes() -> Result<()> {
        use crate::{
            AgentId, Capabilities,
            conversation::Limits,
            filesystem::MemoryHarnessStorage,
            runtime::{Bindings, RuntimeScope, ToolContext},
            tool::files,
        };
        use std::sync::Arc;

        let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
        let limits = Limits {
            file_bytes: 4096,
            render_bytes: 4096,
            ..Limits::default()
        };
        let task = Arc::new(
            Bindings {
                scope: RuntimeScope::new(storage.owner_scope().capabilities().clone(), limits)?,
                content: Some(storage.content_bindings()),
                ..Bindings::local()
            }
            .build()?
            .agent_context(&Capabilities::default(), storage.content_bindings())?,
        );
        let journal = storage.journal();
        let operation = OperationId::new();
        journal
            .append(
                operation,
                "start",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let tool = files::read()?;
        let mut sources = Vec::new();
        for (step, bytes) in [b"first".as_slice(), b"second".as_slice()]
            .into_iter()
            .enumerate()
        {
            let file = storage
                .stage(
                    OperationId::new(),
                    "same.txt",
                    bytes,
                    "text/plain",
                    "same.txt",
                )
                .await?;
            let step = step as u32;
            let call_id = format!("read-{step}");
            let invocation = ToolInvocation {
                operation_id: ToolInvocation::model_call_operation(
                    operation, step, &call_id, None,
                )?,
                call_id: call_id.clone(),
                name: tool.definition.name.clone(),
                arguments: serde_json::json!({"path": file.path(), "version": serde_json::to_string(&file).map_err(export_error)?}),
            };
            let reference = crate::executor::stage_json(
                journal.as_ref(),
                operation,
                &format!("invocation-{step}"),
                &invocation,
                None,
            )
            .await?;
            journal
                .append(
                    operation,
                    &format!("start-{step}"),
                    &ExecutionEvent::ToolStarted {
                        step,
                        call_id: call_id.clone(),
                        invocation: reference,
                        publication: None,
                    },
                    None,
                )?
                .await?;
            // The receipt below comes from the real built-in reader and storage,
            // never a hand-written provider or file-tool response fixture.
            let context = ToolContext::new(task.clone(), invocation.operation_id, &call_id)?;
            let result = tool
                .executor
                .execute(invocation.clone(), Some(context))
                .await?;
            let original: files::ReadFileResult =
                serde_json::from_value(result.value.clone()).map_err(export_error)?;
            assert_eq!(result.value["text"], std::str::from_utf8(bytes).unwrap());
            sources.push(original.file);
            let projection = tool.projection.project(&invocation, &result, None)?.0;
            let result = crate::executor::stage_json(
                journal.as_ref(),
                operation,
                &format!("result-{step}"),
                &result,
                None,
            )
            .await?;
            let projection = crate::executor::stage_json(
                journal.as_ref(),
                operation,
                &format!("projection-{step}"),
                &projection,
                None,
            )
            .await?;
            journal
                .append(
                    operation,
                    &format!("done-{step}"),
                    &ExecutionEvent::ToolCompleted {
                        step,
                        call_id,
                        result,
                        projection,
                    },
                    None,
                )?
                .await?;
        }
        assert_ne!(sources[0], sources[1]);
        let cut = 5;
        let observed = page(journal.as_ref(), "a", operation, 0, cut).await?;
        assert_eq!(observed.end(), cut);
        assert_eq!(observed.observations().len(), 2);
        for (step, observation) in observed.observations().iter().enumerate() {
            assert_eq!(observation.source, sources[step]);
            assert_eq!(observation.step, step as u32);
            assert_eq!(observation.sequence, 3 + 2 * step as u64);
        }
        assert!(
            page(journal.as_ref(), "a", operation, 0, cut + 1)
                .await
                .is_err()
        );
        let first = page(journal.as_ref(), "a", operation, 0, 3).await?;
        let second = page(journal.as_ref(), "a", operation, 3, cut).await?;
        let mut writer = Writer::new(Vec::new(), "a", operation, cut)?;
        writer.append(&first)?;
        writer.append(&second)?;
        let bytes = writer.finish()?;
        assert_eq!(
            bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
            4
        );
        Ok(())
    }

    #[test]
    fn incomplete_foreign_and_repeated_pages_cannot_certify_a_read_set() -> Result<()> {
        let operation = OperationId::from_bytes([1; 16]);
        let page = Page {
            agent: "a".into(),
            operation,
            after: 0,
            end: 1,
            observations: Vec::new(),
        };
        let mut writer = Writer::new(Vec::new(), "a", operation, 1)?;
        writer.append(&page)?;
        let bytes = writer.finish()?;
        let records = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice::<serde_json::Value>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(export_error)?;
        assert_eq!(records[0]["coverage"], "committed_read_tool_results_only");
        assert_eq!(records[1]["end"], 1);
        assert!(
            Writer::new(Vec::new(), "a", operation, 1)?
                .finish()
                .is_err()
        );
        let mut writer = Writer::new(Vec::new(), "a", operation, 1)?;
        writer.append(&page)?;
        assert!(writer.append(&page).is_err());
        assert!(writer.finish().is_err());
        let mut writer = Writer::new(Vec::new(), "b", operation, 1)?;
        assert!(writer.append(&page).is_err());
        assert!(writer.finish().is_err());
        let mut writer = Writer::new(Vec::new(), "a", operation, 2)?;
        writer.append(&page)?;
        assert!(writer.finish().is_err());
        Ok(())
    }
}
