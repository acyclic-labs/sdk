//! Offline schema-v1 observations over the committed journal and agent records.
//!
//! Admissions and completions are separate rows, identified by source stream
//! and sequence. A call joins on agent, operation, step, purpose and request
//! digest; tools join on agent, operation, step and call id. Do not sum both
//! admissions and completions. Unknown measurements remain null, including
//! timing recovered after a restart. No wall time or price is inferred.
//!
//! Each page is an independent shard. Keeping a single writer open across an
//! entire run would retain its growing file index; these functions retain only
//! one page. Consumers union shards and join the six tables offline. Export is
//! an explicit library call and adds no work to execution or agent delivery.

use super::{ExecutionEvent, ExecutionJournal, ModelPriceRecord, OperationId};
use crate::{Error, Result, agent};
pub use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{collections::HashMap, io::Write, sync::Arc};

/// Streaming W3C PROV-O projection over these same original observations.
#[cfg(feature = "provenance-export")]
pub mod provenance;

/// Bounded read-receipt projection; coverage is explicit rather than inferred.
#[cfg(feature = "provenance-export")]
pub mod read_set;

/// The six tables in metrics schema v1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    /// Adoption and role provenance.
    Agents,
    /// Started and settled turns.
    Turns,
    /// Original admitted and completed model calls, with reported usage.
    ModelCalls,
    /// Tool admissions, completions and settled failures.
    ToolCalls,
    /// Directed communication and supervision observations.
    Edges,
    /// Original agent and turn outcomes, without inferred failure labels.
    Outcomes,
}

impl Table {
    /// Stable file/table name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::Turns => "turns",
            Self::ModelCalls => "model_calls",
            Self::ToolCalls => "tool_calls",
            Self::Edges => "edges",
            Self::Outcomes => "outcomes",
        }
    }

    /// Version and table identity are also carried by Arrow schema metadata.
    pub fn schema(self) -> SchemaRef {
        macro_rules! schema {
            ($table:ident) => {{
                static VALUE: std::sync::LazyLock<SchemaRef> =
                    std::sync::LazyLock::new(|| Table::$table.build_schema());
                Arc::clone(&VALUE)
            }};
        }
        match self {
            Self::Agents => schema!(Agents),
            Self::Turns => schema!(Turns),
            Self::ModelCalls => schema!(ModelCalls),
            Self::ToolCalls => schema!(ToolCalls),
            Self::Edges => schema!(Edges),
            Self::Outcomes => schema!(Outcomes),
        }
    }

    fn build_schema(self) -> SchemaRef {
        use DataType::{Boolean, Float64, Int64, UInt32, UInt64, Utf8};
        let mut fields = vec![
            Field::new("version", UInt32, false),
            Field::new("agent_path", Utf8, false),
            Field::new("operation_id", Utf8, true),
            Field::new("source_stream", Utf8, false),
            Field::new("sequence", UInt64, false),
            Field::new("kind", Utf8, false),
        ];
        let columns = match self {
            Self::Agents => vec![("parent", Utf8), ("role", Utf8), ("profile_digest", Utf8)],
            Self::Turns => vec![
                ("inbox", UInt64),
                ("request_digest", Utf8),
                ("artifact", Utf8),
            ],
            Self::ModelCalls => vec![
                ("step", UInt32),
                ("purpose", Utf8),
                ("request_digest", Utf8),
                ("provider", Utf8),
                ("model", Utf8),
                ("sampling_known", Boolean),
                ("temperature", Float64),
                ("top_p", Float64),
                ("seed", Int64),
                ("max_output_tokens", UInt32),
                ("completed", Boolean),
                ("input_tokens", UInt64),
                ("cached_tokens", UInt64),
                ("cache_write_tokens", UInt64),
                ("output_tokens", UInt64),
                ("reasoning_tokens", UInt64),
                ("latency_ns", UInt64),
                ("ttft_ns", UInt64),
                ("provider_request_id", Utf8),
                ("system_fingerprint", Utf8),
                ("price_revision", Utf8),
                ("price_effective_at", UInt64),
                ("currency", Utf8),
                ("token_nano_units", UInt64),
                ("cache_known", Boolean),
                ("credential_scope", Utf8),
                ("profile_prefix_digest", Utf8),
                ("cache_ttl", Utf8),
                ("cache_ttl_seconds", UInt64),
                ("cache_prefix_digest", Utf8),
                ("cache_parent_digest", Utf8),
                ("cache_messages", UInt64),
                ("cache_savings_nano_units", DataType::Decimal128(38, 0)),
            ],
            Self::ToolCalls => vec![("step", UInt32), ("call_id", Utf8), ("artifact", Utf8)],
            Self::Edges => vec![
                ("source", Utf8),
                ("source_type", Utf8),
                ("target", Utf8),
                ("target_type", Utf8),
                ("wake", Boolean),
                ("artifact", Utf8),
            ],
            Self::Outcomes => vec![("artifact", Utf8)],
        };
        fields.extend(
            columns
                .into_iter()
                .map(|(name, data)| Field::new(name, data, true)),
        );
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([
                ("acyclic.metrics.version".into(), "1".into()),
                ("acyclic.metrics.table".into(), self.name().into()),
            ]),
        ))
    }
}

/// One independently writable, bounded table shard.
pub struct Batch {
    /// Table identity; also sealed in `data.schema()`.
    pub table: Table,
    /// Typed columns. Nullable values are unknown, never imputed zeroes.
    pub data: RecordBatch,
}

/// One bounded visit of a turn's original journal.
pub struct Page {
    /// Nonempty table shards, in a fixed table order.
    pub batches: Vec<Batch>,
    /// Exclusive cursor after every visited observation, including ignored ones.
    pub end: u64,
    /// Continuation if the page was full; absence is the current tail, not finality.
    pub next: Option<u64>,
}

// One typed column projection. Strings copy directly into Arrow once; there is
// no intermediate JSON serialization, tape, value tree or per-column row vector.
macro_rules! columns {
    (typed { $($array:ident: $ty:ty => [$($field:ident),*];)* } strings => [$($text:ident),*]) => {
        #[derive(Default)]
        struct Row {
            version: u32,
            agent_path: String,
            operation_id: Option<String>,
            source_stream: &'static str,
            sequence: u64,
            kind: String,
            purpose: Option<&'static str>,
            source_type: Option<&'static str>,
            target_type: Option<&'static str>,
            cache_savings_nano_units: Option<i128>,
            $($( $field: Option<$ty>, )*)*
            $( $text: Option<String>, )*
        }

        impl Row {
            fn column(name: &str, rows: &[Self]) -> Result<arrow_array::ArrayRef> {
                use arrow_array::*;
                Ok(match name {
                    "version" => Arc::new(UInt32Array::from_iter_values(rows.iter().map(|row| row.version))),
                    "agent_path" => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.agent_path.as_str()))),
                    "operation_id" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.operation_id.as_deref()))),
                    "source_stream" => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.source_stream))),
                    "sequence" => Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.sequence))),
                    "kind" => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.kind.as_str()))),
                    "purpose" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.purpose))),
                    "source_type" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.source_type))),
                    "target_type" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.target_type))),
                    "cache_savings_nano_units" => Arc::new(Decimal128Array::from_iter(rows.iter().map(|row| row.cache_savings_nano_units)).with_precision_and_scale(38, 0).map_err(export_error)?),
                    $($(stringify!($field) => Arc::new($array::from_iter(rows.iter().map(|row| row.$field))),)*)*
                    $(stringify!($text) => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.$text.as_deref()))),)*
                    _ => return Err(export_error("unknown metrics column")),
                })
            }
        }
    };
}

columns! {
    typed {
    UInt32Array: u32 => [step, max_output_tokens];
    UInt64Array: u64 => [inbox, input_tokens, cached_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
        latency_ns, ttft_ns, price_effective_at, token_nano_units, cache_ttl_seconds, cache_messages];
    Int64Array: i64 => [seed];
    Float64Array: f64 => [temperature, top_p];
    BooleanArray: bool => [sampling_known, completed, wake, cache_known];
    }
    strings => [parent, role, profile_digest, artifact, request_digest, provider, model, provider_request_id,
        system_fingerprint, price_revision, currency, call_id, source, target, credential_scope,
        profile_prefix_digest, cache_ttl, cache_prefix_digest, cache_parent_digest]
}

impl Row {
    fn new(
        agent: &str,
        operation: Option<OperationId>,
        sequence: u64,
        stream: &'static str,
        kind: &str,
    ) -> Self {
        Self {
            version: 1,
            agent_path: agent.into(),
            operation_id: operation.map(|id| id.to_string()),
            source_stream: stream,
            sequence,
            kind: kind.into(),
            ..Self::default()
        }
    }

    fn model_identity(
        &mut self,
        step: u32,
        purpose: super::ModelPurpose,
        request_digest: [u8; 32],
    ) {
        self.step = Some(step);
        self.purpose = Some(match purpose {
            super::ModelPurpose::Response => "response",
            super::ModelPurpose::Summary => "summary",
        });
        self.request_digest = Some(digest(request_digest));
    }
}

fn batch(table: Table, rows: &[Row]) -> Result<Batch> {
    let schema = table.schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| Row::column(field.name(), rows))
        .collect::<Result<Vec<_>>>()?;
    let data = RecordBatch::try_new(schema, columns).map_err(export_error)?;
    Ok(Batch { table, data })
}

fn export_error(error: impl std::fmt::Display) -> Error {
    Error::Storage(format!(
        "metrics projection failed: {error}; recovery: repeat the projection at the same source cursor"
    ))
}

fn artifact(file: &crate::conversation::FileRef) -> Result<String> {
    serde_json::to_string(file).map_err(export_error)
}

fn digest(value: [u8; 32]) -> String {
    blake3::Hash::from(value).to_hex().to_string()
}

/// Reads one page exactly once. Only referenced usage files are loaded;
/// requests, tool payloads and earlier pages are never reconstructed. Admissions
/// retain call identity with unknown measurements until the terminal observation.
/// `price` explicitly selects an immutable price for each call; it may select none.
/// All waits are the journal's replay/load result, including its typed failures.
pub async fn journal_page<'price>(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    agent_path: &str,
    after: u64,
    price: impl Fn(&super::ModelUsageRecord) -> Option<&'price ModelPriceRecord>,
) -> Result<Page> {
    const PAGE: u32 = 32;
    let records = journal.replay(operation, after, PAGE, None)?.await?;
    let end = after
        .checked_add(records.len() as u64)
        .ok_or_else(|| export_error("journal cursor overflow"))?;
    let next = (records.len() == PAGE as usize).then_some(end);
    let mut turns = Vec::new();
    let mut models = Vec::new();
    let mut tools = Vec::new();
    let mut edges = Vec::new();
    for record in records {
        if !matches!(
            record.event,
            ExecutionEvent::Started { .. }
                | ExecutionEvent::ModelStarted { .. }
                | ExecutionEvent::Model { usage: Some(_), .. }
                | ExecutionEvent::ModelInline { usage: Some(_), .. }
                | ExecutionEvent::ToolStarted { .. }
                | ExecutionEvent::ToolCompleted { .. }
                | ExecutionEvent::ToolFailed { .. }
        ) {
            continue;
        }
        let mut row = Row::new(agent_path, Some(operation), record.sequence, "journal", "");
        if let ExecutionEvent::ModelStarted {
            step,
            purpose,
            request_digest,
            ..
        } = &record.event
        {
            row.kind = "admitted".into();
            row.model_identity(*step, *purpose, *request_digest);
            row.completed = Some(false);
            models.push(row);
            continue;
        }
        if let Some(usage) =
            super::model_usage::project_record(journal, operation, agent_path, &record).await?
        {
            let selected = price(&usage);
            cache_edges(
                agent_path,
                operation,
                record.sequence,
                usage.cache,
                &mut edges,
            );
            models.push(model_call(row, usage, selected)?);
            continue;
        }
        match record.event {
            ExecutionEvent::Started { request_digest } => {
                row.kind = "started".into();
                row.request_digest = Some(digest(request_digest));
                turns.push(row);
            }
            ExecutionEvent::ToolStarted {
                step,
                call_id,
                invocation,
                ..
            } => {
                row.kind = "admitted".into();
                row.step = Some(step);
                row.call_id = Some(call_id);
                row.artifact = Some(artifact(&invocation)?);
                tools.push(row);
            }
            ExecutionEvent::ToolCompleted {
                step,
                call_id,
                result,
                ..
            } => {
                row.kind = "completed".into();
                row.step = Some(step);
                row.call_id = Some(call_id);
                row.artifact = Some(artifact(&result)?);
                tools.push(row);
            }
            ExecutionEvent::ToolFailed {
                step,
                call_id,
                reason,
                ..
            } => {
                row.kind = format!(
                    "failed:{}",
                    serde_json::to_value(reason)
                        .map_err(export_error)?
                        .as_str()
                        .unwrap_or("unknown")
                );
                row.step = Some(step);
                row.call_id = Some(call_id);
                tools.push(row);
            }
            _ => {}
        }
    }
    let mut batches = Vec::new();
    for (table, rows) in [
        (Table::Turns, turns),
        (Table::ModelCalls, models),
        (Table::ToolCalls, tools),
        (Table::Edges, edges),
    ] {
        if !rows.is_empty() {
            batches.push(batch(table, &rows)?);
        }
    }
    Ok(Page { batches, end, next })
}

fn model_call(
    mut row: Row,
    usage: super::ModelUsageRecord,
    price: Option<&ModelPriceRecord>,
) -> Result<Row> {
    row.cache_savings_nano_units = price
        .map(|price| usage.cache_savings(price))
        .transpose()
        .map_err(export_error)?
        .flatten();
    let projection = usage.estimate(price).map_err(export_error)?;
    let cost = projection.estimated_cost;
    row.price_revision = cost.price_revision.map(|value| value.into_owned());
    row.price_effective_at = cost.effective_at;
    row.currency = cost.currency.map(|value| value.into_owned());
    row.token_nano_units = cost.token_nano_units;
    row.cache_known = Some(usage.cache.is_some());
    if let Some(cache) = usage.cache {
        row.credential_scope = cache.credential_scope.map(digest);
        row.profile_prefix_digest = cache.profile_prefix_digest.map(digest);
        match cache.ttl {
            super::ModelCacheTtl::ProviderManaged => {
                row.cache_ttl = Some("provider_managed".into())
            }
            super::ModelCacheTtl::Explicit { seconds } => {
                row.cache_ttl = Some("explicit".into());
                row.cache_ttl_seconds = Some(seconds);
            }
        }
        if let Some(cut) = cache.breakpoint {
            row.cache_prefix_digest = Some(digest(cut.digest));
            row.cache_parent_digest = Some(digest(cut.parent_digest));
            row.cache_messages = Some(cut.messages);
        }
    }
    row.kind = "completed".into();
    row.model_identity(
        usage.dispatch.step,
        usage.purpose,
        usage.dispatch.request_digest,
    );
    row.provider = Some(usage.provider);
    row.model = Some(usage.model);
    row.sampling_known = Some(usage.sampling.is_some());
    if let Some(sampling) = usage.sampling {
        row.temperature = sampling.temperature;
        row.top_p = sampling.top_p;
        row.seed = sampling.seed;
    }
    row.max_output_tokens = usage.max_output_tokens;
    row.completed = Some(usage.completed);
    row.input_tokens = usage.tokens.input;
    row.cached_tokens = usage.tokens.cached;
    row.cache_write_tokens = usage.tokens.cache_write;
    row.output_tokens = usage.tokens.output;
    row.reasoning_tokens = usage.tokens.reasoning;
    row.latency_ns = usage.latency_ns;
    row.ttft_ns = usage.ttft_ns;
    row.provider_request_id = usage.provider_request_id;
    row.system_fingerprint = usage.system_fingerprint;
    Ok(row)
}

// At most two edges per original call. Unknown credentials forbid sharing
// joins, even when prefix bytes match. These edges prove prefix identity only;
// neither a requested TTL nor a read-token count proves residency at this cut.
fn cache_edges(
    agent: &str,
    operation: OperationId,
    sequence: u64,
    cache: Option<super::ModelCacheObservation>,
    rows: &mut Vec<Row>,
) {
    let Some(cache) = cache else { return };
    let Some(key) = cache.key() else { return };
    let Some(cut) = cache.breakpoint else { return };
    let scope = digest(key.credential_scope);
    let target = format!("cache:{scope}:{}", digest(key.prefix_digest));
    let mut row = Row::new(agent, Some(operation), sequence, "journal", "cache_prefix");
    row.source = Some(format!("cache:{scope}:{}", digest(cut.parent_digest)));
    row.source_type = Some("cache_prefix");
    row.target = Some(target.clone());
    row.target_type = Some("cache_prefix");
    rows.push(row);
    if let Some(profile) = cache.profile_prefix_digest {
        let mut row = Row::new(
            agent,
            Some(operation),
            sequence,
            "journal",
            "cache_profile_prefix",
        );
        row.source = Some(format!("cache-profile:{scope}:{}", digest(profile)));
        row.source_type = Some("profile_prefix");
        row.target = Some(target);
        row.target_type = Some("cache_prefix");
        rows.push(row);
    }
}

/// Projects one inbox record. Immutable waking payloads stay references; their
/// producer is resolved by the provenance projection, never from a current head.
pub fn inbox(agent_path: &str, sequence: u64, record: &agent::InboxRecord) -> Result<Vec<Batch>> {
    let mut row = Row::new(agent_path, None, sequence, "inbox", "");
    row.target_type = Some("agent");
    let mut batches = Vec::new();
    match record {
        agent::InboxRecord::ForkAdopted(adopted) => {
            row.kind = "adopted".into();
            row.parent = adopted.parent.as_ref().map(|path| path.as_str().into());
            row.role = adopted.profile.as_ref().map(|profile| profile.role.clone());
            row.profile_digest = adopted
                .profile
                .as_ref()
                .map(|profile| digest(profile.digest));
            batches.push(batch(Table::Agents, std::slice::from_ref(&row))?);
            if let Some(parent) = &adopted.parent {
                row.kind = "fork".into();
                row.source = Some(parent.as_str().into());
                row.source_type = Some("agent");
                row.target = Some(agent_path.into());
                batches.push(batch(Table::Edges, &[row])?);
            }
        }
        agent::InboxRecord::Message(message) => {
            row.kind = "message".into();
            row.source_type = Some(match &message.from {
                agent::Sender::Agent { .. } => "agent",
                agent::Sender::User { .. } => "user",
                agent::Sender::Host { .. } => "host",
            });
            row.source = Some(match &message.from {
                agent::Sender::Agent { path } => path.as_str().into(),
                agent::Sender::User { principal } => format!("user:{principal}"),
                agent::Sender::Host { host } => format!("host:{host}"),
            });
            row.target = Some(agent_path.into());
            row.artifact = Some(artifact(&message.body)?);
            batches.push(batch(Table::Edges, &[row])?);
        }
        agent::InboxRecord::Notice { notice, .. } => {
            row.kind = "notice".into();
            row.target = Some(agent_path.into());
            row.wake = Some(notice.wakes());
            match notice {
                agent::NoticePayload::Quiet(notice) => {
                    row.source = Some(notice.source.clone());
                    row.source_type = Some("producer");
                }
                agent::NoticePayload::Waking(file) => row.artifact = Some(artifact(file)?),
            }
            batches.push(batch(Table::Edges, &[row])?);
        }
        agent::InboxRecord::ChildDone(done) => {
            row.kind = "child_done".into();
            row.source = Some(done.child.as_str().into());
            row.source_type = Some("agent");
            row.target = Some(agent_path.into());
            batches.push(batch(Table::Edges, &[row])?);
        }
        _ => {}
    }
    Ok(batches)
}

/// Projects the original settled entry. `operation` is the turn driver's
/// original identity, retained by the caller; no new identity is invented here.
pub fn conversation(
    agent_path: &str,
    operation: OperationId,
    sequence: u64,
    entry: &agent::ConversationEntry,
) -> Result<Vec<Batch>> {
    let kind = match entry.outcome {
        agent::TurnOutcome::Completed => "completed",
        agent::TurnOutcome::Cancelled => "cancelled",
        agent::TurnOutcome::RestoreConflict => "restore_conflict",
    };
    let mut row = Row::new(agent_path, Some(operation), sequence, "conversation", kind);
    row.inbox = Some(entry.inbox);
    row.artifact = Some(artifact(&entry.output)?);
    Ok(vec![
        batch(Table::Turns, std::slice::from_ref(&row))?,
        batch(Table::Outcomes, &[row])?,
    ])
}

/// Projects one terminal status, preserving the original optional result ref.
pub fn status(
    agent_path: &str,
    sequence: u64,
    status: &agent::StatusRecord,
) -> Result<Option<Batch>> {
    let agent::Phase::Done { outcome } = &status.phase else {
        return Ok(None);
    };
    let mut row = Row::new(agent_path, None, sequence, "status", "");
    match outcome {
        agent::AgentOutcome::Succeeded { result } => {
            row.kind = "succeeded".into();
            row.artifact = result.as_deref().map(artifact).transpose()?;
        }
        agent::AgentOutcome::Failed { .. } => row.kind = "failed".into(),
        agent::AgentOutcome::Archived => row.kind = "archived".into(),
    }
    batch(Table::Outcomes, &[row]).map(Some)
}

/// Independent shard encoding; neither format retains state across pages.
#[derive(Clone, Copy, Debug)]
pub enum Format {
    /// Arrow IPC stream, directly readable by Arrow and DuckDB integrations.
    Arrow,
    /// Uncompressed Parquet with a single bounded batch.
    Parquet,
}

/// Writes and closes one page. Publication and durability belong to the caller's
/// volume commit. No fsync, retry, remote call or wait is introduced here.
pub fn write<W: Write + Send>(batch: &Batch, writer: W, format: Format) -> Result<()> {
    if batch.data.schema() != batch.table.schema() {
        return Err(export_error("batch schema differs from metrics v1 table"));
    }
    match format {
        Format::Arrow => {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(writer, &batch.data.schema())
                .map_err(export_error)?;
            writer.write(&batch.data).map_err(export_error)?;
            writer.finish().map_err(export_error)?;
        }
        Format::Parquet => {
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(writer, batch.data.schema(), None)
                    .map_err(export_error)?;
            writer.write(&batch.data).map_err(export_error)?;
            writer.close().map_err(export_error)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, StringArray, UInt64Array};

    #[test]
    fn shards_roundtrip_known_and_unknown_values_and_refuse_foreign_schemas() -> Result<()> {
        // Generated column values exercise the encoding, not a provider fixture.
        let mut row = Row::new(
            "agents/root",
            Some(OperationId::from_bytes([7; 16])),
            23,
            "journal",
            "completed",
        );
        row.input_tokens = Some(0);
        row.output_tokens = Some(u64::MAX);
        row.provider_request_id = Some("generated-column-identity".into());
        row.artifact = Some("generated-column-reference".into());
        row.cache_savings_nano_units = Some(-1_234_567_890_123_456_789_012_345_678_901);
        for table in [
            Table::Agents,
            Table::Turns,
            Table::ModelCalls,
            Table::ToolCalls,
            Table::Edges,
            Table::Outcomes,
        ] {
            let original = batch(table, std::slice::from_ref(&row))?;
            for format in [Format::Arrow, Format::Parquet] {
                let mut bytes = Vec::new();
                write(&original, &mut bytes, format)?;
                let decoded = match format {
                    Format::Arrow => {
                        arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                            .map_err(export_error)?
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(export_error)?
                    }
                    Format::Parquet => {
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                            bytes::Bytes::from(bytes),
                        )
                        .map_err(export_error)?
                        .build()
                        .map_err(export_error)?
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(export_error)?
                    }
                };
                assert_eq!(decoded.len(), 1);
                assert_eq!(decoded[0], original.data);
                if table == Table::ModelCalls {
                    let column = |name| decoded[0].column_by_name(name).expect("projected column");
                    assert_eq!(
                        column("input_tokens")
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(0),
                        0
                    );
                    assert_eq!(
                        column("output_tokens")
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(0),
                        u64::MAX
                    );
                    assert!(column("ttft_ns").is_null(0));
                    assert_eq!(
                        column("cache_savings_nano_units")
                            .as_any()
                            .downcast_ref::<arrow_array::Decimal128Array>()
                            .unwrap()
                            .value(0),
                        row.cache_savings_nano_units.unwrap()
                    );
                    assert_eq!(
                        column("provider_request_id")
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0),
                        "generated-column-identity"
                    );
                }
                let foreign = Batch {
                    table,
                    data: RecordBatch::try_new(
                        Arc::new(Schema::new(original.data.schema().fields().clone())),
                        original.data.columns().to_vec(),
                    )
                    .map_err(export_error)?,
                };
                let mut bytes = Vec::new();
                assert!(write(&foreign, &mut bytes, format).is_err());
                assert!(bytes.is_empty(), "schema refusal must precede publication");
                #[cfg(feature = "provenance-export")]
                {
                    assert!(provenance::write(&foreign, &mut bytes).is_err());
                    assert!(bytes.is_empty());
                }
            }
            for format in [Format::Arrow, Format::Parquet] {
                assert!(
                    write(&original, Refused, format).is_err(),
                    "a failed output must not be reported as a finished shard"
                );
            }
        }
        #[cfg(feature = "provenance-export")]
        assert!(provenance::write(&batch(Table::Agents, &[row])?, Refused).is_err());
        #[cfg(feature = "provenance-export")]
        assert!(read_set::Writer::new(Refused, "a", OperationId::from_bytes([1; 16]), 0).is_err());
        Ok(())
    }

    #[test]
    fn cache_edges_require_original_scope_and_never_identify_a_hit() {
        use super::super::{ModelCacheBreakpoint, ModelCacheObservation, ModelCacheTtl};
        let operation = OperationId::from_bytes([1; 16]);
        let mut cache = ModelCacheObservation {
            version: 1,
            credential_scope: None,
            profile_prefix_digest: Some([2; 32]),
            ttl: ModelCacheTtl::Explicit { seconds: 0 },
            breakpoint: Some(ModelCacheBreakpoint {
                digest: [3; 32],
                parent_digest: [4; 32],
                messages: 0,
            }),
        };
        let mut rows = Vec::new();
        cache_edges("a", operation, 1, Some(cache), &mut rows);
        assert!(rows.is_empty(), "unknown scope cannot prove sharing");
        cache.credential_scope = Some([5; 32]);
        cache_edges("a", operation, 1, Some(cache), &mut rows);
        assert_eq!(rows.len(), 2);
        let first = rows[0].target.clone();
        cache.credential_scope = Some([6; 32]);
        cache_edges("b", operation, 2, Some(cache), &mut rows);
        assert_ne!(
            first, rows[2].target,
            "equal bytes across accounts do not join"
        );
        assert!(
            rows.iter()
                .all(|row| row.kind == "cache_prefix" || row.kind == "cache_profile_prefix")
        );
        cache.version = 2;
        cache_edges("a", operation, 3, Some(cache), &mut rows);
        assert_eq!(
            rows.len(),
            4,
            "unsupported observations cannot prove sharing"
        );
    }

    struct Refused;
    impl Write for Refused {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "generated write refusal",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
