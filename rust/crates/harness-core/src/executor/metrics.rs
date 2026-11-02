//! Offline versioned observations over the committed journal and agent records.
//!
//! Admissions and completions are separate rows, identified by source stream
//! and sequence. A call joins on agent, operation, step, purpose and request
//! digest; tools join on agent, operation, step and call id. Do not sum both
//! admissions and completions. Unknown measurements remain null, including
//! timing recovered after a restart. No wall time or price is inferred.
//!
//! Each page is an independent shard. Keeping a single writer open across an
//! entire run would retain its growing file index; these functions retain only
//! one page. Consumers union shards of the same schema version and table, then
//! join the six tables offline. Earlier layouts must be reprojected from their
//! original sources; positional or null-filled unions cannot certify this
//! layout's source and nullable ownership contracts. Export is
//! an explicit library call and adds no work to execution or agent delivery.

use super::{ExecutionEvent, ExecutionJournal, ModelPriceRecord, OperationId};
use crate::{Error, Result, agent};
pub use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::{collections::HashMap, io::Write, sync::Arc};

/// Arrow/Parquet layout identity, independent of usage/cache producer versions.
/// Version 4 retains creation provenance and requires original call terminals.
pub const VERSION: u32 = 4;

/// Streaming W3C PROV-O projection over these same original observations.
#[cfg(feature = "provenance-export")]
pub mod provenance;

/// Bounded read-receipt projection; coverage is explicit rather than inferred.
#[cfg(feature = "research")]
pub use super::read_set;

#[cfg(feature = "research")]
pub(super) fn read_observation(
    agent: &str,
    operation: OperationId,
    observation: &super::read_set::Observation,
) -> Result<Batch> {
    let mut row = Row::new(
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
    batch(Table::Edges, &[row])
}
/// Inspect evaluation logs, also accepted by Docent's Inspect loader.
#[cfg(feature = "research")]
pub mod inspect;

/// Original stream-record references, including references shared by forks.
#[cfg(feature = "research")]
pub mod stream_records;

/// Original settled logical Machine placement, without current configuration.
#[cfg(all(feature = "research", feature = "machines"))]
pub mod placement;

/// Caller-owned invocation receipts from the shared Actors call protocol.
#[cfg(feature = "research")]
pub mod calls;

/// The six tables in the current metrics schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Table {
    /// Adoption context and immutable template provenance.
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
            Field::new("agent_path", Utf8, true),
            Field::new("scope_path", Utf8, true),
            Field::new("operation_id", Utf8, true),
            Field::new("source_stream", Utf8, false),
            Field::new("sequence", UInt64, false),
            Field::new("kind", Utf8, false),
            Field::new("source_account", Utf8, true),
            Field::new("source_origin", Utf8, true),
            Field::new("source_commit_id", Utf8, true),
            Field::new("source_committed_at_micros", UInt64, true),
        ];
        let columns = match self {
            Self::Agents => vec![
                ("parent", Utf8),
                ("creation_context", Utf8),
                ("artifact", Utf8),
                ("binding_version", UInt32),
                ("target", Utf8),
                ("machine", Utf8),
                ("provider", Utf8),
                ("forked_from", Utf8),
                ("physical_host", Utf8),
                ("region", Utf8),
                ("core", UInt32),
                ("numa", UInt32),
                ("memory_bytes", UInt64),
                ("coverage", Utf8),
            ],
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
                ("replay_key", Utf8),
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
            Self::ToolCalls => vec![
                ("step", UInt32),
                ("call_id", Utf8),
                ("artifact", Utf8),
                ("replay_key", Utf8),
            ],
            Self::Edges => vec![
                ("source", Utf8),
                ("source_type", Utf8),
                ("target", Utf8),
                ("target_type", Utf8),
                ("wake", Boolean),
                ("artifact", Utf8),
                ("logical_value_bytes", UInt64),
                ("physical_copied_bytes", UInt64),
                ("physical_shared_bytes", UInt64),
                ("coverage", Utf8),
            ],
            Self::Outcomes => vec![
                ("artifact", Utf8),
                ("caller", Utf8),
                ("call_log", Utf8),
                ("outcome", Utf8),
                ("request_sequence", UInt64),
                ("request_commit_id", Utf8),
                ("terminal_sequence", UInt64),
                ("logical_value_bytes", UInt64),
                ("coverage", Utf8),
            ],
        };
        fields.extend(
            columns
                .into_iter()
                .map(|(name, data)| Field::new(name, data, true)),
        );
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([
                ("acyclic.metrics.version".into(), VERSION.to_string()),
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

impl Batch {
    fn validate_schema(&self) -> Result<()> {
        if self.data.schema() != self.table.schema() {
            return Err(export_error(format_args!(
                "batch schema differs from metrics v{VERSION} table; recovery: reproject the original source with the current metrics schema before writing"
            )));
        }
        Ok(())
    }
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
        struct Row<'a> {
            version: u32,
            agent_path: Option<String>,
            operation_id: Option<String>,
            source_stream: &'a str,
            sequence: u64,
            kind: String,
            purpose: Option<&'static str>,
            creation_context: Option<&'static str>,
            source_type: Option<&'static str>,
            target_type: Option<&'static str>,
            cache_savings_nano_units: Option<i128>,
            $($( $field: Option<$ty>, )*)*
            $( $text: Option<String>, )*
        }

        impl Row<'_> {
            fn column(name: &str, rows: &[Self]) -> Result<arrow_array::ArrayRef> {
                use arrow_array::*;
                Ok(match name {
                    "version" => Arc::new(UInt32Array::from_iter_values(rows.iter().map(|row| row.version))),
                    "agent_path" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.agent_path.as_deref()))),
                    "operation_id" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.operation_id.as_deref()))),
                    "source_stream" => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.source_stream))),
                    "sequence" => Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.sequence))),
                    "kind" => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.kind.as_str()))),
                    "purpose" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.purpose))),
                    "creation_context" => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.creation_context))),
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
    UInt32Array: u32 => [step, max_output_tokens, binding_version, core, numa];
    UInt64Array: u64 => [inbox, input_tokens, cached_tokens, cache_write_tokens, output_tokens, reasoning_tokens,
        latency_ns, ttft_ns, price_effective_at, token_nano_units, cache_ttl_seconds, cache_messages,
        logical_value_bytes, physical_copied_bytes, physical_shared_bytes,
        source_committed_at_micros, memory_bytes, request_sequence, terminal_sequence];
    Int64Array: i64 => [seed];
    Float64Array: f64 => [temperature, top_p];
    BooleanArray: bool => [sampling_known, completed, wake, cache_known];
    }
    strings => [parent, artifact, request_digest, replay_key, provider, model, provider_request_id,
        system_fingerprint, price_revision, currency, call_id, source, target, credential_scope,
        profile_prefix_digest, cache_ttl, cache_prefix_digest, cache_parent_digest,
        source_account, source_origin, source_commit_id, coverage,
        machine, forked_from, physical_host, region, scope_path,
        caller, call_log, outcome, request_commit_id]
}

impl<'a> Row<'a> {
    #[cfg(feature = "research")]
    fn stream(
        agent: Option<&str>,
        account: &acyclic_auth::addressing::AccountId,
        selected: &'a acyclic_stream::StreamPath,
        record: &acyclic_stream::Record,
        kind: &str,
    ) -> Self {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
        let mut row = Self::base(None, record.sequence, selected.as_str(), kind);
        row.agent_path = agent.map(str::to_owned);
        row.source_account = Some(URL_SAFE_NO_PAD.encode(account.as_bytes()));
        row.source_origin = Some(record.origin.as_str().into());
        row.source_commit_id = Some(digest(*record.commit_id.as_bytes()));
        row.source_committed_at_micros = Some(record.committed_at_micros);
        row
    }

    fn new(
        agent: &str,
        operation: Option<OperationId>,
        sequence: u64,
        stream: &'a str,
        kind: &str,
    ) -> Self {
        let mut row = Self::base(operation, sequence, stream, kind);
        row.agent_path = Some(agent.into());
        row
    }

    fn base(operation: Option<OperationId>, sequence: u64, stream: &'a str, kind: &str) -> Self {
        Self {
            version: VERSION,
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

fn batch(table: Table, rows: &[Row<'_>]) -> Result<Batch> {
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

/// Reads one page exactly once. Referenced usage and admitted requests are loaded;
/// tool payloads and earlier journal pages are never reconstructed. Admissions
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
    let (end, next) = super::event_page::page_continuation(after, records.len(), PAGE)?;
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
            row.replay_key = Some(admitted_replay_key(journal, &record.event).await?);
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
            row.replay_key =
                step_replay_key(journal, operation, usage.dispatch.step, usage.purpose).await?;
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
                row.replay_key =
                    step_replay_key(journal, operation, step, super::ModelPurpose::Response)
                        .await?;
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
                row.replay_key =
                    step_replay_key(journal, operation, step, super::ModelPurpose::Response)
                        .await?;
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
                row.replay_key =
                    step_replay_key(journal, operation, step, super::ModelPurpose::Response)
                        .await?;
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

async fn admitted_replay_key(
    journal: &dyn ExecutionJournal,
    event: &ExecutionEvent,
) -> Result<String> {
    let request = super::ExecutionPayloadField::admitted_request(journal, event).await?;
    crate::model::replay_key(request.bytes()).map(digest)
}

// One original indexed admission, never a history search or retained run index.
// Custom journals without this capability leave the join key unknown; their
// ModelStarted row still supplies it for an offline join on the original step.
async fn step_replay_key(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    step: u32,
    purpose: super::ModelPurpose,
) -> Result<Option<String>> {
    let select = |event: &ExecutionEvent| {
        matches!(event,
        ExecutionEvent::ModelStarted { purpose: recorded, .. } if *recorded == purpose)
    };
    let Some(records) = journal.indexed_records(
        super::ExecutionRecordQuery {
            operation,
            step,
            call_id: None,
            include_started: false,
            maximum_step_records: super::ExecutionRecordQuery::step_bound(
                crate::conversation::Limits::default(),
            ),
            maximum: 1,
            select: &select,
        },
        None,
    )?
    else {
        return Ok(None);
    };
    let Some((_, records)) = records.await? else {
        return Ok(None);
    };
    if records.len() != 1 {
        return Err(export_error(
            "step replay key requires one original model admission",
        ));
    }
    let record = &records[0];
    if record.operation_id != operation
        || !matches!(&record.event,
        ExecutionEvent::ModelStarted { step: recorded, purpose: recorded_purpose, .. }
        if *recorded == step && *recorded_purpose == purpose)
    {
        return Err(export_error(
            "step replay key admission has a foreign binding",
        ));
    }
    admitted_replay_key(journal, &record.event).await.map(Some)
}

fn model_call<'a>(
    mut row: Row<'a>,
    usage: super::ModelUsageRecord,
    price: Option<&ModelPriceRecord>,
) -> Result<Row<'a>> {
    let (projection, savings) = usage
        .estimate_with_cache_savings(price)
        .map_err(export_error)?;
    row.cache_savings_nano_units = savings;
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
    rows: &mut Vec<Row<'_>>,
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
            if let Some(creation) = &adopted.profile {
                row.creation_context = Some(match creation.context {
                    agent::SpawnContext::TopLevel => "top_level",
                    agent::SpawnContext::Fresh => "fresh",
                    agent::SpawnContext::Fork => "fork",
                });
                row.artifact = creation.template.as_ref().map(artifact).transpose()?;
            }
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
    batch.validate_schema()?;
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
    fn adoption_preserves_the_original_template_reference() -> Result<()> {
        use crate::conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef};

        let owner = crate::AgentId::from_bytes([5; 16]);
        let agent = agent::AgentId::from_bytes([5; 16]);
        let path = agent::AgentPath::root(&agent);
        for version in [None, Some("first-cut"), Some("later-cut")] {
            let template = version
                .map(|version| {
                    FileRef::new(
                        VolumeRef::new(
                            crate::resources::ProviderRef::new(
                                "acyclic".into(),
                                "filesystem".into(),
                                "2".into(),
                                &mut None,
                            )?,
                            "templates".into(),
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(owner),
                            &mut None,
                        )?,
                        "worker.md".into(),
                        version.into(),
                        FileDescriptor::from_bytes(
                            b"original template",
                            "text/markdown",
                            &mut None,
                        )?,
                        "worker.md".into(),
                        &mut None,
                    )
                })
                .transpose()?;
            let record = agent::InboxRecord::ForkAdopted(Box::new(agent::ForkAdopted {
                agent: agent.clone(),
                parent: None,
                profile: Some(agent::CreationConfig {
                    context: agent::SpawnContext::TopLevel,
                    template: template.clone(),
                }),
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes: Vec::new(),
                brief: None,
                policy: None,
            }));
            let projected = inbox(path.as_str(), 0, &record)?;
            let column = projected[0]
                .data
                .column_by_name("artifact")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            if let Some(template) = &template {
                assert_eq!(
                    serde_json::from_str::<FileRef>(column.value(0)).map_err(export_error)?,
                    *template
                );
            } else {
                assert!(
                    column.is_null(0),
                    "absent templates cannot acquire an inferred reference"
                );
            }
            #[cfg(feature = "provenance-export")]
            {
                let mut bytes = Vec::new();
                provenance::write(&projected[0], &mut bytes)?;
                let triples = oxttl::NTriplesParser::new()
                    .for_reader(bytes.as_slice())
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(export_error)?;
                let references: Vec<_> = triples
                    .iter()
                    .filter(|triple| {
                        triple.predicate.as_str() == "urn:acyclic:research:fileReference"
                    })
                    .collect();
                if let Some(template) = &template {
                    assert_eq!(references.len(), 1);
                    assert!(matches!(&references[0].object, oxrdf::Term::Literal(value)
                        if value.value() == artifact(template)?));
                } else {
                    assert!(references.is_empty());
                }
            }
        }
        Ok(())
    }

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
                // A saved earlier layout cannot acquire the new ownership
                // contract by copying its columns or erasing its version.
                for saved_version in [None, Some("1"), Some("2"), Some("3")] {
                    let mut metadata = original.data.schema().metadata().clone();
                    match saved_version {
                        Some(version) => {
                            metadata.insert("acyclic.metrics.version".into(), version.into());
                        }
                        None => metadata.clear(),
                    }
                    let foreign = Batch {
                        table,
                        data: RecordBatch::try_new(
                            Arc::new(Schema::new_with_metadata(
                                original.data.schema().fields().clone(),
                                metadata,
                            )),
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
        #[cfg(feature = "research")]
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
