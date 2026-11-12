//! Immutable request segments shared by dispatch and context projections.
use super::*;

mod digests;
mod projection;

pub(crate) use projection::load_projection;

pub(crate) const REQUEST_MEDIA: &str = "application/vnd.acyclic.request-segments+json";

#[derive(Deserialize)]
pub(super) struct UsageModel {
    pub provider: String,
    pub name: String,
}

/// Read call parameters from the original admitted request/header. Serde skips
/// context and tool schemas instead of allocating them again.
#[derive(Deserialize)]
pub(super) struct UsageRequest {
    pub(super) model: UsageModel,
    #[serde(default)]
    pub(super) sampling: crate::model::ModelSampling,
}

pub(super) async fn usage_request(
    journal: &dyn ExecutionJournal,
    request: &FileRef,
) -> Result<UsageRequest> {
    #[derive(Deserialize)]
    struct Manifest {
        header: FileRef,
    }
    let bytes = journal.load(request, None)?.await?;
    let header = if request.descriptor().media_type() == REQUEST_MEDIA {
        admit_manifest(request, &bytes, None)?;
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Storage("usage request manifest is malformed".into()))?;
        journal.load(&manifest.header, None)?.await?
    } else {
        bytes
    };
    let header: UsageRequest = serde_json::from_slice(&header)
        .map_err(|_| Error::Storage("usage request header is malformed".into()))?;
    Ok(header)
}

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32"), not(loom)))]
mod metadata_tests;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestManifest {
    // Canonical JSON sorts object keys. Reserve a key that sorts before every
    // payload field so the version can be admitted before decoding the body.
    #[serde(rename = "$version")]
    version: u32,
    header: FileRef,
    header_digest: [u8; 32],
    messages: MessagePrefix,
    request_digest: [u8; 32],
    admitted_limits: Limits,
    proof: Option<([u8; 32], [u8; 32])>,
}

/// Admission is monotone in these bounds. Control-loop limits do not affect
/// `ModelRequest` validation and therefore are not part of this comparison.
pub(crate) fn limits_admit(admitted: Limits, current: Limits) -> bool {
    admitted.file_bytes <= current.file_bytes
        && current
            .path_bytes
            .is_none_or(|maximum| admitted.path_bytes.is_some_and(|limit| limit <= maximum))
        && current
            .attachments
            .is_none_or(|maximum| admitted.attachments.is_some_and(|limit| limit <= maximum))
        && admitted.render_bytes <= current.render_bytes
        && current.context_messages.is_none_or(|maximum| {
            admitted
                .context_messages
                .is_some_and(|limit| limit <= maximum)
        })
}

impl RequestManifest {
    fn payload_digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        preparation_digest(
            &(
                self.version,
                &self.header,
                self.header_digest,
                &self.messages,
                self.request_digest,
                self.admitted_limits,
            ),
            io,
        )
    }

    async fn attestation(
        &self,
        verifier: &crate::core::AuthorityVerifier,
        io: Option<&PreparationIo>,
    ) -> Result<([u8; 32], [u8; 32])> {
        let digest = self.payload_digest(io)?;
        let signature = with_history_io(io, |work| {
            Ok::<_, Error>(verifier.model_request_proof(digest, work))
        })?
        .await?;
        let mut first = [0; 32];
        let mut second = [0; 32];
        first.copy_from_slice(&signature[..32]);
        second.copy_from_slice(&signature[32..]);
        Ok((first, second))
    }

    pub(crate) fn authenticates(
        &self,
        verifier: &crate::core::AuthorityVerifier,
        limits: Limits,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        if self.request_digest != digest || !limits_admit(self.admitted_limits, limits) {
            return Ok(false);
        }
        let Some((first, second)) = self.proof else {
            return Ok(false);
        };
        let mut signature = [0; 64];
        signature[..32].copy_from_slice(&first);
        signature[32..].copy_from_slice(&second);
        let digest = self.payload_digest(io)?;
        with_history_io(io, |work| {
            verifier.authenticates_model_request(digest, &signature, work)
        })
    }
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct RequestHeader {
    version: u32,
    summary: bool,
    compose_tools: bool,
    model: Model,
    tools: Vec<crate::tool::ToolDefinition>,
    #[serde(
        default,
        skip_serializing_if = "crate::model::ModelSampling::is_default"
    )]
    sampling: crate::model::ModelSampling,
}

#[derive(Serialize)]
struct HeaderView<'a> {
    version: u32,
    summary: bool,
    compose_tools: bool,
    model: &'a Model,
    tools: &'a [crate::tool::ToolDefinition],
    #[serde(
        default,
        skip_serializing_if = "crate::model::ModelSampling::is_default"
    )]
    sampling: crate::model::ModelSampling,
}

impl<'a> HeaderView<'a> {
    fn new(request: &'a ModelRequest) -> Self {
        Self {
            version: 1,
            summary: request.summary,
            compose_tools: request.compose_tools,
            model: &request.model,
            tools: &request.tools,
            sampling: request.sampling,
        }
    }
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ProjectionRecord {
    #[serde(rename = "$version")]
    version: u32,
    request: FileRef,
    projection: ProjectionMetadata,
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ProjectionMetadata {
    context: ContextMetadata,
    steering: Option<SteeringPrefix>,
    canonical: Option<FileRef>,
    checkpoint: Option<FileRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    working: Option<FileRef>,
    accounting: Option<ContextAccounting>,
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct ContextMetadata {
    metadata: std::collections::BTreeMap<String, FileRef>,
    current_input_index: Option<u32>,
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct SteeringPrefix {
    through: u64,
    messages: Option<MessagePrefix>,
}

#[derive(Serialize)]
struct ProjectionRecordView<'a> {
    #[serde(rename = "$version")]
    version: u32,
    request: &'a FileRef,
    projection: ProjectionView<'a>,
}

impl PreparationRecord for ProjectionRecord {}

async fn load_projection_record(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<ProjectionRecord> {
    let bytes = super::payload::load_json_bytes(journal, reference, io).await?;
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    let unsupported = || {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "unsupported context projection format",
        )
    };
    acyclic_auth::format::header(&bytes, b"{\"$version\":", b"2,", unsupported, unsupported)?;
    let record = retain_preparation(super::payload::decode_json::<ProjectionRecord>(&bytes, io)?)?;
    if record.version != 2 {
        return Err(invalid(io));
    }
    Ok(record)
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct MessagePrefix {
    reference: FileRef,
    count: u32,
    content_digest: [u8; 32],
    chain_digest: [u8; 32],
}

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct MessageSegment {
    parent: Option<MessagePrefix>,
    suffix: Vec<ModelMessage>,
}

#[derive(Serialize)]
struct SegmentView<'a> {
    parent: Option<&'a MessagePrefix>,
    suffix: &'a [ModelMessage],
}

#[derive(Serialize)]
struct ProjectionView<'a> {
    context: ContextView<'a>,
    steering: &'a Option<SteeringPrefix>,
    canonical: &'a Option<FileRef>,
    checkpoint: &'a Option<FileRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    working: &'a Option<FileRef>,
    accounting: Option<&'a ContextAccounting>,
}

#[derive(Serialize)]
struct ContextView<'a> {
    metadata: &'a std::collections::BTreeMap<String, FileRef>,
    current_input_index: Option<u32>,
}

impl PreparationRecord for RequestManifest {
    fn diagnostic_label_bytes() -> u64 {
        // This closed record cannot reach model/tool/workflow diagnostics.
        // Fund all labels in its own DTO graph instead of those unrelated
        // schemas; caller-controlled unexpected data is funded separately.
        concat!(
            "RequestManifest $version header header_digest messages request_digest admitted_limits proof ",
            "MessagePrefix reference count content_digest chain_digest ",
            "FileRef volume path version descriptor display_name ",
            "VolumeRef provider id class owner VolumeOwner kind id project agent session ",
            "VolumeClass project agent_private session_shared ",
            "ProviderRef ProviderRefWire namespace family version ",
            "FileDescriptor sha256 byte_length media_type ",
            "Limits file_bytes path_bytes attachments render_bytes ",
            "model_events_per_step tool_calls_per_step context_messages ",
            "a sequence of length 2 a sequence of length 32 u8 u32 u64 usize UUID ",
            "provider identity is invalid file or volume label is invalid ",
            "volume-relative path is invalid file byte length exceeds cross-language precision ",
            "media type is invalid a hyphenated UUID string"
        )
        .len() as u64
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            self.header.logical_owned_bytes()?,
            self.messages.reference.logical_owned_bytes()?,
        ])
        .map(Some)
    }
}
impl PreparationRecord for RequestHeader {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ModelRequest
    }
}
impl PreparationRecord for MessageSegment {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::MessageSegment
    }

    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        let bytes = crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            crate::core::owned_buffer::<ModelMessage>(self.suffix.capacity())?,
            self.parent
                .as_ref()
                .map_or(Ok(0), |parent| parent.reference.logical_owned_bytes())?,
        ])?;
        self.suffix
            .iter()
            .try_fold(bytes, |bytes, message| {
                crate::core::owned_sum([bytes, message.content.retained_payload_bytes()?])
            })
            .map(Some)
    }
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    stock_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "invalid segmented model request",
    )
}

fn chain_digest(
    parent: Option<&MessagePrefix>,
    suffix: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    // Fixed-size chaining commits to the segment's exact bytes and the entire
    // preceding chain without re-hashing any historical payload.
    let mut frame = [0; 65];
    if let Some(parent) = parent {
        frame[0] = 1;
        frame[1..33].copy_from_slice(&parent.chain_digest);
    }
    frame[33..].copy_from_slice(suffix.descriptor().sha256());
    crate::model::hash_bytes(&frame, io)
}

pub(crate) async fn load_manifest(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<RequestManifest> {
    validate_journal(journal, io)?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != REQUEST_MEDIA
    {
        return Err(invalid(io));
    }
    let bytes = journal.load(reference, io)?.await?;
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    decode_manifest(reference, &bytes, io)
}

/// Admit the immutable format before either ordinary loading or cached proof
/// verification can decode its payload. A valid proof does not admit a version.
pub(crate) fn decode_manifest(
    reference: &FileRef,
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<RequestManifest> {
    admit_manifest(reference, bytes, io)?;
    retain_preparation(super::payload::decode_json::<RequestManifest>(bytes, io)?)
}

fn admit_manifest(reference: &FileRef, bytes: &[u8], io: Option<&PreparationIo>) -> Result<()> {
    with_history_io(io, |work| reference.descriptor().verify(bytes, work))?;
    acyclic_auth::format::header(
        bytes,
        b"{\"$version\":",
        b"3,",
        || unsupported_manifest(io),
        || unsupported_manifest(io),
    )
    .map(|_| ())
}

fn unsupported_manifest(io: Option<&PreparationIo>) -> Error {
    stock_boundary(
        io,
        crate::wire::ErrorCode::Unsupported,
        "unsupported segmented model request format",
    )
}

async fn stage_manifest(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    manifest: &RequestManifest,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    validate_journal(journal, io)?;
    let _hold;
    let bytes: acyclic_stream::OwnedBytes = match io {
        Some(io) => {
            let bytes = io.encode(manifest, io.remaining()?.bytes_encoded, true)?;
            _hold = io.guard_bytes(&bytes)?;
            bytes
        }
        _ => crate::contract::canonical_json_bytes(manifest)?.into(),
    };
    let reference = journal
        .stage(operation, key, &bytes, REQUEST_MEDIA, io)?
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != REQUEST_MEDIA
    {
        return Err(invalid(io));
    }
    with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
    Ok(reference)
}

struct RequestStage<'a> {
    operation: OperationId,
    key: &'a str,
    prepared: &'a crate::model::PreparedModelRequest,
    previous: Option<&'a FileRef>,
}

async fn previous_manifest(
    journal: &dyn ExecutionJournal,
    previous: Option<&FileRef>,
    limits: Limits,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<Option<RequestManifest>> {
    let _ = verifier;
    Ok(match previous {
        Some(reference) if reference.descriptor().media_type() == REQUEST_MEDIA => {
            let manifest = load_manifest(journal, reference, io).await?;
            let authenticated = verifier
                .map(|verifier| {
                    manifest.authenticates(verifier, limits, manifest.request_digest, io)
                })
                .transpose()?
                .unwrap_or(false);
            if !authenticated {
                // An untrusted descriptor is not evidence that its claimed
                // prefix was admitted. Reconstruct and validate before reuse.
                load_request(journal, reference, limits, io).await?;
            }
            Some(manifest)
        }
        _ => None,
    })
}

async fn stage_request_inner(
    journal: &dyn ExecutionJournal,
    staging: RequestStage<'_>,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let RequestStage {
        operation,
        key,
        prepared,
        previous,
    } = staging;
    let request = prepared.request();
    let header = HeaderView::new(request);
    let header_digest = preparation_digest(&header, io)?;
    let previous =
        previous_manifest(journal, previous, prepared.admitted_limits(), verifier, io).await?;
    let header = match &previous {
        Some(previous) if previous.header_digest == header_digest => {
            crate::model::clone_file(&previous.header, io)?
        }
        _ => {
            let key = with_history_io(io, |work| {
                preparation_format(format_args!("{key}:header"), work)
            })?;
            stage_json(journal, operation, &key, &header, io).await?
        }
    };
    let messages = stage_messages(
        journal,
        operation,
        key,
        prepared,
        previous.map(|manifest| manifest.messages),
        io,
    )
    .await?;
    let manifest = RequestManifest {
        version: 3,
        header,
        header_digest,
        messages,
        request_digest: prepared.request_digest(),
        admitted_limits: prepared.admitted_limits(),
        proof: None,
    };
    let manifest = if let Some(verifier) = verifier {
        let proof = Some(manifest.attestation(verifier, io).await?);
        RequestManifest { proof, ..manifest }
    } else {
        manifest
    };
    stage_manifest(journal, operation, key, &manifest, io).await
}

async fn stage_messages(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
    previous: Option<MessagePrefix>,
    io: Option<&PreparationIo>,
) -> Result<MessagePrefix> {
    let messages = &prepared.request().messages;
    let previous_count = previous.as_ref().map(|previous| previous.count as usize);
    let (content_digest, prefix_digest) =
        match prepared.cached_message_digests(previous_count, io)? {
            Some(digests) => digests,
            None => digests::messages(messages, previous_count, io)?,
        };
    stage_message_delta(
        journal,
        operation,
        key,
        (messages, (content_digest, prefix_digest)),
        previous,
        io,
    )
    .await
}

async fn stage_message_delta(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    (messages, (content_digest, prefix_digest)): (&[ModelMessage], ([u8; 32], Option<[u8; 32]>)),
    previous: Option<MessagePrefix>,
    io: Option<&PreparationIo>,
) -> Result<MessagePrefix> {
    let parent = previous.filter(|previous| Some(previous.content_digest) == prefix_digest);
    let count = u32::try_from(messages.len()).map_err(|_| invalid(io))?;
    let messages = match parent {
        Some(parent) if parent.count == count => parent,
        parent => {
            let parent_count = parent.as_ref().map_or(0, |parent| parent.count as usize);
            let suffix = messages.get(parent_count..).ok_or_else(|| invalid(io))?;
            let view = SegmentView {
                parent: parent.as_ref(),
                suffix,
            };
            let segment_key = with_history_io(io, |work| {
                preparation_format(format_args!("{key}:messages"), work)
            })?;
            let reference = stage_json(journal, operation, &segment_key, &view, io).await?;
            MessagePrefix {
                chain_digest: chain_digest(parent.as_ref(), &reference, io)?,
                reference,
                count,
                content_digest,
            }
        }
    };
    Ok(messages)
}

async fn load_request_inner(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::PreparedModelRequest> {
    let manifest = load_manifest(journal, reference, io).await?;
    if manifest.messages.count == 0
        || limits
            .context_messages
            .is_some_and(|maximum| manifest.messages.count as usize > maximum.get())
    {
        return Err(invalid(io));
    }
    let header =
        retain_preparation(load_json::<RequestHeader>(journal, &manifest.header, io).await?)?;
    if header.version != 1 || preparation_digest(&header, io)? != manifest.header_digest {
        return Err(invalid(io));
    }
    let messages = load_messages(journal, manifest.messages, io).await?;
    let prepared = crate::model::PreparedModelRequest::prepare(
        ModelRequest {
            summary: header.summary,
            compose_tools: header.compose_tools,
            model: header.model,
            tools: header.tools,
            sampling: header.sampling,
            messages,
        },
        limits,
        io,
    )?;
    if prepared.request_digest() != manifest.request_digest {
        return Err(invalid(io));
    }
    Ok(prepared)
}

async fn load_messages(
    journal: &dyn ExecutionJournal,
    prefix: MessagePrefix,
    io: Option<&PreparationIo>,
) -> Result<Vec<ModelMessage>> {
    let expected_count = prefix.count as usize;
    let expected_content = prefix.content_digest;
    let mut cursor = Some(prefix);
    let mut segments = Vec::new();
    while let Some(prefix) = cursor {
        let segment = load_segment(journal, &prefix.reference, io).await?;
        let parent_count = segment
            .parent
            .as_ref()
            .map_or(0, |parent| parent.count as usize);
        if segment.suffix.is_empty()
            || parent_count.checked_add(segment.suffix.len()) != Some(prefix.count as usize)
            || chain_digest(segment.parent.as_ref(), &prefix.reference, io)? != prefix.chain_digest
        {
            return Err(invalid(io));
        }
        cursor = segment.parent;
        stock_vec_push(&mut segments, segment.suffix, io)?;
    }
    let mut messages = Vec::new();
    stock_vec_reserve(&mut messages, expected_count, io)?;
    for segment in segments.into_iter().rev() {
        for message in segment {
            stock_vec_push(&mut messages, message, io)?;
        }
    }
    if digests::messages(&messages, None, io)?.0 != expected_content {
        return Err(invalid(io));
    }
    Ok(messages)
}

async fn load_segment(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<MessageSegment> {
    let scope = crate::filesystem::PreparationScope::new(io)?;
    let io = scope.io();
    let segment = retain_preparation(load_json::<MessageSegment>(journal, reference, io).await?)?;
    {
        let retained = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        scope.finish(retained)?;
    }
    Ok(segment)
}

pub(crate) async fn stage_request(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
    previous: Option<&FileRef>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    if let Some(staging) =
        journal.stage_prepared_model_request(operation, key, prepared, previous, io)?
    {
        return staging.await;
    }
    stage_scoped_request(
        journal,
        RequestStage {
            operation,
            key,
            prepared,
            previous,
        },
        None,
        io,
    )
    .await
}

pub(crate) async fn stage_attested_request(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    prepared: &crate::model::PreparedModelRequest,
    previous: Option<&FileRef>,
    verifier: &crate::core::AuthorityVerifier,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    stage_scoped_request(
        journal,
        RequestStage {
            operation,
            key,
            prepared,
            previous,
        },
        Some(verifier),
        io,
    )
    .await
}

async fn stage_scoped_request(
    journal: &dyn ExecutionJournal,
    staging: RequestStage<'_>,
    verifier: Option<&crate::core::AuthorityVerifier>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let scope = crate::filesystem::PreparationScope::new(io)?;
    let io = scope.io();
    let reference = stage_request_inner(journal, staging, verifier, io).await?;
    scope.finish(reference.logical_owned_bytes()?)?;
    Ok(reference)
}

pub(crate) async fn load_request(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::PreparedModelRequest> {
    let scope = crate::filesystem::PreparationScope::new(io)?;
    let io = scope.io();
    let request = load_request_inner(journal, reference, limits, io).await?;
    {
        let retained = io.map_or(Ok(0), |io| request.preparation_owned_bytes(io))?;
        scope.finish(retained)?;
    }
    Ok(request)
}

pub(crate) async fn stage_projection(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    (projection, prepared): (&ResponseProjection, &crate::model::PreparedModelRequest),
    accounting: Option<&ContextAccounting>,
    previous: Option<&FileRef>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let scope = crate::filesystem::PreparationScope::new(io)?;
    let io = scope.io();
    // Admit projection metadata against the exact immutable request whose
    // message and instruction bounds were already established at preparation.
    crate::context::validate_projected_request(&projection.context, prepared, io)?;
    let previous = match previous {
        Some(previous) => Some(load_projection_record(journal, previous, io).await?),
        None => None,
    };
    let request_key = with_history_io(io, |work| {
        preparation_format(format_args!("{key}:request"), work)
    })?;
    let request = stage_request(
        journal,
        operation,
        &request_key,
        prepared,
        previous.as_ref().map(|previous| &previous.request),
        io,
    )
    .await?;
    let steering = match &projection.steering {
        Some(snapshot) if !snapshot.messages.is_empty() => {
            let previous = previous
                .and_then(|previous| previous.projection.steering)
                .and_then(|steering| steering.messages);
            let digests = digests::messages(
                &snapshot.messages,
                previous.as_ref().map(|previous| previous.count as usize),
                io,
            )?;
            let steering_key = with_history_io(io, |work| {
                preparation_format(format_args!("{key}:steering"), work)
            })?;
            Some(SteeringPrefix {
                through: snapshot.through,
                messages: Some(
                    stage_message_delta(
                        journal,
                        operation,
                        &steering_key,
                        (&snapshot.messages, digests),
                        previous,
                        io,
                    )
                    .await?,
                ),
            })
        }
        Some(snapshot) => Some(SteeringPrefix {
            through: snapshot.through,
            messages: None,
        }),
        None => None,
    };
    let reference = stage_json(
        journal,
        operation,
        key,
        &ProjectionRecordView {
            version: 2,
            request: &request,
            projection: ProjectionView {
                context: ContextView {
                    metadata: &projection.context.metadata,
                    current_input_index: projection.context.current_input_index,
                },
                steering: &steering,
                canonical: &projection.canonical,
                checkpoint: &projection.checkpoint,
                working: &projection.working,
                accounting,
            },
        },
        io,
    )
    .await?;
    scope.finish(reference.logical_owned_bytes()?)?;
    Ok(reference)
}

/// Read checkpoint metadata without reconstructing the historical request.
pub(super) async fn working_reference(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    original: Option<&PreparationIo>,
) -> Result<Option<FileRef>> {
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let stored = load_projection_record(journal, reference, io)
        .await?
        .projection;
    let working = stored.working;
    drop((
        stored.context,
        stored.steering,
        stored.canonical,
        stored.checkpoint,
        stored.accounting,
    ));
    {
        let retained = working
            .as_ref()
            .map_or(Ok(0), FileRef::logical_owned_bytes)?;
        scope.finish(retained)?;
    }
    Ok(working)
}

impl StockExecution<'_> {
    pub(super) async fn stage_response_request(
        &self,
        journal: &dyn ExecutionJournal,
        input: &TurnInput,
        step: u32,
        prepared: &crate::model::PreparedModelRequest,
    ) -> Result<FileRef> {
        let projection = self
            .response_projection(journal, input.operation_id, step)
            .await?
            .ok_or_else(|| self.invalid_working_cut())?;
        let projection_hold = self
            .io
            .map(|io| io.guard_owned(projection.logical_owned_bytes()?))
            .transpose()?;
        let scope = crate::filesystem::PreparationScope::new(self.io)?;
        let stored = load_projection_record(journal, &projection, scope.io()).await?;
        let manifest = load_manifest(journal, &stored.request, scope.io()).await?;
        if manifest.request_digest != prepared.request_digest() {
            return Err(invalid(self.io));
        }
        scope.finish(stored.request.logical_owned_bytes()?)?;
        drop(projection);
        drop(projection_hold);
        Ok(stored.request)
    }
}
