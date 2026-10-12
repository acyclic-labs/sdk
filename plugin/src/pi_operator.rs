//! Host-only Pi operator transport. Signing material never enters a native caller scope.
use crate::{
    ControlCommand, ControlRequest, pi_owner::PiOwnerConfig, pi_root_provider,
    pi_selected_host::PiSelectedCommand,
};
use acyclic_fs::{
    GenerationId, LazySnapshotRef, MaterializationJournal, NativeAllocation,
    NativeMaterializationWork, WorkCounters,
};
use acyclic_harness::{
    Capabilities, Error, JsonBounds, JsonCodecCharge, OperationId, Result, conversation::VolumeRef,
    core::Scope, json_from_slice_bounded, json_from_value_bounded,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(
        deserialize = "S: Deserialize<'de> + Default, V: Deserialize<'de>, P: Deserialize<'de> + Default, J: Deserialize<'de>"
    )
)]
pub(crate) struct PiPreparedApproval<
    S = String,
    V = VolumeRef,
    P = Vec<String>,
    J = MaterializationJournal,
> {
    pub(crate) version: u32,
    pub(crate) session: S,
    pub(crate) command: PiSelectedCommand<S, P>,
    pub(crate) operation: OperationId,
    pub(crate) approval_digest: S,
    pub(crate) private_volume: V,
    /// Original Root baseline inherited by the child; canonical digest tuple[10].
    pub(crate) source_snapshot: LazySnapshotRef,
    /// Child snapshot sealed by the actual selected import, not its native mount.
    pub(crate) selected_snapshot: LazySnapshotRef,
    pub(crate) imported: GenerationId,
    pub(crate) import_receipt: crate::pi_selected_host::PiSelectedImportReceipt<S, V, P>,
    pub(crate) journal: J,
    pub(crate) canonical_digest_input: S,
    pub(crate) root_path: S,
    pub(crate) source_path: S,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiOperatorDecision {
    Approve,
    Decline,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiOperatorFileCursor<S = String> {
    pub(crate) operation: S,
    pub(crate) root: S,
    pub(crate) generation: GenerationId,
    pub(crate) path: S,
    pub(crate) offset: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiOperatorFileEvidence {
    pub(crate) path: String,
    pub(crate) cursor: Option<PiOperatorFileCursor>,
    pub(crate) maximum_bytes: u32,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PiOperatorBody {
    Inspect {
        session: String,
        operation: String,
        project_files: bool,
        file_evidence: Option<PiOperatorFileEvidence>,
    },
    Approval {
        prepared: PiPreparedApproval,
        digest: String,
        decision: PiOperatorDecision,
    },
}
impl PiOperatorBody {
    pub(crate) fn session(&self) -> &str {
        match self {
            Self::Inspect { session, .. } => session,
            Self::Approval { prepared, .. } => &prepared.session,
        }
    }
    pub(crate) fn operation(&self) -> &str {
        match self {
            Self::Inspect { operation, .. } => operation,
            Self::Approval { prepared, .. } => &prepared.command.sdk_operation,
        }
    }
}
#[derive(Serialize)]
pub(crate) struct PiOperatorRequest {
    pub(crate) session_id: String,
    pub(crate) body: PiOperatorBody,
    pub(crate) operator: Scope,
}

impl PiOperatorRequest {
    pub(crate) fn from_value_with_native_work(
        value: &serde_json::Value,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, NativeAllocation)> {
        let invalid = |reason| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason,
        };
        let fields = value
            .as_object()
            .ok_or_else(|| invalid("operator request must be an object"))?;
        if fields.len() != 3
            || fields
                .keys()
                .any(|key| !matches!(key.as_str(), "session_id" | "body" | "operator"))
        {
            return Err(invalid(
                "operator request must contain only its exact original fields",
            ));
        }
        let session = fields
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .filter(|session| !session.is_empty() && session.len() <= 512)
            .ok_or_else(|| invalid("operator request requires its bounded original session"))?;
        let body_value = fields
            .get("body")
            .ok_or_else(|| invalid("operator request body is missing"))?;
        let scope_value = fields
            .get("operator")
            .ok_or_else(|| invalid("operator request scope is missing"))?;
        let remaining = work.remaining().map_err(acyclic_harness::Error::from)?;
        let bounds = JsonBounds {
            maximum_bytes: crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            maximum_items: remaining.items_examined,
            maximum_depth: 64,
        };
        let mut custody = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(acyclic_harness::Error::from)?;
        let decoded = json_from_value_bounded(
            std::borrow::Cow::Borrowed(body_value),
            bounds,
            0,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            &mut |charge| operator_decode_charge(charge, work, &mut custody),
        );
        let body: PiOperatorBody = match decoded {
            Ok(body) => body,
            Err(error) => return Err(operator_decode_error(error, custody)),
        };
        let mut session = work
            .display(session)
            .map_err(acyclic_harness::Error::from)?;
        let mut scope = Scope::from_value_with_native_work(scope_value, work)?;
        let mut request = (
            Self {
                session_id: session.0,
                body,
                operator: scope.0,
            },
            custody,
        );
        request
            .1
            .absorb(&mut session.1)
            .map_err(acyclic_harness::Error::from)?;
        request
            .1
            .absorb(&mut scope.1)
            .map_err(acyclic_harness::Error::from)?;
        Ok(request)
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
pub(crate) fn body_hash(
    body: &PiOperatorBody,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    if body.session().is_empty()
        || body.session().len() > 512
        || body.operation().is_empty()
        || body.operation().len() > 512
    {
        return Err(invalid(
            "operator requires its bounded original session and SDK operation",
        ));
    }
    pi_root_provider::digest_pi_value(body, crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64, work)
}
pub(crate) fn approval_body_hash(
    prepared: &PiPreparedApproval,
    digest: &str,
    decision: PiOperatorDecision,
    work: &NativeMaterializationWork,
) -> Result<(String, NativeAllocation)> {
    #[derive(Serialize)]
    struct BorrowedApproval<'a> {
        action: &'static str,
        prepared: &'a PiPreparedApproval,
        digest: &'a str,
        decision: PiOperatorDecision,
    }
    pi_root_provider::digest_pi_value(
        &BorrowedApproval {
            action: "approval",
            prepared,
            digest,
            decision,
        },
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        work,
    )
}
fn capability(body: &PiOperatorBody, hash: &str) -> String {
    format!("pi:operator:{}:{hash}", body.session())
}

pub(crate) fn build_control_request(
    owner: &PiOwnerConfig,
    cwd: PathBuf,
    body: PiOperatorBody,
    work: &NativeMaterializationWork,
) -> Result<(ControlRequest, acyclic_fs::NativeAllocation)> {
    let hash_custody = body_hash(&body, work)?;
    let hash = &hash_custody.0;
    let authority = owner.authority(work)?;
    let operator = authority
        .issuer
        .issue(
            &format!("pi:operator:{}:{hash}", body.session()),
            &Capabilities::new([capability(&body, &hash)]),
            Some(owner.policy().agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let session_id = body.session().to_owned();
    let event = match &body {
        PiOperatorBody::Inspect { .. } => "OperatorInspect",
        PiOperatorBody::Approval { .. } => "OperatorApproval",
    };
    let request = PiOperatorRequest {
        session_id,
        body,
        operator,
    };
    let custody;
    let arguments;
    (arguments, custody) = pi_root_provider::guarded_pi_value(
        &request,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
        work,
    )?;
    drop(request);
    Ok((
        ControlRequest {
            version: 1,
            command: ControlCommand::Hook,
            cwd,
            argv: Vec::new(),
            name: format!("pi:{event}"),
            arguments,
        },
        custody,
    ))
}

pub(crate) fn verify_request(
    owner: &PiOwnerConfig,
    session: &str,
    request: &PiOperatorRequest,
    work: &NativeMaterializationWork,
) -> Result<()> {
    owner.authority(work)?.verifier.verify(
        &request.operator,
        &mut Some(
            &mut acyclic_harness::store::native_work_io(work)
                .map_err(acyclic_harness::Error::from)?,
        ),
    )?;
    let hash_custody = body_hash(&request.body, work)?;
    let hash = &hash_custody.0;
    if request.session_id != session
        || request.body.session() != session
        || request.operator.agent() != Some(owner.policy().agent)
        || request.operator.id() != format!("pi:operator:{session}:{hash}")
        || request.operator.capabilities() != &Capabilities::new([capability(&request.body, &hash)])
    {
        return Err(Error::Unauthorized(
            "operator response differs from its original protected Root and exact body".into(),
        ));
    }
    Ok(())
}

pub(crate) struct PiOperatorCli {
    pub(crate) session: String,
    pub(crate) operation: String,
    pub(crate) approval: Option<(String, PiOperatorDecision)>,
    pub(crate) project_files: bool,
    pub(crate) file: Option<String>,
    pub(crate) offset: u64,
    pub(crate) maximum_bytes: u32,
}
pub(crate) fn parse_cli(args: &[String]) -> std::result::Result<PiOperatorCli, String> {
    let command = args
        .first()
        .map(String::as_str)
        .ok_or("Pi operator command is missing")?;
    if !matches!(command, "approvals" | "approve") {
        return Err("unknown Pi operator command".into());
    }
    let mut session = None;
    let mut operation = None;
    let mut digest = None;
    let mut decision = None;
    let mut json = false;
    let mut project_files = false;
    let mut file = None;
    let mut offset = None;
    let mut maximum_bytes = None;
    let mut index = 1;
    while index < args.len() {
        let flag = args[index].as_str();
        index += 1;
        if flag == "--json" {
            if json {
                return Err("duplicate --json".into());
            }
            json = true;
            continue;
        }
        if flag == "--project-files" {
            if project_files {
                return Err("duplicate --project-files".into());
            }
            project_files = true;
            continue;
        }
        let value = args
            .get(index)
            .ok_or_else(|| format!("{flag} requires a value"))?
            .clone();
        index += 1;
        let slot = match flag {
            "--session" => &mut session,
            "--operation" => &mut operation,
            "--digest" => &mut digest,
            "--decision" => &mut decision,
            "--file" => &mut file,
            "--offset" => &mut offset,
            "--maximum-bytes" => &mut maximum_bytes,
            _ => return Err(format!("unknown Pi operator argument {flag}")),
        };
        if slot.replace(value).is_some() {
            return Err(format!("duplicate {flag}"));
        }
    }
    let session = session
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .ok_or("--session requires the original Root native session")?;
    let operation = operation
        .filter(|value| !value.is_empty() && value.len() <= 512)
        .ok_or("--operation requires the original SDK tool ID")?;
    let approval = if command == "approve" {
        let digest = digest.ok_or("--digest is required")?;
        parse_digest(&digest).map_err(|error| error.to_string())?;
        let decision = match decision.as_deref() {
            Some("approve") => PiOperatorDecision::Approve,
            Some("decline") => PiOperatorDecision::Decline,
            _ => return Err("--decision must be approve or decline".into()),
        };
        Some((digest, decision))
    } else {
        if digest.is_some() || decision.is_some() || !json {
            return Err("approvals requires --json and accepts no decision".into());
        }
        None
    };
    if (file.is_some() || offset.is_some() || maximum_bytes.is_some()) && !project_files {
        return Err("file evidence requires --project-files".into());
    }
    if project_files && approval.is_some() {
        return Err("approve accepts no file projection arguments".into());
    }
    if (offset.is_some() || maximum_bytes.is_some()) && file.is_none() {
        return Err("file offsets and bounds require --file".into());
    }
    if file
        .as_deref()
        .is_some_and(|path| path.is_empty() || path.len() > 4096)
    {
        return Err("--file requires a bounded selected relative path".into());
    }
    let offset = offset
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| "--offset must be an unsigned byte offset")
        })
        .transpose()?
        .unwrap_or(0);
    let maximum_bytes = maximum_bytes
        .map(|value| {
            value
                .parse::<u32>()
                .map_err(|_| "--maximum-bytes must be unsigned")
        })
        .transpose()?
        .unwrap_or(262_144);
    if !(1..=262_144).contains(&maximum_bytes) {
        return Err("--maximum-bytes must be 1..262144".into());
    }
    Ok(PiOperatorCli {
        session,
        operation,
        approval,
        project_files,
        file,
        offset,
        maximum_bytes,
    })
}
pub(crate) fn parse_digest(digest: &str) -> Result<[u8; 32]> {
    if digest.len() != 64 {
        return Err(invalid(
            "approval requires the exact 64-character prepared digest",
        ));
    }
    let mut bytes = [0; 32];
    hex::decode_to_slice(digest, &mut bytes)
        .map_err(|_| invalid("approval digest must be hexadecimal"))?;
    Ok(bytes)
}

pub(crate) const fn prepared_labels_bytes() -> u64 {
    pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiPreparedApproval",
        "PiSelectedCommand",
        "PiSelectedImportReceipt",
        "VolumeRefWire",
        "ProviderRefWire",
        "VolumeOwner",
        "MaterializationJournal",
        "MaterializationPlan",
        "MaterializationEdit",
        "MaterializationIntent",
        "MaterializationPreparationIdentity",
        "MaterializationPreparationProgress",
        "MaterializationPreparationAllocation",
        "MaterializationPreparationSelection",
        "MaterializationPreimage",
        "LazySnapshotRef",
        "SourceReference",
        "WorkCounters",
        "version",
        "session",
        "command",
        "operation",
        "approval_digest",
        "private_volume",
        "source_snapshot",
        "selected_snapshot",
        "imported",
        "import_receipt",
        "journal",
        "canonical_digest_input",
        "root_path",
        "source_path",
        "caller",
        "sdk_operation",
        "source",
        "root",
        "directory",
        "cursor",
        "maximum_entries",
        "destination",
        "paths",
        "confirm",
        "import_operation",
        "destination_snapshot",
        "provider",
        "id",
        "class",
        "owner",
        "namespace",
        "family",
        "kind",
        "project",
        "agent",
        "session",
        "agent_private",
        "session_shared",
        "revision",
        "plan",
        "root_identity",
        "preparation_identity",
        "preparation_progress",
        "work",
        "preimages",
        "phase",
        "applied",
        "restored",
        "operation_id",
        "intent",
        "edits",
        "GenerationTransition",
        "NativePreparation",
        "from",
        "to",
        "selection",
        "original",
        "provider_identity",
        "volume",
        "generation",
        "Install",
        "Remove",
        "Rename",
        "SetMetadata",
        "path",
        "image",
        "target",
        "backup",
        "parent",
        "pending",
        "Operation",
        "Target",
        "Backup",
        "Preparing",
        "Prepared",
        "Applying",
        "Applied",
        "RollingBack",
        "RolledBack",
        "workspace_id",
        "authored_generation",
        "identity",
        "epoch",
        "overlay",
        "shadows",
        "authorityRecordsRead",
        "authorityRecordsAppended",
        "authorityBytesRead",
        "authorityBytesWritten",
        "objectProbes",
        "backendReadOperations",
        "backendWriteOperations",
        "durabilityOperations",
        "pageReads",
        "pageWrites",
        "objectBytesRead",
        "objectBytesWritten",
        "bytesHashed",
        "bytesCopied",
        "bytesEncoded",
        "sourceBytesRead",
        "sourcePathComponents",
        "sourceEntriesVisited",
        "outputBytes",
        "itemsExamined",
        "itemsReturned",
        "allocationOperations",
        "peakAllocationBytes",
        "materializations",
        "provider identity is invalid",
        "file or volume label is invalid",
        "unknown variant ``, expected one of ",
        "unknown field ``, expected one of ",
        "duplicate field ``",
        "missing field ``",
        "invalid type: , expected ",
        "invalid value: , expected ",
        "invalid length , expected ",
        " at line  column ",
        "a sequence",
        "a map",
        "a string",
        "a byte array",
        "an integer",
        "an array of length 16",
        "an array of length 32",
        "a valid UUID string",
        "invalid request: ",
        "storage failure: ",
    ])
}

/// Audited closed operator DTO quota, including its internally tagged body and adjacent VolumeOwner content.
fn operator_owned_quota(items: u64, input_bytes: u64, scratch: u64) -> Result<u64> {
    let body_labels_bytes = pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiOperatorBody",
        "PiOperatorFileEvidence",
        "PiOperatorFileCursor",
        "PiOperatorDecision",
        "action",
        "inspect",
        "approval",
        "prepared",
        "digest",
        "decision",
        "session_id",
        "operator",
        "project_files",
        "file_evidence",
        "offset",
        "maximum_bytes",
        "approve",
        "decline",
    ]);
    let diagnostic_static_bytes =
        4 * (prepared_labels_bytes() + body_labels_bytes + 2 * u64::from(usize::BITS));
    let content_slot =
        std::mem::size_of::<serde::__private229::de::Content<'static>>().max(std::mem::size_of::<(
            serde::__private229::de::Content<'static>,
            serde::__private229::de::Content<'static>,
        )>()) as u64;
    let maximum_slot = [
        std::mem::size_of::<acyclic_fs::MaterializationEdit>(),
        std::mem::size_of::<acyclic_fs::MaterializationPreparationSelection>(),
        std::mem::size_of::<acyclic_fs::MaterializationPreimage>(),
        std::mem::size_of::<String>(),
        std::mem::size_of::<u8>(),
        // These are the complete dynamic element types of the typed DTO.
    ]
    .into_iter()
    .max()
    .expect("nonempty approval schema") as u64;
    items
        .checked_mul(10)
        .and_then(|slots| slots.checked_mul(maximum_slot + 2 * content_slot))
        .and_then(|slots| {
            input_bytes
                .checked_mul(28)
                .and_then(|strings| slots.checked_add(strings))
        })
        .and_then(|owned| owned.checked_add(diagnostic_static_bytes))
        .and_then(|owned| owned.checked_add(scratch))
        .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(
            acyclic_fs::WorkError::Overflow,
        ))
        .map_err(acyclic_harness::Error::from)
}

fn decode_prepared_approval(
    bytes: &[u8],
    work: &NativeMaterializationWork,
) -> Result<(PiPreparedApproval, NativeAllocation)> {
    let remaining = work.remaining().map_err(acyclic_harness::Error::from)?;
    let bounds = JsonBounds {
        maximum_bytes: crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        maximum_items: remaining.items_examined,
        maximum_depth: 64,
    };
    let mut custody = work
        .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(acyclic_harness::Error::from)?;
    let decoded = json_from_slice_bounded(
        std::marker::PhantomData::<PiPreparedApproval>,
        bytes,
        bounds,
        0,
        Some(acyclic_harness::wire::ErrorCode::Invalid),
        Some(acyclic_harness::JsonValueSchema::None),
        &mut |charge| operator_decode_charge(charge, work, &mut custody),
    );
    match decoded {
        Ok(prepared) => Ok((prepared, custody)),
        Err(error) => Err(operator_decode_error(error, custody)),
    }
}

fn operator_decode_error(error: Error, custody: NativeAllocation) -> Error {
    match error {
        Error::NativeWork {
            cause,
            custody: None,
        } => Error::NativeWork {
            cause,
            custody: Some(custody),
        },
        Error::NativeDecode {
            code,
            source,
            custody: None,
        } => Error::NativeDecode {
            code,
            source,
            custody: Some(custody),
        },
        error => error,
    }
}

fn operator_decode_charge(
    charge: JsonCodecCharge,
    work: &NativeMaterializationWork,
    custody: &mut NativeAllocation,
) -> Result<()> {
    match charge {
        JsonCodecCharge::Admission {
            maximum_owned_bytes: scratch,
            value_shape,
        } => {
            let items = value_shape.lexical_items();
            let input_bytes = items
                .checked_mul(64)
                .and_then(|scalars| scalars.checked_add(value_shape.input_bytes()))
                .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow,
                ))
                .map_err(acyclic_harness::Error::from)?;
            let mut quota = work
                .claim(
                    operator_owned_quota(items, input_bytes, scratch)?,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )
                .map_err(acyclic_harness::Error::from)?;
            custody
                .absorb(&mut quota)
                .map_err(acyclic_harness::Error::from)
        }
        charge => pi_root_provider::native_buffer_codec_charge(work, custody, charge),
    }
}

/// The only path loading operator credentials: a trusted host CLI action, not a hook supplied by an agent.
pub(crate) async fn run_cli(
    data: &Path,
    cwd: PathBuf,
    args: &[String],
) -> std::result::Result<serde_json::Value, crate::service_process::ControlRequestError> {
    use crate::service_process::ControlRequestError;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(pi_root_provider::allocation_budget(), &cancellation)
        .map_err(|(cause, work)| {
            ControlRequestError::NativeWork(Error::NativeConstruction { cause, work })
        })?;
    let cli = parse_cli(args).map_err(ControlRequestError::Response)?;
    let directory = crate::service::session_directory(data, &cli.session);
    let private =
        acyclic_fs::NativeHostPrivateStateRoot::bootstrap_configured_with_native_work(data, &work)
            .map_err(|cause| {
                ControlRequestError::NativeWork(acyclic_harness::Error::from(cause))
            })?;
    let relative = directory
        .strip_prefix(data)
        .map_err(|_| ControlRequestError::Response("Pi owner escaped its data directory".into()))?;
    let owner =
        PiOwnerConfig::load(&private, relative, &work).map_err(ControlRequestError::NativeWork)?;
    let inspect_custody;
    let inspect;
    (inspect, inspect_custody) = build_control_request(
        &owner,
        cwd.clone(),
        PiOperatorBody::Inspect {
            session: cli.session.clone(),
            operation: cli.operation.clone(),
            project_files: false,
            file_evidence: None,
        },
        &work,
    )
    .map_err(ControlRequestError::NativeWork)?;
    let envelope = crate::ControlEnvelope::new(inspect);
    let result = crate::service_process::send_control_envelope(data, &envelope).await?;
    drop(envelope);
    drop(inspect_custody);
    if cli.approval.is_none() && !cli.project_files {
        return Ok(result);
    }
    let descriptor = result.get("prepared").ok_or_else(|| {
        ControlRequestError::Response("original operation has no prepared approval".into())
    })?;
    let bytes_custody;
    let bytes;
    (bytes, bytes_custody) = crate::service_process::encode_native_control_value(descriptor, &work)
        .map_err(ControlRequestError::NativeWork)?;
    let prepared_custody;
    let prepared;
    (prepared, prepared_custody) =
        decode_prepared_approval(&bytes, &work).map_err(ControlRequestError::NativeWork)?;
    drop(bytes);
    drop(bytes_custody);
    if prepared.session != cli.session || prepared.command.sdk_operation != cli.operation {
        return Err(ControlRequestError::Response(
            "operator decision does not match the original prepared session and operation".into(),
        ));
    }
    let body = if let Some((digest, decision)) = cli.approval {
        PiOperatorBody::Approval {
            prepared,
            digest,
            decision,
        }
    } else {
        let file_evidence = cli
            .file
            .map(|path| {
                let cursor = if cli.offset == 0 {
                    None
                } else {
                    Some(PiOperatorFileCursor {
                        operation: cli.operation.clone(),
                        root: prepared.command.root.clone().ok_or_else(|| {
                            ControlRequestError::Response(
                                "prepared operation has no original Root".into(),
                            )
                        })?,
                        generation: prepared.imported,
                        path: path.clone(),
                        offset: cli.offset,
                    })
                };
                Ok::<_, ControlRequestError>(PiOperatorFileEvidence {
                    path,
                    cursor,
                    maximum_bytes: cli.maximum_bytes,
                })
            })
            .transpose()?;
        drop(prepared);
        PiOperatorBody::Inspect {
            session: cli.session,
            operation: cli.operation,
            project_files: true,
            file_evidence,
        }
    };
    let request_custody;
    let request;
    (request, request_custody) =
        build_control_request(&owner, cwd, body, &work).map_err(ControlRequestError::NativeWork)?;
    drop(prepared_custody);
    drop(result);
    let envelope = crate::ControlEnvelope::new(request);
    let response = crate::service_process::send_control_envelope(data, &envelope).await;
    drop(envelope);
    drop(request_custody);
    response
}
#[cfg(test)]
#[path = "pi_operator_tests.rs"]
mod tests;
