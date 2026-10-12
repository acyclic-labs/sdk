use super::*;
/// Host-owned conversation signing credentials and its exact signed actor.
#[derive(Clone)]
pub struct ConfiguredRecursiveAuthorityBinding {
    issuer: AuthorityIssuer,
    pub scope: Scope,
}

impl ConfiguredRecursiveAuthorityBinding {
    /// Retains the signer and scope for configured-source validation.
    #[must_use]
    pub fn new(issuer: AuthorityIssuer, scope: Scope) -> Self {
        Self { issuer, scope }
    }

    #[must_use]
    pub fn authority(&self) -> &Authority {
        self.issuer.audience()
    }

    #[must_use]
    pub const fn issuer(&self) -> &AuthorityIssuer {
        &self.issuer
    }
}

impl<'de> Deserialize<'de> for ConfiguredRecursiveAuthorityBinding {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireBinding {
            authority: Authority,
            issuer: String,
            key: [u8; 32],
            scope: Scope,
        }
        let wire = WireBinding::deserialize(deserializer)?;
        Ok(Self {
            issuer: AuthorityIssuer::new(wire.issuer, wire.key, wire.authority),
            scope: wire.scope,
        })
    }
}

/// Explicit root binding or descendant binding checked against real admission.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveCaller {
    pub task: TaskId,
    pub owner: ConfiguredRecursiveAuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    /// Actual unmounted output binding; never substituted for source scratch.
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    /// Genuine producer descriptor, verified by the installed trusted policy.
    #[serde(default)]
    pub native_snapshot: Option<Value>,
}

/// One independently allocated child, its ordinary launch input and prep bounds.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveFork {
    pub caller: TaskId,
    pub child: ConfiguredRecursiveAuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    #[serde(deserialize_with = "canonical_admission")]
    pub admission: TaskAdmissionRecord,
    pub child_message_pointer: String,
    #[serde(default)]
    pub child_message_file_pointer: Option<String>,
    #[serde(default)]
    pub input_files: Vec<crate::conversation::FileRef>,
    pub limits: ForkPreparationLimits,
}

fn canonical_admission<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<TaskAdmissionRecord, D::Error> {
    TaskAdmissionRecord::from_canonical_value(Value::deserialize(deserializer)?, &mut None)
        .map(|(admission, _)| admission)
        .map_err(serde::de::Error::custom)
}

/// An explicit read-only Project import and the caller-owned canonical notice.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveMerge {
    pub caller: TaskId,
    pub source: TaskId,
    pub notice: ConversationMessage,
}

/// Existing trusted host-retained original cut, never a model-provided ledger.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveSourceCheckpoint {
    pub caller: TaskId,
    pub request: ForkRequest,
    pub snapshot: Snapshot,
}

/// Finite trusted configuration shared by native and browser default consumers.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveToolsConfig {
    pub callers: Vec<ConfiguredRecursiveCaller>,
    pub forks: Vec<ConfiguredRecursiveFork>,
    /// Browser host's explicit actual graph root for the creator-only policy.
    #[serde(default)]
    pub project_policy_graph_root: Option<TaskId>,
    pub merges: Vec<ConfiguredRecursiveMerge>,
    #[serde(default)]
    pub original_sources: Vec<ConfiguredRecursiveSourceCheckpoint>,
    pub stream_provider: ProviderRef,
    pub history_limits: HistoryReadLimits,
    pub inherited_messages: u64,
    pub inherited_references: u32,
    /// Finite original-checkpoint observer, reader and publisher allowance.
    pub content_budget: WorkBudget,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceCheckpoint {
    pub(super) snapshot: Snapshot,
    pub(super) request: ForkRequest,
    pub(super) caller: TaskId,
    pub(super) arguments: ForkToolArguments,
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::filesystem::PreparationRecord for SourceCheckpoint {
    fn maximum_slot_bytes() -> u64 {
        [
            crate::filesystem::preparation_slot_bytes::<Self>(),
            std::mem::size_of::<crate::fork::ForkRequest>() as u64,
            std::mem::size_of::<crate::fork::ForkSelection>() as u64,
            std::mem::size_of::<crate::fork::ResourceRevision>() as u64,
            std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>() as u64,
            std::mem::size_of::<crate::core::Scope>() as u64,
            std::mem::size_of::<crate::conversation::FileRef>() as u64,
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
    }

    fn diagnostic_label_bytes() -> u64 {
        const LABELS: &str = concat!(
            "SourceCheckpoint snapshot request caller arguments ForkToolArguments id msg source ",
            "Snapshot Projection ConversationStateWire ArchivedConversationPrefix ",
            "format_version authority revision events projection state_digest attestation ",
            "lifecycle active_extensions active_extension_revision extension_states ",
            "configured_extensions active_configurations effects indexed_terminal_effects ",
            "effects_archived forks published_merges conversation latest_context_checkpoint ",
            "interactions bindings capabilities issuer intent_digest scope causal_parent ",
            "messages archived_prefix through_sequence history_digest ",
            "archived conversation prefix is unbound conversation order or identity is invalid ",
            "message identity is nil message sequence must start at one ",
            "extension key must be namespaced tool linkage does not match message kind ",
            "file or volume label is invalid checkpoint snapshot is invalid ",
        );
        crate::contract::preparation_serde_diagnostic_label_bytes() + LABELS.len() as u64 + 4
    }

    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::ConfiguredRecursiveSourceCheckpoint
    }

    fn decode_owned_allowance(
        shape: crate::contract::JsonValueShape,
        codec_scratch: u64,
        input_bytes: u64,
        native_code: Option<crate::wire::ErrorCode>,
    ) -> Result<u64> {
        let overflow = || match native_code {
            Some(code) => Error::NativeBoundary {
                code,
                reason: "configured source checkpoint ownership overflows",
            },
            None => Error::Invalid("configured source checkpoint ownership overflows".into()),
        };
        let snapshot = crate::contract::json_decode_owned_bytes(shape, true, native_code)?;
        let outer = shape
            .lexical_items()
            .checked_mul(10)
            .and_then(|items| items.checked_mul(Self::maximum_slot_bytes()))
            .and_then(|bytes| {
                input_bytes
                    .checked_mul(4)
                    .and_then(|strings| bytes.checked_add(strings))
            })
            .and_then(|bytes| bytes.checked_add(codec_scratch.max(input_bytes)))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(overflow)?;
        snapshot
            .0
            .checked_sub(snapshot.1)
            .and_then(|bytes| bytes.checked_add(crate::contract::sdk_json_diagnostic_bytes()))
            .and_then(|bytes| bytes.checked_add(outer))
            .ok_or_else(overflow)
    }
}
