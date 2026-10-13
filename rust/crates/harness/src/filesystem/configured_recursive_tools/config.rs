use super::*;

/// Host-owned conversation credentials and its signed actor.
#[derive(Clone)]
pub struct ConfiguredRecursiveAuthorityBinding {
    issuer: AuthorityIssuer,
    pub scope: Scope,
}
impl ConfiguredRecursiveAuthorityBinding {
    /// Binds the configured signer and scope.
    pub fn new(issuer: AuthorityIssuer, scope: Scope) -> Self {
        Self { issuer, scope }
    }
    /// Returns the signing audience.
    pub fn authority(&self) -> &Authority {
        self.issuer.audience()
    }
    /// Returns the host-owned signer.
    pub fn issuer(&self) -> &AuthorityIssuer {
        &self.issuer
    }
}
impl<'de> Deserialize<'de> for ConfiguredRecursiveAuthorityBinding {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Binding {
            authority: Authority,
            issuer: String,
            key: [u8; 32],
            scope: Scope,
        }
        let binding = Binding::deserialize(deserializer)?;
        Ok(Self::new(
            AuthorityIssuer::new(binding.issuer, binding.key, binding.authority),
            binding.scope,
        ))
    }
}

/// A configured caller checked against its retained admission.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveCaller {
    pub task: TaskId,
    pub owner: ConfiguredRecursiveAuthorityBinding,
    pub project: VolumeRef,
    pub private_volume: VolumeRef,
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    #[serde(default)]
    pub native_snapshot: Option<Value>,
}

/// One independently allocated child and its finite preparation bounds.
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

/// An explicit read-only Project import and its canonical notice.
#[derive(Clone, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveMerge {
    pub caller: TaskId,
    pub source: TaskId,
    pub notice: ConversationMessage,
}

/// Finite provider configuration for the portable recursive tools.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveToolsConfig {
    pub callers: Vec<ConfiguredRecursiveCaller>,
    pub forks: Vec<ConfiguredRecursiveFork>,
    #[serde(default)]
    pub project_policy_graph_root: Option<TaskId>,
    pub merges: Vec<ConfiguredRecursiveMerge>,
    pub stream_provider: ProviderRef,
    pub history_limits: HistoryReadLimits,
    pub inherited_messages: u64,
    pub inherited_references: u32,
    pub content_budget: WorkBudget,
}
