use super::*;

/// Host-owned conversation credentials and its signed actor.
#[derive(Clone)]
pub struct ConfiguredRecursiveAuthorityBinding {
    /// Host-owned signing identity.
    pub issuer: AuthorityIssuer,
    /// Signed actor scope.
    pub scope: Scope,
}

impl ConfiguredRecursiveAuthorityBinding {
    pub(super) fn validate_volumes(
        &self,
        provider: &ProviderRef,
        project: &VolumeRef,
        private: &VolumeRef,
        result: Option<&VolumeRef>,
    ) -> Result<()> {
        let denied = |reason: &str| Error::Unauthorized(reason.into());
        verifier(self)?;
        let agent = self
            .scope
            .agent()
            .ok_or_else(|| denied("configured binding has no signed agent"))?;
        for volume in [project, private].into_iter().chain(result) {
            volume.validate(&mut None)?;
            if volume.provider() != provider {
                return Err(denied("configured volume belongs to another provider"));
            }
        }
        if private.owner() != &VolumeOwner::Agent(agent)
            || result.is_some_and(|result| {
                result == private
                    || result.owner() != &VolumeOwner::Agent(agent)
                    || result.class() != crate::conversation::VolumeClass::AgentPrivate
            })
        {
            return Err(denied(
                "configured private/result volume differs from its signed owner",
            ));
        }
        Ok(())
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
        Ok(Self {
            issuer: AuthorityIssuer::new(binding.issuer, binding.key, binding.authority),
            scope: binding.scope,
        })
    }
}

/// A configured caller checked against its retained admission.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveCaller {
    /// Admitted caller task.
    pub task: TaskId,
    /// Caller authority and signed scope.
    pub owner: ConfiguredRecursiveAuthorityBinding,
    /// Selected Project volume.
    pub project: VolumeRef,
    /// Caller scratch volume.
    pub private_volume: VolumeRef,
    /// Optional artifact volume.
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    /// Retained native snapshot metadata.
    #[serde(default)]
    pub native_snapshot: Option<Value>,
}

/// One independently allocated child and its finite preparation bounds.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveFork {
    /// Admitted parent task.
    pub caller: TaskId,
    /// Child authority and signed scope.
    pub child: ConfiguredRecursiveAuthorityBinding,
    /// Child Project volume.
    pub project: VolumeRef,
    /// Child scratch volume.
    pub private_volume: VolumeRef,
    /// Optional child artifact volume.
    #[serde(default)]
    pub result_volume: Option<VolumeRef>,
    /// Canonical child admission.
    #[serde(deserialize_with = "canonical_admission")]
    pub admission: TaskAdmissionRecord,
    /// JSON pointer for the child message.
    pub child_message_pointer: String,
    /// Optional JSON pointer for the message file.
    #[serde(default)]
    pub child_message_file_pointer: Option<String>,
    /// Files available to the child input.
    #[serde(default)]
    pub input_files: Vec<crate::conversation::FileRef>,
    /// Finite fork preparation limits.
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
    /// Task receiving the import.
    pub caller: TaskId,
    /// Task providing the Project snapshot.
    pub source: TaskId,
    /// Canonical import notice.
    pub notice: ConversationMessage,
}

/// Finite provider configuration for the portable recursive tools.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredRecursiveToolsConfig {
    /// Authorized callers.
    pub callers: Vec<ConfiguredRecursiveCaller>,
    /// Configured child allocations.
    pub forks: Vec<ConfiguredRecursiveFork>,
    /// Configured Project imports.
    pub merges: Vec<ConfiguredRecursiveMerge>,
    /// Provider for retained history.
    pub stream_provider: ProviderRef,
    /// Finite history replay limits.
    pub history_limits: HistoryReadLimits,
    /// Maximum inherited messages.
    pub inherited_messages: u64,
    /// Maximum inherited references.
    pub inherited_references: u32,
    /// Finite content verification budget.
    pub content_budget: WorkBudget,
}
