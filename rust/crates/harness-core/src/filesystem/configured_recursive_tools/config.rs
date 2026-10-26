use super::*;

/// Host-owned conversation credentials and its signed actor.
///
/// Runtime composition supplies the existing issuer handle. A configuration
/// document cannot import a signing seed or manufacture a new authority root.
///
/// ```compile_fail
/// use acyclic_harness_core::filesystem::ConfiguredRecursiveAuthorityBinding;
/// let binding: ConfiguredRecursiveAuthorityBinding =
///     serde_json::from_str(r#"{"issuer":"owner","key":[0],"scope":{}}"#).unwrap();
/// ```
#[derive(Clone)]
pub struct ConfiguredRecursiveAuthorityBinding {
    /// Host-owned signing identity.
    pub issuer: AuthorityRoot,
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
/// A configured caller checked against its retained admission.
#[derive(Clone)]
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
    pub result_volume: Option<VolumeRef>,
    /// Retained native snapshot metadata.
    pub native_snapshot: Option<Value>,
}

/// One independently allocated child and its finite preparation bounds.
#[derive(Clone)]
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
    pub result_volume: Option<VolumeRef>,
    /// Canonical child admission.
    pub admission: TaskAdmissionRecord,
    /// JSON pointer for the child message.
    pub child_message_pointer: String,
    /// Optional JSON pointer for the message file.
    pub child_message_file_pointer: Option<String>,
    /// Files available to the child input.
    pub input_files: Vec<crate::conversation::FileRef>,
    /// Finite fork preparation limits.
    pub limits: ForkPreparationLimits,
}
/// An explicit read-only Project import and its canonical notice.
#[derive(Clone, serde::Serialize, Deserialize, crate::HeapLayout)]
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
///
/// Build this value alongside the runtime that owns its issuer handles.
///
/// ```compile_fail
/// use acyclic_harness_core::filesystem::ConfiguredRecursiveToolsConfig;
/// let config: ConfiguredRecursiveToolsConfig = serde_json::from_str("{}").unwrap();
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_binding_uses_runtime_custody_and_rejects_a_foreign_scope() -> Result<()> {
        let authority = Authority {
            kind: crate::core::AggregateKind::Task,
            id: "configured-owner".into(),
        };
        let issuer = AuthorityRoot::new("configured", [71; 32], authority.clone());
        let agent = crate::AgentId::from_bytes([72; 16]);
        let binding = ConfiguredRecursiveAuthorityBinding {
            scope: issuer.bootstrap_scope(
                "owner",
                &Capabilities::default(),
                Some(agent),
                &mut None,
            )?,
            issuer,
        };
        let provider =
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None)?;
        let volume = |id: &str, class, owner| {
            VolumeRef::new(provider.clone(), id.into(), class, owner, &mut None)
        };
        let project = volume(
            "project",
            crate::conversation::VolumeClass::Project,
            VolumeOwner::Project("project".into()),
        )?;
        let private = volume(
            "private",
            crate::conversation::VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        binding.validate_volumes(&provider, &project, &private, None)?;
        let foreign = AuthorityRoot::new("configured", [73; 32], authority);
        let forged = ConfiguredRecursiveAuthorityBinding {
            issuer: binding.issuer,
            scope: foreign.bootstrap_scope(
                "owner",
                &Capabilities::default(),
                Some(agent),
                &mut None,
            )?,
        };
        assert!(matches!(
            forged.validate_volumes(&provider, &project, &private, None),
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }
}
