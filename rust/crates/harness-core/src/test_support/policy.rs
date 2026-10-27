//! Explicit policy fixtures shared by core and downstream contract tests.
use crate::{
    Result,
    registry::ComponentIdentity,
    runtime::{RuntimeScope, ToolPolicy, ToolPolicyDecision},
    tool::ToolInvocation,
};

/// Fixed, explicitly selected policy decision for non-approval contract tests.
#[derive(Clone)]
pub struct FixedToolPolicy {
    /// Explicit fixture implementation identity.
    pub identity: ComponentIdentity,
    /// Explicit fixture decision; this type has no default.
    pub decision: ToolPolicyDecision,
}

impl FixedToolPolicy {
    /// Pins an explicit fixture decision to its encoded data identity.
    ///
    /// # Errors
    /// Returns an error if the fixture decision cannot be encoded canonically.
    pub fn new(decision: ToolPolicyDecision) -> Result<Self> {
        let value = match &decision {
            ToolPolicyDecision::Allow => serde_json::json!({"allow":{}}),
            ToolPolicyDecision::Deny { reason } => serde_json::json!({"deny":reason}),
            ToolPolicyDecision::RequireApproval { prompt } => serde_json::json!({"ask":prompt}),
        };
        Ok(Self {
            identity: ComponentIdentity {
                name: "test.fixed-policy".into(),
                version: "1".into(),
                digest: *blake3::hash(&crate::contract::canonical_json_bytes(&value)?).as_bytes(),
            },
            decision,
        })
    }
}

impl ToolPolicy for FixedToolPolicy {
    fn identity(&self) -> ComponentIdentity {
        self.identity.clone()
    }

    fn evaluate<'a>(
        &'a self,
        _: &'a ToolInvocation,
        _: &'a RuntimeScope,
        _native_io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::store::HistoryContentFuture<'a, (ToolPolicyDecision, crate::NativeCustody)>>
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _native_io
            .map(|io| {
                io.original_native_work()
                    .ok_or(crate::Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "native policy requires original work",
                    })
            })
            .transpose()?
        {
            return crate::NativeFutureBundle::new(work, || async move {
                use crate::store::history::HistoryIo;
                let mut io = crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                );
                let value = match &self.decision {
                    ToolPolicyDecision::Allow => ToolPolicyDecision::Allow,
                    ToolPolicyDecision::Deny { reason } => ToolPolicyDecision::Deny {
                        reason: io.clone_text(reason)?,
                    },
                    ToolPolicyDecision::RequireApproval { prompt } => {
                        ToolPolicyDecision::RequireApproval {
                            prompt: io.clone_text(prompt)?,
                        }
                    }
                };
                Ok((value, Some(io.into_custody()?)))
            })
            .map(crate::store::HistoryContentFuture::Native);
        }
        let decision = self.decision.clone();
        Ok(crate::store::HistoryContentFuture::Ordinary {
            future: Box::pin(async move { Ok((decision, None)) }),
            frame_bytes: 0,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
        native_fixture_policy_identity(&self.identity, work)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_fixture_policy_identity(
    identity: &ComponentIdentity,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(ComponentIdentity, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo;
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    io.retain_owned(std::mem::size_of::<ComponentIdentity>() as u64)?;
    let name = io.clone_text(&identity.name)?;
    let version = io.clone_text(&identity.version)?;
    Ok((
        ComponentIdentity {
            name,
            version,
            digest: identity.digest,
        },
        io.into_custody()?,
    ))
}
