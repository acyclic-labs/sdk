use super::*;

/// Addressable, ref-only request admitted before presentation to a responder.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionTicket {
    /// Stable address used by responders and retries.
    pub id: Uuid,
    /// Validated request discriminator.
    pub kind: InteractionKind,
    /// Immutable provider-owned request JSON.
    pub request: FileRef,
    /// Optional absolute deadline in Unix milliseconds.
    pub deadline_unix_ms: Option<u64>,
    /// Exact action identity for an approval, absent otherwise.
    pub approval: Option<ApprovalBinding>,
}

impl InteractionTicket {
    /// Capability for viewing this ticket without reading its private bytes.
    #[must_use]
    pub fn viewer_grant(&self) -> String {
        format!("interaction:view:{}", self.id)
    }
    /// Capability for resolving this exact request.
    #[must_use]
    pub fn responder_grant(&self) -> String {
        capability::interaction_respond(self.id)
    }
    /// Checks the ref-only envelope before byte admission.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.id.is_nil()
            || self.request.descriptor().media_type() != "application/json"
            || self.deadline_unix_ms == Some(0)
            || (self.kind == InteractionKind::Approval) != self.approval.is_some()
            || self
                .approval
                .as_ref()
                .is_some_and(|binding| binding.action_digest == [0; 32])
        {
            return Err(boundary(work, Invalid, "invalid interaction ticket"));
        }
        self.request.validate(work)
    }
    /// Returns the validated request with its retained bytes or native custody.
    pub fn validate_request_bytes(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(Interaction, u64, crate::NativeCustody)> {
        self.validate(work)?;
        self.request.descriptor().verify(bytes, work)?;
        let mut decoded = decode_interaction_json::<Interaction>(
            bytes,
            crate::executor::json::JsonValueSchema::Interaction,
            std::mem::size_of::<ChoiceOption>() as u64,
            work,
        )?;
        let mut retained = 0;
        if decoded.1.work().is_some() {
            retained = interaction_request_owned_bytes(&decoded.0, decoded.1.work())?;
            let release = decoded.1.retained().checked_sub(retained).ok_or_else(|| {
                boundary(
                    decoded.1.work(),
                    Invalid,
                    "interaction DTO exceeds its original decoder custody",
                )
            })?;
            decoded.1.relinquish(release)?;
        }
        decoded.0.validate(decoded.1.work())?;
        if decoded.0.kind() != self.kind {
            return Err(boundary(
                decoded.1.work(),
                Invalid,
                "interaction request kind mismatch",
            ));
        }
        match (&decoded.0, &self.approval) {
            (
                Interaction::Approval {
                    operation_id,
                    action_digest,
                    ..
                },
                Some(binding),
            ) if *operation_id == binding.operation_id
                && *action_digest == binding.action_digest => {}
            (Interaction::Approval { .. }, _) => {
                return Err(boundary(
                    decoded.1.work(),
                    Conflict,
                    "approval action binding mismatch",
                ));
            }
            (_, None) => {}
            _ => {
                return Err(boundary(
                    decoded.1.work(),
                    Invalid,
                    "non-approval carries an approval binding",
                ));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = if decoded
            .1
            .work()
            .as_deref()
            .is_some_and(original_native_decode_mode)
        {
            let allocation = decoded.1.observer()?.take_native_custody(retained)?;
            retained = 0;
            Some(allocation)
        } else {
            None
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        decoded.1.transfer();
        let (request, quota) = decoded;
        drop(quota);
        // Request fields drop before their native custody.
        Ok((request, retained, custody))
    }
    /// Checks staged answer bytes against the caller-custodied request.
    pub(crate) fn validate_answer_bytes(
        &self,
        request: &Interaction,
        answer: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if self.kind == InteractionKind::Approval {
            return Err(boundary(
                work,
                Invalid,
                "approval cannot use a free-form answer",
            ));
        }
        answer.validate(work)?;
        if answer.descriptor().media_type() != "application/json" {
            return Err(boundary(work, Invalid, "interaction answer must be JSON"));
        }
        answer.descriptor().verify(bytes, work)?;
        let mut response = decode_interaction_json::<InteractionResponse>(
            bytes,
            crate::executor::json::JsonValueSchema::InteractionResponse,
            std::mem::size_of::<String>() as u64,
            work,
        )?;
        request.validate_response(&response.0, response.1.work())
    }
    /// Checks the staged decision file against its committed resolution and
    /// returns its principal's file digest. `principal` is the resolving scope
    /// when the decision is being admitted.
    pub(crate) fn validate_decision_bytes(
        &self,
        resolution: &InteractionResolution,
        principal: Option<&str>,
        detail: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<[u8; 16]> {
        if self.kind != InteractionKind::Approval
            || detail.descriptor().media_type() != "application/json"
        {
            return Err(boundary(
                work,
                Invalid,
                "approval decision artifact is invalid",
            ));
        }
        detail.validate(work)?;
        detail.descriptor().verify(bytes, work)?;
        let mut decision = decode_interaction_json::<ApprovalDecisionFile>(
            bytes,
            crate::executor::json::JsonValueSchema::None,
            std::mem::size_of::<String>() as u64,
            work,
        )?;
        let approved = match resolution.outcome {
            InteractionOutcome::Approved {} => true,
            InteractionOutcome::Declined {} => false,
            _ => {
                return Err(boundary(
                    decision.1.work(),
                    Invalid,
                    "only an approval verdict carries a decision file",
                ));
            }
        };
        if decision.0.validate().is_err()
            || decision.0.approved() != approved
            || !decision.0.binds(
                self.request.descriptor().sha256(),
                resolution.expected_version,
            )
            || principal.is_some_and(|principal| principal != decision.0.principal)
        {
            return Err(boundary(
                decision.1.work(),
                Conflict,
                "approval artifact disagrees with outcome",
            ));
        }
        Ok(ApprovalDecisionFile::principal_digest(&decision.0.principal))
    }
}
