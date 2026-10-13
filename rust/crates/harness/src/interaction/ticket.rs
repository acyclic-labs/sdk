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
    /// A scoped observer may see the ticket and outcome refs without being authorized to answer it or read the referenced private bytes.
    #[must_use]
    pub fn viewer_grant(&self) -> String {
        format!("interaction:view:{}", self.id)
    }

    /// The one exact capability that may resolve this request.
    #[must_use]
    pub fn responder_grant(&self) -> String {
        capability::interaction_respond(self.id)
    }

    /// Checks the ref-only envelope before provider byte admission.
    pub(crate) fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
            return Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "invalid interaction ticket",
            ));
        }
        self.request.validate(work)
    }

    /// Returns the validated request, retained byte count, and native custody.
    pub fn validate_request_bytes(
        &self,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(Interaction, u64, crate::NativeCustody)> {
        if work.is_none() {
            self.validate(work)?;
            self.request.descriptor().verify(bytes, work)?;
            let request: Interaction =
                crate::contract::json_from_slice(bytes).map_err(|error| {
                    Error::Invalid(format!("invalid interaction request JSON: {error}"))
                })?;
            self.validate_decoded_request(&request, work)?;
            return Ok((request, 0, None));
        }
        self.validate(work)?;
        self.request.descriptor().verify(bytes, work)?;
        let mut decoded = decode_interaction_json::<Interaction>(
            bytes,
            crate::executor::json::JsonValueSchema::Interaction,
            std::mem::size_of::<ChoiceOption>() as u64,
            work,
        )?;
        let owned = interaction_request_owned_bytes(&decoded.0, decoded.1.work())?;
        let release = decoded.1.retained().checked_sub(owned).ok_or_else(|| {
            interaction_denial(
                decoded.1.work(),
                crate::wire::ErrorCode::Invalid,
                "interaction DTO exceeds its original decoder custody",
            )
        })?;
        decoded.1.relinquish(release)?;
        decoded.0.validate(decoded.1.work())?;
        self.validate_request_binding(&decoded.0, decoded.1.work())?;
        let measured_owned_bytes = owned;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (custody, measured_owned_bytes) = if interaction_native_mode(decoded.1.work()) {
            let allocation = decoded
                .1
                .work()
                .as_deref_mut()
                .expect("observed interaction")
                .take_native_custody(owned)
                .expect("actual native DTO custody is a pure original split");
            (Some(allocation), 0)
        } else {
            (None, measured_owned_bytes)
        };
        decoded.1.transfer();
        let (request, quota) = decoded;
        drop(quota);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        // LAST: request fields drop before native custody.
        Ok((request, measured_owned_bytes, custody))
    }

    /// Validates a funded typed request after its original bytes were verified against this ticket's descriptor.
    pub(crate) fn validate_decoded_request(
        &self,
        request: &Interaction,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        request.validate(work)?;
        self.validate_request_binding(request, work)
    }

    fn validate_request_binding(
        &self,
        request: &Interaction,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if request.kind() != self.kind {
            return Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "interaction request kind mismatch",
            ));
        }
        match (request, &self.approval) {
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
                return Err(interaction_denial(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "approval action binding mismatch",
                ));
            }
            (_, None) => {}
            _ => {
                return Err(interaction_denial(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "non-approval carries an approval binding",
                ));
            }
        }
        Ok(())
    }

    /// Uses the already validated, caller-custodied request after response IO.
    pub(crate) fn validate_answer_bytes(
        &self,
        request: &Interaction,
        answer: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_answer_envelope(answer, work)?;
        answer.descriptor().verify(bytes, work)?;
        let mut response = decode_interaction_json::<InteractionResponse>(
            bytes,
            crate::executor::json::JsonValueSchema::InteractionResponse,
            std::mem::size_of::<String>() as u64,
            work,
        )?;
        let result = request.validate_response(&response.0, response.1.work());
        drop(response);
        result
    }

    /// Uses the same admitted request through the exact decision artifact check.
    pub(crate) fn validate_decision_bytes(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        detail: &FileRef,
        bytes: &[u8],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_decision_envelope(detail, work)?;
        detail.descriptor().verify(bytes, work)?;
        let mut response = decode_interaction_json::<InteractionResponse>(
            bytes,
            crate::executor::json::JsonValueSchema::InteractionResponse,
            std::mem::size_of::<String>() as u64,
            work,
        )?;
        let result =
            self.validate_decoded_decision(request, outcome, &response.0, response.1.work());
        drop(response);
        result
    }

    /// Borrowed counterpart for already verified/admitted producer-owned DTOs.
    pub(crate) fn validate_decision(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        detail: &FileRef,
        response: &InteractionResponse,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_decision_envelope(detail, work)?;
        self.validate_decoded_decision(request, outcome, response, work)
    }

    fn validate_answer_envelope(
        &self,
        answer: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.kind == InteractionKind::Approval {
            return Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "approval cannot use a free-form answer",
            ));
        }
        answer.validate(work)?;
        if answer.descriptor().media_type() != "application/json" {
            return Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "interaction answer must be JSON",
            ));
        }
        Ok(())
    }

    fn validate_decision_envelope(
        &self,
        detail: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.kind != InteractionKind::Approval
            || detail.descriptor().media_type() != "application/json"
        {
            return Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Invalid,
                "approval decision artifact is invalid",
            ));
        }
        detail.validate(work)
    }

    fn validate_decoded_decision(
        &self,
        request: &Interaction,
        outcome: &InteractionOutcome,
        response: &InteractionResponse,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        request.validate_response(response, work)?;
        match (outcome, response) {
            (
                InteractionOutcome::Approved,
                InteractionResponse::Approval { approved: true, .. },
            )
            | (
                InteractionOutcome::Declined,
                InteractionResponse::Approval {
                    approved: false, ..
                },
            ) => Ok(()),
            _ => Err(interaction_denial(
                work,
                crate::wire::ErrorCode::Conflict,
                "approval artifact disagrees with outcome",
            )),
        }
    }
}
