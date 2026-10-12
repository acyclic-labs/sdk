use super::*;

impl ContextSource for Context {
    fn load<'a>(
        &'a self,
        _: &'a ContextInput,
        limits: Limits,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ModelMessage>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            validate_source_messages(&self.messages, limits, io)?;
            crate::executor::with_history_io(io, |work| {
                compaction::clone_messages(&self.messages, None, &[], work)
                    .map(|(messages, _)| messages)
            })
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native(&self) -> bool {
        true
    }
}
/// Mutable context assembled for one model step.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Context {
    /// Ordered model-visible messages.
    #[cfg_attr(feature = "wasm", tsify(type = "ModelMessage[]"))]
    pub messages: Vec<ModelMessage>,
    /// Stage-owned, namespaced version-pinned metadata files.
    #[cfg_attr(feature = "wasm", tsify(type = "Record<string, WasmFileRefWire>"))]
    pub metadata: BTreeMap<String, FileRef>,
    /// Position of the active turn input.
    #[cfg_attr(feature = "wasm", tsify(optional))]
    pub current_input_index: Option<u32>,
}
impl Context {
    pub(super) fn validate_current_input(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        if self
            .current_input_index
            .is_some_and(|index| index as usize >= self.messages.len())
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "current input index is outside context",
            ));
        }
        Ok(())
    }
}
/// A borrowed view of exactly the messages that will form a base or continuation.
pub(super) struct ContextMessages<'a> {
    pub(super) first: &'a [ModelMessage],
    pub(super) user: Option<&'a ModelContent>,
    pub(super) second: &'a [ModelMessage],
}
impl Serialize for ContextMessages<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;

        #[derive(Serialize)]
        struct UserMessage<'a> {
            role: ModelRole,
            content: &'a ModelContent,
        }

        let mut sequence = serializer.serialize_seq(None)?;
        for message in self.first {
            sequence.serialize_element(message)?;
        }
        if let Some(content) = self.user {
            sequence.serialize_element(&UserMessage {
                role: ModelRole::User,
                content,
            })?;
        }
        for message in self.second {
            sequence.serialize_element(message)?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
pub(super) struct BorrowedContext<'a> {
    pub(super) messages: ContextMessages<'a>,
    pub(super) metadata: &'a BTreeMap<String, FileRef>,
    pub(super) current_input_index: Option<u32>,
}
impl BorrowedContext<'_> {
    pub(super) fn validate_bounds(
        &self,
        limits: Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let count = self
            .messages
            .first
            .len()
            .checked_add(usize::from(self.messages.user.is_some()))
            .and_then(|count| count.checked_add(self.messages.second.len()))
            .filter(|count| *count <= limits.context_messages);
        if count.is_none() || self.metadata.len() > limits.attachments {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "context projection exceeded declared bounds",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            for message in self.messages.first.iter().chain(self.messages.second) {
                message.content.validate_limits(limits, work)?;
            }
            if let Some(content) = self.messages.user {
                content.validate_limits(limits, work)?;
            }
            crate::contract::validate_trusted_json_byte_bound(self, limits.render_bytes, work)
        })
    }
}
/// Inputs visible to every context stage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextInput {
    /// Current durable turn input.
    pub input: ModelContent,
    /// Recorded canonical selection; when present it replaces the synthesized one-message context without changing its provenance.
    #[serde(default)]
    pub selected_context: Option<SelectedModelContext>,
    /// Zero-based executor step.
    pub step: u32,
    /// Model-visible results accumulated by earlier executor steps.
    pub prior_messages: Vec<ModelMessage>,
}
impl ContextInput {
    pub(crate) fn copy_for_turn(
        turn: &crate::executor::TurnInput,
        step: u32,
        prior: &[ModelMessage],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        let input = crate::executor::with_history_io(io, |work| {
            crate::model::clone_content(&turn.input, work).map(|(copy, _)| copy)
        })?;
        let selected_context = turn
            .selected_context
            .as_ref()
            .map(|selected| {
                crate::executor::with_history_io(io, |work| {
                    if work.is_none() {
                        return Ok::<_, crate::Error>(selected.clone());
                    }
                    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                    let mut message_ids = Vec::new();
                    let copied = (selected.selection.message_ids.len()
                        * std::mem::size_of::<uuid::Uuid>())
                        as u64;
                    quota
                        .work()
                        .as_deref()
                        .expect("observed selection")
                        .admit_copy(copied)?;
                    compaction::reserve_vector(
                        &mut message_ids,
                        selected.selection.message_ids.len(),
                        quota.work(),
                    )?;
                    quota.adopt(
                        (message_ids.capacity() * std::mem::size_of::<uuid::Uuid>()) as u64,
                    )?;
                    let result = (|| {
                        quota
                            .work()
                            .as_deref_mut()
                            .expect("observed selection")
                            .copy_bytes(copied)?;
                        message_ids.extend_from_slice(&selected.selection.message_ids);
                        let checkpoint = match &selected.selection.checkpoint {
                            Some(file) => Some(file.clone_in_quota(&mut quota)?),
                            None => None,
                        };
                        let (messages, claim) = compaction::clone_messages(
                            &selected.messages,
                            None,
                            &[],
                            quota.work(),
                        )?;
                        quota.adopt(claim)?;
                        Ok((checkpoint, messages))
                    })();
                    let (checkpoint, messages) = match result {
                        Ok(value) => value,
                        Err(error) => {
                            drop(message_ids);
                            return Err(error);
                        }
                    };
                    quota.transfer();
                    Ok(SelectedModelContext {
                        selection: crate::conversation::ModelContextSelection {
                            conversation_revision: selected.selection.conversation_revision,
                            message_ids,
                            checkpoint,
                        },
                        messages,
                    })
                })
            })
            .transpose()?;
        let prior_messages = crate::executor::with_history_io(io, |work| {
            compaction::clone_messages(prior, None, &[], work).map(|(copy, _)| copy)
        })?;
        Ok(Self {
            input,
            selected_context,
            step,
            prior_messages,
        })
    }
}
pub(crate) fn copy_context(
    context: &Context,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Context> {
    crate::executor::with_history_io(io, |work| {
        compaction::clone_context(context, work).map(|(copy, _)| copy)
    })
}
