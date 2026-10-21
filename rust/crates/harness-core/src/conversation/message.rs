use super::*;

/// Semantic event type retained in a conversation history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Human input.
    User,
    /// Agent output.
    Assistant,
    /// Instruction supplied by the host.
    System,
    /// Agent request to invoke a tool.
    ToolCall,
    /// Complete result of a tool call.
    ToolResult,
    /// Question, answer, or other human interaction.
    Interaction,
    /// Permission request or resolution.
    Permission,
    /// Fork announcement or inherited-context reference.
    Fork,
    /// Project merge notice.
    Merge,
}

/// One canonical, payload-free conversation record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMessage {
    /// Caller-assigned stable identity.
    pub id: Uuid,
    /// Ordered position within this conversation, starting at one.
    pub sequence: u64,
    /// Message type.
    pub kind: MessageKind,
    /// Primary text or structured body, stored as a file.
    pub content: FileRef,
    /// Ordered file attachments.
    pub attachments: ReferencedAttachments,
    /// Causal parent message within this conversation.
    pub reply_to: Option<Uuid>,
    /// Tool call identity shared by a call/result pair.
    pub tool_call_id: Option<String>,
    /// Namespaced extension revisions; values are refs rather than arbitrary JSON.
    pub extensions: BTreeMap<String, FileRef>,
}

impl ConversationMessage {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.id.is_nil() || self.reply_to.is_some_and(|id| id.is_nil()) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "message identity is nil",
            ));
        }
        if self.sequence == 0 {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "message sequence must start at one",
            ));
        }
        self.content.validate(work)?;
        self.attachments.validate(work)?;
        for (name, reference) in &self.extensions {
            validate_label(name, work)?;
            if !name.contains('.') {
                return Err(conversation_error(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "extension key must be namespaced",
                ));
            }
            reference.validate(work)?;
        }
        let has_call = self.tool_call_id.is_some();
        if has_call != matches!(self.kind, MessageKind::ToolCall | MessageKind::ToolResult) {
            return Err(conversation_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "tool linkage does not match message kind",
            ));
        }
        if let Some(call) = &self.tool_call_id {
            validate_label(call, work)?;
        }
        Ok(())
    }
}

impl ConversationMessage {
    pub(crate) fn clone_in_quota(
        &self,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() {
            return Ok(self.clone());
        }
        self.validate(quota.work())?;
        let content = self.content.clone_in_quota(quota)?;
        let attachments = match &self.attachments {
            ReferencedAttachments::Inline { items } => {
                let items = crate::core::copy_vector(items, quota, |item, quota| {
                    let file = item.file.clone_in_quota(quota)?;
                    let label = item
                        .label
                        .as_deref()
                        .map(|label| crate::core::copy_text(label, quota))
                        .transpose()?;
                    Ok(Attachment { file, label })
                })?;
                ReferencedAttachments::Inline { items }
            }
            ReferencedAttachments::Manifest {
                manifest,
                item_count,
            } => ReferencedAttachments::Manifest {
                manifest: manifest.clone_in_quota(quota)?,
                item_count: *item_count,
            },
        };
        let tool_call_id = self
            .tool_call_id
            .as_deref()
            .map(|id| crate::core::copy_text(id, quota))
            .transpose()?;
        let mut extensions = BTreeMap::new();
        for (key, file) in &self.extensions {
            let bytes = std::mem::size_of::<(String, FileRef)>() as u64;
            if let Some(io) = quota.work().as_deref_mut() {
                io.admit_owned(bytes)?;
                io.retain_owned(bytes)?;
            }
            quota.adopt(bytes)?;
            let key = crate::core::copy_text(key, quota)?;
            let file = file.clone_in_quota(quota)?;
            extensions.insert(key, file);
        }
        Ok(Self {
            id: self.id,
            sequence: self.sequence,
            kind: self.kind.clone(),
            content,
            attachments,
            reply_to: self.reply_to,
            tool_call_id,
            extensions,
        })
    }
}
