use super::*;

/// Semantic event type retained in a conversation history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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

impl ConversationMessage {}

crate::core::ownership::owned_enum!(MessageKind {
    User,
    Assistant,
    System,
    ToolCall,
    ToolResult,
    Interaction,
    Permission,
    Fork,
    Merge,
});
crate::core::ownership::owned_struct!(ConversationMessage {
    id,
    sequence,
    kind,
    content,
    attachments,
    reply_to,
    tool_call_id,
    extensions
});
