use super::*;

#[cfg(not(target_arch = "wasm32"))]
fn native_conversation_budget() -> acyclic_fs_work::performance::WorkBudget {
    acyclic_fs_work::performance::WorkBudget {
        allocation_operations: 100_000,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        items_examined: 16_777_216,
        ..acyclic_fs_work::performance::WorkBudget::default()
    }
}

fn message(content: &FileRef, sequence: u64) -> ConversationMessage {
    ConversationMessage {
        id: Uuid::from_u128(u128::from(sequence)),
        sequence,
        kind: if sequence % 2 == 1 {
            MessageKind::User
        } else {
            MessageKind::Assistant
        },
        content: content.clone(),
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: sequence
            .is_multiple_of(2)
            .then(|| Uuid::from_u128(u128::from(sequence - 1))),
        tool_call_id: None,
        extensions: BTreeMap::new(),
    }
}

#[path = "../../tests/conversation_contract/fixture.rs"]
mod fixture;
pub(super) use fixture::file;

mod history;
mod native;
mod security;
mod wire;
