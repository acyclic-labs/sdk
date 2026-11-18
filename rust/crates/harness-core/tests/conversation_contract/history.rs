use super::fixture::{file, message};
use acyclic_harness_core::{
    AgentId, Error, Result,
    conversation::{ConversationMessage, ConversationState, MessageKind},
};
use uuid::Uuid;

#[test]
fn history_rebuilds_and_pinned_pages_exclude_later_appends() -> Result<()> {
    let agent = AgentId::new();
    let mut state = ConversationState::default();
    state.bind(agent)?;
    let content = file(agent, "history.txt")?;
    for sequence in 1..=65 {
        state.append(
            ConversationMessage {
                id: Uuid::new_v4(),
                kind: MessageKind::User,
                reply_to: None,
                ..message(&content, sequence)
            },
            &mut None,
        )?;
    }
    let pinned = 64;
    let bytes = serde_json::to_vec(&state).map_err(|error| Error::Invalid(error.to_string()))?;
    let reopened: ConversationState =
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(reopened, state);
    let mut after = 0;
    let mut visited = Vec::new();
    loop {
        let page = reopened.page(after, pinned, 7)?;
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 7);
        for message in page {
            assert_eq!(reopened.message(message.id), Some(message));
            visited.push(message.sequence);
        }
        after = page
            .last()
            .ok_or_else(|| Error::Invalid("empty page".into()))?
            .sequence;
    }
    assert_eq!(visited, (1..=pinned).collect::<Vec<_>>());
    assert!(reopened.page(0, 66, 7).is_err());
    assert!(reopened.page(0, pinned, 0).is_err());
    let mut corrupt =
        serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
    corrupt["messages"][1]["id"] = corrupt["messages"][0]["id"].clone();
    assert!(serde_json::from_value::<ConversationState>(corrupt).is_err());
    Ok(())
}
