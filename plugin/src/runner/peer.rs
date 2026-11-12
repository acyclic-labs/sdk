//! Presentation adapter over the harness's prepared peer delivery.
use super::{Connection, Conversation};
use acyclic_harness::local::{agent_conversation::Sent, sessions};

pub(super) async fn send(
    connection: &Connection,
    from: &str,
    recipient: &str,
    intent: &str,
    message: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let sender = sessions::select(connection, from).await?;
    let recipient = sessions::select(connection, recipient).await?;
    let conversation = Conversation::open(sender);
    let prepared = conversation
        .prepare_peer_message(recipient.root().clone(), intent, message)
        .await?;
    match conversation.deliver_peer_message(&prepared).await? {
        Sent::Accepted(inbox) => Ok(serde_json::json!({
            "kind": "delivered", "from": conversation.agent(),
            "to": recipient.root(), "id": intent, "inbox": inbox,
        })),
        Sent::Refused(reason) => Err(reason.into()),
        // This bare remote client has no durable outbox. Never report a lost
        // acknowledgement as refusal or rebuild admission under the same intent.
        Sent::Unknown => Err("peer delivery outcome is unknown; inspect the original intent before sending another message".into()),
    }
}
