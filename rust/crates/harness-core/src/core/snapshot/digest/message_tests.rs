use super::*;
use crate::conversation::{Attachment, MessageKind, ReferencedAttachments};

struct Uncached<'a>(&'a [ConversationMessage]);

impl Serialize for Uncached<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for message in self.0 {
            sequence.serialize_element(&Canonical(message))?;
        }
        sequence.end()
    }
}

fn encode(value: &impl Serialize) -> Result<(Vec<u8>, u64)> {
    let mut bytes = None;
    let allocation = acyclic_test_support::measure(|| {
        bytes = Some(serde_json::to_vec(value));
    });
    Ok((
        bytes
            .expect("encoder ran")
            .map_err(|error| Error::Invalid(error.to_string()))?,
        allocation.count_total,
    ))
}

#[test]
fn checkpoint_messages_reuse_only_an_equal_complete_content_reference() -> Result<()> {
    let file = crate::core::tests::request_file(b"repeated canonical content")?;
    let mut messages = (1..=128)
        .map(|sequence| ConversationMessage {
            id: uuid::Uuid::from_u128(sequence.into()),
            sequence,
            kind: MessageKind::User,
            content: file.clone(),
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    let (expected, old_allocations) = encode(&Uncached(&messages))?;
    let (actual, allocations) = encode(&CanonicalMessages(&messages))?;
    assert_eq!(actual, expected);
    assert!(
        allocations * 2 < old_allocations,
        "repeated references were re-encoded: {allocations} versus {old_allocations} allocations"
    );

    let original =
        serde_json::to_value(&file).map_err(|error| Error::Invalid(error.to_string()))?;
    let mut another_volume = original["volume"].clone();
    another_volume["id"] = serde_json::json!("other-volume");
    for (field, value) in [
        ("volume", another_volume),
        ("path", serde_json::json!("different/content")),
        ("version", serde_json::json!("another-version")),
        ("display_name", serde_json::json!("escaped\"name")),
        (
            "descriptor",
            serde_json::json!({
                "sha256": vec![9_u8; 32], "byte_length": 17, "media_type": "application/octet-stream",
            }),
        ),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        messages[1].content =
            serde_json::from_value(changed).map_err(|error| Error::Invalid(error.to_string()))?;
        messages[1].kind = MessageKind::ToolCall;
        messages[1].tool_call_id = Some("escaped\n\"call".into());
        messages[1].reply_to = Some(messages[0].id);
        messages[1]
            .extensions
            .insert("example.content".into(), file.clone());
        messages[1].attachments = ReferencedAttachments::Inline {
            items: vec![Attachment {
                file: file.clone(),
                label: Some("attached\n\"file".into()),
            }],
        };
        assert_eq!(
            encode(&CanonicalMessages(&messages))?.0,
            encode(&Uncached(&messages))?.0,
            "canonical message field {field} changed"
        );
    }
    Ok(())
}
