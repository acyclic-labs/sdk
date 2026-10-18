#[cfg(test)]
use crate as acyclic_harness;
pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::{
    AgentId,
    conversation::{
        Attachment, ContentResidencyVerifier, ConversationMessage, ConversationState,
        FileDescriptor, FileRef, Limits, MessageKind, ModelContextSelection, ReferencedAttachments,
        VolumeClass, VolumeOwner, VolumeRef,
    },
    executor::json::{JsonBounds, JsonValueSchema, json_from_slice_bounded},
    filesystem::PreparationIo,
    model::{ModelContentPart, ToolCall, ToolResultContent},
    projection::select_model_context,
    resources::ProviderRef,
    tool::{ToolInvocation, ToolResult},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, marker::PhantomData};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub invocation: ToolInvocation,
    pub metadata: Value,
}

struct Resolver(BTreeMap<String, Vec<u8>>);
impl ContentResidencyVerifier for Resolver {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> acyclic_harness::Result<
        acyclic_harness::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>,
    > {
        acyclic_harness::conversation::BoundedContentFuture::new(io, || async move {
            Ok(self
                .0
                .get(file.path())
                .expect("known fixture path")
                .clone()
                .into())
        })
    }
}

fn reference(path: &str, bytes: &[u8]) -> FileRef {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
            &mut None,
        )
        .unwrap(),
        path.into(),
        "v1".into(),
        fuzz_support::must_decode(FileDescriptor::from_bytes(
            bytes,
            "application/json",
            &mut None,
        )),
        "fixture".into(),
        &mut None,
    )
    .unwrap()
}

pub fn assert_replay(record: Record, limits: Limits) {
    if record
        .invocation
        .validate_recorded(&record.metadata, limits, None)
        .is_err()
    {
        return;
    }
    let encoded = serde_json::to_vec(&record).unwrap();
    assert!(encoded.len() as u64 <= limits.file_bytes.min(limits.render_bytes));
    let result = serde_json::to_vec(&ToolResult { value: Value::Null }).unwrap();
    let projection = serde_json::to_vec(&ToolResultContent::Json { value: Value::Null }).unwrap();
    let files = Resolver(BTreeMap::from([
        ("call.json".into(), encoded.clone()),
        ("result.json".into(), result.clone()),
        ("projection.json".into(), projection.clone()),
        ("user.json".into(), b"user".to_vec()),
    ]));
    let call_id = record.invocation.call_id.clone();
    let mut conversation = ConversationState::default();
    conversation.bind(AgentId::from_bytes([1; 16])).unwrap();
    for (index, kind, path, bytes, tool_call_id) in [
        (
            1u8,
            MessageKind::User,
            "user.json",
            b"user".as_slice(),
            None,
        ),
        (
            2,
            MessageKind::ToolCall,
            "call.json",
            encoded.as_slice(),
            Some(call_id.clone()),
        ),
        (
            3,
            MessageKind::ToolResult,
            "result.json",
            result.as_slice(),
            Some(call_id),
        ),
        (4, MessageKind::User, "user.json", b"user".as_slice(), None),
    ] {
        conversation
            .append(
                ConversationMessage {
                    id: uuid::Uuid::from_bytes([index; 16]),
                    sequence: u64::from(index),
                    kind,
                    content: reference(path, bytes),
                    attachments: ReferencedAttachments::Inline {
                        items: if index == 3 {
                            vec![Attachment {
                                file: reference("projection.json", &projection),
                                label: Some("model_projection".into()),
                            }]
                        } else {
                            Vec::new()
                        },
                    },
                    reply_to: match index {
                        2 => Some(uuid::Uuid::from_bytes([1; 16])),
                        3 => Some(uuid::Uuid::from_bytes([2; 16])),
                        _ => None,
                    },
                    tool_call_id,
                    extensions: BTreeMap::new(),
                },
                &mut None,
            )
            .unwrap();
    }
    let selection = ModelContextSelection {
        conversation_revision: 4,
        message_ids: conversation.messages().iter().map(|m| m.id).collect(),
        checkpoint: None,
    };
    let projected = futures::executor::block_on(select_model_context(
        &conversation,
        4,
        selection,
        &files,
        limits,
        8,
        None,
    ))
    .expect("admitted saved record must project");
    let call = projected
        .messages
        .iter()
        .flat_map(|m| m.content.parts())
        .find_map(|p| {
            if let ModelContentPart::ToolCall(call) = p {
                Some(call)
            } else {
                None
            }
        })
        .expect("saved call");
    assert_eq!(
        call,
        &ToolCall::new(
            record.invocation.call_id,
            record.invocation.name,
            record.invocation.arguments
        )
        .with_metadata(record.metadata)
    );
    let bytes = serde_json::to_vec(&projected).unwrap();
    let _ = fuzz_support::round_trip(
        &projected,
        json_from_slice_bounded(
            PhantomData,
            &bytes,
            JsonBounds {
                maximum_bytes: bytes.len() as u64,
                maximum_items: u64::MAX,
                maximum_depth: 256,
            },
            0,
            None,
            Some(JsonValueSchema::Root),
            &mut |_| Ok(()),
        ),
    );
}

pub fn check(fuzz_input: fuzz_support::FuzzInput<'_>) {
    if let Some(record) = fuzz_support::FuzzInput::parse(fuzz_input, |data| {
        json_from_slice_bounded(
            PhantomData::<Record>,
            data,
            JsonBounds {
                maximum_bytes: 4096,
                maximum_items: 4096,
                maximum_depth: 256,
            },
            0,
            None,
            Some(JsonValueSchema::Root),
            &mut |_| Ok(()),
        )
    }) {
        assert_replay(
            record,
            Limits {
                file_bytes: 4096,
                render_bytes: 4096,
                ..Limits::default()
            },
        );
    }
}
