#![no_main]
use acyclic_harness::{
    AgentId, Result,
    client::{ClientSession, MemoryClientState},
    conversation::{
        Attachment, BoundedContentFuture, ContentResidencyVerifier, ConversationMessage,
        FileDescriptor, FileRef, MessageKind, ReferencedAttachments, VolumeClass, VolumeOwner,
        VolumeRef,
    },
    core::EventPayload,
    executor::PreparationIo,
    resources::ProviderRef,
    wire,
    wire_api::current_protocol,
};
use acyclic_stream::OwnedBytes;
use libfuzzer_sys::fuzz_target;

struct Files(OwnedBytes);
impl ContentResidencyVerifier for Files {
    fn read<'a>(
        &'a self,
        _: &'a FileRef,
        _: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        BoundedContentFuture::new(io, || async { Ok(self.0.clone()) })
    }
}

fuzz_target!(|input: &[u8]| {
    if input.is_empty() || input.len() > 4096 {
        return;
    }
    futures::executor::block_on(async {
        let (selector, bytes) = input.split_first().unwrap();
        let files = Files(bytes.to_vec().into());
        let reference = FileRef::new(
            VolumeRef::new(
                ProviderRef::new("fuzz".into(), "memory".into(), "1".into(), &mut None).unwrap(),
                "task".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )
            .unwrap(),
            "artifact.json".into(),
            "v1".into(),
            FileDescriptor::from_bytes(bytes, "application/json", &mut None).unwrap(),
            "artifact.json".into(),
            &mut None,
        )
        .unwrap();
        let mut message = ConversationMessage {
            id: acyclic_harness::conversation::assistant_message_id(
                acyclic_harness::OperationId::from_bytes([1; 16]),
            ),
            sequence: 1,
            kind: match selector % 3 {
                0 => MessageKind::ToolCall,
                1 => MessageKind::ToolResult,
                _ => MessageKind::Assistant,
            },
            content: reference.clone(),
            attachments: ReferencedAttachments::Inline { items: vec![] },
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        match message.kind {
            MessageKind::ToolCall => message.tool_call_id = Some("call".into()),
            MessageKind::ToolResult => {
                message.tool_call_id = Some("call".into());
                message.attachments = ReferencedAttachments::Inline {
                    items: vec![Attachment {
                        label: Some("model_projection".into()),
                        file: reference,
                    }],
                };
            }
            _ => {
                message
                    .extensions
                    .insert("acyclic.model.metadata".into(), reference);
            }
        }
        let authority = wire::Authority {
            kind: wire::AggregateKind::Conversation as i32,
            id: "task".into(),
        };
        let delivery = wire::Delivery {
            authority: Some(authority.clone()),
            generation: "g".into(),
            from_revision: 0,
            through_revision: 1,
            live: true,
            events: vec![wire::EventEnvelope {
                protocol: Some(current_protocol()),
                authority: Some(authority.clone()),
                revision: 1,
                operation_id: "00000000-0000-0000-0000-000000000001".into(),
                event_type: "conversation_message_appended".into(),
                canonical_payload_json: serde_json::to_vec(
                    &EventPayload::ConversationMessageAppended {
                        message: Box::new(message),
                    },
                )
                .unwrap(),
                ..Default::default()
            }],
        };
        let mut session = ClientSession::open(MemoryClientState::default()).unwrap();
        session
            .track(wire::ReplayCursor {
                authority: Some(authority),
                generation: "g".into(),
                revision: 0,
            })
            .unwrap();
        if session
            .receive_delivery(delivery.clone(), Some(&files), |_| Ok(()))
            .await
            .is_ok()
        {
            let expected = session.views().clone();
            let mut reopened = ClientSession::open(session.into_store()).unwrap();
            reopened
                .receive_delivery(delivery, Some(&files), |_| Ok(()))
                .await
                .unwrap();
            assert_eq!(reopened.views(), &expected);
        } else {
            assert_eq!(session.resume().cursors[0].revision, 0);
            assert_eq!(session.views(), &Default::default());
        }
    });
});
