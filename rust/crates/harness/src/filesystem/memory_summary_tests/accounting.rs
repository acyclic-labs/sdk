use super::*;
use crate::{
    conversation::FileDescriptor,
    model::{
        FileProjectionPolicy, ModelContent, ModelContentPart, ModelMessage, ModelRequest, ModelRole,
    },
};
use proptest::prelude::*;

fn message() -> impl Strategy<Value = (ModelMessage, u32)> {
    (
        0_u8..3,
        any::<bool>(),
        proptest::collection::vec(any::<char>(), 0..32),
        proptest::collection::vec(0_u16..2048, 0..5),
    )
        .prop_map(|(role, single, text, lengths)| {
            let role =
                [ModelRole::System, ModelRole::User, ModelRole::Assistant][usize::from(role)];
            let text: String = text.into_iter().collect();
            let tokens = 512 + lengths.iter().map(|length| u32::from(*length)).sum::<u32>();
            let mut parts = Vec::new();
            for length in lengths {
                parts.push(ModelContentPart::File {
                    file: FileRef::new(
                        VolumeRef::new(
                            ProviderRef::new(
                                "test".into(),
                                "filesystem".into(),
                                "2".into(),
                                &mut None,
                            )
                            .unwrap(),
                            "accounting".into(),
                            VolumeClass::AgentPrivate,
                            VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                            &mut None,
                        )
                        .unwrap(),
                        "file.txt".into(),
                        "pinned".into(),
                        FileDescriptor::from_bytes(
                            &vec![b'x'; usize::from(length)],
                            "text/plain",
                            &mut None,
                        )
                        .unwrap(),
                        "file.txt".into(),
                        &mut None,
                    )
                    .unwrap(),
                    policy: FileProjectionPolicy::BoundedFull,
                });
            }
            let content = if parts.is_empty() {
                if single {
                    ModelContent::Text(text)
                } else {
                    ModelContent::Part(ModelContentPart::Text { text })
                }
            } else if single && parts.len() == 1 {
                ModelContent::Part(parts.pop().unwrap())
            } else {
                parts.insert(0, ModelContentPart::Text { text });
                ModelContent::Parts(parts)
            };
            (ModelMessage { role, content }, tokens)
        })
}

proptest! {
    // For arbitrary request trees, only file bytes contribute beyond framing;
    // reordering messages permutes their charges and binds a fresh request digest.
    #[test]
    fn summary_accounting_is_additive_and_request_bound(
        messages in proptest::collection::vec(message(), 1..9),
    ) {
        let (messages, mut expected): (Vec<_>, Vec<_>) = messages.into_iter().unzip();
        let provider = summary_model(0);
        let mut request = ModelRequest {
            model: Model::new("test".into(), "summary".into(), "1".into(), json!({})).unwrap(),
            messages,
            tools: Vec::new(),
            max_output_tokens: Some(4096),
        };
        let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None).unwrap();
        let count = futures::executor::block_on(provider.count_tokens(&prepared)).unwrap();
        prop_assert_eq!(count.fixed_tokens, 512);
        prop_assert_eq!(&count.message_tokens, &expected);
        prop_assert_eq!(count.request_digest, prepared.request_digest());
        prop_assert_eq!(count.validate(&prepared, &mut None).unwrap(),
            512 + expected.iter().map(|tokens| u64::from(*tokens)).sum::<u64>());
        prop_assert_eq!(futures::executor::block_on(provider.count_tokens(&prepared)).unwrap(), count.clone());
        request.messages.reverse();
        expected.reverse();
        let reordered = PreparedModelRequest::prepare(request, Limits::default(), None).unwrap();
        prop_assert_eq!(count.validate(&reordered, &mut None).is_err(),
            reordered.request() != prepared.request());
        let reordered_count = futures::executor::block_on(provider.count_tokens(&reordered)).unwrap();
        prop_assert_eq!(&reordered_count.message_tokens, &expected);
        prop_assert_eq!(reordered_count.validate(&reordered, &mut None).unwrap(),
            count.validate(&prepared, &mut None).unwrap());
        let mut revised = reordered.request().clone();
        revised.model.revision = "2".into();
        let revised = PreparedModelRequest::prepare(revised, Limits::default(), None).unwrap();
        prop_assert!(reordered_count.validate(&revised, &mut None).is_err());
        prop_assert!(provider.dispatches().is_empty());
        prop_assert!(provider.requests().is_empty());
    }
}
