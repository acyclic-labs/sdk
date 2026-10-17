//! Additive accounting counts wire bytes once and keeps content out of framing.
use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn serialized_accounting_partitions_framing_and_message_content(
        texts in prop::collection::vec(prop::collection::vec(any::<char>(), 0..512), 1..8),
        output in 1u32..=u32::MAX,
    ) {
        let request = ModelRequest {
            model: Model::new("scripted".into(), "test".into(), "1".into(), Value::Null).unwrap(),
            messages: texts.into_iter().map(|text| ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(text.into_iter().collect()),
            }).collect(),
            tools: Vec::new(),
            max_output_tokens: Some(output),
        };
        let prepared = PreparedModelRequest::prepare(request.clone(), Limits::default(), None).unwrap();
        let count = prepared.serialized_token_upper_bound(None).unwrap().0;
        prop_assert_eq!(count.validate(&prepared, &mut None).unwrap(), prepared.bytes().len() as u64);
        for (message, tokens) in request.messages.iter().zip(&count.message_tokens) {
            prop_assert_eq!(u64::from(*tokens), serde_json::to_vec(message).unwrap().len() as u64);
        }
        let mut empty = request;
        for message in &mut empty.messages {
            message.content = ModelContent::Text(String::new());
        }
        let empty = PreparedModelRequest::prepare(empty, Limits::default(), None).unwrap();
        let framing = empty.serialized_token_upper_bound(None).unwrap().0;
        prop_assert_eq!(count.fixed_tokens, framing.fixed_tokens);
    }
}
