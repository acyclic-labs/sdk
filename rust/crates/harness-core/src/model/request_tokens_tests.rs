//! Additive accounting counts wire bytes once and keeps content out of framing.
use super::*;
use proptest::prelude::*;

#[test]
fn continuing_an_admitted_request_transfers_every_history_buffer_at_one_to_one_thousand_messages()
-> Result<()> {
    for count in [1, 10, 100, 1_000] {
        let request = ModelRequest {
            model: Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?,
            messages: (0..count)
                .map(|index| ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(format!("retained message {index}")),
                })
                .collect(),
            tools: Vec::new(),
            max_output_tokens: Some(1),
        };
        let prepared = PreparedModelRequest::prepare(request, Limits::default(), None)?;
        let slots = prepared.request().messages.as_ptr();
        let strings: Vec<_> = prepared
            .request()
            .messages
            .iter()
            .map(|message| {
                let ModelContent::Text(text) = &message.content else {
                    unreachable!()
                };
                text.as_ptr()
            })
            .collect();
        // The old path owns a new vector and duplicates every historical text.
        // Keep it alive through the comparison so allocator reuse cannot mask it.
        let copied = clone_request(prepared.request(), None)?;
        assert_ne!(copied.messages.as_ptr(), slots);
        let continued = prepared
            .into_continuation()
            .finish(Limits::default(), None)?;
        let continued = continued.request();
        assert_eq!(continued, &copied);
        assert_eq!(
            continued.messages.as_ptr(),
            slots,
            "history vector was copied at {count}"
        );
        for ((message, copied), original) in
            continued.messages.iter().zip(&copied.messages).zip(strings)
        {
            let (ModelContent::Text(text), ModelContent::Text(copy)) =
                (&message.content, &copied.content)
            else {
                unreachable!()
            };
            assert_eq!(
                text.as_ptr(),
                original,
                "history text was copied at {count}"
            );
            assert_ne!(copy.as_ptr(), original);
        }
    }
    Ok(())
}

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
