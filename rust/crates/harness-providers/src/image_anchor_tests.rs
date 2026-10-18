use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn image_usage_anchor_charges_only_unanchored_occurrences(
        prior in 1usize..4, added in 0usize..4, usage in 1u64..100_000,
        allowance in 1u32..16385, changed in any::<bool>(),
    ) {
            let bytes = base64::engine::general_purpose::STANDARD.decode(PNG).unwrap();
            let fixture: FileRef = serde_json::from_str(include_str!("../../harness/fixtures/v2/file-ref.json")).unwrap();
            let file = FileRef::new(fixture.volume().clone(), "attachments/image.png".into(), "pinned".into(),
                acyclic_harness::conversation::FileDescriptor::from_bytes(&bytes, "image/png", &mut None).unwrap(),
                "image".into(), &mut None).unwrap();
            let (mut provider, _) = provider(Adapter::Responses, 200);
            enable(&mut provider);
            provider.codex_login = Some(Arc::new(crate::CodexLogin::from_path(
                std::path::PathBuf::from("unused-for-image-counting.json"))));
            let bound = Arc::make_mut(&mut provider.config).images.get_mut(&provider.model.name).unwrap();
            bound.maximum_tokens = allowance;
            bound.maximum_images = 8;
            let make = |count| user((0..count).map(|_| {
                let mut part = image(&file);
                if let ModelContentPart::File { policy: FileProjectionPolicy::Native(p), .. } = &mut part {
                    p.maximum_work = u64::from(allowance);
                }
                part
            }).collect());
            let (first, _) = prepare(&provider, vec![make(prior)]).unwrap();
            let counter = &provider.codex_count;
            let count = |request: &PreparedModelRequest| futures::executor::block_on(
                crate::counting::count(&provider, request, None)).unwrap().0.input_tokens;
            let image_tokens = |m: &ModelMessage| crate::images::tokens(&provider, m);
            counter.observe(&first, &json!({"usage":{"input_tokens":usage}}), image_tokens).unwrap();
            assert_eq!(count(&first), usage);
            let mut messages = first.request().messages.clone();
            if added != 0 { messages.push(make(added)); }
            if changed { messages[0] = user(vec![text("unrelated prefix")]); }
            let (next, _) = prepare(&provider, messages).unwrap();
            let actual = count(&next);
            if !changed {
                if added == 0 { assert_eq!(actual, usage); }
                else {
                    assert!(actual >= usage + u64::from(allowance) * added as u64);
                    // Authoritative earlier images must never be added again.
                    let fresh = crate::codex_count::Counter::default().count(&next, image_tokens).unwrap();
                    let first_fresh = crate::codex_count::Counter::default().count(&first, image_tokens).unwrap();
                    assert!(actual.abs_diff(usage + fresh - first_fresh) <= 1);
                }
            } else {
                assert_eq!(actual, crate::codex_count::Counter::default().count(&next, image_tokens).unwrap());
            }
    }
}
