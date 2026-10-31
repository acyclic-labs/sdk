use crate::*;
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn notice(key: Option<String>, position: usize, paths: Vec<String>) -> Notice {
    Notice {
        source: "filesystem".into(),
        kind: "arbitrary.new.kind".into(),
        body: json!({"position":position,"paths":paths}),
        wake: false,
        coalesce_key: key,
    }
}

proptest! {
    #[test]
    fn pending_notice_coalescing_matches_an_independent_reference(
        events in prop::collection::vec((prop::option::of("[a-c]{0,8}"), any::<bool>(), prop::collection::vec("[a-z/]{0,16}", 0..5)), 0..100)
    ) {
        let mut pending = PendingNotices::default();
        let mut expected = BTreeMap::new();
        let mut keyed: BTreeMap<String, (usize, BTreeSet<String>)> = BTreeMap::new();
        for (position, (key, wake, paths)) in events.into_iter().enumerate() {
            let mut event = notice(key.clone(), position, paths.clone());
            event.wake = wake;
            pending.push(event.clone()).unwrap();
            if let Some(key) = key.filter(|_| !wake) {
                let (last, union) = keyed.entry(key).or_insert_with(|| (position, BTreeSet::new()));
                expected.remove(last);
                *last = position;
                union.extend(paths);
                event.body["paths"] = json!(union);
            }
            expected.insert(position, event);
        }
        prop_assert_eq!(pending.finish(), expected.into_values().collect::<Vec<_>>());
    }

    #[test]
    fn frozen_notice_prefix_survives_later_non_waking_events(text in "[a-zA-Z<>\"&]{0,100}") {
        for family in [Family::Responses, Family::Chat, Family::Anthropic, Family::Gemini] {
            let target = Provenance {
                family, provider:"provider".into(), model:"model".into(),
                endpoint:"https://example.invalid".into(), credential_scope:None,
            };
            let header = Header::new(target.clone(), [1;32], RENDERER_VERSION,
                json!({"model":"model"}).as_object().unwrap()).unwrap();
            let first = notice(Some("files".into()), 1, vec![text.clone()]);
            let frozen = notice_block(target.clone(), &[first]).unwrap();
            let stored = serde_json::to_vec(&frozen).unwrap();
            let reopened: WireItem = serde_json::from_slice(&stored).unwrap();
            let mut before = Vec::new();
            render(&header, [&reopened], Tail, &mut before).unwrap();
            let later = notice_block(target.clone(), &[notice(Some("files".into()), 2, vec!["later".into()])]).unwrap();
            let mut after = Vec::new();
            render(&header, [&reopened, &later], Tail, &mut after).unwrap();
            let prefix = &before[..before.len()-Tail.bytes().len()];
            prop_assert!(after.starts_with(prefix));
            let rewritten = notice_block(target, &[notice(Some("files".into()), 3, vec!["rewritten".into()])]).unwrap();
            let mut negative = Vec::new();
            render(&header, [&rewritten, &later], Tail, &mut negative).unwrap();
            prop_assert!(!negative.starts_with(prefix), "editing rendered history must break the oracle");
            let value: Value = serde_json::from_slice(&after).unwrap();
            let field = match family { Family::Responses=>"input", Family::Gemini=>"contents", _=>"messages" };
            let items = value[field].as_array().unwrap();
            prop_assert_eq!(items.len(), 2);
            prop_assert_eq!(items[0]["role"].as_str(), Some(if matches!(family, Family::Responses | Family::Chat) {"developer"} else {"user"}));
            if family == Family::Anthropic {
                let content = items[0]["content"].as_str().unwrap();
                prop_assert_eq!(content.matches("<notice ").count(), 1);
                prop_assert_eq!(content.matches("</notice>").count(), 1);
            }
        }
    }
}

#[test]
fn malformed_pending_notice_does_not_change_the_batch() {
    let mut pending = PendingNotices::default();
    let original = notice(None, 0, vec!["p".into()]);
    pending.push(original.clone()).unwrap();
    let mut invalid = original.clone();
    invalid.body = json!({"paths":[1]});
    assert!(pending.push(invalid).is_err());
    assert_eq!(pending.finish(), vec![original]);
}
