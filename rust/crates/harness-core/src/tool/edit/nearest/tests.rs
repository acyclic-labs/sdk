use super::*;
use proptest::prelude::*;

#[test]
fn normalized_literal_spans_preserve_original_unicode_offsets() -> Result<()> {
    let strings: Vec<String> = (0..=4)
        .flat_map(|length| {
            (0..3usize.pow(length)).map(move |mut digits| {
                (0..length)
                    .map(|_| {
                        let c = ['a', 'é', '🦀'][digits % 3];
                        digits /= 3;
                        c
                    })
                    .collect()
            })
        })
        .collect();
    for source in &strings {
        for needle in strings.iter().filter(|needle| !needle.is_empty()) {
            if let Some(start) = source.find(needle) {
                assert_eq!(
                    closest(source, needle, &mut None)?,
                    start..start + needle.len()
                );
            }
        }
    }
    assert_eq!(
        closest("é\n  \"🦀\"  \nend", "\\\"🦀\\\"", &mut None)?,
        5..11
    );
    let blank_lines = rejection("a\n\n", "b\n\n", &None)?.to_string();
    assert!(blank_lines.contains("lines 1-2"), "{blank_lines}");
    assert!(blank_lines.contains(r#"+ "a\n\n""#), "{blank_lines}");
    Ok(())
}

fn oracle_bag(text: &str) -> std::collections::BTreeMap<Vec<char>, usize> {
    let mut bag = std::collections::BTreeMap::new();
    for line in text.lines() {
        let chars: Vec<_> = line.chars().filter(|c| !c.is_whitespace()).collect();
        for c in &chars {
            *bag.entry(vec![*c]).or_default() += 1;
        }
        for pair in chars.windows(2) {
            *bag.entry(pair.to_vec()).or_default() += 1;
        }
    }
    bag
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn rolling_windows_agree_with_an_independent_full_window_oracle(source in "[abé\n]{0,48}", needle in "[abé\n]{1,16}") {
        let lines: Vec<_> = source.split_inclusive('\n').collect();
        let count = needle.lines().count().max(1);
        let expected = oracle_bag(&needle);
        let mut best = None;
        let mut start = 0;
        for index in 0..lines.len().saturating_sub(count).saturating_add(1) {
            let end = start + lines.iter().skip(index).take(count).map(|line| line.len()).sum::<usize>();
            let actual = oracle_bag(&source[start..end]);
            let common: usize = expected.iter().map(|(key,n)| (*n).min(*actual.get(key).unwrap_or(&0))).sum();
            let total = expected.values().sum::<usize>() + actual.values().sum::<usize>();
            if best.as_ref().is_none_or(|(n,d,_): &(usize,usize,Range<usize>)| common * *d > *n * total) {
                best = Some((common,total,start..end));
            }
            if let Some(line) = lines.get(index) { start += line.len(); }
        }
        let mut expected = best.map_or(0..0, |(_,_,range)| range);
        if !needle.ends_with('\n') && source[expected.clone()].ends_with('\n') { expected.end -= 1; }
        let actual = ranked_window(&source, &needle, needle.chars().filter(|c| !c.is_whitespace()).count(), &mut None).unwrap();
        prop_assert_eq!(actual, expected);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn portable_and_native_miss_search_do_not_multiply_file_and_needle_lengths() -> Result<()> {
    let source = "a".repeat(100_000);
    let needle = "b".repeat(10_000);
    let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
    budget.items_examined = 2_000_000;
    assert!(source.len() * needle.len() > budget.items_examined as usize);
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let mut custody = allocation(Some(&work), 0)?;
    let native = closest(&source, &needle, &mut custody)?;
    assert_eq!(closest(&source, &needle, &mut None)?, native);
    assert!(work.receipt().items_examined < budget.items_examined);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn refused_edits_retain_a_replayable_diagnostic_without_changing_exact_semantics() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let original = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = original.live_allocation_bytes();
    for (source, needle, hint, actual) in [
        (
            "header\n\"Consumption rules\"\nfooter",
            "\\\"Consumption rules\\\"",
            Some("escaped-quote mismatch"),
            "\"Consumption rules\"",
        ),
        (
            "header\nlet x = 1;\nfooter",
            "let  x = 1;",
            Some("whitespace-only mismatch"),
            "let x = 1;",
        ),
        (
            "header\nlet x = 1;\nfooter",
            "let x = 2;",
            None,
            "let x = 1;",
        ),
        ("", "not present", None, ""),
    ] {
        for all in [false, true] {
            let mut custody = allocation(Some(&original), 0)?;
            let error = replace(source, needle, "WRONG", all, None, &mut custody).unwrap_err();
            assert_eq!(
                custody.as_ref().unwrap().bytes(),
                0,
                "planner scratch retired"
            );
            assert_eq!(error.code(), crate::wire::ErrorCode::Conflict);
            assert!(!error.is_native_failure());
            let portable = replace(source, needle, "WRONG", all, None, &mut None).unwrap_err();
            assert_eq!(error.to_string(), portable.to_string());
            let text = error.to_string();
            assert!(text.contains("diff (- attempted, + actual)"), "{text}");
            assert!(text.contains(&format!("+ {actual:?}")), "{text}");
            if let Some(hint) = hint {
                assert!(text.contains(hint), "{text}");
            } else {
                assert!(!text.contains("mismatch"), "{text}");
            }
            if !source.is_empty() {
                assert!(text.contains("lines 2-2"), "{text}");
            }
            let io = crate::executor::PreparationIo::from_native_work(&original)?;
            let cause = crate::executor::with_history_io(Some(&io), |history| {
                crate::executor::ToolFailureCause::of(&error, history)
            })?;
            assert!(cause.is_canonical());
            assert!(
                cause.message.contains("+ "),
                "sanitized replay retains diff labels"
            );
            let encoded = serde_json::to_vec(&cause).unwrap();
            let replayed: crate::executor::ToolFailureCause =
                serde_json::from_slice(&encoded).unwrap();
            assert_eq!(replayed, cause);
            drop(cause);
            drop(io);
            drop(error);
            drop(custody);
            assert_eq!(original.live_allocation_bytes(), baseline);
        }
        // Negative control: only the actual exact text can authorize replacement.
        if !actual.is_empty() {
            assert_eq!(
                replace(source, actual, "OK", false, None, &mut None)?,
                source.replacen(actual, "OK", 1)
            );
        }
    }
    // A real ambiguity remains an ambiguity, with no misleading nearest suggestion.
    let error = replace("same same", "same", "x", false, None, &mut None).unwrap_err();
    assert!(error.to_string().contains("ambiguous"));
    assert!(!error.to_string().contains("closest"));
    cancellation.cancel();
    let custody = allocation(Some(&original), 0);
    assert!(custody.is_err());
    Ok(())
}
