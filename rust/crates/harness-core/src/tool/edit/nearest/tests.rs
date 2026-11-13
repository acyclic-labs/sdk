use super::*;

fn distance(left: &str, right: &str) -> usize {
    let mut row: Vec<_> = (0..=right.chars().count()).collect();
    for (i, a) in left.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, b) in right.chars().enumerate() {
            let previous = row[j + 1];
            row[j + 1] = (diagonal + usize::from(a != b))
                .min(previous + 1)
                .min(row[j] + 1);
            diagonal = previous;
        }
    }
    *row.last().unwrap()
}

#[test]
fn closest_span_agrees_with_exhaustive_unicode_substrings() -> Result<()> {
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
            let mut expected = None;
            for start in source.char_indices().map(|(offset, _)| offset) {
                for end in source
                    .char_indices()
                    .map(|(offset, c)| offset + c.len_utf8())
                    .filter(|end| *end > start)
                {
                    let candidate = (distance(&source[start..end], needle), start, end);
                    if expected.is_none_or(|best| candidate < best) {
                        expected = Some(candidate);
                    }
                }
            }
            let expected = expected.map_or(0..0, |(_, start, end)| start..end);
            assert_eq!(
                closest(source, needle, &mut None)?,
                expected,
                "{source:?} / {needle:?}"
            );
        }
    }
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
