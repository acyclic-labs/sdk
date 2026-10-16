use super::*;
use proptest::{prelude::*, test_runner::TestRunner};

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(
        prop_oneof![
            Just('a'),
            Just('b'),
            Just('\n'),
            any::<char>().prop_filter("text without NUL", |c| *c != '\0'),
        ],
        0..80,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

fn bytes() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        text().prop_map(String::into_bytes),
        proptest::collection::vec(any::<u8>(), 0..256)
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // A thin side has at most four lines, so frontier width and repeated snake
    // comparisons are constant-bounded. Every case MUST succeed with work
    // linear in input bytes/lines, rather than ignoring budget exhaustion.
    #[test]
    fn additions_deletions_and_thin_sides_finish_with_linear_work(
        count in 1_usize..4096,
        line in prop_oneof![Just("a".to_owned()), "[a-zλé]{0,8}"],
        short in proptest::collection::vec("[a-zλé]{0,8}", 0..5),
    ) {
        let long = format!("{line}\n").repeat(count);
        let short: String = short.iter().map(|line| format!("{line}\n")).collect();
        let prefixed = format!("{short}{long}");
        for (before, after) in [
            (None, Some(long.as_str())), (Some(long.as_str()), None),
            (Some(""), Some(long.as_str())), (Some(long.as_str()), Some("")),
            (Some(short.as_str()), Some(long.as_str())),
            (Some(long.as_str()), Some(short.as_str())),
            (Some(short.as_str()), Some(prefixed.as_str())),
            (Some(prefixed.as_str()), Some(short.as_str())),
        ] {
            let bytes = before.unwrap_or_default().len() + after.unwrap_or_default().len();
            let lines = before.unwrap_or_default().lines().count() + after.unwrap_or_default().lines().count();
            let diff = FileDiff::between(
                before.map(str::as_bytes), after.map(str::as_bytes),
                4 * bytes + 256, 64 * (bytes + lines + 1),
            ).expect("linear work must suffice for empty/thin/prefix inputs");
            if let Some(diff) = diff {
                let patch = diffy::Patch::from_str(&diff.patch).expect("patch");
                prop_assert_eq!(diffy::apply(before.unwrap_or_default(), &patch).expect("apply"), after.unwrap_or_default());
            } else {
                prop_assert_eq!(before, after);
            }
        }
    }

    // Every accepted text patch reconstructs the endpoint, uses the minimum
    // number of edits, and round-trips through the client model's JSON codec.
    #[test]
    fn text_patches_reconstruct_with_minimal_counts(
        before in proptest::option::of(text()),
        after in proptest::option::of(text()),
    ) {
        let diff = FileDiff::between(
            before.as_deref().map(str::as_bytes),
            after.as_deref().map(str::as_bytes), 8192, 1_048_576,
        ).expect("bounded generated text");
        let Some(diff) = diff else {
            prop_assert_eq!(before, after);
            return Ok(());
        };
        prop_assert!(!diff.binary);
        let patch = diffy::Patch::from_str(&diff.patch).expect("generated unified patch");
        let before = before.as_deref().unwrap_or_default();
        let after = after.as_deref().unwrap_or_default();
        prop_assert_eq!(diffy::apply(before, &patch).expect("patch applies"), after);
        let oracle = diffy::create_patch(before, after);
        let (mut added, mut removed) = (0, 0);
        for line in oracle.hunks().iter().flat_map(|hunk| hunk.lines()) {
            match line {
                diffy::Line::Insert(_) => added += 1,
                diffy::Line::Delete(_) => removed += 1,
                _ => {}
            }
        }
        prop_assert_eq!((diff.added, diff.removed), (added, removed));
        let encoded = serde_json::to_vec(&diff).expect("encode diff");
        prop_assert_eq!(serde_json::from_slice::<FileDiff>(&encoded).expect("decode diff"), diff);
    }

    // Increasing the work budget cannot change a successful result; decreasing
    // the render bound below its patch size must reject before retaining it.
    #[test]
    fn arbitrary_bytes_obey_work_and_render_bounds(
        before in proptest::option::of(bytes()),
        after in proptest::option::of(bytes()),
        work in 0_usize..8192,
    ) {
        let (before, after) = (before.as_deref(), after.as_deref());
        if let Ok(diff) = FileDiff::between(before, after, 8192, work) {
            prop_assert_eq!(FileDiff::between(before, after, 8192, work + 1).expect("larger budget"), diff.clone());
            if let Some(diff) = diff {
                prop_assert!(diff.patch.len() <= 8192);
                if !diff.patch.is_empty() {
                    prop_assert!(FileDiff::between(before, after, diff.patch.len() - 1, work).is_err());
                }
            }
        }
    }
}

#[test]
fn publication_failures_are_atomic_for_arbitrary_text_and_budgets() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("property runtime");
    let strategy = (text(), text(), 1_usize..4096, 1_usize..512);
    let mut runner = TestRunner::new(ProptestConfig::with_cases(32));
    runner
        .run(&strategy, |(before, after, work, render_bytes)| {
            runtime.block_on(async {
                let fixture = Fixture::new().await.expect("fixture");
                fixture.put("a.txt", &before, "base").await.expect("base");
                let base = fixture.source("a.txt").await.expect("base reference");
                fixture.put("a.txt", &after, "edit").await.expect("edit");
                let current = fixture.source("a.txt").await.expect("current reference");
                let mut view = DiffView::new(fixture.root.volume.clone(), base.generation.clone())
                    .expect("view");
                let original = view.clone();
                let publication = DiffPublication {
                    previous: base.generation.clone(),
                    generation: current.generation.clone(),
                    paths: vec![DiffPath {
                        path: "a.txt".into(),
                        base: Some(base),
                        current: Some(current),
                    }],
                };
                let encoded = serde_json::to_vec(&publication).expect("encode publication");
                prop_assert_eq!(
                    serde_json::from_slice::<DiffPublication>(&encoded)
                        .expect("decode publication"),
                    publication.clone()
                );
                match view
                    .apply(
                        &publication,
                        fixture.reader.as_ref(),
                        DiffLimits {
                            work,
                            render_bytes,
                            ..limits()
                        },
                    )
                    .await
                {
                    Err(_) => prop_assert_eq!(view, original),
                    Ok(_) => {
                        prop_assert_eq!(view.generation(), &publication.generation);
                        let expected = FileDiff::between(
                            Some(before.as_bytes()),
                            Some(after.as_bytes()),
                            render_bytes,
                            work,
                        )
                        .expect("same bounded computation");
                        prop_assert_eq!(view.files().get("a.txt"), expected.as_ref());
                        prop_assert_eq!(
                            view.line_counts(),
                            expected
                                .as_ref()
                                .map_or((0, 0), |diff| (diff.added, diff.removed))
                        );
                        let applied = view.clone();
                        let replay = view
                            .apply(
                                &publication,
                                fixture.reader.as_ref(),
                                DiffLimits {
                                    work,
                                    render_bytes,
                                    ..limits()
                                },
                            )
                            .await
                            .expect("exact replay");
                        prop_assert!(replay.is_empty());
                        prop_assert_eq!(view, applied);
                    }
                }
                Ok(())
            })
        })
        .expect("atomic publication property");
}
