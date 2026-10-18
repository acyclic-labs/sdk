use super::*;
use crate::task_volume::{ChildTurnResult, FileQuerySource, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;

async fn volume(prompt: usize, answer: usize) -> TaskVolume<MemoryStream> {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("listing-property").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::from_bytes([1; 16]);
    let reference = Component::new("worker").unwrap();
    let status = ChildStatus {
        task,
        state: ChildState::Running,
        inputs: 1,
        settled: true,
    };
    let result = ChildTurnResult {
        task,
        turn: 1,
        outcome: crate::Outcome::Failed {
            message: "a".repeat(answer),
        },
    };
    for (file, bytes) in [
        (ChildFile::Request, vec![b'p'; prompt]),
        (ChildFile::Status, serde_json::to_vec(&status).unwrap()),
        (ChildFile::Result(1), serde_json::to_vec(&result).unwrap()),
    ] {
        let file = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file,
            },
            bytes,
        );
        volume
            .append(volume.head().await.unwrap(), &file)
            .await
            .unwrap();
    }
    volume
}

fn unchanged_listing(result: Result<String>, expected: &str, check: bool) {
    if check {
        assert_eq!(
            result.unwrap(),
            expected,
            "file bodies changed a fitting listing"
        );
        assert!(expected.len() <= 4096);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn growing_bodies_cannot_break_a_fitting_listing(
        prompt in 1usize..8192, answer in 0usize..8192,
    ) {
        futures::executor::block_on(async {
            let small = volume(1, 0).await;
            let grown = volume(prompt, answer).await;
            for query in [Query::Children, Query::Child(Component::new("worker").unwrap())] {
                let expected = listing(&small, &query, 3).await.unwrap();
                unchanged_listing(listing(&grown, &query, 3).await, &expected, true);
                prop_assert!(grown.list_files(&query, 2).await.is_err(), "file count bound changed");
            }
            Ok(())
        })?;
    }
}

#[test]
fn body_bounded_listing_negative_control_requires_its_property_assertion() {
    futures::executor::block_on(async {
        let files = volume(3072, 3072).await;
        for query in [
            Query::Children,
            Query::Child(Component::new("worker").unwrap()),
        ] {
            let expected = listing(&files, &query, 3).await.unwrap();
            unchanged_listing(Ok(expected.clone()), &expected, true);
            // The old listing first projected every body through render_bytes.
            let old = || async {
                files
                    .observe_files(query.clone(), None, 3, 4096)
                    .await
                    .map(|observation| {
                        observation
                            .snapshot
                            .unwrap()
                            .files
                            .iter()
                            .map(|file| format!(".volumes/{}\n", file.key.path()))
                            .collect()
                    })
                    .map_err(storage)
            };
            let result = old().await;
            assert!(
                result.is_err(),
                "negative control no longer exceeds the body bound"
            );
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                unchanged_listing(result, &expected, true)
            }))
            .expect_err("body-dependent listing escaped the invariant");
            unchanged_listing(old().await, &expected, false);
        }
    });
}

// An inspection over its bounds is the model's to narrow: a file larger than
// the render bound and a listing with more entries than the file bound both
// answer `Rejected`, never an executor error, and fitting ones answer the
// text. Mapping a volume bound back to an executor error (`map_err(storage)?`)
// fails this property on every over-bound case.
async fn answered(
    command: &str,
    volume: &TaskVolume<MemoryStream>,
    files: usize,
    render: usize,
) -> Result<std::result::Result<String, Rejected>> {
    let inspection = crate::tools::agent_selection::selection(command).unwrap();
    let query = query(&inspection).ok().unwrap();
    answer(volume, volume, inspection, query, files, render).await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn missing_file_rejection_moves_the_original_operand(child in "[a-z]{1,24}") {
        futures::executor::block_on(async {
            let volume = volume(1, 0).await;
            let command = format!("cat .volumes/agents/missing-{child}/status.json");
            let inspection = crate::tools::agent_selection::selection(&command).unwrap();
            let pointer = inspection.path.as_ptr();
            let query = query(&inspection).ok().unwrap();
            let result = answer(&volume, &volume, inspection, query, 3, 8192).await.unwrap();
            let Err(Rejected(Error::NotFound(path))) = result else {
                return Err(TestCaseError::fail("missing file was not rejected"));
            };
            prop_assert_eq!(path.as_ptr(), pointer, "missing path was copied");
            // Reintroducing the old copy violates that same ownership assertion
            // and allocates exactly one path-sized buffer.
            let mut copied = None;
            let allocations = acyclic_test_support::measure(|| copied = Some(path.clone()));
            let copied = copied.unwrap();
            prop_assert_ne!(copied.as_ptr(), pointer);
            prop_assert_eq!((allocations.count_total, allocations.bytes_total), (1, path.len() as u64));
            Ok(())
        })?;
    }
}

#[test]
fn refusal_scope_negative_controls_detect_escaping_validation_and_bounds() {
    fn is_refusal(answer: &Result<std::result::Result<String, Rejected>>) -> bool {
        matches!(answer, Ok(Err(_)))
    }
    // These are the old `?` conversions on actual production validation errors.
    for command in [
        "ls .volumes/agents/",
        "cat .volumes/agents/worker/no-such-file",
    ] {
        let inspection = crate::tools::agent_selection::selection(command).unwrap();
        let rejected = query(&inspection).err().expect("validation must fail");
        assert!(is_refusal(&Ok(Err(rejected))));
        let rejected = query(&inspection).err().unwrap();
        assert!(
            !is_refusal(&Err(rejected.0)),
            "escaping validation passed the refusal invariant"
        );
    }
    let rejected = classify(crate::task_volume::Error::LimitExceeded).unwrap();
    assert!(is_refusal(&Ok(Err(rejected))));
    assert!(
        !is_refusal(&Err(storage(crate::task_volume::Error::LimitExceeded))),
        "escaping query bound passed the refusal invariant"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn over_bound_inspections_are_rejected_input_and_fitting_ones_answer(
        prompt in 1usize..8192, files in 1usize..6, render in 1usize..8192,
    ) {
        futures::executor::block_on(async {
            // Three files for one child: request, status and result.
            let volume = volume(prompt, 0).await;
            let cat = answered("cat .volumes/agents/worker/request.md", &volume, files, render).await;
            let cat = cat.map_err(|error| TestCaseError::fail(format!("cat escaped as executor error: {error}")))?;
            match cat {
                Ok(text) => prop_assert!(text.len() == prompt && prompt <= render),
                Err(Rejected(_)) => prop_assert!(prompt > render),
            }
            let ls = answered("ls .volumes/agents/worker", &volume, files, render).await;
            let ls = ls.map_err(|error| TestCaseError::fail(format!("ls escaped as executor error: {error}")))?;
            let fitting = listing(&volume, &Query::Child(Component::new("worker").unwrap()), 3)
                .await
                .unwrap();
            match ls {
                Ok(text) => prop_assert!(files >= 3 && text == fitting && text.len() <= render),
                Err(Rejected(_)) => prop_assert!(files < 3 || fitting.len() > render),
            }
            Ok(())
        })?;
    }
}
