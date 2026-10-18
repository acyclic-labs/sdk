#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

#[path = "../../harness/tests/support/file_diff_case.rs"]
mod diff_case;
#[path = "../../harness/tests/e2e/local_case.rs"]
mod local_case;
#[path = "../../harness/tests/e2e/local_tool_files_check.rs"]
mod local_tool_files_check;
#[path = "../../harness/tests/e2e/local_tool_files_tests.rs"]
mod local_tool_files_tests;

pub type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Compile the production pure module without pulling in unrelated providers.
#[path = "../../harness/src/client/diff/compute.rs"]
mod compute;
#[path = "../../harness/src/client/diff/file.rs"]
mod file;
use file::{DiffHunk, FileDiff, PatchText, render_bound};

#[derive(Debug)]
pub enum Error {
    Invalid(String),
}
pub type Result<T> = std::result::Result<T, Error>;

fn check(before: Option<&[u8]>, after: Option<&[u8]>, render: usize, work: usize) {
    let Ok(Some(diff)) = crate::file::FileDiff::between(before, after, render, work) else {
        return;
    };
    assert!(diff.patch.len() <= render);
    let encoded = serde_json::to_vec(&diff).expect("encode diff");
    let _ = fuzz_support::round_trip(&(diff), serde_json::from_slice::<FileDiff>(&encoded));
    if !diff.binary {
        let before = fuzz_support::must_decode(std::str::from_utf8(before.unwrap_or_default()));
        let after = fuzz_support::must_decode(std::str::from_utf8(after.unwrap_or_default()));
        let patch = fuzz_support::must_decode(diffy::Patch::from_str(&diff.patch));
        assert_eq!(diffy::apply(before, &patch).expect("patch applies"), after);
    }
}

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    let task = acyclic_harness::TaskId::from_bytes([0; 16]);
    let operation = acyclic_harness::OperationId::from_bytes([1; 16]);
    let (mut files, calls, results) =
        local_tool_files_tests::fixture(task, operation, "output", "error").unwrap();
    local_tool_files_check::check(&files, task, operation, &calls, &results).unwrap();
    if let Some(first) = input.first() {
        let key = files
            .keys()
            .nth(usize::from(*first) % files.len())
            .unwrap()
            .clone();
        let original = files[&key].segments[0].clone();
        let file = files.get_mut(&key).unwrap();
        file.length = input.len() as u64;
        file.segments = vec![input.to_vec().into()];
        // Arbitrary request/result/output bytes may be invalid, but must never
        // panic or manufacture a different expected identity or terminal value.
        if local_tool_files_check::check(&files, task, operation, &calls, &results).is_ok() {
            use acyclic_harness::task_volume::{FileKey, TaskFile, ToolFile};
            match key {
                FileKey::Task {
                    file:
                        TaskFile::Tool {
                            file: ToolFile::Request | ToolFile::Result,
                            ..
                        },
                    ..
                } => {
                    assert_eq!(
                        fuzz_support::must_decode(serde_json::from_slice::<serde_json::Value>(
                            input
                        )),
                        fuzz_support::must_decode(serde_json::from_slice::<serde_json::Value>(
                            &original
                        ))
                    );
                }
                _ => assert_eq!(input, original.as_ref()),
            }
        }
    }
    if let Some(case) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<local_case::LocalCase>)
    {
        let encoded = serde_json::to_vec(&case).expect("encode local case");
        let decoded = fuzz_support::round_trip_by(
            &serde_json::to_value(&case).unwrap(),
            serde_json::from_slice::<local_case::LocalCase>(&encoded),
            |value| serde_json::to_value(value),
        );
        assert_eq!(case.validate(), decoded.validate());
        let references = case
            .seed
            .iter()
            .map(|(path, text)| (path.clone(), serde_json::json!({"path":path,"seed":text})))
            .collect();
        if let Ok(bound) = crate::local_case::bind_seed_files(case.turns.clone(), &references) {
            let repeated = local_case::bind_seed_files(bound.clone(), &references).unwrap();
            assert_eq!(
                serde_json::to_value(bound).unwrap(),
                serde_json::to_value(repeated).unwrap()
            );
        }

        assert_eq!(
            serde_json::to_value(case).unwrap(),
            serde_json::to_value(decoded).unwrap()
        );
    }
    if let Some(case) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<diff_case::DiffCase>)
    {
        let encoded = serde_json::to_vec(&case).expect("encode diff case");
        let _ = fuzz_support::round_trip(
            &(case),
            serde_json::from_slice::<diff_case::DiffCase>(&encoded),
        );
        // Exercise valid scripts independently of their mutable fixture oracle.
        check(
            case.before.as_ref().map(diff_case::Input::bytes),
            case.after.as_ref().map(diff_case::Input::bytes),
            case.render.min(8192),
            case.work.min(65_536),
        );
    }
    if let Some(diff) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<FileDiff>)
    {
        let encoded = serde_json::to_vec(&diff).expect("encode parsed diff");
        let _ = fuzz_support::round_trip(&(diff), serde_json::from_slice::<FileDiff>(&encoded));
    }
    let Some((&flags, bytes)) = input.split_first() else {
        return;
    };
    let Some((split, bytes)) = bytes.split_first_chunk::<2>() else {
        return;
    };
    let split = usize::from(u16::from_le_bytes(*split)) % (bytes.len() + 1);
    let (before, after) = bytes.split_at(split);
    let before = (flags & 1 != 0).then_some(before);
    let after = (flags & 2 != 0).then_some(after);
    check(before, after, 8192, 65_536);
});
