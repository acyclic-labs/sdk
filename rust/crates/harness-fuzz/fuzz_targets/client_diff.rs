#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../harness/tests/support/file_diff_case.rs"]
mod diff_case;
#[path = "../../harness/tests/support/expected_outcome.rs"]
mod expected_outcome;
#[path = "../../harness/tests/e2e/local_case.rs"]
mod local_case;

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
    let Ok(Some(diff)) = FileDiff::between(before, after, render, work) else {
        return;
    };
    assert!(diff.patch.len() <= render);
    let encoded = serde_json::to_vec(&diff).expect("encode diff");
    assert_eq!(
        serde_json::from_slice::<FileDiff>(&encoded).expect("decode diff"),
        diff
    );
    if !diff.binary {
        let before = std::str::from_utf8(before.unwrap_or_default()).expect("text base");
        let after = std::str::from_utf8(after.unwrap_or_default()).expect("text endpoint");
        let patch = diffy::Patch::from_str(&diff.patch).expect("generated patch");
        assert_eq!(diffy::apply(before, &patch).expect("patch applies"), after);
    }
}

fuzz_target!(|input: &[u8]| {
    if let Ok(case) = serde_json::from_slice::<local_case::LocalCase>(input) {
        let encoded = serde_json::to_vec(&case).expect("encode local case");
        let decoded: local_case::LocalCase =
            serde_json::from_slice(&encoded).expect("decode local case");
        assert_eq!(case.validate(), decoded.validate());
        let references = case
            .seed
            .iter()
            .map(|(path, text)| (path.clone(), serde_json::json!({"path":path,"seed":text})))
            .collect();
        if let Ok(bound) = local_case::bind_seed_files(case.turns.clone(), &references) {
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
    if let Ok(case) = serde_json::from_slice::<diff_case::DiffCase>(input) {
        let encoded = serde_json::to_vec(&case).expect("encode diff case");
        assert_eq!(
            serde_json::from_slice::<diff_case::DiffCase>(&encoded).expect("decode diff case"),
            case
        );
        // Exercise valid scripts independently of their mutable fixture oracle.
        check(
            case.before.as_ref().map(diff_case::Input::bytes),
            case.after.as_ref().map(diff_case::Input::bytes),
            case.render.min(8192),
            case.work.min(65_536),
        );
    }
    if let Ok(diff) = serde_json::from_slice::<FileDiff>(input) {
        let encoded = serde_json::to_vec(&diff).expect("encode parsed diff");
        assert_eq!(
            serde_json::from_slice::<FileDiff>(&encoded).expect("decode parsed diff"),
            diff
        );
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
